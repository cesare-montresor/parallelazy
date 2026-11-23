"""Dynamic orchestrator: supervisor plugin spawning workers with async RPC sharding.

Run from repository root:
    python -m examples.example_dynamic_orchestrator
"""

from __future__ import annotations

import logging
import tempfile
from typing import Any, Dict, List

from parallelazy import Plugin, PluginManager


class ComputeWorker(Plugin):
    """Processes assigned shards via RPC."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"scale": 2}

    def compute_shard(self, shard: List[int]) -> Dict[str, Any]:
        scale = int(self.options["scale"])
        results = [x * x * scale for x in shard]
        return {"worker": self.plugin_id, "results": results}

    def main(self) -> None:
        self.wait_for_stop()


class Orchestrator(Plugin):
    """Dynamically loads/starts ComputeWorker instances, shards work via async RPC, and cleans up."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"num_workers": 2, "scale": 3}

    def run_batch(self, items: List[int]) -> Dict[str, Any]:
        num_workers = int(self.options["num_workers"])
        scale = int(self.options["scale"])
        worker_ids = [f"worker-{i}" for i in range(num_workers)]

        loaded_ids: List[str] = []
        try:
            for wid in worker_ids:
                desc = ComputeWorker.Descriptor(
                    version="1.0.0",
                    author="parallelazy",
                    name=wid,
                    options={"scale": scale},
                )
                assert self.plugin_load(desc), f"Failed to load {wid}"
                loaded_ids.append(wid)
                assert self.plugin_start(wid), f"Failed to start {wid}"

            # Inspect cluster status from inside the supervisor plugin
            statuses = self.plugin_status_all()
            for wid in worker_ids:
                assert wid in statuses and statuses[wid].process_alive

            # Shard items round-robin across workers and fan out with async RPC (wait=False)
            shards = [items[i::num_workers] for i in range(num_workers)]
            pending_calls = [
                (wid, self.plugin_call(wid, "compute_shard", shard, wait=False))
                for wid, shard in zip(worker_ids, shards)
            ]

            per_worker: Dict[str, List[int]] = {}
            total_sum = 0
            for wid, pending in pending_calls:
                resp = pending.wait(timeout=5.0)
                assert resp is not None and not resp.error, f"Worker {wid} failed: {resp}"
                per_worker[wid] = resp.result["results"]
                total_sum += sum(resp.result["results"])

            return {
                "per_worker": per_worker,
                "total_sum": total_sum,
                "workers_used": list(per_worker.keys()),
            }
        finally:
            for wid in loaded_ids:
                self.plugin_stop(wid)
                self.plugin_unload(wid)

    def main(self) -> None:
        self.wait_for_stop()


def main() -> None:
    items = list(range(1, 21))
    scale = 3
    expected_sum = sum(x * x * scale for x in items)

    with tempfile.TemporaryDirectory(prefix="parallelazy-orch-") as tmp_dir:
        orch_desc = Orchestrator.Descriptor(
            version="1.0.0",
            author="parallelazy",
            name="orchestrator",
            options={"num_workers": 2, "scale": scale},
        )

        with PluginManager(
            work_dir=tmp_dir,
            log_level=logging.INFO,
            display_logs=False,
            write_logs=False,
        ) as manager:
            assert manager.load_plugin(orch_desc)
            assert manager.start_plugin(orch_desc.name)

            report = manager.plugin_call(orch_desc.name, "run_batch", items, timeout=15.0)
            assert report["total_sum"] == expected_sum, (
                f"Checksum mismatch: {report['total_sum']} != {expected_sum}"
            )
            assert sum(len(v) for v in report["per_worker"].values()) == len(items)

            # Temporary workers must be unloaded after run_batch completes
            remaining = manager.plugin_list()
            assert remaining == [orch_desc.name], f"Unexpected leftover plugins: {remaining}"

        print(
            f"[OK] example_dynamic_orchestrator: total_sum={report['total_sum']}, "
            f"workers={report['workers_used']}"
        )


if __name__ == "__main__":
    main()
