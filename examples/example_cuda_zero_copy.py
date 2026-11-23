"""Zero-copy CUDA tensor streaming with IpcEventPoolManaged and mark_graph_output.

Run from repository root:
    python -m examples.example_cuda_zero_copy
"""

from __future__ import annotations

import logging
import tempfile
import threading
from typing import Any, Dict, List, Tuple

import torch

from parallelazy import IpcEventCuda, IpcEventPoolManaged, Plugin, PluginManager, mark_graph_output


class GpuConsumer(Plugin):
    """Receives CUDA tensors + IPC events, synchronizes, reduces, and calls event.clear()."""

    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.sums: List[float] = []
        assert self.topic_subscribe("gpu.frames", self._on_frame)

    def _on_frame(self, payload: Tuple[torch.Tensor, IpcEventCuda]) -> None:
        tensor, event = payload
        try:
            event.wait()
            total = float(tensor.sum().item())
            with self._lock:
                self.sums.append(total)
        finally:
            event.clear()

    def get_sums(self) -> List[float]:
        with self._lock:
            return list(self.sums)

    def main(self) -> None:
        self.wait_for_stop()


class GpuProducer(Plugin):
    """Streams CUDA tensors using IpcEventPoolManaged and prunes in-flight tensors via is_cleared()."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"pool_size": 3, "num_steps": 8, "tensor_len": 16}

    def on_start(self) -> None:
        self.pool = IpcEventPoolManaged(size=int(self.options["pool_size"]), stale_after_s=10.0)
        self._inflight: List[Tuple[torch.Tensor, IpcEventCuda]] = []

    def stream_frames(self) -> Dict[str, Any]:
        steps = int(self.options["num_steps"])
        length = int(self.options["tensor_len"])
        pruned_count = 0

        for step in range(1, steps + 1):
            # Prune tensors whose consumer has already acknowledged completion
            before = len(self._inflight)
            self._inflight = [
                (t, ev) for t, ev in self._inflight if not ev.is_cleared()
            ]
            pruned_count += before - len(self._inflight)

            event = self.pool.acquire("gpu.frames")
            tensor = torch.full((length,), float(step), device="cuda")
            if step == steps:
                # Demonstrate mark_graph_output for compiled-graph pooled tensors
                tensor = mark_graph_output(tensor)
            event.record()
            self._inflight.append((tensor, event))

            delivered = self.topic_publish("gpu.frames", (tensor, event))
            assert delivered == 1, f"Expected 1 GPU subscriber, got {delivered}"

        drained = self.pool.wait_all_free(timeout=10.0)
        self._inflight = [(t, ev) for t, ev in self._inflight if not ev.is_cleared()]
        return {
            "drained": drained,
            "remaining_inflight": len(self._inflight),
            "pruned_during_stream": pruned_count,
        }

    def main(self) -> None:
        self.wait_for_stop()


def main() -> None:
    if not torch.cuda.is_available():
        print("[SKIP] example_cuda_zero_copy: CUDA is not available on this host.")
        return

    num_steps = 8
    tensor_len = 16
    expected_sums = [float(step * tensor_len) for step in range(1, num_steps + 1)]

    with tempfile.TemporaryDirectory(prefix="parallelazy-cuda-") as tmp_dir:
        consumer_desc = GpuConsumer.Descriptor(
            version="1.0.0", author="parallelazy", name="gpu-consumer"
        )
        producer_desc = GpuProducer.Descriptor(
            version="1.0.0",
            author="parallelazy",
            name="gpu-producer",
            options={"pool_size": 3, "num_steps": num_steps, "tensor_len": tensor_len},
        )

        with PluginManager(
            work_dir=tmp_dir,
            log_level=logging.INFO,
            display_logs=False,
            write_logs=False,
        ) as manager:
            assert manager.load_plugin(consumer_desc)
            assert manager.load_plugin(producer_desc)
            assert manager.start_plugin(consumer_desc.name)
            assert manager.start_plugin(producer_desc.name)

            report = manager.plugin_call(producer_desc.name, "stream_frames", timeout=15.0)
            assert report["drained"], f"Pool did not drain cleanly: {report}"
            assert report["remaining_inflight"] == 0

            sums = manager.plugin_call(consumer_desc.name, "get_sums", timeout=5.0)
            assert sums == expected_sums, f"Expected {expected_sums}, got {sums}"

        print(
            f"[OK] example_cuda_zero_copy: steps={num_steps}, drained={report['drained']}, "
            f"sums={sums}"
        )


if __name__ == "__main__":
    main()
