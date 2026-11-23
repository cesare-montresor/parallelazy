"""Multi-stage PyTorch tensor pipeline with pub/sub and live monitoring.

Run from repository root:
    python -m examples.example_tensor_pipeline
"""

from __future__ import annotations

import logging
import tempfile
import threading
import time
from typing import Any, Dict

import torch
import torch.nn as nn

from parallelazy import Plugin, PluginManager


class FrameSource(Plugin):
    """Generates synthetic float32 tensor batches and publishes them on 'frames.raw'."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"num_batches": 20, "batch_size": 8, "dim": 16, "interval_s": 0.02}

    def on_start(self) -> None:
        self.published = 0
        self.env_set("pipeline.stage1.published", 0)

    def main(self) -> None:
        torch.manual_seed(0)
        num_batches = int(self.options["num_batches"])
        batch_size = int(self.options["batch_size"])
        dim = int(self.options["dim"])
        interval = float(self.options["interval_s"])

        for step in range(num_batches):
            if self.wait_for_stop(timeout=interval):
                break
            frame = torch.randn(batch_size, dim, dtype=torch.float32)
            if step % 5 == 0:
                frame = frame * 6.0  # Inject synthetic anomaly spike
            delivered = self.topic_publish(
                "frames.raw",
                {"step": step, "tensor": frame},
            )
            if delivered > 0:
                self.published += 1
                self.env_set("pipeline.stage1.published", self.published, wait=False)

        self.env_set("pipeline.stage1.done", True)
        self.wait_for_stop()


class FeatureExtractor(Plugin):
    """Projects raw frames through a small nn.Linear layer onto 'frames.features'."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"in_dim": 16, "out_dim": 8}

    def on_start(self) -> None:
        torch.manual_seed(42)
        self._lock = threading.Lock()
        self.processed = 0
        self.proj = nn.Linear(int(self.options["in_dim"]), int(self.options["out_dim"]))
        self.proj.eval()
        self.env_set("pipeline.stage2.processed", 0)
        assert self.topic_subscribe("frames.raw", self._on_raw_frame)

    def _on_raw_frame(self, payload: Dict[str, Any]) -> None:
        with torch.no_grad():
            features = self.proj(payload["tensor"])
        self.topic_publish(
            "frames.features",
            {"step": payload["step"], "features": features},
        )
        with self._lock:
            self.processed += 1
            count = self.processed
        self.env_set("pipeline.stage2.processed", count, wait=False)

    def main(self) -> None:
        self.wait_for_stop()


class AnomalyDetector(Plugin):
    """Detects high-norm feature embeddings and unsubscribes once its quota is met."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"norm_threshold": 5.0, "target_quota": 12}

    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.processed = 0
        self.anomalies = 0
        self.unsubscribed = False
        self.env_set("pipeline.stage3.processed", 0)
        self.env_set("pipeline.stage3.anomalies", 0)
        assert self.topic_subscribe("frames.features", self._on_features)

    def _on_features(self, payload: Dict[str, Any]) -> None:
        norms = torch.linalg.vector_norm(payload["features"], dim=1)
        is_anomaly = bool((norms > float(self.options["norm_threshold"])).any().item())
        should_unsubscribe = False
        with self._lock:
            self.processed += 1
            if is_anomaly:
                self.anomalies += 1
            count = self.processed
            anomalies = self.anomalies
            if count >= int(self.options["target_quota"]) and not self.unsubscribed:
                self.unsubscribed = True
                should_unsubscribe = True

        if is_anomaly:
            self.topic_publish(
                "alerts.anomaly",
                {"step": payload["step"], "max_norm": float(norms.max().item())},
            )
        self.env_set("pipeline.stage3.processed", count, wait=False)
        self.env_set("pipeline.stage3.anomalies", anomalies, wait=False)

        if should_unsubscribe:
            # Unsubscribe from a helper thread so we do not join our own listener thread
            threading.Thread(
                target=self._detach,
                daemon=True,
            ).start()

    def _detach(self) -> None:
        ok = self.topic_unsubscribe("frames.features")
        self.env_set("pipeline.stage3.unsubscribed", ok)

    def get_topics(self) -> list[str]:
        return self.topic_list()

    def main(self) -> None:
        self.wait_for_stop()


def _wait_until(predicate, timeout: float = 10.0, interval: float = 0.05) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("Timed out waiting for pipeline condition")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="parallelazy-pipeline-") as tmp_dir:
        source_desc = FrameSource.Descriptor(
            version="1.0.0", author="parallelazy", name="frame-source"
        )
        extractor_desc = FeatureExtractor.Descriptor(
            version="1.0.0", author="parallelazy", name="feature-extractor"
        )
        detector_desc = AnomalyDetector.Descriptor(
            version="1.0.0", author="parallelazy", name="anomaly-detector"
        )

        with PluginManager(
            work_dir=tmp_dir,
            log_level=logging.INFO,
            display_logs=False,
            write_logs=False,
        ) as manager:
            load_results = manager.load_plugins(
                [detector_desc, extractor_desc, source_desc],
                parallel=True,
            )
            assert all(load_results.values()), f"Load failures: {load_results}"

            manager.enable_monitoring(sample_interval=0.2)

            # Start downstream subscribers before the source so no early frames are missed
            assert manager.start_plugin(detector_desc.name)
            assert manager.start_plugin(extractor_desc.name)
            assert manager.start_plugin(source_desc.name)

            _wait_until(lambda: bool(manager.env_get("pipeline.stage3.unsubscribed")))
            _wait_until(lambda: bool(manager.env_get("pipeline.stage1.done")))

            topics_manager = manager.topic_list()
            topics_plugin = manager.plugin_call(detector_desc.name, "get_topics")
            assert topics_manager == topics_plugin
            assert "frames.raw" in topics_manager
            assert "frames.features" in topics_manager

            snapshot = manager.get_monitoring_snapshot()
            assert len(snapshot.processes) == 3
            assert len(snapshot.environment) > 0
            manager.print_monitoring_report()
            manager.disable_monitoring()

            stage1 = manager.env_get("pipeline.stage1.published", 0)
            stage2 = manager.env_get("pipeline.stage2.processed", 0)
            stage3 = manager.env_get("pipeline.stage3.processed", 0)
            anomalies = manager.env_get("pipeline.stage3.anomalies", 0)

            assert stage1 >= stage2 >= stage3 >= 12, (
                f"Unexpected stage counts: s1={stage1}, s2={stage2}, s3={stage3}"
            )
            assert anomalies > 0, "Expected at least one anomaly alert"

        print(
            f"[OK] example_tensor_pipeline: stage1={stage1}, stage2={stage2}, "
            f"stage3={stage3}, anomalies={anomalies}"
        )


if __name__ == "__main__":
    main()
