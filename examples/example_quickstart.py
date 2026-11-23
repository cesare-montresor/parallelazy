"""Quickstart example: Telemetry agent lifecycle, work_dir, and custom log formats.

Run from repository root:
    python -m examples.example_quickstart
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from typing import Any, Dict

from parallelazy import Plugin, PluginManager


class TelemetryAgent(Plugin):
    """Emits periodic heartbeats to shared env and writes a summary on stop."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {
            "interval_s": 0.1,
            "summary_filename": "telemetry_summary.json",
        }

    def on_start(self) -> None:
        self.heartbeats = 0
        self.started_at = time.time()
        self.set_log_formats(
            console_format="%(asctime)s [%(plugin_id)s] %(levelname)s %(message)s"
        )
        self.logger.info("TelemetryAgent starting in work_dir=%s", self.work_dir)
        self.env_set("telemetry.status", "running")
        self.env_set("telemetry.heartbeats", 0)

    def main(self) -> None:
        interval = float(self.options["interval_s"])
        while not self.wait_for_stop(timeout=interval):
            self.heartbeats += 1
            self.env_set("telemetry.heartbeats", self.heartbeats)
            self.logger.debug("Heartbeat #%d", self.heartbeats)

    def on_stop(self) -> None:
        elapsed = round(time.time() - self.started_at, 3)
        summary = {
            "plugin_id": self.plugin_id,
            "heartbeats": self.heartbeats,
            "elapsed_s": elapsed,
        }
        summary_path = os.path.join(self.work_dir, self.options["summary_filename"])
        with open(summary_path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)
        self.env_set("telemetry.summary_path", summary_path)
        self.env_set("telemetry.status", "stopped")
        self.logger.info("TelemetryAgent wrote summary to %s", summary_path)


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.05) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("Timed out waiting for condition")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="parallelazy-quickstart-") as tmp_dir:
        descriptor = TelemetryAgent.Descriptor(
            version="1.0.0",
            author="parallelazy",
            name="telemetry-agent",
            options={"interval_s": 0.05},
            log_level=logging.INFO,
        )

        with PluginManager(
            work_dir=tmp_dir,
            log_dir="logs",
            log_level=logging.INFO,
            display_logs=False,
            write_logs=True,
        ) as manager:
            assert manager.load_plugin(descriptor), "Failed to load TelemetryAgent"
            assert manager.start_plugin(descriptor.name), "Failed to start TelemetryAgent"
            assert descriptor.name in manager.plugin_list()

            _wait_until(lambda: (manager.env_get("telemetry.heartbeats", 0) or 0) >= 3)

            status = manager.plugin_status(descriptor.name)
            assert status is not None and status.worker_status is not None
            assert status.worker_status.state == "running", f"Unexpected state: {status.worker_status.state}"

            heartbeats = manager.env_get("telemetry.heartbeats")
            assert heartbeats >= 3, f"Expected >= 3 heartbeats, got {heartbeats}"

            assert manager.stop_plugin(descriptor.name)
            assert manager.unload_plugin(descriptor.name)
            assert manager.plugin_list() == []

            summary_path = manager.env_get("telemetry.summary_path")
            assert summary_path and os.path.isfile(summary_path), "Summary artifact missing"
            with open(summary_path, encoding="utf-8") as fh:
                summary = json.load(fh)
            assert summary["heartbeats"] >= 3

        print(
            f"[OK] example_quickstart: heartbeats={summary['heartbeats']}, "
            f"summary={os.path.basename(summary_path)}"
        )


if __name__ == "__main__":
    main()
