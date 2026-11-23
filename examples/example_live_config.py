"""Live configuration control plane: feature flags, stored None, and env_unsubscribe.

Run from repository root:
    python -m examples.example_live_config
"""

from __future__ import annotations

import logging
import tempfile
import threading
import time
from typing import Any, Dict, List

from parallelazy import Plugin, PluginManager


class ScoringWorker(Plugin):
    """Applies live threshold and optional override updates without restarting."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"default_threshold": 0.5, "default_batch_size": 32}

    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.threshold_history: List[Any] = []
        self.batch_size = int(self.options["default_batch_size"])
        assert self.env_subscribe("scoring.threshold", self._on_threshold)
        assert self.env_subscribe("scoring.batch_size", self._on_batch_size)

    def _on_threshold(self, value: Any) -> None:
        with self._lock:
            if value is not None:
                self.threshold_history.append(value)

    def _on_batch_size(self, value: Any) -> None:
        if value is not None:
            with self._lock:
                self.batch_size = int(value)

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "threshold_history": list(self.threshold_history),
                "batch_size": self.batch_size,
                "override": self.env_get("scoring.override", default="unset"),
                "missing": self.env_get("scoring.never_set", default="unset"),
                "all_keys": sorted(self.env_get().keys()),
            }

    def main(self) -> None:
        self.wait_for_stop()


class ConfigAuditor(Plugin):
    """Records threshold updates until detached via env_unsubscribe()."""

    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.events: List[Any] = []
        assert self.env_subscribe("scoring.threshold", self._on_threshold)

    def _on_threshold(self, value: Any) -> None:
        with self._lock:
            self.events.append(value)

    def detach(self) -> bool:
        return self.env_unsubscribe("scoring.threshold")

    def get_events(self) -> List[Any]:
        with self._lock:
            return list(self.events)

    def main(self) -> None:
        self.wait_for_stop()


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("Timed out waiting for config condition")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="parallelazy-config-") as tmp_dir:
        worker_desc = ScoringWorker.Descriptor(
            version="1.0.0",
            author="parallelazy",
            name="scoring-worker",
            options={"default_batch_size": 64},
        )
        auditor_desc = ConfigAuditor.Descriptor(
            version="1.0.0",
            author="parallelazy",
            name="config-auditor",
        )

        with PluginManager(
            work_dir=tmp_dir,
            log_level=logging.INFO,
            display_logs=False,
            write_logs=False,
        ) as manager:
            manager.env_set("scoring.threshold", 0.5)
            manager.env_set("scoring.override", None)  # Explicitly stored None

            assert manager.load_plugin(worker_desc)
            assert manager.load_plugin(auditor_desc)
            assert manager.start_plugin(worker_desc.name)
            assert manager.start_plugin(auditor_desc.name)

            # Live updates before auditor detaches
            manager.env_set("scoring.threshold", 0.65)
            manager.env_set("scoring.batch_size", 128)
            _wait_until(
                lambda: manager.plugin_call(worker_desc.name, "get_state")["threshold_history"]
                == [0.5, 0.65]
            )
            _wait_until(
                lambda: manager.plugin_call(auditor_desc.name, "get_events") == [0.5, 0.65]
            )

            # Detach auditor and apply further updates
            assert manager.plugin_call(auditor_desc.name, "detach")
            manager.env_set("scoring.threshold", 0.8)
            manager.env_set("scoring.threshold", 0.9)

            _wait_until(
                lambda: manager.plugin_call(worker_desc.name, "get_state")["threshold_history"]
                == [0.5, 0.65, 0.8, 0.9]
            )

            state = manager.plugin_call(worker_desc.name, "get_state")
            auditor_events = manager.plugin_call(auditor_desc.name, "get_events")

            assert state["threshold_history"] == [0.5, 0.65, 0.8, 0.9]
            assert state["batch_size"] == 128
            assert state["override"] is None, f"Expected stored None, got {state['override']!r}"
            assert state["missing"] == "unset"
            assert auditor_events == [0.5, 0.65], (
                f"Auditor should not receive updates after unsubscribe: {auditor_events}"
            )

        print(
            f"[OK] example_live_config: thresholds={state['threshold_history']}, "
            f"auditor_events={auditor_events}, override={state['override']!r}"
        )


if __name__ == "__main__":
    main()
