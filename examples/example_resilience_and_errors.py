"""Fault tolerance: on_error, PluginState.ERROR, retries, circuit breaker, and recovery.

Run from repository root:
    python -m examples.example_resilience_and_errors
"""

from __future__ import annotations

import logging
import tempfile
import threading
import time
from typing import Any, Dict, List

from parallelazy import Plugin, PluginManager, PluginState


class InferenceService(Plugin):
    """Fails on specific request IDs to simulate transient errors, and records on_error()."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"fail_on_calls": [1, 2], "fail_on_start": False}

    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.call_count = 0
        self.error_sources: List[str] = []
        if self.options.get("fail_on_start"):
            raise RuntimeError("Simulated fatal startup failure")

    def on_error(self, error: Exception, source: str = None) -> None:
        with self._lock:
            self.error_sources.append(source or "unknown")
        self.logger.warning("Captured error from %s: %s", source, error)

    def infer(self, x: float) -> float:
        with self._lock:
            self.call_count += 1
            n = self.call_count
        if n in self.options["fail_on_calls"]:
            raise RuntimeError(f"Transient backend error on call #{n}")
        return round(x * 2.5, 4)

    def get_error_sources(self) -> List[str]:
        with self._lock:
            return list(self.error_sources)

    def main(self) -> None:
        self.wait_for_stop()


class GatewayClient(Plugin):
    """Calls InferenceService with retry + circuit breaker logic."""

    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {"target": "inference-service", "max_retries": 3, "breaker_threshold": 2}

    def call_with_retry(self, x: float) -> Dict[str, Any]:
        target = self.options["target"]
        attempts = 0
        for attempt in range(1, int(self.options["max_retries"]) + 1):
            attempts = attempt
            result = self.plugin_call(target, "infer", x)
            if result is not None:
                return {"ok": True, "value": result, "attempts": attempts}
            time.sleep(0.02 * attempt)
        return {"ok": False, "value": None, "attempts": attempts}

    def probe_breaker(self, target: str) -> Dict[str, Any]:
        failures = 0
        threshold = int(self.options["breaker_threshold"])
        for _ in range(threshold + 1):
            if failures >= threshold:
                return {"breaker_open": True, "failures": failures}
            res = self.plugin_call(target, "infer", 1.0)
            if res is None:
                failures += 1
        return {"breaker_open": False, "failures": failures}

    def main(self) -> None:
        self.wait_for_stop()


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="parallelazy-resilience-") as tmp_dir:
        svc_desc = InferenceService.Descriptor(
            version="1.0.0",
            author="parallelazy",
            name="inference-service",
            options={"fail_on_calls": [1, 2]},
        )
        gw_desc = GatewayClient.Descriptor(
            version="1.0.0",
            author="parallelazy",
            name="gateway-client",
        )

        with PluginManager(
            work_dir=tmp_dir,
            log_level=logging.INFO,
            display_logs=False,
            write_logs=False,
        ) as manager:
            assert manager.load_plugin(svc_desc)
            assert manager.load_plugin(gw_desc)
            assert manager.start_plugin(svc_desc.name)
            assert manager.start_plugin(gw_desc.name)

            # 1. Transient RPC failures recovered via retries
            outcome = manager.plugin_call(gw_desc.name, "call_with_retry", 4.0)
            assert outcome["ok"] and outcome["value"] == 10.0
            assert outcome["attempts"] == 3

            sources = manager.plugin_call(svc_desc.name, "get_error_sources")
            assert sources == ["rpc:infer", "rpc:infer"], f"Unexpected sources: {sources}"
            svc_errors = manager.plugin_errors(svc_desc.name)
            assert len(svc_errors) == 2

            # 2. Induced fatal startup failure -> PluginState.ERROR -> reload recovery
            broken_desc = InferenceService.Descriptor(
                version="1.0.0",
                author="parallelazy",
                name="broken-service",
                options={"fail_on_start": True},
            )
            assert manager.load_plugin(broken_desc)
            assert not manager.start_plugin(broken_desc.name)
            broken_status = manager.plugin_status(broken_desc.name)
            assert broken_status.worker_status.state == str(PluginState.ERROR)
            assert manager.plugin_errors(broken_desc.name)

            # Circuit breaker trips on persistent failure, then manager recovers by reloading
            breaker = manager.plugin_call(gw_desc.name, "probe_breaker", "missing-service")
            assert breaker["breaker_open"]

            assert manager.unload_plugin(broken_desc.name)
            recovered_desc = InferenceService.Descriptor(
                version="1.0.0",
                author="parallelazy",
                name="broken-service",
                options={"fail_on_calls": []},
            )
            assert manager.load_plugin(recovered_desc)
            assert manager.start_plugin(recovered_desc.name)
            assert manager.plugin_call(recovered_desc.name, "infer", 2.0) == 5.0

        print(
            f"[OK] example_resilience_and_errors: retry_attempts={outcome['attempts']}, "
            f"captured_sources={sources}, recovered=True"
        )


if __name__ == "__main__":
    main()
