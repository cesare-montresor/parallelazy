import tempfile
import threading
import time
import unittest
from typing import Any, Dict, List

import numpy as np
import torch

from parallelazy import Plugin, PluginState
from parallelazy.plugin_data import Channel, Response
from parallelazy.plugin_rpc import PluginRPC
from tests.helpers import make_manager, wait_until


class RpcServicePlugin(Plugin):
    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.error_sources: List[str] = []
        if self.options.get("fail_start"):
            raise RuntimeError("on_start failure")

    def main(self) -> None:
        if self.options.get("fail_main"):
            raise RuntimeError("main failure")
        self.wait_for_stop()

    def on_error(self, error: Exception, source: str = None) -> None:
        if self.options.get("crash_on_error"):
            raise RuntimeError("on_error itself failed")
        with self._lock:
            self.error_sources.append(source or "")

    def compute(self, a: int, b: int, *, scale: float = 1.0) -> Dict[str, Any]:
        return {
            "sum": (a + b) * scale,
            "arr": np.array([a, b], dtype=np.int32),
            "tensor": torch.tensor([float(a), float(b)]),
        }

    def fail_method(self) -> None:
        raise ValueError("method boom")

    def slow_method(self, delay: float = 0.4) -> str:
        time.sleep(delay)
        return "slow-done"

    def call_peer_async(self, target: str, a: int, b: int) -> Any:
        done_event = threading.Event()
        callback_seen = []
        pending = self.plugin_call(target, "compute", a, b, scale=2.0, wait=False)
        pending.on_ready(lambda fut: (callback_seen.append(True), done_event.set()))
        resp = pending.wait(timeout=3.0)
        done_event.wait(timeout=2.0)
        return {
            "sum": resp.result["sum"],
            "callback_fired": bool(callback_seen),
        }

    def get_error_sources(self) -> List[str]:
        with self._lock:
            return list(self.error_sources)

    def get_peer_errors(self, target: str) -> List[str]:
        return self.plugin_errors(target)


class ApiRpcAndErrorsTests(unittest.TestCase):
    def test_sync_async_rpc_complex_payloads_and_error_isolation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                s1 = RpcServicePlugin.Descriptor("1.0.0", "tests", name="svc")
                s2 = RpcServicePlugin.Descriptor("1.0.0", "tests", name="client")
                self.assertEqual(manager.load_plugins([s1, s2]), {"svc": True, "client": True})
                self.assertEqual(manager.start_all_plugins(), {"svc": True, "client": True})

                # Host-to-plugin RPC with complex payloads
                out = manager.plugin_call("svc", "compute", 3, 4, scale=2.0)
                self.assertEqual(out["sum"], 14.0)
                self.assertTrue(np.array_equal(out["arr"], np.array([3, 4], dtype=np.int32)))
                self.assertTrue(torch.equal(out["tensor"], torch.tensor([3.0, 4.0])))

                # Plugin-to-plugin async RPC with on_ready callback
                async_out = manager.plugin_call("client", "call_peer_async", "svc", 5, 6)
                self.assertEqual(async_out, {"sum": 22.0, "callback_fired": True})

                # Remote exception surfaces cleanly, triggers on_error, and keeps target alive
                with self.assertRaises(RuntimeError):
                    manager.plugin_call("svc", "fail_method")
                self.assertEqual(manager.plugin_call("svc", "compute", 1, 2)["sum"], 3.0)
                self.assertIn("rpc:fail_method", manager.plugin_call("svc", "get_error_sources"))
                self.assertIn("method boom", manager.plugin_errors("svc"))
                self.assertIn("method boom", manager.plugin_call("client", "get_peer_errors", "svc"))

                # Unknown plugin / unknown method fail fast
                with self.assertRaises(ValueError):
                    manager.plugin_call("no-such-plugin", "compute", 1, 2)
                with self.assertRaises(RuntimeError):
                    manager.plugin_call("svc", "no_such_method")

    def test_timeout_eviction_and_late_response_warning(self):
        channel = Channel()
        rpc = PluginRPC("timeout-evict", channel, timeout=0.05)
        try:
            pending = rpc.plugin_status()
            self.assertTrue(rpc.has_pending_requests())
            self.assertIsNone(pending.wait(timeout=0.03))
            self.assertFalse(rpc.has_pending_requests())

            late = Response(sender="peer", message_id=pending.message_id, result="late")
            with self.assertLogs("parallelazy.plugin_rpc", level="WARNING") as logs:
                self.assertFalse(rpc._handle_response(late))
            self.assertIn(pending.message_id, logs.output[0])
        finally:
            rpc.shutdown()

    def test_on_start_main_and_crashing_on_error_states(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                d_start = RpcServicePlugin.Descriptor(
                    "1.0.0", "tests", name="bad-start", options={"fail_start": True}
                )
                d_main = RpcServicePlugin.Descriptor(
                    "1.0.0", "tests", name="bad-main", options={"fail_main": True}
                )
                d_onerr = RpcServicePlugin.Descriptor(
                    "1.0.0", "tests", name="bad-onerr", options={"crash_on_error": True}
                )
                self.assertEqual(
                    manager.load_plugins([d_start, d_main, d_onerr]),
                    {"bad-start": True, "bad-main": True, "bad-onerr": True},
                )

                self.assertFalse(manager.start_plugin("bad-start"))
                st = manager.plugin_status("bad-start")
                self.assertEqual(st.worker_status.state, str(PluginState.ERROR))
                self.assertIn("on_start failure", manager.plugin_errors("bad-start"))

                self.assertTrue(manager.start_plugin("bad-main"))
                wait_until(
                    lambda: manager.plugin_status("bad-main").worker_status.state
                    == str(PluginState.ERROR)
                )
                self.assertIn("main failure", manager.plugin_errors("bad-main"))

                # Exception inside on_error() itself must not crash the worker
                self.assertTrue(manager.start_plugin("bad-onerr"))
                with self.assertRaises(RuntimeError):
                    manager.plugin_call("bad-onerr", "fail_method")
                self.assertEqual(manager.plugin_call("bad-onerr", "compute", 2, 3)["sum"], 5.0)

    def test_stopping_endpoint_rejects_without_error_log(self):
        channel = Channel()
        rpc = PluginRPC("stop-quiet", channel, timeout=1.0, start_listener=False)
        try:
            rpc.on_request("plugin_status", lambda _r, _q: "ok")
            rpc.stop_accepting_commands()
            req = rpc.plugin_status()
            from parallelazy.plugin_data import RequestPluginStatus
            with self.assertNoLogs("parallelazy.plugin_rpc", level="ERROR"):
                rpc._handle_request(RequestPluginStatus(sender="mgr", message_id="m1"))
            del req
        finally:
            rpc.shutdown()


if __name__ == "__main__":
    unittest.main()
