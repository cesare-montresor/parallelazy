import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock

from parallelazy import Plugin, PluginManager
from parallelazy.plugin_data import Channel, CommandType, RequestPluginStatus, Response
from parallelazy.plugin_rpc import PluginRPC
from parallelazy.plugin_worker import PluginWorker


class _SlowCalleePlugin(Plugin):
    def on_start(self):
        pass

    def main(self):
        self.wait_for_stop()

    def block_forever(self):
        self.wait_for_stop()
        return "done"


class _CallerPlugin(Plugin):
    def on_start(self):
        pass

    def main(self):
        self.wait_for_stop()

    def call_callee(self, target_id, timeout=5.0):
        return self._worker.plugin_call(target_id, "block_forever", timeout=timeout)


class _OrphanTestPlugin(Plugin):
    def on_start(self):
        pid_file = self.options.get("pid_file")
        if pid_file:
            with open(pid_file, "w", encoding="utf-8") as fh:
                fh.write(str(os.getpid()))

    def main(self):
        self.wait_for_stop()


class RpcReliabilityTests(unittest.TestCase):
    def test_timeout_evicts_pending_and_late_response_logs_warning(self):
        channel = Channel()
        rpc = PluginRPC("timeout-test", channel, timeout=0.1)
        try:
            pending = rpc.plugin_status()
            self.assertTrue(rpc.has_pending_requests())
            res = pending.wait(timeout=0.05)
            self.assertIsNone(res)
            self.assertFalse(rpc.has_pending_requests())

            late = Response(sender="peer", message_id=pending.message_id, result="ok")
            with self.assertLogs("parallelazy.plugin_rpc", level="WARNING") as logs:
                handled = rpc._handle_response(late)
            self.assertFalse(handled)
            self.assertIn(pending.message_id, logs.output[0])
        finally:
            rpc.shutdown()

    def test_disconnected_rpc_returns_failed_pending_request(self):
        channel = Channel()
        rpc = PluginRPC("disconnect-test", channel, timeout=0.5)
        rpc.shutdown()

        pending = rpc.plugin_status()
        res = pending.wait(timeout=0.5)
        self.assertIsNotNone(res)
        self.assertIsNotNone(res.error)

        # Wrapper exception before send also returns a failed PendingRequest
        with unittest.mock.patch.object(rpc, "request", side_effect=RuntimeError("boom")):
            for call in (
                lambda: rpc.plugin_call("p", "m"),
                lambda: rpc.plugin_errors("p"),
                lambda: rpc.env_get_all(),
                lambda: rpc.env_subscribe("k"),
                lambda: rpc.topic_list(),
            ):
                p = call()
                r = p.wait(timeout=0.5)
                self.assertIsNotNone(r)
                self.assertIn("boom", r.error)

    def test_forward_times_out_cleanly(self):
        channel = Channel()
        rpc = PluginRPC("forward-timeout", channel, timeout=0.05)
        try:
            with self.assertRaises(TimeoutError):
                rpc.forward(RequestPluginStatus(plugin_id="p"))
            self.assertFalse(rpc.has_pending_requests())
        finally:
            rpc.shutdown()

    def test_peer_eof_fails_pending_and_invokes_on_disconnect(self):
        channel = Channel()
        remote_desc = channel.remote_descriptor()
        disconnected = threading.Event()
        rpc = PluginRPC(
            "eof-test",
            channel,
            timeout=2.0,
            on_disconnect=disconnected.set,
        )
        try:
            pending = rpc.plugin_status()
            # Close the remote side completely (simulating peer death)
            channel.close_remote()
            remote_desc.send.close()
            remote_desc.receive.close()

            res = pending.wait(timeout=2.0)
            self.assertIsNotNone(res)
            self.assertIsNotNone(res.error)
            self.assertIn("Peer disconnected", res.error)
            self.assertTrue(disconnected.wait(timeout=2.0))
        finally:
            rpc.shutdown()

    def test_worker_rpc_timeout_does_not_raise_attribute_error(self):
        worker = PluginWorker.__new__(PluginWorker)
        worker.plugin_id = "w"
        worker.timeout_request = 0.05
        worker.error_log = []
        fake_pending = Mock()
        fake_pending.wait.return_value = None
        worker.rpc = Mock()
        worker.rpc.plugin_call.return_value = fake_pending
        worker.rpc.env_get.return_value = fake_pending
        worker.rpc.env_set.return_value = fake_pending
        worker.rpc.topic_list.return_value = fake_pending

        self.assertIsNone(worker.plugin_call("other", "foo"))
        self.assertIsNone(worker.env_get("k"))
        self.assertFalse(worker.env_set("k", 1))
        self.assertIsNone(worker.topic_list())
        self.assertEqual(len(worker.error_log), 4)

    def test_sigkill_callee_fails_caller_promptly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = PluginManager(
                work_dir=tmpdir,
                display_logs=False,
                write_logs=False,
                timeout_request=5.0,
            )
            try:
                self.assertTrue(
                    manager.load_plugin(_SlowCalleePlugin.Descriptor("0.1", "test", name="callee"))
                )
                self.assertTrue(
                    manager.load_plugin(_CallerPlugin.Descriptor("0.1", "test", name="caller"))
                )
                self.assertTrue(manager.start_plugin("callee"))
                self.assertTrue(manager.start_plugin("caller"))

                callee_pid = manager._plugins["callee"].process.pid
                caller_rpc = manager._plugins["caller"].rpc

                # Start a blocking RPC call from caller -> callee in background
                pending = caller_rpc.plugin_call("caller", "call_callee", "callee", timeout=5.0)
                time.sleep(0.15)

                t0 = time.monotonic()
                os.kill(callee_pid, signal.SIGKILL)
                res = pending.wait(timeout=4.0)
                elapsed = time.monotonic() - t0

                self.assertIsNotNone(res)
                self.assertLess(elapsed, 3.0)
                # Caller received None because callee disconnected and logged the error
                self.assertIsNone(res.result)
                errs = caller_rpc.plugin_errors().wait(timeout=2.0)
                self.assertIsNotNone(errs)
                self.assertTrue(
                    any("Peer disconnected" in e or "Error on forwarding" in e for e in errs.result),
                    f"Expected disconnect error in caller error_log, got: {errs.result}",
                )
            finally:
                manager.unload_all_plugins()

    def test_sigkill_manager_causes_worker_to_exit_cleanly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pid_file = os.path.join(tmpdir, "worker.pid")
            script = f"""
import time
from parallelazy import PluginManager
from tests.test_rpc_reliability import _OrphanTestPlugin

if __name__ == "__main__":
    m = PluginManager(work_dir={tmpdir!r}, display_logs=False, write_logs=False)
    m.load_plugin(_OrphanTestPlugin.Descriptor("0.1", "test", name="orphan", options={{"pid_file": {pid_file!r}}}))
    m.start_plugin("orphan")
    time.sleep(60)
"""
            proc = subprocess.Popen(
                [sys.executable, "-c", script],
                cwd=os.getcwd(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            worker_pid = None
            try:
                deadline = time.monotonic() + 15.0
                while time.monotonic() < deadline:
                    if os.path.exists(pid_file):
                        with open(pid_file, encoding="utf-8") as fh:
                            content = fh.read().strip()
                        if content:
                            worker_pid = int(content)
                            break
                    time.sleep(0.05)
                self.assertIsNotNone(worker_pid, "Worker did not write its PID in time")

                os.kill(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5.0)

                # Wait for orphaned worker to notice manager EOF and exit
                exit_deadline = time.monotonic() + 5.0
                alive = True
                while time.monotonic() < exit_deadline:
                    try:
                        os.kill(worker_pid, 0)
                    except ProcessLookupError:
                        alive = False
                        break
                    # On Linux, an exited orphan may remain a zombie until reaped
                    stat_path = f"/proc/{worker_pid}/stat"
                    if os.path.exists(stat_path):
                        try:
                            with open(stat_path, encoding="utf-8") as fh:
                                stat_fields = fh.read().rsplit(")", 1)[-1].split()
                            if stat_fields and stat_fields[0] == "Z":
                                alive = False
                                break
                        except FileNotFoundError:
                            alive = False
                            break
                    time.sleep(0.05)
                self.assertFalse(alive, f"Orphaned worker PID {worker_pid} did not exit after manager SIGKILL")
            finally:
                if proc.poll() is None:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5.0)
                if worker_pid is not None:
                    try:
                        os.kill(worker_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass


if __name__ == "__main__":
    unittest.main()
