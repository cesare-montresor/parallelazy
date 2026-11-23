import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from typing import Any, List

from parallelazy import Plugin
from tests.helpers import is_pid_alive, make_manager, wait_until


class ChaosCalleePlugin(Plugin):
    def block_until_stop(self) -> str:
        self.wait_for_stop()
        return "done"

    def echo(self, value: Any) -> Any:
        return value

    def main(self) -> None:
        self.wait_for_stop()


class ChaosCallerPlugin(Plugin):
    def call_blocking(self, target_id: str) -> Any:
        return self.plugin_call(target_id, "block_until_stop")

    def main(self) -> None:
        self.wait_for_stop()


class ChaosPubPlugin(Plugin):
    def publish_many(self, topic: str, count: int, payload_bytes: int) -> List[int]:
        payload = b"z" * payload_bytes
        counts = []
        for _ in range(count):
            counts.append(self.topic_publish(topic, payload))
        return counts

    def publish_one(self, topic: str, data: Any) -> int:
        return self.topic_publish(topic, data)

    def main(self) -> None:
        self.wait_for_stop()


class ChaosSubPlugin(Plugin):
    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.received_count = 0
        for topic in self.options.get("topics", []):
            assert self.topic_subscribe(topic, self._on_data)

    def _on_data(self, _data: Any) -> None:
        with self._lock:
            self.received_count += 1

    def get_received_count(self) -> int:
        with self._lock:
            return self.received_count

    def is_listener_alive(self, topic: str) -> bool:
        sub = self._worker.topic_subscriptions.get(topic)
        return bool(sub and sub.thread and sub.thread.is_alive())

    def main(self) -> None:
        self.wait_for_stop()


class ChaosOrphanPlugin(Plugin):
    def on_start(self) -> None:
        pid_file = self.options.get("pid_file")
        if pid_file:
            with open(pid_file, "w", encoding="utf-8") as fh:
                fh.write(str(os.getpid()))

    def main(self) -> None:
        self.wait_for_stop()


class ChaosAndResilienceTests(unittest.TestCase):
    def test_sigkill_rpc_callee_mid_call_fails_caller_promptly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir, timeout_request=5.0) as manager:
                d_callee = ChaosCalleePlugin.Descriptor("1.0.0", "tests", name="callee")
                d_caller = ChaosCallerPlugin.Descriptor("1.0.0", "tests", name="caller")
                self.assertEqual(
                    manager.load_plugins([d_callee, d_caller]),
                    {"callee": True, "caller": True},
                )
                self.assertEqual(
                    manager.start_all_plugins(),
                    {"callee": True, "caller": True},
                )

                callee_pid = manager.plugin_status("callee").process_pid
                result_box = []
                t = threading.Thread(
                    target=lambda: result_box.append(
                        manager.plugin_call("caller", "call_blocking", "callee", timeout=5.0)
                    )
                )
                t.start()
                time.sleep(0.15)

                t0 = time.monotonic()
                os.kill(callee_pid, signal.SIGKILL)
                t.join(timeout=4.0)
                elapsed = time.monotonic() - t0

                self.assertFalse(t.is_alive(), "Caller hung after callee SIGKILL")
                self.assertLess(elapsed, 3.0)
                self.assertEqual(result_box, [None])
                errs = manager.plugin_errors("caller")
                self.assertTrue(
                    any("Peer disconnected" in e or "Error on forwarding" in e for e in errs),
                    f"Expected disconnect error, got: {errs}",
                )

    def test_sigkill_subscriber_during_large_payload_burst_evicts_and_continues(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir, timeout_request=5.0) as manager:
                d_pub = ChaosPubPlugin.Descriptor("1.0.0", "tests", name="pub")
                d_dead = ChaosSubPlugin.Descriptor(
                    "1.0.0", "tests", name="sub-dead", options={"topics": ["big"]}
                )
                d_live = ChaosSubPlugin.Descriptor(
                    "1.0.0", "tests", name="sub-live", options={"topics": ["big"]}
                )
                self.assertEqual(
                    manager.load_plugins([d_pub, d_dead, d_live]),
                    {"pub": True, "sub-dead": True, "sub-live": True},
                )
                self.assertTrue(manager.start_plugin("sub-dead"))
                self.assertTrue(manager.start_plugin("sub-live"))
                self.assertTrue(manager.start_plugin("pub"))

                # Establish routing
                self.assertEqual(manager.plugin_call("pub", "publish_one", "big", b"init"), 2)

                dead_pid = manager.plugin_status("sub-dead").process_pid
                os.kill(dead_pid, signal.SIGKILL)
                wait_until(lambda: not is_pid_alive(dead_pid))

                # Publish 25 x 300 KB payloads — must not block and must deliver all 25 to sub-live
                counts = manager.plugin_call("pub", "publish_many", "big", 25, 300 * 1024, timeout=10.0)
                self.assertEqual(counts, [1] * 25)
                wait_until(
                    lambda: manager.plugin_call("sub-live", "get_received_count") == 26,
                    timeout=5.0,
                )

    def test_sigkill_publisher_closes_subscriber_listener_cleanly_on_eof(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                d_pub = ChaosPubPlugin.Descriptor("1.0.0", "tests", name="pub")
                d_sub = ChaosSubPlugin.Descriptor(
                    "1.0.0", "tests", name="sub", options={"topics": ["stream"]}
                )
                self.assertEqual(manager.load_plugins([d_pub, d_sub]), {"pub": True, "sub": True})
                self.assertTrue(manager.start_plugin("sub"))
                self.assertTrue(manager.start_plugin("pub"))

                self.assertEqual(manager.plugin_call("pub", "publish_one", "stream", "hello"), 1)
                wait_until(lambda: manager.plugin_call("sub", "get_received_count") == 1)

                pub_pid = manager.plugin_status("pub").process_pid
                os.kill(pub_pid, signal.SIGKILL)
                wait_until(lambda: not is_pid_alive(pub_pid))

                # Subscriber process stays healthy and responsive
                self.assertEqual(manager.plugin_call("sub", "get_received_count"), 1)

    def test_sigkill_manager_causes_orphaned_worker_to_exit_cleanly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pid_file = os.path.join(tmpdir, "worker.pid")
            script = f"""
import time
from parallelazy import PluginManager
from tests.test_chaos_and_resilience import ChaosOrphanPlugin

if __name__ == "__main__":
    m = PluginManager(work_dir={tmpdir!r}, display_logs=False, write_logs=False)
    m.load_plugin(ChaosOrphanPlugin.Descriptor("1.0.0", "tests", name="orphan", options={{"pid_file": {pid_file!r}}}))
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
                def _read_pid():
                    nonlocal worker_pid
                    if os.path.exists(pid_file):
                        with open(pid_file, encoding="utf-8") as fh:
                            text = fh.read().strip()
                        if text:
                            worker_pid = int(text)
                            return True
                    return False

                wait_until(_read_pid, timeout=15.0, message="Worker did not write PID")

                os.kill(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5.0)

                wait_until(
                    lambda: not is_pid_alive(worker_pid),
                    timeout=5.0,
                    message=f"Orphaned worker PID {worker_pid} did not exit after manager SIGKILL",
                )
            finally:
                if proc.poll() is None:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5.0)
                if worker_pid is not None and is_pid_alive(worker_pid):
                    try:
                        os.kill(worker_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_unload_under_concurrent_rpc_and_topic_load_reaps_cleanly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                d_target = ChaosCalleePlugin.Descriptor("1.0.0", "tests", name="target")
                self.assertTrue(manager.load_plugin(d_target))
                self.assertTrue(manager.start_plugin("target"))
                pid = manager.plugin_status("target").process_pid

                stop_spam = threading.Event()

                def _spam():
                    while not stop_spam.is_set():
                        try:
                            manager.plugin_call("target", "echo", "ping", timeout=0.5)
                        except Exception:
                            pass

                threads = [threading.Thread(target=_spam, daemon=True) for _ in range(3)]
                for t in threads:
                    t.start()
                time.sleep(0.1)

                self.assertTrue(manager.unload_plugin("target"))
                stop_spam.set()
                for t in threads:
                    t.join(timeout=2.0)

                wait_until(lambda: not is_pid_alive(pid), message="Unloaded worker PID still alive")
                self.assertNotIn("target", manager.plugin_list())


if __name__ == "__main__":
    unittest.main()
