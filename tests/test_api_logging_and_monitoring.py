import io
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from parallelazy import Plugin
from parallelazy.plugin_data import Queue, QueueReceive, QueueRecive
from parallelazy.plugin_logging import _RecoveringFileHandler, plugin_log_filename
from parallelazy.utils import process_start_spawn
from tests.helpers import make_manager, wait_until


class LoggingProbePlugin(Plugin):
    def on_start(self) -> None:
        self.logger.info("probe-logger-line")
        print("probe-stdout-line", flush=True)

    def emit_line(self, text: str) -> None:
        self.logger.info(text)

    def set_custom_format(self, fmt: str) -> None:
        self.set_log_formats(file_format=fmt)

    def main(self) -> None:
        self.wait_for_stop()


class PausedSubPlugin(Plugin):
    def on_start(self) -> None:
        self._gate = threading.Event()
        self.received = 0
        self._lock = threading.Lock()
        assert self.topic_subscribe("backlog", self._on_msg)

    def _on_msg(self, _data) -> None:
        self._gate.wait(timeout=5.0)
        with self._lock:
            self.received += 1

    def release(self) -> None:
        self._gate.set()

    def get_received(self) -> int:
        with self._lock:
            return self.received

    def main(self) -> None:
        self.wait_for_stop()


class ApiLoggingAndMonitoringTests(unittest.TestCase):
    def test_work_dir_log_capture_custom_format_recovery_and_path_lock(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_root = os.path.join(tmpdir, "run_logs")
            with make_manager(tmpdir, log_dir=log_root, write_logs=True) as manager:
                desc = LoggingProbePlugin.Descriptor("1.0.0", "tests", name="log-probe")
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("log-probe"))

                # Changing work_dir/log_dir after loading starts must raise RuntimeError
                with self.assertRaises(RuntimeError):
                    manager.work_dir = os.path.join(tmpdir, "other_work")
                with self.assertRaises(RuntimeError):
                    manager.log_dir = os.path.join(tmpdir, "other_logs")

                log_file = Path(manager.run_log_dir) / plugin_log_filename("log-probe")
                wait_until(
                    lambda: log_file.is_file()
                    and "probe-logger-line" in log_file.read_text(encoding="utf-8")
                    and "probe-stdout-line" in log_file.read_text(encoding="utf-8"),
                    message="Plugin log file did not capture logger and stdout",
                )

                # Custom file format
                manager.plugin_call("log-probe", "set_custom_format", "CUSTOM|%(message)s")
                manager.plugin_call("log-probe", "emit_line", "formatted-msg")
                wait_until(
                    lambda: "CUSTOM|formatted-msg" in log_file.read_text(encoding="utf-8")
                )

                # Deleting the log directory recovers automatically on next write
                shutil.rmtree(manager.run_log_dir)
                self.assertFalse(log_file.exists())
                manager.plugin_call("log-probe", "emit_line", "recovered-after-delete")
                wait_until(
                    lambda: log_file.is_file()
                    and "recovered-after-delete" in log_file.read_text(encoding="utf-8")
                )

    def test_write_logs_false_creates_no_log_dirs_or_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_root = os.path.join(tmpdir, "no_logs_here")
            with make_manager(tmpdir, log_dir=log_root, write_logs=False) as manager:
                self.assertFalse(os.path.exists(log_root))
                desc = LoggingProbePlugin.Descriptor("1.0.0", "tests", name="silent")
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("silent"))
                self.assertFalse(os.path.exists(log_root))

    def test_monitoring_snapshot_report_single_unload_and_linux_pipe_backlog(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                d_sub = PausedSubPlugin.Descriptor("1.0.0", "tests", name="paused-sub")
                d_extra = LoggingProbePlugin.Descriptor("1.0.0", "tests", name="extra")
                self.assertEqual(
                    manager.load_plugins([d_sub, d_extra]),
                    {"paused-sub": True, "extra": True},
                )
                self.assertEqual(
                    manager.start_all_plugins(),
                    {"paused-sub": True, "extra": True},
                )

                monitor = manager.enable_monitoring(sample_interval=0.1)
                manager.env_set("mon.key", "val")

                # Unloading one plugin must NOT disable monitoring for remaining plugins
                self.assertTrue(manager.unload_plugin("extra"))
                self.assertIs(manager._monitor, monitor)
                self.assertTrue(monitor._thread is not None and monitor._thread.is_alive())

                # Publish payloads while consumer callback is paused on the first message
                self.assertEqual(manager.topic_publish("backlog", b"a" * 1024), 1)
                self.assertEqual(manager.topic_publish("backlog", b"b" * 4096), 1)

                snapshot = manager.get_monitoring_snapshot()
                self.assertEqual(len(snapshot.processes), 1)
                self.assertTrue(any(e.key == "mon.key" for e in snapshot.environment))

                if sys.platform.startswith("linux"):
                    # Trigger fresh sample to inspect pending bytes on write end
                    fresh = monitor.sample()
                    topic_metrics = [m for m in fresh.topics if m.topic == "backlog"]
                    self.assertEqual(len(topic_metrics), 1)
                    self.assertTrue(topic_metrics[0].probe_available)
                    self.assertGreater(topic_metrics[0].pending_bytes, 0)

                out = io.StringIO()
                with redirect_stdout(out):
                    manager.print_monitoring_report()
                self.assertIn("backlog", out.getvalue())

                # Release consumer and verify payloads were NOT consumed by monitoring
                manager.plugin_call("paused-sub", "release")
                wait_until(lambda: manager.plugin_call("paused-sub", "get_received") == 2)
                manager.disable_monitoring()

    def test_core_logging_descriptor_and_spawn_invariants(self):
        self.assertEqual(_RecoveringFileHandler.terminator, "\n")
        self.assertIs(QueueRecive, QueueReceive)
        q = Queue()
        try:
            text = str(q.descriptor())
            self.assertIn("send_closed=False", text)
            self.assertIn("receive_closed=False", text)
        finally:
            q.close()

        with patch("parallelazy.utils.mp.get_start_method", return_value="fork"), patch(
            "parallelazy.utils.mp.set_start_method"
        ) as mock_set:
            process_start_spawn(force=True)
            mock_set.assert_called_once_with("spawn", force=True)


if __name__ == "__main__":
    unittest.main()
