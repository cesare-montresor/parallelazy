import io
import os
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from parallelazy.environment_manager import EnvironmentManager
from parallelazy.plugin_data import QueueDescriptor
from parallelazy.plugin_manager import PluginManager
from parallelazy.plugin_monitor import (
    CapabilityAvailability,
    EnvironmentMetric,
    MonitoringAdapter,
    MonitoringSnapshot,
    PipelineMonitor,
    ProcessMetric,
    TopicMetric,
)


class _FakeProcess:
    def __init__(self, pid):
        self.pid = pid

    def is_alive(self):
        return True


class _FakeManager:
    def __init__(self):
        import threading

        self._plugins_lock = threading.Lock()
        self._topic_publishers = {"scores": "publisher"}
        self._topic_subscribers = {"scores": {}}
        self._env_subscribers = {"stage": ["worker"]}
        self._env = EnvironmentManager()
        self._plugins = {
            "publisher": SimpleNamespace(process=_FakeProcess(os.getpid())),
        }


class PluginMonitoringTests(unittest.TestCase):
    def test_report_renders_three_tables_with_column_headers(self):
        manager = _FakeManager()
        monitor = PipelineMonitor(MonitoringAdapter(manager))
        snapshot = MonitoringSnapshot(
            sampled_at=0.0,
            topics=(
                TopicMetric("scores", "eval", "worker", 128, "growing", True),
            ),
            processes=(
                ProcessMetric(
                    "worker",
                    123,
                    True,
                    "running",
                    1,
                    25.0,
                    1024,
                    2048,
                    None,
                    True,
                    True,
                ),
            ),
            environment=(
                EnvironmentMetric("stage", "str", 2.0, 64, 1, 0.5),
            ),
            capabilities=(),
        )
        output = io.StringIO()
        with redirect_stdout(output):
            monitor.print_report(snapshot)

        report = output.getvalue()
        self.assertIn("Topics:", report)
        self.assertIn("| Topic  | Publisher | Subscriber | Pending bytes | Trend   |", report)
        self.assertIn("| scores | eval      | worker     | 128 B         | growing |", report)
        self.assertIn("| Plugin | PID | Descendants | CPU", report)
        self.assertIn("| Key   | Type | Writes/s | Stored size | Subscribers | Last write |", report)
        self.assertEqual(sum(line.startswith("+") for line in report.splitlines()), 9)

    def test_report_uses_centered_dashes_for_unavailable_samples(self):
        monitor = PipelineMonitor(MonitoringAdapter(_FakeManager()))
        snapshot = MonitoringSnapshot(
            sampled_at=0.0,
            topics=(TopicMetric("scores", None, "worker", None, None, False),),
            processes=(
                ProcessMetric(
                    "worker",
                    123,
                    True,
                    None,
                    0,
                    None,
                    None,
                    None,
                    None,
                    True,
                    True,
                ),
            ),
            environment=(EnvironmentMetric("stage", "str", None, 64, 0, None),),
            capabilities=(
                CapabilityAvailability(
                    "pipe backlog",
                    False,
                    "Pipe byte inspection is unsupported on this platform.",
                ),
            ),
        )
        output = io.StringIO()
        with redirect_stdout(output):
            monitor.print_report(snapshot)
        report = output.getvalue()

        self.assertIn("| scores | unknown   | worker", report)
        self.assertIn("|   -   |", report)
        self.assertIn("Writes/s", report)
        self.assertNotIn("unavailable", report)
        self.assertIn("pipe backlog not collected:", report)

    def test_report_reserves_unavailable_for_missing_dependencies(self):
        monitor = PipelineMonitor(MonitoringAdapter(_FakeManager()))
        snapshot = MonitoringSnapshot(
            sampled_at=0.0,
            topics=(),
            processes=(
                ProcessMetric(
                    "worker",
                    123,
                    True,
                    None,
                    0,
                    None,
                    None,
                    None,
                    None,
                    False,
                    False,
                    gpu_dependency_missing=True,
                ),
            ),
            environment=(),
            capabilities=(
                CapabilityAvailability(
                    "process metrics",
                    False,
                    "psutil is not installed.",
                    "psutil",
                    "pip install psutil",
                ),
                CapabilityAvailability(
                    "GPU metrics",
                    False,
                    "NVML support is not installed.",
                    "nvidia-ml-py",
                    "pip install nvidia-ml-py",
                ),
            ),
        )
        output = io.StringIO()
        with redirect_stdout(output):
            monitor.print_report(snapshot)
        report = output.getvalue()

        self.assertIn("unavailable", report)
        self.assertIn("pip install psutil", report)
        self.assertIn("pip install nvidia-ml-py", report)

    def test_manager_enable_disable_lifecycle_is_repeatable(self):
        manager = PluginManager.__new__(PluginManager)
        manager._monitor_lock = threading.Lock()
        manager._plugins_lock = threading.Lock()
        manager._topic_publishers = {}
        manager._topic_subscribers = {}
        manager._env_subscribers = {}
        manager._env = EnvironmentManager()
        manager._plugins = {}
        manager._monitor = None
        manager.logger = Mock()

        monitor = manager.enable_monitoring(sample_interval=0.01)
        self.assertIs(monitor, manager._monitor)
        self.assertIs(monitor, manager.enable_monitoring(sample_interval=0.25))
        self.assertEqual(monitor.sample_interval, 0.25)

        snapshot = manager.get_monitoring_snapshot()
        self.assertIsNotNone(snapshot)

        manager.disable_monitoring()
        manager.disable_monitoring()
        self.assertIsNone(manager._monitor)

    def test_environment_writes_are_counted_only_when_monitoring_is_active(self):
        environment = EnvironmentManager()
        environment.set("stage", "idle")
        snapshot = environment.monitoring_snapshot()
        self.assertEqual(snapshot["stage"]["write_count"], 0)

        environment.begin_monitoring()
        environment.set("stage", "running")
        environment.set("stage", "done")
        snapshot = environment.monitoring_snapshot()
        self.assertEqual(snapshot["stage"]["write_count"], 2)
        self.assertEqual(snapshot["stage"]["type"], "str")
        self.assertGreaterEqual(snapshot["stage"]["stored_size"], 1)

        environment.end_monitoring()
        environment.set("stage", "idle")
        snapshot = environment.monitoring_snapshot()
        self.assertEqual(snapshot["stage"]["write_count"], 0)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux pipe inspection only")
    def test_pipe_probe_reports_bytes_without_consuming_payload(self):
        receive, send = __import__("torch").multiprocessing.Pipe(duplex=False)
        subscriber = SimpleNamespace(
            queue_descriptor=QueueDescriptor(send=send, recieve=receive)
        )
        monitor = PipelineMonitor(MonitoringAdapter(_FakeManager()))

        send.send("probe")
        pending = monitor._read_pipe_pending_bytes(subscriber)
        self.assertIsNotNone(pending)
        self.assertGreater(pending, 0)
        self.assertTrue(receive.poll())
        self.assertEqual(receive.recv(), "probe")

        send.close()
        receive.close()

    def test_first_sample_marks_environment_rate_unavailable(self):
        manager = _FakeManager()
        manager._env.begin_monitoring()
        manager._env.set("stage", "running")
        monitor = PipelineMonitor(MonitoringAdapter(manager), sample_interval=0.01)
        snapshot = monitor.sample()
        metric = next(
            item for item in snapshot.environment if item.key == "stage"
        )
        self.assertIsNone(metric.writes_per_second)

        manager._env.set("stage", "done")
        time.sleep(0.02)
        snapshot = monitor.sample()
        metric = next(
            item for item in snapshot.environment if item.key == "stage"
        )
        self.assertIsNotNone(metric.writes_per_second)

    def test_missing_optional_process_dependency_is_actionable(self):
        monitor = PipelineMonitor(MonitoringAdapter(_FakeManager()))
        monitor._psutil = None
        monitor._psutil_status = CapabilityAvailability(
            "process metrics",
            False,
            "psutil is not installed.",
            "psutil",
            "pip install psutil",
        )
        snapshot = monitor.sample()
        capability = next(
            item
            for item in snapshot.capabilities
            if item.capability == "process metrics"
        )
        self.assertFalse(capability.available)
        self.assertEqual(capability.install_command, "pip install psutil")

    def test_report_file_writes_atomically_and_can_be_toggled(self):
        manager = PluginManager.__new__(PluginManager)
        manager._monitor_lock = threading.Lock()
        manager._plugins_lock = threading.Lock()
        manager._topic_publishers = {}
        manager._topic_subscribers = {}
        manager._env_subscribers = {}
        manager._env = EnvironmentManager()
        manager._plugins = {}
        manager._monitor = None
        manager.logger = Mock()

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "nested", "monitor.txt")
            try:
                resolved = manager.enable_report_file(report_path)
                self.assertEqual(resolved, os.path.abspath(report_path))
                self.assertTrue(os.path.isfile(report_path))

                with open(report_path, encoding="utf-8") as handle:
                    initial = handle.read()
                self.assertIn("Parallelazy pipeline monitor", initial)
                self.assertIn("Topics:", initial)

                manager._env.set("stage", "updated")
                deadline = time.monotonic() + 2.0
                latest = initial
                while time.monotonic() < deadline:
                    with open(report_path, encoding="utf-8") as handle:
                        latest = handle.read()
                    if "stage" in latest:
                        break
                    time.sleep(0.05)
                self.assertIn("stage", latest)

                # No temp files left behind after atomic replace
                entries = os.listdir(os.path.dirname(report_path))
                self.assertEqual(entries, ["monitor.txt"])

                manager.disable_report_file()
                self.assertIsNone(manager._monitor.report_file)

                # After disabling file output, existing file stays on disk and is no longer overwritten
                with open(report_path, "w", encoding="utf-8") as handle:
                    handle.write("sentinel")
                manager._monitor.sample()
                time.sleep(0.05)
                with open(report_path, encoding="utf-8") as handle:
                    self.assertEqual(handle.read(), "sentinel")
            finally:
                manager.disable_monitoring()

    def test_enable_monitoring_supports_report_file_switch(self):
        manager = PluginManager.__new__(PluginManager)
        manager._monitor_lock = threading.Lock()
        manager._plugins_lock = threading.Lock()
        manager._topic_publishers = {}
        manager._topic_subscribers = {}
        manager._env_subscribers = {}
        manager._env = EnvironmentManager()
        manager._plugins = {}
        manager._monitor = None
        manager.logger = Mock()

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "live.txt")
            try:
                monitor = manager.enable_monitoring(
                    sample_interval=0.05,
                    report_file=report_path,
                    report_hotkey=False,
                )
                self.assertEqual(monitor.report_file, os.path.abspath(report_path))
                self.assertTrue(os.path.isfile(report_path))
            finally:
                manager.disable_monitoring()

    def test_report_file_rejects_invalid_paths(self):
        monitor = PipelineMonitor(MonitoringAdapter(_FakeManager()))
        with self.assertRaises(ValueError):
            monitor.start_report_file("")
        with self.assertRaises(TypeError):
            monitor.start_report_file(123)  # type: ignore[arg-type]

    def test_report_file_retries_transient_permission_error_on_replace(self):
        monitor = PipelineMonitor(MonitoringAdapter(_FakeManager()))
        real_replace = os.replace
        calls = {"count": 0}

        def flaky_replace(src, dst):
            calls["count"] += 1
            if calls["count"] < 3:
                raise PermissionError("file busy")
            return real_replace(src, dst)

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "monitor.txt")
            with patch("parallelazy.plugin_monitor.os.replace", side_effect=flaky_replace):
                monitor.start_report_file(report_path)
            self.assertEqual(calls["count"], 3)
            self.assertTrue(os.path.isfile(report_path))

    def test_start_hotkey_returns_false_when_stdin_is_none(self):
        monitor = PipelineMonitor(MonitoringAdapter(_FakeManager()))
        with patch.object(sys, "stdin", None):
            self.assertFalse(monitor.start_hotkey("m"))

    def test_windows_hotkey_handles_extended_keys_hotkey_and_ctrl_c(self):
        monitor = PipelineMonitor(MonitoringAdapter(_FakeManager()))
        printed = threading.Event()
        interrupted = threading.Event()
        keys = ["\xe0", "m", "x", "m", "\x03"]

        fake_msvcrt = SimpleNamespace(
            kbhit=lambda: bool(keys),
            getwch=lambda: keys.pop(0),
        )
        fake_thread_mod = SimpleNamespace(
            interrupt_main=lambda: interrupted.set(),
        )
        monitor.print_report = Mock(side_effect=lambda: printed.set())

        with patch.dict(sys.modules, {"msvcrt": fake_msvcrt, "_thread": fake_thread_mod}):
            self.assertTrue(monitor._start_hotkey_windows("m"))
            self.assertTrue(printed.wait(timeout=1.0))
            self.assertTrue(interrupted.wait(timeout=1.0))
            monitor.stop_hotkey()

        # Extended key sequence ("\xe0", "m") was ignored; only the standalone "m" printed
        self.assertEqual(monitor.print_report.call_count, 1)

    def test_nvml_is_queried_once_per_sample_and_shutdown_on_stop(self):
        manager = _FakeManager()
        manager._plugins = {
            "p1": SimpleNamespace(process=_FakeProcess(1001)),
            "p2": SimpleNamespace(process=_FakeProcess(1002)),
        }

        class _FakePsutilProc:
            def __init__(self, pid):
                self.pid = pid

            def children(self, recursive=True):
                return []

            def memory_info(self):
                return SimpleNamespace(rss=1024)

            def cpu_times(self):
                return SimpleNamespace(user=1.0, system=0.5)

        fake_psutil = SimpleNamespace(
            Process=_FakePsutilProc,
            NoSuchProcess=RuntimeError,
            AccessDenied=PermissionError,
        )
        fake_nvml = SimpleNamespace(
            nvmlInit=Mock(),
            nvmlShutdown=Mock(),
            nvmlDeviceGetCount=Mock(return_value=1),
            nvmlDeviceGetHandleByIndex=Mock(return_value="h0"),
            nvmlDeviceGetComputeRunningProcesses=Mock(
                return_value=[
                    SimpleNamespace(pid=1001, usedGpuMemory=512),
                    SimpleNamespace(pid=1002, usedGpuMemory=256),
                ]
            ),
            nvmlDeviceGetGraphicsRunningProcesses=Mock(return_value=[]),
        )

        monitor = PipelineMonitor(MonitoringAdapter(manager))
        monitor._psutil = fake_psutil
        monitor._psutil_status = CapabilityAvailability("process metrics", True)
        monitor._pynvml = fake_nvml
        monitor._gpu_status = CapabilityAvailability("GPU metrics", True)

        snap1 = monitor.sample()
        # NVML queried once across both plugins, not once per plugin
        self.assertEqual(fake_nvml.nvmlDeviceGetCount.call_count, 1)
        by_id = {m.plugin_id: m for m in snap1.processes}
        self.assertEqual(by_id["p1"].gpu_memory_bytes, 512)
        self.assertEqual(by_id["p2"].gpu_memory_bytes, 256)

        # Second sample with unchanged cpu_times reports 0.0% (not None)
        time.sleep(0.01)
        snap2 = monitor.sample()
        by_id2 = {m.plugin_id: m for m in snap2.processes}
        self.assertEqual(by_id2["p1"].cpu_percent, 0.0)
        self.assertEqual(by_id2["p2"].cpu_percent, 0.0)

        monitor.stop()
        fake_nvml.nvmlShutdown.assert_called_once()
        self.assertIsNone(monitor._pynvml)

    def test_previous_topic_bytes_prunes_removed_subscribers(self):
        manager = _FakeManager()
        monitor = PipelineMonitor(MonitoringAdapter(manager))
        monitor._previous_topic_bytes[("stale", "gone")] = 42
        monitor.sample()
        self.assertNotIn(("stale", "gone"), monitor._previous_topic_bytes)


if __name__ == "__main__":
    unittest.main()
