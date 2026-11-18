import os
import sys
import time
import threading
import unittest
from unittest.mock import Mock
from types import SimpleNamespace

from parallelazy.environment_manager import EnvironmentManager
from parallelazy.plugin_data import QueueDescriptor
from parallelazy.plugin_manager import PluginManager
from parallelazy.plugin_monitor import (
    CapabilityAvailability,
    MonitoringAdapter,
    PipelineMonitor,
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
        self.assertIs(monitor, manager.enable_monitoring(sample_interval=0.01))

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


if __name__ == "__main__":
    unittest.main()
