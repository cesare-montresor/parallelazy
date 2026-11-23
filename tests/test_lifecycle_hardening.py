import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from parallelazy import Plugin, PluginManager
from parallelazy.plugin_data import Response
from parallelazy.plugin_worker import PluginWorker


class _OptionsPlugin(Plugin):
    @classmethod
    def default_options(cls):
        return {"a": 1, "b": 2}

    def main(self):
        self.wait_for_stop()

    def get_options(self):
        return dict(self.options)


class LifecycleHardeningTests(unittest.TestCase):
    def test_unload_single_plugin_keeps_monitoring_active(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PluginManager(work_dir=tmpdir, display_logs=False, write_logs=False) as manager:
                self.assertTrue(manager.load_plugin(_OptionsPlugin.Descriptor("0.1", "test", name="p1")))
                self.assertTrue(manager.load_plugin(_OptionsPlugin.Descriptor("0.1", "test", name="p2")))
                monitor = manager.enable_monitoring(sample_interval=0.05)
                self.assertIsNotNone(manager._monitor)

                self.assertTrue(manager.unload_plugin("p1"))
                self.assertIs(manager._monitor, monitor)
                self.assertTrue(monitor._thread is not None and monitor._thread.is_alive())

    def test_load_plugin_merges_options_with_descriptor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PluginManager(work_dir=tmpdir, display_logs=False, write_logs=False) as manager:
                desc = _OptionsPlugin.Descriptor("0.1", "test", name="opt", options={"b": 20, "c": 30})
                self.assertTrue(manager.load_plugin(desc, options={"c": 300, "d": 400}))
                self.assertTrue(manager.start_plugin("opt"))
                res = manager._plugins["opt"].rpc.plugin_call("opt", "get_options").wait(timeout=3.0)
                self.assertEqual(res.result, {"a": 1, "b": 20, "c": 300, "d": 400})

    def test_context_manager_closes_logging_and_children(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PluginManager(work_dir=tmpdir, display_logs=False, write_logs=True) as manager:
                self.assertTrue(manager.load_plugin(_OptionsPlugin.Descriptor("0.1", "test", name="ctx")))
                proc = manager._plugins["ctx"].process
                self.assertTrue(proc.is_alive())
                self.assertGreater(len(manager._logging._handlers), 0)

            self.assertFalse(proc.is_alive())
            self.assertEqual(len(manager._logging._handlers), 0)
            self.assertEqual(manager.plugin_list(), [])

    def test_env_subscribe_orders_initial_value_before_concurrent_updates(self):
        worker = PluginWorker.__new__(PluginWorker)
        worker.plugin_id = "w"
        worker.timeout_request = 2.0
        worker.error_log = []
        worker.env_subscriptions = {}
        worker._env_subscriptions_lock = threading.RLock()
        worker._env_initializing = {}
        worker._lifecycle = threading.Condition()
        worker._stop_requested = threading.Event()
        worker._active_callbacks = 0

        received = []
        update_sent = threading.Event()

        def fake_env_subscribe(var_name):
            p = Mock()
            p.wait.return_value = Response(result=True)
            return p

        def fake_env_get(var_name):
            # Simulate a concurrent ENV_UPDATE arriving while env_get is in flight
            worker._respond_env_update(Mock(), SimpleNamespace(name=var_name, value="updated"))
            update_sent.set()
            p = Mock()
            p.wait.return_value = Response(result="initial")
            return p

        worker.rpc = Mock()
        worker.rpc.env_subscribe.side_effect = fake_env_subscribe
        worker.rpc.env_get.side_effect = fake_env_get

        self.assertTrue(worker.env_subscribe("k", received.append))
        self.assertTrue(update_sent.is_set())
        self.assertEqual(received, ["initial", "updated"])


if __name__ == "__main__":
    unittest.main()
