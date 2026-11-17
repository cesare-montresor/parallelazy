import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from parallelazy.plugin import PluginState
from parallelazy.plugin_data import Channel, CommandType, PluginStatus, ProcessStatus, RequestPluginStop
from parallelazy.plugin_manager import PluginManager
from parallelazy.plugin_rpc import PluginRPC
from parallelazy.plugin_worker import PluginWorker


class _FakeRPC:
    command_handlers = {}

    def stop_accepting_commands(self):
        pass

    def wait_for_commands(self, exclude=()):
        pass

    def drain_commands(self):
        pass

    def shutdown(self):
        pass


class _LifecyclePlugin:
    def __init__(self, worker, block_start=False, block_main=False):
        self.worker = worker
        self.block_start = block_start
        self.block_main = block_main
        self.state = PluginState.INITIALIZED
        self._is_running = False
        self.start_entered = threading.Event()
        self.release_start = threading.Event()
        self.main_entered = threading.Event()
        self.main_returned = threading.Event()
        self.main_thread_id = None
        self.stop_thread_id = None
        self.stop_order = []

    def on_start(self):
        self.stop_order.append("start-enter")
        self.start_entered.set()
        if self.block_start:
            self.release_start.wait()
        self.stop_order.append("start-exit")

    def main(self):
        self.stop_order.append("main")
        self.main_thread_id = threading.get_ident()
        self.main_entered.set()
        if self.block_main:
            self.worker.wait_for_stop()
        self.main_returned.set()

    def on_stop(self):
        self.stop_thread_id = threading.get_ident()
        self.stop_order.append("stop")


class _RaisingCallbackPlugin:
    def on_scores(self, data):
        raise RuntimeError("callback failed")


def _make_worker(plugin):
    worker = PluginWorker.__new__(PluginWorker)
    worker.plugin_id = "test-plugin"
    worker.rpc = _FakeRPC()
    worker.error_log = []
    worker.main_thread = None
    worker._is_running = False
    worker._initialized = threading.Event()
    worker.did_start = threading.Event()
    worker.did_stop = threading.Event()
    worker._stop_requested = threading.Event()
    worker._lifecycle = threading.Condition()
    worker._start_in_progress = False
    worker._start_complete = False
    worker._active_callbacks = 0
    worker.topic_subscriptions = {}
    worker.plugin = plugin
    return worker


class PluginLifecycleTests(unittest.TestCase):
    def test_topic_callback_error_log_identifies_plugin(self):
        plugin = _RaisingCallbackPlugin()
        worker = _make_worker(plugin)
        worker._is_running = True
        worker.did_start.set()
        messages = iter(("score", None))
        subscription = SimpleNamespace(
            callback=plugin.on_scores,
            queue=SimpleNamespace(get=lambda: next(messages)),
        )
        worker.topic_subscriptions["scores"] = subscription

        with self.assertLogs("parallelazy.plugin_worker", level="ERROR") as logs:
            worker._topic_listener("scores")

        self.assertIn("plugin 'test-plugin'", logs.output[0])
        self.assertIn("topic 'scores' callback", logs.output[0])

    def test_closed_topic_queue_log_identifies_plugin(self):
        worker = _make_worker(None)
        worker._is_running = True
        worker.did_start.set()
        subscription = SimpleNamespace(
            callback=None,
            queue=SimpleNamespace(get=Mock(side_effect=FileNotFoundError(2, "No such file or directory"))),
        )
        worker.topic_subscriptions["scores"] = subscription

        with self.assertLogs("parallelazy.plugin_worker", level="INFO") as logs:
            worker._topic_listener("scores")

        self.assertIn("plugin 'test-plugin'", logs.output[0])
        self.assertIn("topic 'scores' closed", logs.output[0])
        self.assertIn("No such file or directory", logs.output[0])

    def _start_worker(self, worker):
        thread = threading.Thread(target=worker.main)
        thread.start()
        self.assertTrue(worker._initialized.wait(timeout=2))
        return thread

    def test_main_runs_once_on_worker_thread_and_worker_waits_after_return(self):
        plugin = _LifecyclePlugin(None)
        worker = _make_worker(plugin)
        plugin.worker = worker
        thread = self._start_worker(worker)

        self.assertTrue(worker.start())
        self.assertTrue(plugin.main_returned.wait(timeout=2))
        self.assertEqual(plugin.main_thread_id, thread.ident)
        self.assertTrue(thread.is_alive())

        worker.stop(wait=True)
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(plugin.stop_order.count("stop"), 1)
        self.assertEqual(plugin.stop_thread_id, thread.ident)

    def test_stop_during_start_waits_for_start_before_on_stop(self):
        plugin = _LifecyclePlugin(None, block_start=True)
        worker = _make_worker(plugin)
        plugin.worker = worker
        worker_thread = self._start_worker(worker)

        start_thread = threading.Thread(target=worker.start)
        start_thread.start()
        self.assertTrue(plugin.start_entered.wait(timeout=2))

        stop_thread = threading.Thread(target=lambda: worker.stop(wait=True))
        stop_thread.start()
        self.assertTrue(worker._stop_requested.wait(timeout=2))
        self.assertNotIn("stop", plugin.stop_order)

        plugin.release_start.set()
        start_thread.join(timeout=2)
        stop_thread.join(timeout=2)
        worker_thread.join(timeout=2)

        self.assertFalse(start_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertFalse(worker_thread.is_alive())
        self.assertLess(plugin.stop_order.index("start-exit"), plugin.stop_order.index("stop"))
        self.assertNotIn("main", plugin.stop_order)

    def test_stop_request_releases_blocking_main(self):
        plugin = _LifecyclePlugin(None, block_main=True)
        worker = _make_worker(plugin)
        plugin.worker = worker
        worker_thread = self._start_worker(worker)

        self.assertTrue(worker.start())
        self.assertTrue(plugin.main_entered.wait(timeout=2))
        worker.stop(wait=True)
        self.assertTrue(plugin.main_returned.is_set())
        worker_thread.join(timeout=2)

        self.assertFalse(worker_thread.is_alive())
        self.assertEqual(plugin.stop_order.count("main"), 1)
        self.assertEqual(plugin.stop_order.count("stop"), 1)

    def test_rpc_drain_sends_stop_response_before_closing_channel(self):
        manager_channel = Channel()
        worker_channel = Channel(manager_channel.remote_descriptor())
        manager_rpc = PluginRPC("manager", manager_channel, start_listener=False)
        worker_rpc = PluginRPC("worker", worker_channel, start_listener=False)
        handler_entered = threading.Event()
        release_handler = threading.Event()

        def stop_handler(rpc, request):
            handler_entered.set()
            release_handler.wait()
            return True

        worker_rpc.on_request(CommandType.PLUGIN_STOP, stop_handler)
        manager_rpc.start_listener()
        worker_rpc.start_listener()
        pending = manager_rpc.request(RequestPluginStop(plugin_id="worker"))
        self.assertTrue(handler_entered.wait(timeout=2))

        worker_rpc.stop_accepting_commands()
        drain_thread = threading.Thread(target=worker_rpc.drain_commands)
        drain_thread.start()
        release_handler.set()

        response = pending.wait(timeout=2)
        drain_thread.join(timeout=2)
        self.assertIsNotNone(response)
        self.assertTrue(response.result)
        self.assertFalse(drain_thread.is_alive())

        manager_rpc.shutdown()
        worker_rpc.shutdown()

    def test_worker_can_query_plugin_status_and_all_statuses(self):
        manager_channel = Channel()
        worker_channel = Channel(manager_channel.remote_descriptor())
        manager_rpc = PluginRPC("manager", manager_channel, start_listener=False)
        worker_rpc = PluginRPC("test-plugin", worker_channel, start_listener=False)

        worker_status = PluginStatus(
            plugin_id="test-plugin",
            state="running",
            is_running=True,
            subscriptions=0,
            subscribers=0,
            published=0,
            errors=0,
        )
        status = ProcessStatus(
            plugin_id="test-plugin",
            process_alive=True,
            process_pid=123,
            worker_status=worker_status,
        )
        manager = PluginManager.__new__(PluginManager)
        manager.plugin_status = Mock(return_value=status)
        manager.plugin_status_all = Mock(return_value={"test-plugin": status})
        manager._init_plugin_rpc(manager_rpc)

        worker = _make_worker(None)
        worker.rpc = worker_rpc
        manager_rpc.start_listener()
        worker_rpc.start_listener()
        try:
            self.assertEqual(worker.plugin_status(), status)
            self.assertEqual(worker.plugin_status_all(), {"test-plugin": status})
            manager.plugin_status.assert_called_once_with("test-plugin")
            manager.plugin_status_all.assert_called_once_with()
        finally:
            manager_rpc.shutdown()
            worker_rpc.shutdown()


if __name__ == "__main__":
    unittest.main()
