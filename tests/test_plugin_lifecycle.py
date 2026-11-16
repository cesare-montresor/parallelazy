import threading
import unittest

from parallelazy.plugin import PluginState
from parallelazy.plugin_data import Channel, CommandType, RequestPluginStop
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


if __name__ == "__main__":
    unittest.main()
