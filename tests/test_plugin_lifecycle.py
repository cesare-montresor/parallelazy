import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from parallelazy.plugin import PluginState
from parallelazy.plugin_data import (
    Channel,
    CommandType,
    PluginStartupPhase,
    PluginStatus,
    ProcessStatus,
    RequestPluginStop,
)
from parallelazy.plugin_manager import PluginInfo, PluginManager
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
    worker._startup_phase = SimpleNamespace(
        value=int(PluginStartupPhase.INITIALIZING)
    )
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
    def test_environment_update_callback_receives_value_not_name(self):
        callback_values = []
        worker = _make_worker(None)
        worker.env_subscriptions = {
            "config.batch_size": callback_values.append,
        }

        request = SimpleNamespace(
            name="config.batch_size",
            value=64,
        )

        result = worker._respond_env_update(Mock(), request)

        self.assertTrue(result)
        self.assertEqual(callback_values, [64])

    def test_start_plugins_sequentially(self):
        manager = PluginManager.__new__(PluginManager)
        manager.start_plugin = Mock(return_value=True)

        results = manager.start_plugins(["first", "second"], parallel=False)

        self.assertEqual(results, {"first": True, "second": True})
        self.assertEqual(
            [args[0] for args, _ in manager.start_plugin.call_args_list],
            ["first", "second"],
        )

    def test_start_plugins_in_parallel(self):
        manager = PluginManager.__new__(PluginManager)
        start_barrier = threading.Barrier(2)

        def start_plugin(plugin_id):
            start_barrier.wait(timeout=2)
            return plugin_id == "first"

        manager.start_plugin = Mock(side_effect=start_plugin)
        results = manager.start_plugins(["first", "second"], parallel=True)

        self.assertEqual(results, {"first": True, "second": False})
        self.assertEqual(manager.start_plugin.call_count, 2)

    def test_load_plugins_in_parallel_returns_each_result(self):
        manager = PluginManager.__new__(PluginManager)
        load_barrier = threading.Barrier(2)

        def load_plugin(descriptor):
            load_barrier.wait(timeout=2)
            return descriptor.name == "first"

        manager.load_plugin = Mock(side_effect=load_plugin)
        descriptors = [
            SimpleNamespace(name="first"),
            SimpleNamespace(name="second"),
        ]

        results = manager.load_plugins(descriptors, parallel=True)

        self.assertEqual(results, {"first": True, "second": False})
        self.assertEqual(manager.load_plugin.call_count, 2)

    def test_worker_initialization_deadline_excludes_debugger_wait(self):
        manager = PluginManager.__new__(PluginManager)
        manager.timeout_initialize = 0.02
        manager.timeout_debugger_attach = None
        manager.timeout_start = 1.0
        manager.logger = Mock()
        startup_phase = SimpleNamespace(
            value=int(PluginStartupPhase.WAITING_FOR_DEBUGGER)
        )
        initialized = threading.Event()
        process = Mock()
        process.is_alive.return_value = True
        result = []

        def finish_debugger_wait():
            threading.Event().wait(0.06)
            startup_phase.value = int(PluginStartupPhase.INITIALIZING)
            initialized.set()

        release_thread = threading.Thread(target=finish_debugger_wait)
        release_thread.start()
        result.append(
            manager._wait_for_worker_ready(
                "test-plugin",
                process,
                initialized,
                startup_phase,
            )
        )
        release_thread.join(timeout=1)

        self.assertEqual(result, [True])
        self.assertEqual(startup_phase.value, PluginStartupPhase.INITIALIZING)

    def test_debugger_attach_deadline_is_configurable(self):
        manager = PluginManager.__new__(PluginManager)
        manager.timeout_initialize = 1.0
        manager.timeout_debugger_attach = 0.02
        manager.timeout_start = 1.0
        manager.logger = Mock()
        startup_phase = SimpleNamespace(
            value=int(PluginStartupPhase.WAITING_FOR_DEBUGGER)
        )
        initialized = threading.Event()
        process = Mock()
        process.is_alive.return_value = True

        result = manager._wait_for_worker_ready(
            "test-plugin",
            process,
            initialized,
            startup_phase,
        )

        self.assertFalse(result)
        self.assertEqual(startup_phase.value, PluginStartupPhase.FAILED)
        manager.logger.error.assert_called_once()

    def test_initialization_deadline_marks_worker_failed(self):
        manager = PluginManager.__new__(PluginManager)
        manager.timeout_initialize = 0.02
        manager.timeout_debugger_attach = None
        manager.timeout_start = 1.0
        manager.logger = Mock()
        startup_phase = SimpleNamespace(
            value=int(PluginStartupPhase.INITIALIZING)
        )
        initialized = threading.Event()
        process = Mock()
        process.is_alive.return_value = True

        result = manager._wait_for_worker_ready(
            "test-plugin",
            process,
            initialized,
            startup_phase,
        )

        self.assertFalse(result)
        self.assertEqual(startup_phase.value, PluginStartupPhase.FAILED)
        manager.logger.error.assert_called_once()

    def test_topic_callback_error_log_identifies_topic_without_repeating_plugin_id(self):
        from parallelazy.plugin_data import _TOPIC_SHUTDOWN_SENTINEL

        plugin = _RaisingCallbackPlugin()
        worker = _make_worker(plugin)
        worker._is_running = True
        worker.did_start.set()
        messages = iter(("score", _TOPIC_SHUTDOWN_SENTINEL))
        subscription = SimpleNamespace(
            callback=plugin.on_scores,
            queue=SimpleNamespace(get=lambda: next(messages)),
        )
        worker.topic_subscriptions["scores"] = subscription

        with self.assertLogs("parallelazy.plugin_worker", level="ERROR") as logs:
            worker._topic_listener("scores")

        self.assertIn("topic 'scores' callback", logs.output[0])
        self.assertNotIn("test-plugin", logs.output[0])

    def test_closed_topic_queue_log_does_not_repeat_plugin_id(self):
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

        self.assertIn("Subscription for topic 'scores' closed", logs.output[0])
        self.assertNotIn("test-plugin", logs.output[0])
        self.assertIn("No such file or directory", logs.output[0])

    def test_worker_reports_debugger_wait_before_initialization(self):
        startup_phase = SimpleNamespace(value=int(PluginStartupPhase.NOT_STARTED))
        observed_phases = []
        runtime_settings = SimpleNamespace(display_logs=True)
        logging_session = Mock()
        output_capture = Mock()
        worker = Mock()

        def init_debugger(_plugin_id):
            observed_phases.append(startup_phase.value)

        def create_worker(*args, **kwargs):
            self.assertEqual(
                startup_phase.value,
                PluginStartupPhase.INITIALIZING,
            )
            return worker

        with patch(
            "parallelazy.plugin_worker.default_runtime_settings",
            return_value=runtime_settings,
        ), patch(
            "parallelazy.plugin_worker.configure_worker_logging",
            return_value=logging_session,
        ), patch(
            "parallelazy.plugin_worker.configure_worker_output",
            return_value=output_capture,
        ), patch(
            "parallelazy.plugin_worker.is_debugging_enabled",
            return_value=True,
        ), patch(
            "parallelazy.plugin_worker.init_debugger",
            side_effect=init_debugger,
        ), patch(
            "parallelazy.plugin_worker.PluginWorker",
            side_effect=create_worker,
        ):
            PluginWorker.run(
                "test-plugin",
                {},
                {},
                Mock(),
                threading.Event(),
                startup_phase=startup_phase,
            )

        self.assertEqual(
            observed_phases,
            [PluginStartupPhase.WAITING_FOR_DEBUGGER],
        )
        self.assertEqual(startup_phase.value, PluginStartupPhase.INITIALIZING)
        worker.main.assert_called_once_with()
        output_capture.close.assert_called_once_with()
        logging_session.close.assert_called_once_with()

    def test_worker_marks_startup_failed_when_initialization_raises(self):
        startup_phase = SimpleNamespace(value=int(PluginStartupPhase.NOT_STARTED))

        with patch(
            "parallelazy.plugin_worker.configure_worker_logging",
            return_value=Mock(),
        ), patch(
            "parallelazy.plugin_worker.configure_worker_output",
            return_value=Mock(),
        ), patch(
            "parallelazy.plugin_worker.is_debugging_enabled",
            return_value=False,
        ), patch(
            "parallelazy.plugin_worker.PluginWorker",
            side_effect=RuntimeError("initialization failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "initialization failed"):
                PluginWorker.run(
                    "test-plugin",
                    {},
                    {},
                    Mock(),
                    threading.Event(),
                    runtime_settings=SimpleNamespace(display_logs=True),
                    startup_phase=startup_phase,
                )

        self.assertEqual(startup_phase.value, PluginStartupPhase.FAILED)

    def test_process_status_exposes_startup_phase(self):
        process = Mock()
        process.is_alive.return_value = False
        process.pid = 1234
        manager = PluginManager.__new__(PluginManager)
        manager._plugins_lock = threading.RLock()
        manager._plugins = {
            "test-plugin": PluginInfo(
                plugin_id="test-plugin",
                process=process,
                rpc=Mock(),
                descriptor=Mock(),
                options={},
                startup_phase=SimpleNamespace(
                    value=int(PluginStartupPhase.WAITING_FOR_DEBUGGER)
                ),
            )
        }

        status = manager.plugin_status("test-plugin")

        self.assertEqual(status.startup_phase, "waiting_for_debugger")

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
        self.assertEqual(
            worker._startup_phase.value,
            PluginStartupPhase.READY,
        )

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
