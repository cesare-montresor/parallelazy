import tempfile
import threading
import unittest

from parallelazy import Plugin
from tests.helpers import (
    BlockingMainPlugin,
    CoordinatorPlugin,
    CrashInitPlugin,
    EchoPlugin,
    HangInitPlugin,
    SelfStoppingPlugin,
    SlowStartPlugin,
    is_pid_alive,
    make_manager,
    wait_until,
)


class _NoDocstringProbe(Plugin):
    pass


class ApiLifecycleTests(unittest.TestCase):
    def test_full_lifecycle_roundtrip_and_context_manager_cleanup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir, write_logs=True) as manager:
                desc = EchoPlugin.Descriptor("1.0.0", "tests", name="echo-1")
                self.assertTrue(manager.load_plugin(desc))
                self.assertIn("echo-1", manager.plugin_list())
                self.assertTrue(manager.start_plugin("echo-1"))

                status = manager.plugin_status("echo-1")
                self.assertIsNotNone(status)
                self.assertTrue(status.process_alive)
                self.assertEqual(status.worker_status.state, "running")
                pid = status.process_pid

                all_statuses = manager.plugin_status_all()
                self.assertIn("echo-1", all_statuses)

                # main() returned immediately, yet worker remains alive for RPC
                self.assertEqual(manager.plugin_call("echo-1", "echo", "alive"), "alive")

                self.assertTrue(manager.stop_plugin("echo-1"))
                self.assertTrue(manager.unload_plugin("echo-1"))
                self.assertEqual(manager.plugin_list(), [])
                wait_until(lambda: not is_pid_alive(pid), message="worker pid still alive")

            self.assertEqual(len(manager._logging._handlers), 0)

    def test_parallel_load_and_start_all(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                descs = [
                    EchoPlugin.Descriptor("1.0.0", "tests", name=f"p-{i}")
                    for i in range(3)
                ]
                loaded = manager.load_plugins(descs, parallel=True)
                self.assertEqual(loaded, {"p-0": True, "p-1": True, "p-2": True})

                started = manager.start_all_plugins(parallel=True)
                self.assertEqual(started, {"p-0": True, "p-1": True, "p-2": True})

                pids = [manager.plugin_status(f"p-{i}").process_pid for i in range(3)]
            for pid in pids:
                wait_until(lambda p=pid: not is_pid_alive(p))

    def test_blocking_main_exits_on_stop(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                desc = BlockingMainPlugin.Descriptor("1.0.0", "tests", name="blocker")
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("blocker"))
                wait_until(lambda: manager.env_get("blocker.in_main") is True)

                self.assertTrue(manager.stop_plugin("blocker"))
                self.assertTrue(manager.env_get("blocker.main_exited"))
                self.assertTrue(manager.env_get("blocker.stopped"))

    def test_option_precedence_default_descriptor_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                desc = EchoPlugin.Descriptor(
                    "1.0.0", "tests", name="opt", options={"b": 20, "c": 30}
                )
                self.assertTrue(manager.load_plugin(desc, options={"c": 300, "d": 400}))
                self.assertTrue(manager.start_plugin("opt"))
                opts = manager.plugin_call("opt", "get_options")
                self.assertEqual(opts, {"a": 1, "b": 20, "c": 300, "d": 400})

    def test_plugin_driven_lifecycle_and_self_guard(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                coord_desc = CoordinatorPlugin.Descriptor("1.0.0", "tests", name="coord")
                self.assertTrue(manager.load_plugin(coord_desc))
                self.assertTrue(manager.start_plugin("coord"))

                res = manager.plugin_call("coord", "manage_child", "child-1", timeout=10.0)
                self.assertTrue(res["loaded"])
                self.assertTrue(res["started"])
                self.assertTrue(res["alive_when_running"])
                self.assertEqual(res["echoed"], "hi")
                self.assertTrue(res["stopped"])
                self.assertTrue(res["unloaded"])
                self.assertNotIn("child-1", manager.plugin_list())

                guard = manager.plugin_call("coord", "attempt_self_lifecycle")
                self.assertEqual(guard, {"start": False, "stop": False, "unload": False})

    def test_duplicate_id_and_double_start_idempotency(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                desc = EchoPlugin.Descriptor("1.0.0", "tests", name="dup")
                self.assertTrue(manager.load_plugin(desc))
                self.assertFalse(manager.load_plugin(desc))

                self.assertTrue(manager.start_plugin("dup"))
                self.assertTrue(manager.start_plugin("dup"))
                self.assertEqual(manager.plugin_call("dup", "echo", "ok"), "ok")

    def test_stop_during_on_start_waits_for_start_skips_main_runs_on_stop(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                desc = SlowStartPlugin.Descriptor(
                    "1.0.0", "tests", name="slow", options={"start_delay": 0.4}
                )
                self.assertTrue(manager.load_plugin(desc))

                start_result = []
                t = threading.Thread(
                    target=lambda: start_result.append(manager.start_plugin("slow"))
                )
                t.start()
                wait_until(lambda: manager.env_get("slow.start_entered") is True)

                self.assertTrue(manager.stop_plugin("slow"))
                t.join(timeout=5.0)
                self.assertEqual(start_result, [False])

                order = manager.env_get("slow.order")
                self.assertEqual(order, ["on_start:enter", "on_start:exit", "on_stop"])

    def test_init_crash_and_init_timeout_cleanup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir, timeout_initialize=10.0) as manager:
                crash_desc = CrashInitPlugin.Descriptor("1.0.0", "tests", name="crash")
                with self.assertLogs(manager.logger.name, level="ERROR") as logs:
                    self.assertFalse(manager.load_plugin(crash_desc))
                self.assertIn("CrashInitPlugin intentional init failure", "\n".join(logs.output))
                self.assertNotIn("crash", manager.plugin_list())

            with make_manager(tmpdir, timeout_initialize=0.6) as manager:
                hang_desc = HangInitPlugin.Descriptor("1.0.0", "tests", name="hang")
                self.assertFalse(manager.load_plugin(hang_desc))
                self.assertNotIn("hang", manager.plugin_list())

    def test_mixed_parallel_load_preserves_healthy_workers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                descs = [
                    EchoPlugin.Descriptor("1.0.0", "tests", name="ok-1"),
                    CrashInitPlugin.Descriptor("1.0.0", "tests", name="bad"),
                    EchoPlugin.Descriptor("1.0.0", "tests", name="ok-2"),
                ]
                results = manager.load_plugins(descs, parallel=True)
                self.assertEqual(results, {"ok-1": True, "bad": False, "ok-2": True})
                self.assertEqual(sorted(manager.plugin_list()), ["ok-1", "ok-2"])

    def test_descriptor_empty_docstring_redefinition_and_unknown_ids(self):
        self.assertEqual(_NoDocstringProbe.Descriptor("1.0.0", "tests").description, "")

        class _RedefProbe(Plugin):
            pass

        class _RedefProbe(Plugin):
            pass

        self.assertIs(Plugin._registry["_RedefProbe"], _RedefProbe)

        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                self.assertFalse(manager.stop_plugin("unknown"))
                self.assertTrue(manager.unload_plugin("unknown"))
                self.assertIsNone(manager.plugin_status("unknown"))

    def test_self_stopping_plugin_shuts_down_without_manager_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = make_manager(tmpdir)
            try:
                desc = SelfStoppingPlugin.Descriptor("1.0.0", "tests", name="self-stop")
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("self-stop"))
                wait_until(lambda: manager.env_get("self-stop.stopped") is True)

                with self.assertNoLogs(manager.logger.name, level="ERROR"):
                    self.assertTrue(manager.stop_plugin("self-stop"))
                    manager.stop_all_plugins()
                    manager.close()
            finally:
                manager.close()

    def test_self_stop_racing_with_manager_stop_waits_for_on_stop_cleanly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = make_manager(tmpdir)
            try:
                desc = SelfStoppingPlugin.Descriptor(
                    "1.0.0", "tests", name="self-stop-race", options={"stop_delay": 0.35}
                )
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("self-stop-race"))
                wait_until(lambda: manager.env_get("self-stop-race.stopping") is True)

                with self.assertNoLogs(manager.logger.name, level="ERROR"):
                    self.assertTrue(manager.stop_plugin("self-stop-race"))
                    self.assertTrue(manager.env_get("self-stop-race.stopped"))
                    manager.close()
            finally:
                manager.close()

    def test_repeated_stop_and_close_logs_no_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = make_manager(tmpdir)
            try:
                desc = BlockingMainPlugin.Descriptor("1.0.0", "tests", name="blocker-clean")
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("blocker-clean"))
                wait_until(lambda: manager.env_get("blocker-clean.in_main") is True)

                with self.assertNoLogs(manager.logger.name, level="ERROR"):
                    self.assertTrue(manager.stop_plugin("blocker-clean"))
                    self.assertTrue(manager.stop_plugin("blocker-clean"))
                    manager.close()
            finally:
                manager.close()


if __name__ == "__main__":
    unittest.main()
