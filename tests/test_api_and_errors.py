import tempfile
import threading
import time
import unittest

from parallelazy import Plugin, PluginManager, PluginState


class _ErrorTrackingPlugin(Plugin):
    def on_start(self):
        self.recorded_errors = []
        self._err_lock = threading.Lock()
        if self.options.get("fail_start"):
            raise RuntimeError("start boom")
        if self.options.get("subscribe_topic"):
            self.topic_subscribe("bad-topic", self._bad_topic_cb)
        if self.options.get("subscribe_env"):
            self.env_subscribe("bad_env", self._bad_env_cb)

    def main(self):
        if self.options.get("fail_main"):
            raise RuntimeError("main boom")
        self.wait_for_stop()

    def on_error(self, error: Exception, source: str = None):
        with self._err_lock:
            self.recorded_errors.append((source, str(error)))

    def _bad_topic_cb(self, data):
        raise ValueError(f"topic boom: {data}")

    def _bad_env_cb(self, val):
        if val == "trigger":
            raise ValueError("env boom")

    def bad_rpc(self):
        raise KeyError("rpc boom")

    def get_recorded_errors(self):
        with self._err_lock:
            return list(self.recorded_errors)

    def check_wrappers(self):
        ok_sub = self.topic_subscribe("wrapper-topic", lambda x: None)
        topics = self.topic_list()
        pub_count = self.topic_publish("wrapper-topic", "hello")
        ok_unsub = self.topic_unsubscribe("wrapper-topic")
        ok_env_set = self.env_set("w_key", 42)
        ok_env_sub = self.env_subscribe("w_key", lambda v: None)
        ok_env_unsub = self.env_unsubscribe("w_key")
        errs = self.plugin_errors()
        return {
            "ok_sub": ok_sub,
            "topics": topics,
            "pub_count": pub_count,
            "ok_unsub": ok_unsub,
            "ok_env_set": ok_env_set,
            "ok_env_sub": ok_env_sub,
            "ok_env_unsub": ok_env_unsub,
            "errs": errs,
        }


class _FailingInitPlugin(Plugin):
    def __init__(self, worker, plugin_id, options=None):
        super().__init__(worker, plugin_id, options)
        raise RuntimeError("init exploded with secret details")


class ApiAndErrorsTests(unittest.TestCase):
    def test_manager_plugin_call_and_plugin_wrappers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PluginManager(work_dir=tmpdir, display_logs=False, write_logs=False) as manager:
                self.assertTrue(
                    manager.load_plugin(_ErrorTrackingPlugin.Descriptor("0.1", "test", name="p1"))
                )
                self.assertTrue(manager.start_plugin("p1"))

                result = manager.plugin_call("p1", "check_wrappers")
                self.assertTrue(result["ok_sub"])
                self.assertIn("wrapper-topic", result["topics"])
                self.assertEqual(result["pub_count"], 1)
                self.assertTrue(result["ok_unsub"])
                self.assertTrue(result["ok_env_set"])
                self.assertTrue(result["ok_env_sub"])
                self.assertTrue(result["ok_env_unsub"])
                self.assertEqual(result["errs"], [])

    def test_on_error_and_error_state_for_lifecycle_callbacks_and_rpc(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PluginManager(work_dir=tmpdir, display_logs=False, write_logs=False) as manager:
                # 1. on_start failure sets ERROR state
                self.assertTrue(
                    manager.load_plugin(
                        _ErrorTrackingPlugin.Descriptor(
                            "0.1", "test", name="fail-start", options={"fail_start": True}
                        )
                    )
                )
                self.assertFalse(manager.start_plugin("fail-start"))
                status = manager.plugin_status("fail-start")
                self.assertEqual(status.worker_status.state, str(PluginState.ERROR))
                self.assertIn("start boom", manager.plugin_errors("fail-start"))

                # 2. main failure sets ERROR state and invokes on_error(source="main")
                self.assertTrue(
                    manager.load_plugin(
                        _ErrorTrackingPlugin.Descriptor(
                            "0.1", "test", name="fail-main", options={"fail_main": True}
                        )
                    )
                )
                self.assertTrue(manager.start_plugin("fail-main"))
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline:
                    st = manager.plugin_status("fail-main")
                    if st.worker_status and st.worker_status.state == str(PluginState.ERROR):
                        break
                    time.sleep(0.05)
                self.assertEqual(st.worker_status.state, str(PluginState.ERROR))
                self.assertIn("main boom", manager.plugin_errors("fail-main"))

                # 3. topic, env, and RPC callback errors invoke on_error with expected sources
                self.assertTrue(
                    manager.load_plugin(
                        _ErrorTrackingPlugin.Descriptor(
                            "0.1",
                            "test",
                            name="cb-errors",
                            options={"subscribe_topic": True, "subscribe_env": True},
                        )
                    )
                )
                self.assertTrue(manager.start_plugin("cb-errors"))

                manager.topic_publish("bad-topic", "payload")
                manager.env_set("bad_env", "trigger")
                with self.assertRaises(RuntimeError):
                    manager.plugin_call("cb-errors", "bad_rpc")

                deadline = time.monotonic() + 3.0
                recorded = []
                while time.monotonic() < deadline:
                    recorded = manager.plugin_call("cb-errors", "get_recorded_errors")
                    if len(recorded) >= 3:
                        break
                    time.sleep(0.05)

                sources = {src for src, _ in recorded}
                self.assertIn("topic:bad-topic", sources)
                self.assertIn("env:bad_env", sources)
                self.assertIn("rpc:bad_rpc", sources)
                self.assertGreaterEqual(len(manager.plugin_errors("cb-errors")), 3)

    def test_worker_init_exception_is_logged_by_manager(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PluginManager(work_dir=tmpdir, display_logs=False, write_logs=False) as manager:
                with self.assertLogs(manager.logger.name, level="ERROR") as logs:
                    loaded = manager.load_plugin(
                        _FailingInitPlugin.Descriptor("0.1", "test", name="bad-init")
                    )
                self.assertFalse(loaded)
                combined = "\n".join(logs.output)
                self.assertIn("init exploded with secret details", combined)


if __name__ == "__main__":
    unittest.main()
