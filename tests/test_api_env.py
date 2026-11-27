import tempfile
import threading
import unittest
from typing import Any

from parallelazy import EnvironmentManager, ManagerOptions, Plugin
from tests.helpers import make_manager, wait_until


class EnvErrorOnInitialPlugin(Plugin):
    """Raises on the initial env value but should stay subscribed for subsequent updates."""

    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.received: list[Any] = []
        self.error_sources: list[str] = []
        assert self.env_subscribe("cfg", self._on_cfg)

    def _on_cfg(self, value: Any) -> None:
        if value == "bad_initial":
            raise ValueError("initial callback failure")
        with self._lock:
            self.received.append(value)

    def on_error(self, error: Exception, source: str = None) -> None:
        with self._lock:
            self.error_sources.append(source or "")

    def get_received(self) -> list[Any]:
        with self._lock:
            return list(self.received)

    def get_error_sources(self) -> list[str]:
        with self._lock:
            return list(self.error_sources)

    def main(self) -> None:
        self.wait_for_stop()


class EnvProbePlugin(Plugin):
    """Tracks FIFO env updates and exposes env operations via RPC."""

    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.received: list[Any] = []
        for key in self.options.get("subscribe_keys", []):
            assert self.env_subscribe(key, self._on_update)

    def _on_update(self, value: Any) -> None:
        with self._lock:
            self.received.append(value)

    def get_received(self) -> list[Any]:
        with self._lock:
            return list(self.received)

    def unsubscribe_key(self, key: str) -> bool:
        return self.env_unsubscribe(key)

    def set_key(self, key: str, value: Any, wait: bool = True) -> bool:
        return self.env_set(key, value, wait=wait)

    def get_key(self, key: str, default: Any = None) -> Any:
        return self.env_get(key, default=default)

    def get_all_keys(self) -> dict:
        return self.env_get()

    def main(self) -> None:
        self.wait_for_stop()


class ApiEnvTests(unittest.TestCase):
    def test_env_set_get_and_fifo_subscription_ordering(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                manager.env_set("seq", 0)
                desc = EnvProbePlugin.Descriptor(
                    "1.0.0", "tests", name="env-sub", options={"subscribe_keys": ["seq"]}
                )
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("env-sub"))

                # Rapid sequential updates must arrive in strict FIFO order
                for i in range(1, 15):
                    manager.env_set("seq", i)

                expected = list(range(15))
                wait_until(
                    lambda: manager.plugin_call("env-sub", "get_received") == expected,
                    message="FIFO env updates did not match",
                )

                # Plugin-side env_set (sync & async) and env_get (single & all)
                self.assertTrue(manager.plugin_call("env-sub", "set_key", "from_plugin", "yes", True))
                self.assertEqual(manager.env_get("from_plugin"), "yes")
                self.assertEqual(manager.plugin_call("env-sub", "get_key", "from_plugin"), "yes")
                self.assertIn("from_plugin", manager.plugin_call("env-sub", "get_all_keys"))

                # Unsubscribe stops further notifications
                self.assertTrue(manager.plugin_call("env-sub", "unsubscribe_key", "seq"))
                manager.env_set("seq", 999)
                # Round-trip another key to flush dispatcher
                manager.env_set("flush", 1)
                self.assertEqual(manager.plugin_call("env-sub", "get_received"), expected)

    def test_stored_none_vs_unset_key_and_clear_semantics(self):
        env = EnvironmentManager()
        env.begin_monitoring()
        env.set("a", 1)
        env.set("b", 2)
        env.clear("missing")
        self.assertEqual(env.get("a"), 1)
        self.assertEqual(env.get("b"), 2)
        env.clear("a")
        self.assertNotIn("a", env)
        self.assertNotIn("a", env.monitoring_snapshot())
        env.clear()
        self.assertEqual(env.get_all(), {})
        self.assertEqual(env.monitoring_snapshot(), {})

        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                desc = EnvProbePlugin.Descriptor("1.0.0", "tests", name="none-probe")
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("none-probe"))

                manager.env_set("explicit_none", None)
                self.assertIsNone(manager.env_get("explicit_none", default="fallback"))
                self.assertEqual(manager.env_get("never_set", default="fallback"), "fallback")
                self.assertIsNone(
                    manager.plugin_call("none-probe", "get_key", "explicit_none", "fallback")
                )
                self.assertEqual(
                    manager.plugin_call("none-probe", "get_key", "never_set", "fallback"),
                    "fallback",
                )

    def test_unloading_one_subscriber_keeps_other_receiving_and_unset_subscribe_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                d1 = EnvProbePlugin.Descriptor(
                    "1.0.0", "tests", name="s1", options={"subscribe_keys": ["k"]}
                )
                d2 = EnvProbePlugin.Descriptor(
                    "1.0.0", "tests", name="s2", options={"subscribe_keys": ["k"]}
                )
                self.assertEqual(manager.load_plugins([d1, d2]), {"s1": True, "s2": True})
                self.assertEqual(manager.start_all_plugins(), {"s1": True, "s2": True})

                # Unset initial subscription delivers None
                wait_until(lambda: manager.plugin_call("s1", "get_received") == [None])
                wait_until(lambda: manager.plugin_call("s2", "get_received") == [None])

                manager.env_set("k", "v1")
                wait_until(lambda: manager.plugin_call("s1", "get_received") == [None, "v1"])
                wait_until(lambda: manager.plugin_call("s2", "get_received") == [None, "v1"])

                self.assertTrue(manager.unload_plugin("s1"))
                manager.env_set("k", "v2")
                wait_until(lambda: manager.plugin_call("s2", "get_received") == [None, "v1", "v2"])

    def test_env_subscribe_initial_callback_error_notifies_and_keeps_subscription(self):
        opts = ManagerOptions(write_logs=False, display_logs=False)
        self.assertEqual(opts.to_manager_kwargs()["write_logs"], False)

        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                manager.env_set("cfg", "bad_initial")
                desc = EnvErrorOnInitialPlugin.Descriptor("1.0.0", "tests", name="err-sub")
                self.assertTrue(manager.load_plugin(desc))
                self.assertTrue(manager.start_plugin("err-sub"))

                self.assertIn("env:cfg", manager.plugin_call("err-sub", "get_error_sources"))
                self.assertTrue(manager.plugin_errors("err-sub"))

                manager.env_set("cfg", "good_next")
                wait_until(
                    lambda: manager.plugin_call("err-sub", "get_received") == ["good_next"],
                    message="Subscription did not remain active after initial callback error",
                )


if __name__ == "__main__":
    unittest.main()
