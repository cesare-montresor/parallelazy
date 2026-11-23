import os
import signal
import tempfile
import threading
import time
import unittest

from parallelazy import Plugin, PluginManager


class _PubPlugin(Plugin):
    def on_start(self):
        pass

    def main(self):
        self.wait_for_stop()

    def publish_msg(self, topic, data):
        return self._worker.topic_publish(topic, data)

    def get_topics(self, plugin_id=None):
        return self._worker.topic_list(plugin_id=plugin_id)


class _SubPlugin(Plugin):
    def on_start(self):
        self.received = []
        self._lock = threading.Lock()
        for topic in self.options.get("topics", []):
            self.topic_subscribe(topic, self._on_data)

    def _on_data(self, data):
        with self._lock:
            self.received.append(data)

    def main(self):
        self.wait_for_stop()

    def get_received(self):
        with self._lock:
            return list(self.received)

    def unsubscribe(self, topic):
        return self._worker.topic_unsubscribe(topic)

    def get_topics(self, plugin_id=None):
        return self._worker.topic_list(plugin_id=plugin_id)


class PubSubResilienceTests(unittest.TestCase):
    def test_sigkill_subscriber_does_not_block_publisher_on_large_messages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = PluginManager(
                work_dir=tmpdir,
                display_logs=False,
                write_logs=False,
            )
            try:
                self.assertTrue(
                    manager.load_plugin(_PubPlugin.Descriptor("0.1", "test", name="pub"))
                )
                self.assertTrue(
                    manager.load_plugin(
                        _SubPlugin.Descriptor("0.1", "test", name="sub-dead", options={"topics": ["big"]})
                    )
                )
                self.assertTrue(
                    manager.load_plugin(
                        _SubPlugin.Descriptor("0.1", "test", name="sub-alive", options={"topics": ["big"]})
                    )
                )
                self.assertTrue(manager.start_plugin("sub-dead"))
                self.assertTrue(manager.start_plugin("sub-alive"))
                self.assertTrue(manager.start_plugin("pub"))

                pub_rpc = manager._plugins["pub"].rpc
                alive_rpc = manager._plugins["sub-alive"].rpc

                # Establish topic routing first
                r0 = pub_rpc.plugin_call("pub", "publish_msg", "big", b"init").wait(timeout=5.0)
                self.assertIsNotNone(r0)
                self.assertEqual(r0.result, 2)

                # SIGKILL sub-dead
                dead_pid = manager._plugins["sub-dead"].process.pid
                os.kill(dead_pid, signal.SIGKILL)
                manager._plugins["sub-dead"].process.join(timeout=3.0)

                # Publish several >256 KB messages; publisher must not block and must evict dead sub
                payload = b"x" * (300 * 1024)
                counts = []
                for _ in range(3):
                    res = pub_rpc.plugin_call("pub", "publish_msg", "big", payload).wait(timeout=5.0)
                    self.assertIsNotNone(res)
                    self.assertIsNone(res.error)
                    counts.append(res.result)

                # Healthy subscriber received all 3 large messages plus init
                self.assertEqual(counts, [1, 1, 1])
                deadline = time.monotonic() + 3.0
                received = []
                while time.monotonic() < deadline:
                    r = alive_rpc.plugin_call("sub-alive", "get_received").wait(timeout=2.0)
                    received = r.result
                    if len(received) == 4:
                        break
                    time.sleep(0.05)
                self.assertEqual(len(received), 4)
                self.assertEqual(len(received[-1]), 300 * 1024)
            finally:
                manager.unload_all_plugins()

    def test_publishing_none_delivers_none_and_keeps_listener_alive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = PluginManager(
                work_dir=tmpdir,
                display_logs=False,
                write_logs=False,
            )
            try:
                self.assertTrue(
                    manager.load_plugin(
                        _SubPlugin.Descriptor("0.1", "test", name="sub", options={"topics": ["nullable"]})
                    )
                )
                self.assertTrue(manager.start_plugin("sub"))

                self.assertEqual(manager.topic_publish("nullable", None), 1)
                self.assertEqual(manager.topic_publish("nullable", "after-none"), 1)

                sub_rpc = manager._plugins["sub"].rpc
                deadline = time.monotonic() + 3.0
                received = []
                while time.monotonic() < deadline:
                    r = sub_rpc.plugin_call("sub", "get_received").wait(timeout=2.0)
                    received = r.result
                    if len(received) == 2:
                        break
                    time.sleep(0.05)
                self.assertEqual(received, [None, "after-none"])
            finally:
                manager.unload_all_plugins()

    def test_unsubscribe_from_unannounced_topic_removes_manager_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = PluginManager(
                work_dir=tmpdir,
                display_logs=False,
                write_logs=False,
            )
            try:
                self.assertTrue(
                    manager.load_plugin(
                        _SubPlugin.Descriptor("0.1", "test", name="sub", options={"topics": ["ghost"]})
                    )
                )
                self.assertTrue(manager.start_plugin("sub"))
                self.assertIn("ghost", manager.topic_list())

                sub_rpc = manager._plugins["sub"].rpc
                res = sub_rpc.plugin_call("sub", "unsubscribe", "ghost").wait(timeout=3.0)
                self.assertIsNotNone(res)
                self.assertTrue(res.result)
                self.assertNotIn("ghost", manager.topic_list())
                self.assertNotIn("ghost", manager._topic_subscribers)
            finally:
                manager.unload_all_plugins()

    def test_topic_list_consistent_between_manager_and_plugins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = PluginManager(
                work_dir=tmpdir,
                display_logs=False,
                write_logs=False,
            )
            try:
                self.assertTrue(
                    manager.load_plugin(_PubPlugin.Descriptor("0.1", "test", name="pub"))
                )
                self.assertTrue(
                    manager.load_plugin(
                        _SubPlugin.Descriptor("0.1", "test", name="sub", options={"topics": ["beta"]})
                    )
                )
                self.assertTrue(manager.start_plugin("pub"))
                self.assertTrue(manager.start_plugin("sub"))

                pub_rpc = manager._plugins["pub"].rpc
                sub_rpc = manager._plugins["sub"].rpc

                # pub announces "alpha" (no subscribers yet); sub subscribes to "beta" (no publisher yet)
                pub_rpc.plugin_call("pub", "publish_msg", "alpha", 123).wait(timeout=3.0)

                self.assertEqual(manager.topic_list(), ["alpha", "beta"])
                self.assertEqual(manager.topic_list(plugin_id="pub"), ["alpha"])
                self.assertEqual(manager.topic_list(plugin_id="sub"), [])

                pub_all = pub_rpc.plugin_call("pub", "get_topics").wait(timeout=3.0).result
                sub_all = sub_rpc.plugin_call("sub", "get_topics").wait(timeout=3.0).result
                pub_own = pub_rpc.plugin_call("pub", "get_topics", "pub").wait(timeout=3.0).result

                self.assertEqual(pub_all, ["alpha", "beta"])
                self.assertEqual(sub_all, ["alpha", "beta"])
                self.assertEqual(pub_own, ["alpha"])
            finally:
                manager.unload_all_plugins()


if __name__ == "__main__":
    unittest.main()
