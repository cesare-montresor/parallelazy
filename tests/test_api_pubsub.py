import tempfile
import threading
import unittest
from typing import Any

import torch

from parallelazy import Plugin, PluginRPC
from tests.helpers import make_manager, wait_until


class PubPlugin(Plugin):
    def publish(self, topic: str, data: Any) -> int:
        return self.topic_publish(topic, data)

    def publish_burst(self, topic: str, count: int) -> int:
        total = 0
        for i in range(count):
            total += self.topic_publish(topic, i)
        return total

    def list_topics(self, plugin_id: str = None) -> list[str]:
        return self.topic_list(plugin_id=plugin_id)

    def main(self) -> None:
        self.wait_for_stop()


class SubPlugin(Plugin):
    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.received: list[Any] = []
        self.error_sources: list[str] = []
        for topic in self.options.get("topics", []):
            assert self.topic_subscribe(topic, self._on_msg)

    def _on_msg(self, data: Any) -> None:
        if data == "__raise__":
            raise ValueError("intentional subscriber callback failure")
        with self._lock:
            self.received.append(data)

    def on_error(self, error: Exception, source: str = None) -> None:
        with self._lock:
            self.error_sources.append(source or "")

    def subscribe(self, topic: str) -> bool:
        return self.topic_subscribe(topic, self._on_msg)

    def unsubscribe(self, topic: str) -> bool:
        return self.topic_unsubscribe(topic)

    def get_received(self) -> list[Any]:
        with self._lock:
            return list(self.received)

    def get_error_sources(self) -> list[str]:
        with self._lock:
            return list(self.error_sources)

    def list_topics(self, plugin_id: str = None) -> list[str]:
        return self.topic_list(plugin_id=plugin_id)

    def main(self) -> None:
        self.wait_for_stop()


class ApiPubSubTests(unittest.TestCase):
    def test_fanout_late_discovery_unsubscribe_and_topic_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                # Late publisher discovery: sub1 subscribes before pub announces
                s1 = SubPlugin.Descriptor("1.0.0", "tests", name="s1", options={"topics": ["stream"]})
                s2 = SubPlugin.Descriptor("1.0.0", "tests", name="s2")
                p1 = PubPlugin.Descriptor("1.0.0", "tests", name="p1")
                self.assertEqual(manager.load_plugins([s1, s2, p1]), {"s1": True, "s2": True, "p1": True})
                self.assertTrue(manager.start_plugin("s1"))
                self.assertTrue(manager.start_plugin("s2"))
                self.assertTrue(manager.start_plugin("p1"))

                self.assertEqual(manager.plugin_call("p1", "publish", "stream", "m1"), 1)
                wait_until(lambda: manager.plugin_call("s1", "get_received") == ["m1"])

                # Late subscriber discovery: s2 subscribes after p1 already announced
                self.assertTrue(manager.plugin_call("s2", "subscribe", "stream"))
                self.assertEqual(manager.plugin_call("p1", "publish", "stream", "m2"), 2)
                wait_until(lambda: manager.plugin_call("s1", "get_received") == ["m1", "m2"])
                wait_until(lambda: manager.plugin_call("s2", "get_received") == ["m2"])

                # topic_list consistency across manager and plugins
                self.assertEqual(manager.topic_list(), ["stream"])
                self.assertEqual(manager.topic_list(plugin_id="p1"), ["stream"])
                self.assertEqual(manager.topic_list(plugin_id="s1"), [])
                self.assertEqual(manager.plugin_call("s1", "list_topics"), ["stream"])
                self.assertEqual(manager.plugin_call("p1", "list_topics", "p1"), ["stream"])

                # Host-side manager.topic_publish with CPU tensor
                t = torch.tensor([1, 2, 3])
                self.assertEqual(manager.topic_publish("stream", t), 2)
                wait_until(lambda: len(manager.plugin_call("s2", "get_received")) == 2)
                self.assertTrue(torch.equal(manager.plugin_call("s2", "get_received")[-1], t))

                # Unsubscribe s1 -> only s2 receives
                self.assertTrue(manager.plugin_call("s1", "unsubscribe", "stream"))
                self.assertEqual(manager.plugin_call("p1", "publish", "stream", "m3"), 1)
                wait_until(lambda: manager.plugin_call("s2", "get_received")[-1] == "m3")
                self.assertEqual(len(manager.plugin_call("s1", "get_received")), 3)

    def test_none_payload_callback_exception_unannounced_unsub_and_high_rate_burst(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                s1 = SubPlugin.Descriptor(
                    "1.0.0", "tests", name="s1", options={"topics": ["t", "ghost"]}
                )
                p1 = PubPlugin.Descriptor("1.0.0", "tests", name="p1")
                self.assertEqual(manager.load_plugins([s1, p1]), {"s1": True, "p1": True})
                self.assertTrue(manager.start_plugin("s1"))
                self.assertTrue(manager.start_plugin("p1"))

                # Unsubscribe from unannounced topic removes manager registration
                self.assertIn("ghost", manager.topic_list())
                self.assertTrue(manager.plugin_call("s1", "unsubscribe", "ghost"))
                self.assertNotIn("ghost", manager.topic_list())

                # Publishing None delivers None and keeps listener alive
                self.assertEqual(manager.plugin_call("p1", "publish", "t", None), 1)
                # Callback exception triggers on_error and keeps listener alive
                self.assertEqual(manager.plugin_call("p1", "publish", "t", "__raise__"), 1)
                self.assertEqual(manager.plugin_call("p1", "publish", "t", "after"), 1)

                wait_until(lambda: manager.plugin_call("s1", "get_received") == [None, "after"])
                self.assertIn("topic:t", manager.plugin_call("s1", "get_error_sources"))
                self.assertTrue(manager.plugin_errors("s1"))

                # High-rate burst (500 messages) exercises persistent selector + poll(0) fast path
                delivered = manager.plugin_call("p1", "publish_burst", "t", 500, timeout=10.0)
                self.assertEqual(delivered, 500)
                wait_until(
                    lambda: len(manager.plugin_call("s1", "get_received")) == 502,
                    timeout=10.0,
                    message="Burst messages did not all arrive",
                )
                self.assertEqual(manager.plugin_call("s1", "get_received")[-1], 499)

    def test_unloading_subscriber_or_publisher_recovers_cleanly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                s1 = SubPlugin.Descriptor("1.0.0", "tests", name="s1", options={"topics": ["ch"]})
                s2 = SubPlugin.Descriptor("1.0.0", "tests", name="s2", options={"topics": ["ch"]})
                p1 = PubPlugin.Descriptor("1.0.0", "tests", name="p1")
                self.assertEqual(manager.load_plugins([s1, s2, p1]), {"s1": True, "s2": True, "p1": True})
                self.assertEqual(manager.start_all_plugins(), {"s1": True, "s2": True, "p1": True})

                self.assertEqual(manager.plugin_call("p1", "publish", "ch", "v1"), 2)
                wait_until(lambda: manager.plugin_call("s2", "get_received") == ["v1"])

                # Unload s1 -> p1 evicts dead subscriber and continues delivering to s2
                self.assertTrue(manager.unload_plugin("s1"))
                self.assertEqual(manager.plugin_call("p1", "publish", "ch", "v2"), 1)
                wait_until(lambda: manager.plugin_call("s2", "get_received") == ["v1", "v2"])

                # Unload p1 and load replacement publisher p2 -> s2 still receives
                self.assertTrue(manager.unload_plugin("p1"))
                p2 = PubPlugin.Descriptor("1.0.0", "tests", name="p2")
                self.assertTrue(manager.load_plugin(p2))
                self.assertTrue(manager.start_plugin("p2"))
                self.assertEqual(manager.plugin_call("p2", "publish", "ch", "v3"), 1)
                wait_until(lambda: manager.plugin_call("s2", "get_received") == ["v1", "v2", "v3"])

    def test_single_publisher_ownership_enforced_and_rpc_shutdown_closes_wakeup_pipe(self):
        rpc = PluginRPC("fd-check", start_listener=False)
        rpc.shutdown()
        self.assertTrue(rpc._shutdown.closed())

        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                s1 = SubPlugin.Descriptor("1.0.0", "tests", name="s1", options={"topics": ["solo"]})
                p1 = PubPlugin.Descriptor("1.0.0", "tests", name="p1")
                p2 = PubPlugin.Descriptor("1.0.0", "tests", name="p2")
                self.assertEqual(manager.load_plugins([s1, p1, p2]), {"s1": True, "p1": True, "p2": True})
                self.assertEqual(manager.start_all_plugins(), {"s1": True, "p1": True, "p2": True})

                self.assertEqual(manager.plugin_call("p1", "publish", "solo", "from-p1"), 1)
                wait_until(lambda: manager.plugin_call("s1", "get_received") == ["from-p1"])

                # Second publisher cannot hijack an active single-publisher topic
                self.assertEqual(manager.plugin_call("p2", "publish", "solo", "from-p2"), 0)
                self.assertTrue(manager.plugin_errors("p2"))

                # Original publisher still delivers normally
                self.assertEqual(manager.plugin_call("p1", "publish", "solo", "still-p1"), 1)
                wait_until(lambda: manager.plugin_call("s1", "get_received") == ["from-p1", "still-p1"])


if __name__ == "__main__":
    unittest.main()
