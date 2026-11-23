from collections import OrderedDict, defaultdict, namedtuple
from dataclasses import dataclass
import threading
import time
import unittest

import torch
import torch.nn as nn

from parallelazy import Plugin, PluginManager
from parallelazy.plugin_transport import (
    IpcEventPoolManaged,
    mark_graph_output,
    sanitize_for_ipc,
)


_Point = namedtuple("_Point", ["x", "y"])


@dataclass
class _SampleBox:
    label: str
    tensor: torch.Tensor


class _CudaEventConsumer(Plugin):
    def on_start(self):
        self.received_sums = []
        self._lock = threading.Lock()
        self.topic_subscribe("cuda-events", self._on_frame)

    def _on_frame(self, payload):
        tensor, event = payload
        event.wait()
        val = float(tensor.sum().item())
        with self._lock:
            self.received_sums.append(val)
        event.clear()

    def main(self):
        self.wait_for_stop()

    def get_sums(self):
        with self._lock:
            return list(self.received_sums)


class _CudaEventProducer(Plugin):
    def on_start(self):
        self.pool = IpcEventPoolManaged(size=2, stale_after_s=5.0)

    def main(self):
        self.wait_for_stop()

    def produce_and_verify(self, steps=4):
        busy_seen = False
        for i in range(steps):
            event = self.pool.acquire("stream")
            t = torch.full((4,), float(i + 1), device="cuda")
            event.record()
            # Before consumer acks, the slot must report busy
            if self.pool.has_busy():
                busy_seen = True
            self.topic_publish("cuda-events", (t, event))
            if not self.pool.wait_all_free(timeout=5.0):
                return {"ok": False, "busy_seen": busy_seen, "step": i}
        return {"ok": True, "busy_seen": busy_seen, "steps": steps}


class TransportSanitizationTests(unittest.TestCase):
    def test_sanitize_preserves_container_types(self):
        t = mark_graph_output(torch.tensor([1.0, 2.0]))

        pt = _Point(t, "ok")
        sanitized_pt = sanitize_for_ipc(pt)
        self.assertIsInstance(sanitized_pt, _Point)
        self.assertIsNot(sanitized_pt.x, t)
        self.assertTrue(torch.equal(sanitized_pt.x, t))
        self.assertEqual(sanitized_pt.y, "ok")

        od = OrderedDict([("first", t), ("second", 2)])
        sanitized_od = sanitize_for_ipc(od)
        self.assertIsInstance(sanitized_od, OrderedDict)
        self.assertEqual(list(sanitized_od.keys()), ["first", "second"])
        self.assertIsNot(sanitized_od["first"], t)

        dd = defaultdict(list, {"a": [t]})
        sanitized_dd = sanitize_for_ipc(dd)
        self.assertIsInstance(sanitized_dd, defaultdict)
        self.assertIs(sanitized_dd.default_factory, list)
        self.assertIsNot(sanitized_dd["a"][0], t)

        s = {1, 2, 3}
        self.assertIsInstance(sanitize_for_ipc(s), set)
        fs = frozenset({4, 5})
        self.assertIsInstance(sanitize_for_ipc(fs), frozenset)

        box = _SampleBox(label="b", tensor=t)
        sanitized_box = sanitize_for_ipc(box)
        self.assertIsInstance(sanitized_box, _SampleBox)
        self.assertIsNot(sanitized_box, box)
        self.assertIsNot(sanitized_box.tensor, t)

        # Unchanged dataclass returns same instance
        plain_box = _SampleBox(label="plain", tensor=torch.tensor([3.0]))
        self.assertIs(sanitize_for_ipc(plain_box), plain_box)

    def test_sanitize_handles_cyclic_structures(self):
        cyclic_list = []
        cyclic_list.append(cyclic_list)
        out_list = sanitize_for_ipc(cyclic_list)
        self.assertIsInstance(out_list, list)
        self.assertIs(out_list[0], out_list)

        cyclic_dict = {}
        cyclic_dict["self"] = cyclic_dict
        out_dict = sanitize_for_ipc(cyclic_dict)
        self.assertIsInstance(out_dict, dict)
        self.assertIs(out_dict["self"], out_dict)

    def test_sanitize_module_strips_unpicklable_hooks_when_cloning(self):
        mod = nn.Linear(2, 2)
        mark_graph_output(mod.weight)
        lock = threading.Lock()
        mod.register_forward_hook(lambda m, inp, out: lock)

        cloned = sanitize_for_ipc(mod)
        self.assertIsNot(cloned, mod)
        self.assertEqual(len(cloned._forward_hooks), 0)
        # Original module hooks are restored
        self.assertEqual(len(mod._forward_hooks), 1)

    @unittest.skipUnless(torch.cuda.is_available(), "CUDA is not available")
    def test_cuda_ipc_event_pool_requires_real_consumer_ack(self):
        pool = IpcEventPoolManaged(size=2, stale_after_s=10.0)
        self.assertFalse(pool.has_busy())

        ev1 = pool.acquire("local")
        ev1.record()
        # Even after GPU work completes, slot must remain busy until ev1.clear()
        ev1.ready.synchronize()
        self.assertFalse(ev1.is_cleared())
        self.assertTrue(pool.has_busy())
        self.assertFalse(pool.wait_all_free(timeout=0.05))

        ev1.clear()
        self.assertTrue(pool.wait_all_free(timeout=2.0))
        self.assertTrue(ev1.is_cleared())
        self.assertFalse(pool.has_busy())

    @unittest.skipUnless(torch.cuda.is_available(), "CUDA is not available")
    def test_cuda_ipc_event_pool_two_process_roundtrip(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            with PluginManager(work_dir=tmpdir, display_logs=False, write_logs=False) as manager:
                self.assertTrue(
                    manager.load_plugin(_CudaEventConsumer.Descriptor("0.1", "test", name="consumer"))
                )
                self.assertTrue(
                    manager.load_plugin(_CudaEventProducer.Descriptor("0.1", "test", name="producer"))
                )
                self.assertTrue(manager.start_plugin("consumer"))
                self.assertTrue(manager.start_plugin("producer"))

                res = manager.plugin_call("producer", "produce_and_verify", 4, timeout=15.0)
                self.assertTrue(res["ok"], f"Producer failed: {res}")
                self.assertTrue(res["busy_seen"])

                sums = manager.plugin_call("consumer", "get_sums", timeout=5.0)
                self.assertEqual(sums, [4.0, 8.0, 12.0, 16.0])


if __name__ == "__main__":
    unittest.main()
