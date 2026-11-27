import tempfile
import threading
import time
import unittest
from collections import OrderedDict, defaultdict, namedtuple
from dataclasses import dataclass
from typing import Any

import numpy as np
import torch
import torch.nn as nn

from parallelazy import IpcEventPoolManaged, Plugin, mark_graph_output, sanitize_for_ipc
from parallelazy.plugin_transport import sanitize_for_ipc_with_info
from tests.helpers import make_manager, wait_until

_Point = namedtuple("_Point", ["x", "y"])


@dataclass
class _TensorBox:
    label: str
    tensor: torch.Tensor


class CpuTensorRelayPlugin(Plugin):
    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.received: list[Any] = []
        if self.options.get("subscribe_topic"):
            assert self.topic_subscribe(self.options["subscribe_topic"], self._on_msg)

    def _on_msg(self, data: Any) -> None:
        with self._lock:
            self.received.append(data)

    def echo_tensor(self, t: torch.Tensor, arr: np.ndarray) -> dict:
        return {"t": t * 2, "arr": arr + 1}

    def get_received(self) -> list[Any]:
        with self._lock:
            return list(self.received)

    def main(self) -> None:
        self.wait_for_stop()


class CudaRelayPlugin(Plugin):
    """Receives a CUDA tensor on topic_in and re-publishes it on topic_out."""

    def on_start(self) -> None:
        self._lock = threading.Lock()
        self.received_sums: list[float] = []
        topic_in = self.options.get("topic_in")
        if topic_in:
            assert self.topic_subscribe(topic_in, self._on_msg)

    def _on_msg(self, payload: Any) -> None:
        if isinstance(payload, tuple) and len(payload) == 2:
            tensor, event = payload
            try:
                event.wait()
                delay = float(self.options.get("slow_delay", 0.0))
                if delay > 0:
                    time.sleep(delay)
                val = float(tensor.sum().item())
                with self._lock:
                    self.received_sums.append(val)
            finally:
                event.clear()
        else:
            val = float(payload.sum().item())
            with self._lock:
                self.received_sums.append(val)
            topic_out = self.options.get("topic_out")
            if topic_out:
                self.topic_publish(topic_out, payload)

    def publish_cuda(self, topic: str, value: float, graph_marked: bool = False) -> int:
        t = torch.full((4,), value, device="cuda")
        if graph_marked:
            t = mark_graph_output(t)
        return self.topic_publish(topic, t)

    def set_cuda_env(self, key: str, value: float) -> bool:
        t = torch.full((4,), value, device="cuda")
        return self.env_set(key, t)

    def get_cuda_env_sum(self, key: str) -> float:
        t = self.env_get(key)
        return float(t.sum().item())

    def stress_event_pool(self, topic: str, pool_size: int = 2, steps: int = 6) -> dict:
        pool = IpcEventPoolManaged(size=pool_size, stale_after_s=10.0)
        busy_seen = False
        not_cleared_before_ack = False
        for i in range(1, steps + 1):
            ev = pool.acquire(topic)
            t = torch.full((4,), float(i), device="cuda")
            ev.record()
            if pool.has_busy():
                busy_seen = True
            if not ev.is_cleared():
                not_cleared_before_ack = True
            self.topic_publish(topic, (t, ev))
            if not pool.wait_all_free(timeout=5.0):
                return {"ok": False, "step": i}
            if not ev.is_cleared():
                return {"ok": False, "step": i, "reason": "is_cleared False after wait_all_free"}
        return {
            "ok": True,
            "busy_seen": busy_seen,
            "not_cleared_before_ack": not_cleared_before_ack,
        }

    def get_received_sums(self) -> list[float]:
        with self._lock:
            return list(self.received_sums)

    def main(self) -> None:
        self.wait_for_stop()


class ApiIpcTransportTests(unittest.TestCase):
    def test_sanitize_preserves_containers_cycles_hooks_and_tensor_info(self):
        marked = mark_graph_output(torch.tensor([1.0, 2.0]))
        plain = torch.tensor([3.0, 4.0])

        # Unmarked CPU tensor passes through without cloning
        out_plain, has_t = sanitize_for_ipc_with_info(plain)
        self.assertIs(out_plain, plain)
        self.assertTrue(has_t)

        # Pure Python container reports has_tensor == False and returns same object (copy-on-write)
        plain_payload = {"a": [1, 2, (3, "x")], "s": {4, 5}, "fs": frozenset({6})}
        out_plain_payload, has_t_none = sanitize_for_ipc_with_info(plain_payload)
        self.assertFalse(has_t_none)
        self.assertIs(out_plain_payload, plain_payload)

        # Container preservation
        pt = sanitize_for_ipc(_Point(marked, "y"))
        self.assertIsInstance(pt, _Point)
        self.assertIsNot(pt.x, marked)
        self.assertTrue(torch.equal(pt.x, marked))

        od = sanitize_for_ipc(OrderedDict([("k1", marked), ("k2", 2)]))
        self.assertIsInstance(od, OrderedDict)
        self.assertIsNot(od["k1"], marked)

        dd = sanitize_for_ipc(defaultdict(list, {"a": [marked]}))
        self.assertIsInstance(dd, defaultdict)
        self.assertIs(dd.default_factory, list)
        self.assertIsNot(dd["a"][0], marked)

        self.assertIsInstance(sanitize_for_ipc({1, 2}), set)
        self.assertIsInstance(sanitize_for_ipc(frozenset({3, 4})), frozenset)

        box = _TensorBox(label="m", tensor=marked)
        s_box = sanitize_for_ipc(box)
        self.assertIsInstance(s_box, _TensorBox)
        self.assertIsNot(s_box, box)
        self.assertIsNot(s_box.tensor, marked)

        plain_box = _TensorBox(label="p", tensor=plain)
        self.assertIs(sanitize_for_ipc(plain_box), plain_box)

        # Cycles
        cyc_list = []
        cyc_list.append(cyc_list)
        out_list = sanitize_for_ipc(cyc_list)
        self.assertIs(out_list[0], out_list)

        cyc_dict = {}
        cyc_dict["self"] = cyc_dict
        out_dict = sanitize_for_ipc(cyc_dict)
        self.assertIs(out_dict["self"], out_dict)

        # nn.Module with unpicklable hook
        mod = nn.Linear(2, 2)
        mark_graph_output(mod.weight)
        lock = threading.Lock()
        mod.register_forward_hook(lambda m, i, o: lock)
        cloned_mod, has_mod = sanitize_for_ipc_with_info(mod)
        self.assertTrue(has_mod)
        self.assertIsNot(cloned_mod, mod)
        self.assertEqual(len(cloned_mod._forward_hooks), 0)
        self.assertEqual(len(mod._forward_hooks), 1)

    def test_cpu_tensor_and_numpy_cross_process_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir) as manager:
                d = CpuTensorRelayPlugin.Descriptor(
                    "1.0.0", "tests", name="cpu-relay", options={"subscribe_topic": "tensors"}
                )
                self.assertTrue(manager.load_plugin(d))
                self.assertTrue(manager.start_plugin("cpu-relay"))

                t = torch.tensor([1.0, 2.0, 3.0])
                arr = np.array([10, 20], dtype=np.int32)

                # RPC round-trip
                rpc_res = manager.plugin_call("cpu-relay", "echo_tensor", t, arr)
                self.assertTrue(torch.equal(rpc_res["t"], t * 2))
                self.assertTrue(np.array_equal(rpc_res["arr"], arr + 1))

                # Env round-trip
                manager.env_set("cpu.tensor", t)
                self.assertTrue(torch.equal(manager.env_get("cpu.tensor"), t))

                # Topic round-trip
                self.assertEqual(manager.topic_publish("tensors", {"t": t, "arr": arr}), 1)
                wait_until(lambda: len(manager.plugin_call("cpu-relay", "get_received")) == 1)
                got = manager.plugin_call("cpu-relay", "get_received")[0]
                self.assertTrue(torch.equal(got["t"], t))
                self.assertTrue(np.array_equal(got["arr"], arr))

    @unittest.skipUnless(torch.cuda.is_available(), "CUDA is not available")
    def test_cuda_tensor_env_topic_republish_and_event_pool_stress(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with make_manager(tmpdir, timeout_start=15.0) as manager:
                d_prod = CudaRelayPlugin.Descriptor("1.0.0", "tests", name="cuda-prod")
                d_mid = CudaRelayPlugin.Descriptor(
                    "1.0.0",
                    "tests",
                    name="cuda-mid",
                    options={"topic_in": "hop1", "topic_out": "hop2"},
                )
                d_sink = CudaRelayPlugin.Descriptor(
                    "1.0.0",
                    "tests",
                    name="cuda-sink",
                    options={"topic_in": "hop2"},
                )
                d_ev_sub = CudaRelayPlugin.Descriptor(
                    "1.0.0",
                    "tests",
                    name="cuda-ev-sub",
                    options={"topic_in": "ev-stream", "slow_delay": 0.03},
                )
                self.assertEqual(
                    manager.load_plugins([d_prod, d_mid, d_sink, d_ev_sub]),
                    {"cuda-prod": True, "cuda-mid": True, "cuda-sink": True, "cuda-ev-sub": True},
                )
                self.assertTrue(manager.start_plugin("cuda-sink"))
                self.assertTrue(manager.start_plugin("cuda-mid"))
                self.assertTrue(manager.start_plugin("cuda-ev-sub"))
                self.assertTrue(manager.start_plugin("cuda-prod"))

                # 1. CUDA env round-trip
                self.assertTrue(manager.plugin_call("cuda-prod", "set_cuda_env", "cuda.val", 3.0))
                self.assertEqual(manager.plugin_call("cuda-sink", "get_cuda_env_sum", "cuda.val"), 12.0)

                # 2. Multi-hop CUDA topic re-publish (validates _is_ipc_received cloning) + mark_graph_output
                self.assertEqual(manager.plugin_call("cuda-prod", "publish_cuda", "hop1", 5.0, False), 1)
                self.assertEqual(manager.plugin_call("cuda-prod", "publish_cuda", "hop1", 7.0, True), 1)
                wait_until(
                    lambda: manager.plugin_call("cuda-sink", "get_received_sums") == [20.0, 28.0],
                    timeout=5.0,
                )

                # 3. IpcEventPoolManaged stress test (> 2 * pool_size with slow consumer)
                stress = manager.plugin_call(
                    "cuda-prod", "stress_event_pool", "ev-stream", 2, 6, timeout=15.0
                )
                self.assertTrue(stress["ok"], f"Event pool stress failed: {stress}")
                self.assertTrue(stress["busy_seen"])
                self.assertTrue(stress["not_cleared_before_ack"])
                self.assertEqual(
                    manager.plugin_call("cuda-ev-sub", "get_received_sums"),
                    [4.0, 8.0, 12.0, 16.0, 20.0, 24.0],
                )

                manager.env_set("cuda.val", None)


if __name__ == "__main__":
    unittest.main()
