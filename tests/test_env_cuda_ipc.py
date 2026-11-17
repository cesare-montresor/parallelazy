import unittest

import torch

from parallelazy import Plugin, PluginManager


class _CudaEnvPlugin(Plugin):
    def on_start(self):
        payload = torch.arange(4, device="cuda")
        self.env_set("cuda.payload", payload)
        received = self.env_get("cuda.payload")
        self.env_set("cuda.roundtrip", torch.equal(received, payload))

    def main(self):
        self.wait_for_stop()


@unittest.skipUnless(torch.cuda.is_available(), "CUDA is not available")
class CudaEnvironmentIpcTests(unittest.TestCase):
    def test_cuda_tensor_round_trips_through_plugin_env_set_and_get(self):
        manager = PluginManager(timeout_start=20)
        descriptor = _CudaEnvPlugin.Descriptor(version="1.0.0", author="tests")
        received = None

        try:
            self.assertTrue(manager.load_plugin(descriptor))
            self.assertTrue(manager.start_plugin(descriptor.name))

            received = manager.env_get("cuda.payload")
            self.assertTrue(received.is_cuda)
            self.assertTrue(torch.equal(received, torch.arange(4, device="cuda")))
            self.assertTrue(manager.env_get("cuda.roundtrip"))
        finally:
            received = None
            manager.env_set("cuda.payload", None)
            manager.unload_all_plugins()


if __name__ == "__main__":
    unittest.main()
