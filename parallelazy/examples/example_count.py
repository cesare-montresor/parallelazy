"""
Basic test plugin for lifecycle testing.

A minimal plugin that exercises the load/unload/start/stop/status lifecycle
without any external dependencies.
"""

import logging
from typing import Any, Dict

from parallelazy.plugin import Plugin, PluginDescriptor
from parallelazy.plugin_worker import PluginWorker
import time
from parallelazy.plugin_manager import PluginManager



logger = logging.getLogger(__name__)

class ExamplePluginCount(Plugin):
    """
    Minimal test plugin that exercises core lifecycle.
    
    Features:
    - Simple on_start/on_stop implementation
    - Tracks call count
    - Can be used to verify manager lifecycle
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        """Create plugin descriptor."""
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Basic test plugin for lifecycle verification",
            options=options or {}
        )

    def __init__(
        self,
        worker: PluginWorker,
        plugin_id: str,
        options: Dict[str, Any],
    ):
        super().__init__(worker, plugin_id, options)
        self.count = None

    def main(self):
        self.count +=1
        self.env_set("count", self.count, wait=False)


    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] ExamplePluginCount starting...")
        self.count = 0
        self.env_set("count", self.count)

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] Total loop: {self.count}")
        self.env_set("count", self.count)
        

def main():
    """Example main function to run the plugin standalone for testing."""
    manager = PluginManager()
    descriptor = ExamplePluginCount.Descriptor()
    manager.load_plugin(descriptor)
    manager.start_plugin(descriptor.name)

    # Let it run for a few seconds
    time.sleep(3)

    
    manager.unload_plugin(descriptor.name)

    all_vars = manager.env_get()

    print("All environment variables:")
    for name, value in all_vars.items():
        print(f"  {name}: {value}")

if __name__ == "__main__":    main()

