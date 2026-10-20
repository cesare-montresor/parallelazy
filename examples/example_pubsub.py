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


class ExamplePluginPub(Plugin):
    """
    Minimal plugin to test publish/subscribe functionality.
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


    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] ExamplePluginCount starting...")
        self.count = 0
        self.env_set("count_pub", self.count)

    def main(self):
        self.count +=1
        self.topic_publish("count", self.count)

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] Total loop: {self.count}")
        self.env_set("count_pub", self.count)


class ExamplePluginSub(Plugin):
    """  """

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


    def on_start(self) -> None:
        self.topic_subscribe("count", self.on_count_update)
    
    def on_count_update(self, value):
        #logger.info(f"[{self.plugin_id}] Received count update: {value}")
        self.count = value

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] Total loop: {self.count}")
        self.env_set("count_sub", self.count)
        

def main():
    """Example main function to run the plugin standalone for testing."""
    manager = PluginManager()
    pub_descriptor = ExamplePluginPub.Descriptor()
    sub_descriptor = ExamplePluginSub.Descriptor()
    
    manager.load_plugin(pub_descriptor)
    manager.start_plugin(pub_descriptor.name)

    manager.load_plugin(sub_descriptor)
    manager.start_plugin(sub_descriptor.name)
    
    

    # Let it run for a few seconds
    print("Plugins are running... Press Ctrl+C to stop.")
    time.sleep(5)
    print("Stopping plugins...")

    #manager.stop_plugin(pub_descriptor.name)
    #manager.stop_plugin(sub_descriptor.name)

    #time.sleep(1)  # Give some time for plugins to stop

    manager.unload_plugin(pub_descriptor.name)    
    manager.unload_plugin(sub_descriptor.name)

    all_vars = manager.env_get()

    print("All environment variables:")
    for name, value in all_vars.items():
        print(f"  {name}: {value}")

    c_pub = all_vars.get("count_pub", 0)
    c_sub = all_vars.get("count_sub", -1)


    if c_pub == c_sub:
        print("Count values match!")
    else:
        print(f"ERROR: mismatch: pub={c_pub}, sub={c_sub}")

if __name__ == "__main__": 
    main()

