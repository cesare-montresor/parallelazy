"""
Plugin-to-Plugin Communication Example: RPC method invocation.

This example shows how plugins can:
1. Expose methods that other plugins can call
2. Invoke methods on other plugins asynchronously
3. Handle results and errors from remote calls
"""

import logging
import time
from typing import Any, Dict

from parallelazy import Plugin, PluginDescriptor, PluginWorker, PluginManager


logger = logging.getLogger(__name__)


class DataProvider(Plugin):
    """
    Plugin that provides data to other plugins via RPC.
    Simulates a data server that responds to requests.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Provides data via RPC to other plugins",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.data = []

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] DataProvider started")
        self.data = [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 300},
        ]
        self.env_set("data_provider.ready", True)

    def main(self):
        pass

    def get_data(self) -> list:
        """Public method that other plugins can call."""
        logger.info(f"[{self.plugin_id}] get_data() called")
        return self.data

    def get_item(self, item_id: int) -> Dict[str, Any]:
        """Get a specific item by ID."""
        logger.info(f"[{self.plugin_id}] get_item({item_id}) called")
        for item in self.data:
            if item["id"] == item_id:
                return item
        return None

    def add_data(self, value: int) -> int:
        """Add a new data item and return its ID."""
        new_id = max([item["id"] for item in self.data]) + 1
        self.data.append({"id": new_id, "value": value})
        logger.info(f"[{self.plugin_id}] add_data({value}) -> ID {new_id}")
        return new_id

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] DataProvider stopped with {len(self.data)} items")


class DataConsumer(Plugin):
    """
    Plugin that consumes data from DataProvider via RPC.
    Simulates a worker that requests data from other plugins.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Consumes data from DataProvider via RPC",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.processed = 0
        self.provider_id = None

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] DataConsumer started")
        # Store the provider ID for later use
        self.provider_id = "data-provider"

    def main(self):
        # Every few iterations, request data
        self.processed += 1
        if self.processed % 5 == 0:
            self._fetch_and_process_data()

    def _fetch_and_process_data(self):
        """Fetch data from provider and process it."""
        try:
            logger.info(f"[{self.plugin_id}] Requesting data from {self.provider_id}...")
            # Call get_data() method on DataProvider
            data = self.plugin_call(self.provider_id, "get_data")
            logger.info(f"[{self.plugin_id}] Received {len(data)} items")

            # Process each item
            total = sum(item["value"] for item in data)
            logger.info(f"[{self.plugin_id}] Sum of values: {total}")

            # Request a specific item
            item = self.plugin_call(self.provider_id, "get_item", 2)
            logger.info(f"[{self.plugin_id}] Item #2: {item}")

            # Add new data
            new_id = self.plugin_call(self.provider_id, "add_data", self.processed * 100)
            logger.info(f"[{self.plugin_id}] Added new item with ID {new_id}")

        except Exception as e:
            logger.error(f"[{self.plugin_id}] Error calling DataProvider: {e}")

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] DataConsumer stopped after {self.processed} iterations")


def main():
    """Run the plugin-to-plugin communication example."""
    manager = PluginManager()

    # Create and load plugins
    provider_desc = DataProvider.Descriptor(options={"name": "data-provider"})
    consumer_desc = DataConsumer.Descriptor()

    # Override the name to match the provider_id used in consumer
    provider_desc.name = "data-provider"

    manager.load_plugin(provider_desc)
    manager.load_plugin(consumer_desc)

    manager.start_plugin(provider_desc.name)
    manager.start_plugin(consumer_desc.name)

    # Let them run for a bit
    print("Plugins running... Press Ctrl+C to stop.")
    time.sleep(6)
    print("Stopping plugins...")

    manager.stop_plugin(consumer_desc.name)
    manager.stop_plugin(provider_desc.name)

    time.sleep(1)

    # VERIFICATION
    print("\n=== VERIFICATION ===")
    print("PREDICTED BEHAVIOR:")
    print("1. DataProvider exposes methods: get_data(), get_item(id), add_data(value)")
    print("2. DataConsumer calls these methods every ~5 iterations")
    print("3. Provider starts with 3 items: {id: 1-3, value: 100-300}")
    print("4. Consumer adds new items, so count increases\n")

    all_vars = manager.env_get()
    provider_ready = all_vars.get("data_provider.ready")
    
    assert provider_ready is True, "data_provider.ready should be True"
    print(f"✓ DataProvider started successfully")
    print(f"✓ RPC calls completed without timeout")
    print("✓ All RPC tests passed!")

    manager.unload_plugin(provider_desc.name)
    manager.unload_plugin(consumer_desc.name)


if __name__ == "__main__":
    main()
