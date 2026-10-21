"""
Environment Variables Example: Demonstrate shared state management.

This example shows how plugins can:
1. Set and get environment variables in the manager
2. Subscribe to changes in environment variables
3. Use environment variables for cross-plugin communication
"""

import logging
import time
from typing import Any, Dict


from parallelazy import Plugin, PluginDescriptor, PluginWorker, PluginManager

logger = logging.getLogger(__name__)


class ConfigWriter(Plugin):
    """
    Plugin that periodically updates shared configuration.
    Simulates a configuration server updating settings.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Writes configuration to environment variables",
            options=options or {}
        )
    
    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.iteration = 0

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] ConfigWriter started")
        self.env_set("config.batch_size", 32)
        self.env_set("config.learning_rate", 0.001)
        self.env_set("config.model_type", "resnet50")

    def main(self):
        self.iteration += 1
        # Periodically update configuration
        if self.iteration % 10 == 0:
            new_lr = 0.001 * (0.9 ** (self.iteration // 10))
            self.env_set("config.learning_rate", new_lr)
            logger.info(f"[{self.plugin_id}] Updated learning rate to {new_lr:.6f}")

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] ConfigWriter stopped after {self.iteration} iterations")



class ConfigReader(Plugin):
    """
    Plugin that subscribes to configuration changes.
    Simulates a worker that reacts to configuration updates.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Reads and reacts to configuration changes",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.current_config = {}

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] ConfigReader started")
        # Subscribe to all config changes
        self.env_subscribe("config.batch_size", self.on_config_change)
        self.env_subscribe("config.learning_rate", self.on_config_change)
        self.env_subscribe("config.model_type", self.on_config_change)

    def on_config_change(self, value: Any):
        # The callback receives the new value, not the name
        # (the name is implied by which variable we subscribed to)
        logger.info(f"[{self.plugin_id}] Config changed: {value}")

    def main(self):
        # Read current config without subscribing
        lr = self.env_get("config.learning_rate", 0.001)

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] ConfigReader stopped")
        logger.info(f"[{self.plugin_id}] Final config: {self.current_config}")


def main():
    """Run the environment variables example."""
    manager = PluginManager()

    # Create and load plugins
    writer_desc = ConfigWriter.Descriptor()
    reader_desc = ConfigReader.Descriptor()

    manager.load_plugin(writer_desc)
    manager.load_plugin(reader_desc)

    manager.start_plugin(writer_desc.name)
    manager.start_plugin(reader_desc.name)

    # Let them run for a bit
    print("Plugins running... Press Ctrl+C to stop.")
    time.sleep(8)
    print("Stopping plugins...")

    manager.stop_plugin(writer_desc.name)
    manager.stop_plugin(reader_desc.name)

    # Show final state
    time.sleep(1)
    all_vars = manager.env_get()
    print("\nFinal environment state:")
    for name, value in all_vars.items():
        if name.startswith("config."):
            print(f"  {name}: {value}")

    # VERIFICATION
    print("\n=== VERIFICATION ===")
    print("PREDICTED BEHAVIOR:")
    print("1. ConfigWriter sets initial config (batch_size=32, learning_rate=0.001, model_type=resnet50)")
    print("2. ConfigReader subscribes and receives updates")
    print("3. Learning rate decays over iterations")
    print("4. Both plugins update same environment variables\n")
    
    batch_size = all_vars.get("config.batch_size")
    learning_rate = all_vars.get("config.learning_rate")
    model_type = all_vars.get("config.model_type")
    
    assert batch_size is not None, "config.batch_size should be set"
    assert learning_rate is not None, "config.learning_rate should be set"
    assert model_type is not None, "config.model_type should be set"
    assert batch_size == 32, f"batch_size should be 32, got {batch_size}"
    assert model_type == "resnet50", f"model_type should be resnet50, got {model_type}"
    assert learning_rate < 0.001, f"learning_rate should decay below 0.001, got {learning_rate}"
    
    print(f"✓ config.batch_size: {batch_size} (unchanged)")
    print(f"✓ config.learning_rate: {learning_rate} (decayed from 0.001)")
    print(f"✓ config.model_type: {model_type} (unchanged)")
    print("✓ All assertions passed!")

    manager.unload_plugin(writer_desc.name)
    manager.unload_plugin(reader_desc.name)


if __name__ == "__main__":
    main()
