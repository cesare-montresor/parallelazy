"""
Configuration & Options Example: Plugin customization at load time.

This example shows how plugins can:
1. Accept configuration options when loaded
2. Use options to customize behavior
3. Override configuration dynamically
4. Validate configuration options
"""

import logging
import time
from typing import Any, Dict

from parallelazy.plugin import Plugin, PluginDescriptor
from parallelazy.plugin_worker import PluginWorker
from parallelazy.plugin_manager import PluginManager

logger = logging.getLogger(__name__)


class Sensor(Plugin):
    """
    Plugin that simulates a sensor with configurable sampling.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        default_options = {
            "sample_rate": 10,  # Hz
            "noise_level": 0.1,
            "sensor_type": "temperature",
        }
        if options:
            default_options.update(options)

        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Configurable sensor plugin",
            options=default_options
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.sample_count = 0
        self.sample_rate = options.get("sample_rate", 10)
        self.noise_level = options.get("noise_level", 0.1)
        self.sensor_type = options.get("sensor_type", "temperature")

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] Sensor started with options:")
        logger.info(f"  - type: {self.sensor_type}")
        logger.info(f"  - sample_rate: {self.sample_rate} Hz")
        logger.info(f"  - noise_level: {self.noise_level}")
        self.env_set("sensor.config", {
            "type": self.sensor_type,
            "sample_rate": self.sample_rate,
            "noise_level": self.noise_level
        })
        self.env_set("sensor.samples", 0)

    def main(self):
        self.sample_count += 1
        # Generate simulated sensor reading
        base_value = 20 + (self.sample_count % 10)  # Temperature 20-30°C
        import random
        noise = random.gauss(0, self.noise_level)
        reading = base_value + noise

        if self.sample_count % 5 == 0:
            logger.info(f"[{self.plugin_id}] Sample #{self.sample_count}: {reading:.2f}")
            self.topic_publish("sensor.readings", {
                "sample": self.sample_count,
                "value": reading,
                "type": self.sensor_type
            })
            self.env_set("sensor.samples", self.sample_count, wait=False)

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] Sensor stopped, collected {self.sample_count} samples")


class SensorLogger(Plugin):
    """
    Plugin that logs sensor readings with configurable verbosity.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        default_options = {
            "verbose": False,
            "log_every_n": 5,
        }
        if options:
            default_options.update(options)

        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Sensor logger with configurable output",
            options=default_options
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.verbose = options.get("verbose", False)
        self.log_every_n = options.get("log_every_n", 5)
        self.readings = []

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] SensorLogger started (verbose={self.verbose})")
        self.topic_subscribe("sensor.readings", self.on_reading)

    def on_reading(self, reading: Dict[str, Any]):
        self.readings.append(reading)
        if self.verbose:
            logger.info(f"[{self.plugin_id}] Received: {reading}")
        elif len(self.readings) % self.log_every_n == 0:
            avg_value = sum(r["value"] for r in self.readings) / len(self.readings)
            logger.info(f"[{self.plugin_id}] {len(self.readings)} readings, avg={avg_value:.2f}")
            self.env_set("logger.avg_value", avg_value, wait=False)

    def main(self):
        pass

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] SensorLogger stopped, logged {len(self.readings)} readings")


def main():
    """Run the configuration example."""
    manager = PluginManager()

    # Load sensor with custom options
    sensor_desc = Sensor.Descriptor(options={
        "sample_rate": 20,
        "noise_level": 0.5,
        "sensor_type": "humidity"
    })

    # Load logger with verbose mode
    logger_desc = SensorLogger.Descriptor(options={
        "verbose": False,
        "log_every_n": 3
    })

    manager.load_plugin(sensor_desc)
    manager.load_plugin(logger_desc)

    manager.start_plugin(sensor_desc.name)
    manager.start_plugin(logger_desc.name)

    print("Sensor and logger running... Press Ctrl+C to stop.")
    time.sleep(5)
    print("Stopping plugins...")

    manager.stop_plugin(logger_desc.name)
    manager.stop_plugin(sensor_desc.name)

    # Show configuration
    print("\nFinal state:")
    all_vars = manager.env_get()
    for name, value in all_vars.items():
        if name.startswith("sensor.") or name.startswith("logger."):
            print(f"  {name}: {value}")

    # VERIFICATION
    print("\n=== VERIFICATION ===")
    print("PREDICTED BEHAVIOR:")
    print("1. Sensor loads with custom options (sample_rate=20, noise_level=0.5, type=humidity)")
    print("2. Logger loads with verbose=False, log_every_n=3")
    print("3. Sensor publishes readings, logger subscribes and aggregates")
    print("4. Config persists in environment\n")

    sensor_config = all_vars.get("sensor.config")
    logger_avg = all_vars.get("logger.avg_value")

    assert sensor_config is not None, "sensor.config should be set"
    assert sensor_config.get("type") == "humidity", f"Sensor type should be humidity, got {sensor_config.get('type')}"
    assert sensor_config.get("sample_rate") == 20, f"Sample rate should be 20, got {sensor_config.get('sample_rate')}"
    assert sensor_config.get("noise_level") == 0.5, f"Noise level should be 0.5, got {sensor_config.get('noise_level')}"
    
    print(f"✓ Sensor config: type={sensor_config['type']}, rate={sensor_config['sample_rate']}Hz, noise={sensor_config['noise_level']}")
    if logger_avg is not None:
        print(f"✓ Logger computed average: {logger_avg:.2f}")
    print("✓ All configuration assertions passed!")

    manager.unload_plugin(sensor_desc.name)
    manager.unload_plugin(logger_desc.name)


if __name__ == "__main__":
    main()
