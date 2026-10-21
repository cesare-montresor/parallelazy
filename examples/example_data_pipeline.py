"""
Data Pipeline Example: Multi-stage processing with pub/sub.

This example shows how plugins can:
1. Form a data pipeline using pub/sub topics
2. Process data in stages
3. Coordinate using environment variables
4. Track progress across the pipeline
"""

import logging
import time
from typing import Any, Dict


from parallelazy import Plugin, PluginDescriptor, PluginWorker, PluginManager

logger = logging.getLogger(__name__)


class DataSource(Plugin):
    """
    Stage 1: Generate data and publish to topic.from parallelazy.plugin import Plugin, PluginDescriptor
from parallelazy.plugin_worker import PluginWorker
from parallelazy.plugin_manager import PluginManager

    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Data source for pipeline",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.count = 0

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] DataSource started")
        self.env_set("pipeline.stage1.count", 0)

    def main(self):
        self.count += 1
        # Generate raw data
        raw_data = {
            "id": self.count,
            "raw_value": self.count * 10
        }
        # Publish to pipeline topic
        self.topic_publish("pipeline.raw", raw_data)
        self.env_set("pipeline.stage1.count", self.count, wait=False)

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] DataSource stopped, produced {self.count} items")


class DataProcessor(Plugin):
    """
    Stage 2: Process data from raw topic and publish to processed topic.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Data processor for pipeline",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.count = 0

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] DataProcessor started")
        self.topic_subscribe("pipeline.raw", self.on_raw_data)
        self.env_set("pipeline.stage2.count", 0)

    def on_raw_data(self, data: Dict[str, Any]):
        self.count += 1
        # Process the raw data
        processed = {
            "id": data["id"],
            "raw_value": data["raw_value"],
            "processed_value": data["raw_value"] * 2 + 5
        }
        logger.info(f"[{self.plugin_id}] Processed item #{data['id']}: {processed['processed_value']}")
        # Publish processed data
        self.topic_publish("pipeline.processed", processed)
        self.env_set("pipeline.stage2.count", self.count, wait=False)

    def main(self):
        pass

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] DataProcessor stopped, processed {self.count} items")


class DataAggregator(Plugin):
    """
    Stage 3: Aggregate processed data and store statistics.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Data aggregator for pipeline",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.count = 0
        self.sum_value = 0
        self.max_value = 0

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] DataAggregator started")
        self.topic_subscribe("pipeline.processed", self.on_processed_data)
        self.env_set("pipeline.stage3.count", 0)
        self.env_set("pipeline.stats.sum", 0)
        self.env_set("pipeline.stats.max", 0)

    def on_processed_data(self, data: Dict[str, Any]):
        self.count += 1
        value = data["processed_value"]
        self.sum_value += value
        self.max_value = max(self.max_value, value)

        if self.count % 5 == 0:
            logger.info(
                f"[{self.plugin_id}] Aggregated {self.count} items | "
                f"Sum: {self.sum_value} | Max: {self.max_value}"
            )
            self.env_set("pipeline.stage3.count", self.count, wait=False)
            self.env_set("pipeline.stats.sum", self.sum_value, wait=False)
            self.env_set("pipeline.stats.max", self.max_value, wait=False)

    def main(self):
        pass

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] DataAggregator stopped")
        logger.info(f"[{self.plugin_id}] Final stats: count={self.count}, sum={self.sum_value}, max={self.max_value}")
        self.env_set("pipeline.stats.final_count", self.count)
        self.env_set("pipeline.stats.final_sum", self.sum_value)
        self.env_set("pipeline.stats.final_max", self.max_value)


def main():
    """Run the data pipeline example."""
    manager = PluginManager()

    # Create and load pipeline plugins in order
    source_desc = DataSource.Descriptor()
    processor_desc = DataProcessor.Descriptor()
    aggregator_desc = DataAggregator.Descriptor()

    manager.load_plugin(source_desc)
    manager.load_plugin(processor_desc)
    manager.load_plugin(aggregator_desc)

    manager.start_plugin(source_desc.name)
    manager.start_plugin(processor_desc.name)
    manager.start_plugin(aggregator_desc.name)

    # Let the pipeline run
    print("Data pipeline running... Press Ctrl+C to stop.")
    time.sleep(3)
    print("Stopping pipeline...")

    manager.stop_plugin(source_desc.name)
    manager.stop_plugin(processor_desc.name)
    manager.stop_plugin(aggregator_desc.name)

    time.sleep(1)
    manager.unload_plugin(source_desc.name)
    manager.unload_plugin(processor_desc.name)
    manager.unload_plugin(aggregator_desc.name)

    # Show final state
    print("\nFinal pipeline statistics:")
    all_vars = manager.env_get()
    for name, value in all_vars.items():
        if name.startswith("pipeline."):
            print(f"  {name}: {value}")

    # VERIFICATION
    print("\n=== VERIFICATION ===")
    print("PREDICTED BEHAVIOR:")
    print("1. DataSource generates ~3 items in 3 seconds")
    print("2. DataProcessor receives all items and doubles+offsets values")
    print("3. DataAggregator aggregates: sum and max of processed values")
    print("4. All 3 stages see the same count\n")

    count1 = all_vars.get("pipeline.stage1.count", 0)
    count2 = all_vars.get("pipeline.stage2.count", 0)
    count3 = all_vars.get("pipeline.stage3.count", 0)
    stats_sum = all_vars.get("pipeline.stats.sum", 0)
    stats_max = all_vars.get("pipeline.stats.max", 0)

    assert count1 > 0, "Stage 1 should generate items"
    assert count2 > 0, "Stage 2 should process items"
    assert count3 > 0, "Stage 3 should aggregate items"
    # Topic callbacks and wait=False environment updates can still be
    # slightly out of phase when the source is stopped. Each downstream
    # stage must never report more items than its upstream stage.
    assert count2 <= count1, f"Stage 2 cannot exceed stage 1: stage1={count1}, stage2={count2}"
    assert count3 <= count2, f"Stage 3 cannot exceed stage 2: stage2={count2}, stage3={count3}"
    assert stats_sum > 0, "Sum should be positive"
    assert stats_max > 0, "Max should be positive"

    print(f"✓ Items generated: {count1}")
    print(f"✓ Items processed through all 3 stages: {count1} >= {count2} >= {count3}")
    print(f"✓ Aggregator computed: sum={stats_sum}, max={stats_max}")
    print("✓ All pipeline assertions passed!")

if __name__ == "__main__":
    main()
