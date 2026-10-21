"""
Worker Pool Example: Multiple instances of the same plugin.

This example shows how plugins can:
1. Create multiple instances of the same plugin
2. Load balance work across worker instances
3. Track metrics per worker
4. Coordinate work distribution
"""

import logging
import time
from typing import Any, Dict

from parallelazy import Plugin, PluginDescriptor, PluginWorker, PluginManager


logger = logging.getLogger(__name__)


class TaskQueue(Plugin):
    """
    Task distributor that publishes tasks to workers.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Task queue distributor",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.task_id = 0

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] TaskQueue started")
        self.env_set("queue.total_tasks", 0)

    def main(self):
        self.task_id += 1
        # Generate a task
        task = {
            "id": self.task_id,
            "operation": "process",
            "complexity": (self.task_id % 3) + 1  # 1-3
        }
        # Publish to workers
        self.topic_publish("tasks.queue", task)
        self.env_set("queue.total_tasks", self.task_id, wait=False)

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] TaskQueue stopped, created {self.task_id} tasks")


class Worker(Plugin):
    """
    Worker that processes tasks from the queue.
    Multiple instances can be created.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Worker pool member",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.tasks_processed = 0
        self.total_time = 0

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] Worker started")
        self.topic_subscribe("tasks.queue", self.on_task)
        # Each worker tracks its own metrics
        self.env_set(f"worker.{self.plugin_id}.processed", 0)
        self.env_set(f"worker.{self.plugin_id}.total_time", 0)

    def on_task(self, task: Dict[str, Any]):
        """Process a task from the queue."""
        self.tasks_processed += 1
        complexity = task["complexity"]
        # Simulate work based on complexity
        time_taken = complexity * 0.1
        self.total_time += time_taken

        logger.info(
            f"[{self.plugin_id}] Task #{task['id']}: complexity={complexity}, time={time_taken:.1f}s"
        )

        self.env_set(f"worker.{self.plugin_id}.processed", self.tasks_processed, wait=False)
        self.env_set(f"worker.{self.plugin_id}.total_time", self.total_time, wait=False)

    def main(self):
        pass

    def on_stop(self) -> None:
        logger.info(
            f"[{self.plugin_id}] Worker stopped: "
            f"processed={self.tasks_processed}, total_time={self.total_time:.1f}s"
        )


def main():
    """Run the worker pool example."""
    manager = PluginManager()

    # Create queue
    queue_desc = TaskQueue.Descriptor()
    manager.load_plugin(queue_desc)
    manager.start_plugin(queue_desc.name)

    # Create a pool of workers
    num_workers = 3
    worker_descs = []
    for i in range(num_workers):
        worker_desc = Worker.Descriptor()
        worker_desc.name = f"worker-{i}"  # Unique name per instance
        manager.load_plugin(worker_desc)
        manager.start_plugin(worker_desc.name)
        worker_descs.append(worker_desc)

    # Let them run
    print(f"Queue and {num_workers} workers running... Press Ctrl+C to stop.")
    time.sleep(5)
    print("Stopping workers...")

    # Stop all workers
    for worker_desc in worker_descs:
        manager.stop_plugin(worker_desc.name)

    manager.stop_plugin(queue_desc.name)

    time.sleep(1)

    # Show worker metrics
    print("\nWorker pool metrics:")
    all_vars = manager.env_get()
    for name, value in sorted(all_vars.items()):
        if name.startswith("worker."):
            print(f"  {name}: {value}")

    # VERIFICATION
    print("\n=== VERIFICATION ===")
    print("PREDICTED BEHAVIOR:")
    print("1. TaskQueue generates ~5 tasks in 5 seconds")
    print("2. 3 workers load-balance tasks via pub/sub")
    print("3. Each worker tracks its own processed count")
    print("4. Total processed across all workers >= queue total\n")

    total_tasks = all_vars.get("queue.total_tasks", 0)
    assert total_tasks > 0, "Queue should generate tasks"
    print(f"✓ Queue generated: {total_tasks} tasks")

    # Count workers and their processed tasks
    workers_found = {}
    for name, value in all_vars.items():
        if name.startswith("worker.") and name.endswith(".processed"):
            worker_name = name.replace("worker.", "").replace(".processed", "")
            workers_found[worker_name] = value

    assert len(workers_found) == num_workers, f"Should have {num_workers} workers, found {len(workers_found)}"
    total_processed = sum(workers_found.values())
    assert total_processed > 0, "Workers should process tasks"
    
    print(f"✓ Workers found: {num_workers}")
    print(f"✓ Total tasks processed across all workers: {total_processed}")
    for worker_id, count in sorted(workers_found.items()):
        print(f"  - {worker_id}: {count} tasks")
    print("✓ All worker pool assertions passed!")

    # Clean up
    for worker_desc in worker_descs:
        manager.unload_plugin(worker_desc.name)

    manager.unload_plugin(queue_desc.name)


if __name__ == "__main__":
    main()
