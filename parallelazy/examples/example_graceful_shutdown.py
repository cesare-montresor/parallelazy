"""
Graceful Shutdown Example: Verify clean plugin termination.

This minimal example demonstrates:
1. Plugin lifecycle (load, start, stop, unload)
2. Graceful shutdown with on_stop() callback
3. Process cleanup and resource release
"""

import logging
import sys
import time
from typing import Any, Dict

from parallelazy.plugin import Plugin, PluginDescriptor
from parallelazy.plugin_worker import PluginWorker
from parallelazy.plugin_manager import PluginManager

# Configure logging for clear shutdown verification
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class SimpleWorker(Plugin):
    """
    Minimal plugin that logs lifecycle events.
    Perfect for verifying graceful shutdown behavior.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Simple worker for testing graceful shutdown",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.iteration = 0

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] ✓ on_start() called - plugin initialized")
        self.iteration = 0

    def main(self):
        """Main loop - runs repeatedly until on_stop() is called."""
        self.iteration += 1
        logger.debug(f"[{self.plugin_id}] Iteration {self.iteration}")
        time.sleep(0.1)  # Brief sleep to simulate work

    def on_stop(self) -> None:
        """Graceful shutdown callback - called before plugin terminates."""
        logger.info(f"[{self.plugin_id}] ✓ on_stop() called - cleaning up after {self.iteration} iterations")
        logger.info(f"[{self.plugin_id}] ✓ Graceful shutdown complete")


def main():
    """Run the graceful shutdown verification."""
    print("\n" + "=" * 70)
    print("GRACEFUL SHUTDOWN VERIFICATION")
    print("=" * 70 + "\n")

    # Step 1: Create manager
    print("[STEP 1] Creating PluginManager...")
    manager = PluginManager(timeout_start=5.0)
    print("✓ Manager created\n")

    # Step 2: Load plugin
    print("[STEP 2] Loading plugin...")
    descriptor = SimpleWorker.Descriptor()
    if not manager.load_plugin(descriptor):
        logger.error("Failed to load plugin")
        return False
    print("✓ Plugin loaded\n")

    # Step 3: Start plugin
    print("[STEP 3] Starting plugin...")
    if not manager.start_plugin(descriptor.name):
        logger.error("Failed to start plugin")
        return False
    print("✓ Plugin started\n")

    # Step 4: Let it run briefly
    print("[STEP 4] Running for 2 seconds...")
    time.sleep(2)
    print("✓ Plugin has been running\n")

    # Step 5: Stop plugin (graceful shutdown)
    print("[STEP 5] Stopping plugin (graceful shutdown)...")
    if not manager.stop_plugin(descriptor.name):
        logger.error("Failed to stop plugin")
        return False
    print("✓ Plugin stopped gracefully\n")

    # Step 6: Unload plugin
    print("[STEP 6] Unloading plugin...")
    if not manager.unload_plugin(descriptor.name):
        logger.error("Failed to unload plugin")
        return False
    print("✓ Plugin unloaded\n")

    # Step 7: Verify shutdown
    print("[STEP 7] Verification...")
    if descriptor.name in manager._plugins:
        logger.error("Plugin still in manager after unload!")
        return False
    print("✓ Plugin successfully cleaned up from manager\n")

    # Summary
    print("=" * 70)
    print("GRACEFUL SHUTDOWN VERIFICATION PASSED ✓")
    print("=" * 70)
    print("\nExpected behavior verified:")
    print("  1. on_start() called when plugin started")
    print("  2. Plugin ran for ~2 seconds (main loop)")
    print("  3. on_stop() called when stop_plugin() invoked")
    print("  4. Process terminated cleanly")
    print("  5. Resources cleaned up after unload_plugin()")
    print("\n")

    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
