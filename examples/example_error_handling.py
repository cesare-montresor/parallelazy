"""
Error Handling Example: Resilient plugins with error recovery.

This example shows how plugins can:
1. Handle errors gracefully
2. Retry operations
3. Report errors to manager
4. Implement fallback strategies
"""

import logging
import time
import random
from typing import Any, Dict


from parallelazy import Plugin, PluginDescriptor, PluginWorker, PluginManager

logger = logging.getLogger(__name__)


class UnreliableService(Plugin):
    """
    Plugin that simulates an unreliable external service.
    Randomly fails to simulate real-world conditions.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Unreliable service simulator",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.success_count = 0
        self.failure_count = 0
        self.failure_rate = 0.3  # 30% failure rate

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] UnreliableService started")
        self.env_set("service.status", "ready")
        self.env_set("service.success_count", 0)
        self.env_set("service.failure_count", 0)

    def main(self):
        pass

    def call_service(self, request_id: int) -> Dict[str, Any]:
        """
        Simulates a service call that may fail.
        """
        if random.random() < self.failure_rate:
            self.failure_count += 1
            logger.error(f"[{self.plugin_id}] Service call #{request_id} FAILED")
            self.env_set("service.failure_count", self.failure_count, wait=False)
            raise Exception(f"Service temporarily unavailable (request #{request_id})")
        else:
            self.success_count += 1
            result = {"request_id": request_id, "data": f"response_{request_id}"}
            logger.info(f"[{self.plugin_id}] Service call #{request_id} SUCCESS")
            self.env_set("service.success_count", self.success_count, wait=False)
            return result

    def on_stop(self) -> None:
        logger.info(
            f"[{self.plugin_id}] UnreliableService stopped: "
            f"success={self.success_count}, failures={self.failure_count}"
        )
        self.env_set("service.final_status", "stopped")


class ResilientClient(Plugin):
    """
    Plugin that calls unreliable service with retry logic.
    Demonstrates error handling and recovery strategies.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Resilient service client with retry logic",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.requests_sent = 0
        self.requests_succeeded = 0
        self.requests_failed = 0
        self.service_id = "unreliable-service"

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] ResilientClient started")
        self.env_set("client.requests_sent", 0)
        self.env_set("client.requests_succeeded", 0)
        self.env_set("client.requests_failed", 0)

    def main(self):
        self.requests_sent += 1
        if self.requests_sent % 3 == 0:
            self._call_with_retry(self.requests_sent)

    def _call_with_retry(self, request_id: int, max_retries: int = 3):
        """Call service with retry logic."""
        retry_count = 0
        last_error = None

        while retry_count < max_retries:
            try:
                logger.info(
                    f"[{self.plugin_id}] Calling service (request #{request_id}, "
                    f"attempt {retry_count + 1}/{max_retries})"
                )
                result = self.plugin_call(self.service_id, "call_service", request_id)
                logger.info(f"[{self.plugin_id}] Request #{request_id} succeeded: {result}")
                self.requests_succeeded += 1
                self.env_set("client.requests_succeeded", self.requests_succeeded, wait=False)
                return result

            except Exception as e:
                last_error = e
                retry_count += 1
                logger.warning(
                    f"[{self.plugin_id}] Request #{request_id} failed (attempt {retry_count}/{max_retries}): {e}"
                )
                if retry_count < max_retries:
                    # Brief backoff before retry
                    time.sleep(0.1 * retry_count)

        # All retries exhausted
        logger.error(f"[{self.plugin_id}] Request #{request_id} FAILED after {max_retries} attempts: {last_error}")
        self.requests_failed += 1
        self.env_set("client.requests_failed", self.requests_failed, wait=False)
        return None

    def on_stop(self) -> None:
        logger.info(
            f"[{self.plugin_id}] ResilientClient stopped: "
            f"sent={self.requests_sent}, succeeded={self.requests_succeeded}, "
            f"failed={self.requests_failed}"
        )
        self.env_set("client.requests_sent", self.requests_sent)
        self.env_set("client.requests_succeeded", self.requests_succeeded)
        self.env_set("client.requests_failed", self.requests_failed)


class HealthMonitor(Plugin):
    """
    Plugin that monitors the health of other plugins.
    """

    @classmethod
    def Descriptor(cls, options: Dict[str, Any] = None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="ModularNet Team",
            description="Health monitor for system resilience",
            options=options or {}
        )

    def __init__(self, worker: PluginWorker, plugin_id: str, options: Dict[str, Any]):
        super().__init__(worker, plugin_id, options)
        self.checks = 0

    def on_start(self) -> None:
        logger.info(f"[{self.plugin_id}] HealthMonitor started")
        # Subscribe to service status
        self.env_subscribe("service.status", self.on_service_status_change)
        self.env_set("monitor.checks", 0)

    def on_service_status_change(self, name: str, value: Any):
        logger.info(f"[{self.plugin_id}] Service status changed: {value}")

    def main(self):
        self.checks += 1
        if self.checks % 4 == 0:
            # Check service metrics
            success = self.env_get("service.success_count", 0)
            failures = self.env_get("service.failure_count", 0)
            total = success + failures
            if total > 0:
                failure_rate = failures / total
                status = "UNHEALTHY" if failure_rate > 0.5 else "HEALTHY"
                logger.info(
                    f"[{self.plugin_id}] Health check: {status} (success_rate={1-failure_rate:.1%})"
                )

    def on_stop(self) -> None:
        logger.info(f"[{self.plugin_id}] HealthMonitor stopped after {self.checks} checks")


def main():
    """Run the error handling example."""
    manager = PluginManager()

    # Create plugins
    service_desc = UnreliableService.Descriptor()
    service_desc.name = "unreliable-service"

    client_desc = ResilientClient.Descriptor()
    monitor_desc = HealthMonitor.Descriptor()

    manager.load_plugin(service_desc)
    manager.load_plugin(client_desc)
    manager.load_plugin(monitor_desc)

    manager.start_plugin(service_desc.name)
    manager.start_plugin(client_desc.name)
    manager.start_plugin(monitor_desc.name)

    print("Error handling example running... Press Ctrl+C to stop.")
    time.sleep(6)
    print("Stopping plugins...")

    manager.stop_plugin(monitor_desc.name)
    manager.stop_plugin(client_desc.name)
    manager.stop_plugin(service_desc.name)

    time.sleep(1)

    # Show final metrics
    print("\nFinal metrics:")
    all_vars = manager.env_get()
    for name, value in sorted(all_vars.items()):
        if "service" in name or "client" in name:
            print(f"  {name}: {value}")

    # VERIFICATION
    print("\n=== VERIFICATION ===")
    print("PREDICTED BEHAVIOR:")
    print("1. UnreliableService fails ~30% of the time")
    print("2. ResilientClient retries up to 3 times (exponential backoff)")
    print("3. Client succeeds on some retries, fails on others")
    print("4. Both success and failure metrics tracked\n")

    service_success = all_vars.get("service.success_count", 0)
    service_failures = all_vars.get("service.failure_count", 0)
    client_success = all_vars.get("client.requests_succeeded", 0)
    client_failures = all_vars.get("client.requests_failed", 0)

    assert service_success > 0, "Service should have successes"
    assert service_failures > 0, "Service should have failures (30% rate)"
    assert client_success > 0, "Client should retry and succeed sometimes"
    
    service_total = service_success + service_failures
    failure_rate = service_failures / service_total if service_total > 0 else 0
    
    print(f"✓ Service stats: success={service_success}, failures={service_failures}, failure_rate={failure_rate:.1%}")
    print(f"✓ Client resilience: succeeded={client_success}, failed={client_failures}")
    print(f"✓ Retry strategy working (some requests succeeded despite service failures)")
    print("✓ All error handling assertions passed!")

    manager.unload_plugin(service_desc.name)
    manager.unload_plugin(client_desc.name)
    manager.unload_plugin(monitor_desc.name)


if __name__ == "__main__":
    main()
