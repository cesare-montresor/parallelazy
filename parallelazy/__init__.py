"""
ModularNet Plugin System: Lightweight multiprocessing-based plugin architecture.

Provides modular plugin execution with pub/sub messaging, environment variables,
and service invocation.

Main Components:
- PluginDescriptor: Serializable plugin metadata
- Plugin: Abstract base class for plugins
- PluginManager: Plugin orchestration and management
- PluginWorker: Plugin subprocess worker
- PluginMessages: Unified IPC message protocol (Request, Response)
- DataExchange: Unified subscription model (topics + env vars)
- ManagerProxy: Client-side proxy for direct manager access from plugins
"""

from parallelazy.plugin import Plugin, PluginState, PluginDescriptor
from parallelazy.environment_manager import EnvironmentManager
from parallelazy.plugin_manager import PluginManager

from parallelazy.plugin_data import (
    Request, Response,
    Subscription, 
    PluginStatus, ProcessStatus
)
from parallelazy.plugin_rpc import PluginRPC

__all__ = [
    "PluginDescriptor",
    "Plugin",
    "PluginState",
    "EnvironmentManager",
    "EnvUpdate",
    "PluginManager",
    "Request",
    "Response",
    "Channel",
    "Subscription",
    "SubscriptionType",
    "PluginStatus",
    "ProcessStatus",
    "PluginRPC",
]
