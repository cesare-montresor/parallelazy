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

from parallelazy.environment_manager import EnvironmentManager
from parallelazy.plugin import Plugin, PluginDescriptor, PluginState
from parallelazy.plugin_data import (
    Channel,
    CommandType,
    Message,
    PendingRequest,
    PluginStatus,
    ProcessStatus,
    Queue,
    QueueDescriptor,
    QueueRecive,
    QueueSend,
    Request,
    RequestCallback,
    RequestEnvGet,
    RequestEnvGetAll,
    RequestEnvSet,
    RequestEnvSubscribe,
    RequestEnvUnsubscribe,
    RequestEnvUpdate,
    RequestPluginCall,
    RequestPluginErrors,
    RequestPluginStart,
    RequestPluginStatus,
    RequestPluginStatusAll,
    RequestPluginStop,
    RequestTopicAnnounce,
    RequestTopicList,
    RequestTopicSubscribe,
    RequestTopicUnsubscribe,
    Response,
    ResponseCallback,
    Subscriber,
    Subscription,
)
from parallelazy.plugin_manager import PluginInfo, PluginManager
from parallelazy.plugin_rpc import (
    MessageUID,
    PluginCoreRPC,
    PluginEnvRPC,
    PluginLifeCycleRPC,
    PluginRPC,
    PluginTopicRPC,
)
from parallelazy.plugin_transport import (
    IpcEventCuda,
    IpcEventPool,
    IpcEventPoolManaged,
    mark_graph_output,
    sanitize_for_ipc,
)
from parallelazy.plugin_worker import PluginWorker

__all__ = [
    "EnvironmentManager",
    "Plugin",
    "PluginDescriptor",
    "PluginState",
    "Channel",
    "CommandType",
    "Message",
    "PendingRequest",
    "PluginStatus",
    "ProcessStatus",
    "Queue",
    "QueueDescriptor",
    "QueueRecive",
    "QueueSend",
    "Request",
    "RequestCallback",
    "RequestEnvGet",
    "RequestEnvGetAll",
    "RequestEnvSet",
    "RequestEnvSubscribe",
    "RequestEnvUnsubscribe",
    "RequestEnvUpdate",
    "RequestPluginCall",
    "RequestPluginErrors",
    "RequestPluginStart",
    "RequestPluginStatus",
    "RequestPluginStatusAll",
    "RequestPluginStop",
    "RequestTopicAnnounce",
    "RequestTopicList",
    "RequestTopicSubscribe",
    "RequestTopicUnsubscribe",
    "Response",
    "ResponseCallback",
    "Subscriber",
    "Subscription",
    "PluginInfo",
    "PluginManager",
    "MessageUID",
    "PluginCoreRPC",
    "PluginEnvRPC",
    "PluginLifeCycleRPC",
    "PluginRPC",
    "PluginTopicRPC",
    "PluginWorker",
    "IpcEventCuda",
    "IpcEventPool",
    "IpcEventPoolManaged",
    "mark_graph_output",
    "sanitize_for_ipc",
]
