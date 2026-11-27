"""
parallelazy Plugin System: Lightweight multiprocessing-based plugin architecture.

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


from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _package_version

try:
    __version__ = _package_version("parallelazy")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

from parallelazy.__main__ import (
    get_agent_guide,
    get_agent_guide_path,
    install_skill,
)
from parallelazy.environment_manager import EnvironmentManager
from parallelazy.plugin import Plugin, PluginDescriptor, PluginState
from parallelazy.plugin_data import (
    Channel,
    CommandType,
    ManagerOptions,
    Message,
    PendingRequest,
    PluginStatus,
    ProcessStatus,
    Queue,
    QueueDescriptor,
    QueueReceive,
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
from parallelazy.plugin_monitor import (
    CapabilityAvailability,
    EnvironmentMetric,
    MonitoringSnapshot,
    PipelineMonitor,
    ProcessMetric,
    TopicMetric,
)
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
    IpcEventPoolManaged,
    mark_graph_output,
    sanitize_for_ipc,
)
from parallelazy.plugin_worker import PluginWorker
from parallelazy.utils import is_debugpy_attached

__all__ = [
    "__version__",
    "EnvironmentManager",
    "Plugin",
    "PluginDescriptor",
    "PluginState",
    "Channel",
    "CommandType",
    "ManagerOptions",
    "Message",
    "PendingRequest",
    "PluginStatus",
    "ProcessStatus",
    "Queue",
    "QueueDescriptor",
    "QueueReceive",
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
    "IpcEventPoolManaged",
    "mark_graph_output",
    "sanitize_for_ipc",
    "is_debugpy_attached",
    "CapabilityAvailability",
    "EnvironmentMetric",
    "MonitoringSnapshot",
    "PipelineMonitor",
    "ProcessMetric",
    "TopicMetric",
    "get_agent_guide",
    "get_agent_guide_path",
    "install_skill",
]
