"""
Base Plugin Interface: Abstract base class for all plugins.

Defines the lifecycle, subscription, and service interfaces that all
plugins must implement or can override.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING, Self
from enum import Enum

from parallelazy.utils import camel_to_kebab


if TYPE_CHECKING:
    from parallelazy.plugin_worker import PluginWorker 


class PluginState(Enum):
    """Plugin lifecycle states."""
    INITIALIZED = "initialized"
    LOADED = "loaded"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"

    def __str__(self): return str(self.value)

"""
Plugin Descriptor: Serializable metadata for dynamic plugin loading.

Describes a plugin's identity, location, and configuration options.
"""

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional
import json


@dataclass
class PluginDescriptor:
    """
    Serializable descriptor for plugin metadata and instantiation.
    
    Attributes:
        name: Unique plugin identifier
        version: Semantic version (e.g., "1.0.0")
        package: Full package path (e.g., "parallelazy.plugins.my_plugin")
        class_name: Plugin class name in the package
        description: Human-readable description
        author: Plugin author
        options: Default configuration options for the plugin
        dependencies: List of required plugin names
    """
    name: str
    version: str
    package: str
    class_name: str
    description: str = ""
    author: str = ""
    options: Dict[str, Any] = field(default_factory=dict)
    dependencies: list = field(default_factory=list)
    work_dir: Optional[str] = None
    log_dir: Optional[str] = None
    log_level: Optional[int] = None
    display_logs: Optional[bool] = None
    write_logs: Optional[bool] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert descriptor to dictionary (JSON-serializable)."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Serialize descriptor to JSON."""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PluginDescriptor":
        """Create descriptor from dictionary."""
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> "PluginDescriptor":
        """Create descriptor from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    def __str__(self) -> str:
        return f"{self.name}@{self.version} ({self.package}.{self.class_name})"
    
    def __repr__(self) -> str:
        return f"PluginDescriptor({self})"


class Plugin:
    """
    Base class for all parallelazy plugins.
    
    Plugins run in isolated processes and communicate via:
    1. Control channel: Load/unload/configure/service commands
    2. Data plane: Pub/sub topics for high-bandwidth data exchange
    3. Env plane: Shared environment variables with change notifications
    4. Manager proxy: Direct access to manager services (get/set env, publish, invoke)
    
    Subclasses must implement:
    - on_start: Initialize plugin resources (optional)
    - on_stop: Clean up resources (optional)
    - main: Main-thread work hook, called once after on_start (optional)
    - on_error: Handle errors (optional)


    Direct manager access via self.manager:
        model = self.env_get("model")
        self.env_set("status", "ready")
        self.topic_publish("topic", data)
    """

    _registry: dict[str, type[Self]] = {}
        
    def __init_subclass__(cls, plugin_name: str | None = None, **kwargs):
        super().__init_subclass__(**kwargs)
        
        # Determine registry key: custom plugin_name or default class name
        key = plugin_name or cls.__name__
        
        if key in cls._registry:
            logging.getLogger(__name__).debug(
                "Overwriting existing plugin registration for '%s'", key
            )
            
        cls._registry[key] = cls


    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {}

    @classmethod
    def Descriptor(
        cls,
        version: str,
        author: str,
        description: str = None,
        name: str = None,
        options: Dict[str, Any] = None,
        work_dir: Optional[str] = None,
        log_dir: Optional[str] = None,
        log_level: Optional[int] = None,
        display_logs: Optional[bool] = None,
        write_logs: Optional[bool] = None,
    ) -> PluginDescriptor:
        all_options = cls.default_options().copy()
        all_options.update(options or {})

        resolved_description = (
            description
            if description is not None
            else (cls.__doc__.strip() if cls.__doc__ else "")
        )

        return PluginDescriptor(
            version=version,
            author=author,
            options=all_options,
            name= camel_to_kebab(name or cls.__name__),
            package=f"{cls.__module__}",
            class_name=cls.__name__,
            description=resolved_description,
            work_dir=work_dir,
            log_dir=log_dir,
            log_level=log_level,
            display_logs=display_logs,
            write_logs=write_logs,
            
        )

    def __init__(self, worker: "PluginWorker", plugin_id: str, options: Optional[Dict[str, Any]] = None):
        """
        Initialize plugin.
        
        Args:
            plugin_id: Unique identifier for this plugin instance
            config: Configuration options dictionary
            worker: PluginWorker instance for managing the plugin
        """
        all_options = self.default_options().copy()
        all_options.update(options or {})

        self._worker = worker
        self.plugin_id = plugin_id
        self.state = PluginState.INITIALIZED
        self.options = all_options
        self._is_running = False
        self._work_dir = worker.work_dir
        self.logger = logging.getLogger(self.__class__.__module__)

    @property
    def work_dir(self) -> str:
        """Absolute working directory resolved for this plugin instance."""
        return self._work_dir

    def set_log_formats(
        self,
        file_format: Optional[str] = None,
        console_format: Optional[str] = None,
    ) -> None:
        """Change this worker's file and/or console log format."""
        self._worker.set_log_formats(file_format, console_format)
    
    
    def main(self) -> None:
        """Run once on the worker's main thread after startup."""
        pass

    def wait_for_stop(self, timeout: Optional[float] = None) -> bool:
        """Wait for a stop request, typically from a blocking main() method."""
        return self._worker.wait_for_stop(timeout)
    
    def on_start(self) -> None:
        """
        Called when plugin is started.
        
        Initialize resources, open connections, start background tasks, etc.
        Raise exception on failure.
        """
        pass
    
    def on_stop(self) -> None:
        """
        Called when plugin is stopped.
        
        Clean up resources, close connections, stop background tasks, etc.
        """
        pass
    
    def on_error(self, error: Exception, source: str=None) -> None:
        """ Called when an error occurs in the plugin."""
        pass
    
    @property
    def is_running(self) -> bool:
        """Whether plugin is currently running."""
        return self._is_running
    
    def stop(self) -> None:
        """Request plugin shutdown; use wait_for_stop() in blocking main() work."""
        self._worker.stop()

    def topic_subscribe(self, topic: str, callback: Callable) -> bool:
        """Subscribe to a data topic."""
        return self._worker.topic_subscribe(topic, callback)

    def topic_unsubscribe(self, topic: str) -> bool:
        """Unsubscribe from a data topic."""
        return self._worker.topic_unsubscribe(topic)

    def topic_publish(self, topic: str, data: Any) -> int:
        """Publish data to a topic. Returns the number of subscribers that received the message."""
        return self._worker.topic_publish(topic, data)

    def topic_list(self, plugin_id: Optional[str] = None) -> List[str]:
        """List available topics, or topics published by a specific plugin."""
        return self._worker.topic_list(plugin_id=plugin_id)

    def env_set(self, name: str, value: Any, wait: bool = True) -> bool:
        """Set an environment variable."""
        return self._worker.env_set(name, value, wait=wait)

    def env_get(self, name: str = None, default: Any = None) -> dict | Any:
        """Get an environment variable."""
        if name is None:
            return self._worker.env_get_all()

        return self._worker.env_get(name, default=default)
    
    def env_subscribe(self, name: str, callback: Callable) -> bool:
        """Subscribe to changes in an environment variable."""
        return self._worker.env_subscribe(name, callback)

    def env_unsubscribe(self, name: str) -> bool:
        """Unsubscribe from changes in an environment variable."""
        return self._worker.env_unsubscribe(name)
    
    def plugin_call(self, plugin_id: str, method_name: str, *args, **kwargs) -> Any:
        """Call a method provided by the manager or another plugin."""
        return self._worker.plugin_call(plugin_id, method_name, *args, **kwargs)

    def plugin_errors(self, plugin_id: Optional[str] = None) -> List[str]:
        """Get recorded error messages for this plugin or another plugin."""
        return self._worker.plugin_errors(plugin_id)

    def plugin_load(
        self,
        descriptor: PluginDescriptor,
        options: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Load a plugin instance from its descriptor."""
        return self._worker.plugin_load(descriptor, options)

    def plugin_start(self, plugin_id: str) -> bool:
        """Start another loaded plugin."""
        return self._worker.plugin_start(plugin_id)

    def plugin_stop(self, plugin_id: str) -> bool:
        """Stop another loaded plugin."""
        return self._worker.plugin_stop(plugin_id)

    def plugin_unload(self, plugin_id: str) -> bool:
        """Unload another plugin."""
        return self._worker.plugin_unload(plugin_id)

    def plugin_status(self, plugin_id: str = None) -> Any:
        """Get the status of this plugin or another loaded plugin."""
        return self._worker.plugin_status(plugin_id)

    def plugin_status_all(self) -> Dict[str, Any]:
        """Get the status of all loaded plugins."""
        return self._worker.plugin_status_all()