"""
Base Plugin Interface: Abstract base class for all plugins.

Defines the lifecycle, subscription, and service interfaces that all
plugins must implement or can override.
"""

from typing import Any, Dict, Optional, TYPE_CHECKING, Self
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
        package: Full package path (e.g., "modularnet.plugins.my_plugin")
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
    Base class for all ModularNet plugins.
    
    Plugins run in isolated processes and communicate via:
    1. Control channel: Load/unload/configure/service commands
    2. Data plane: Pub/sub topics for high-bandwidth data exchange
    3. Env plane: Shared environment variables with change notifications
    4. Manager proxy: Direct access to manager services (get/set env, publish, invoke)
    
    Subclasses must implement:
    - on_start: Initialize plugin resources (optional)
    - on_stop: Clean up resources (optional)
    - main: Main execution loop (optional)
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
            raise ValueError(f"Plugin '{key}' is already registered.")
            
        cls._registry[key] = cls


    @classmethod
    def default_options(cls) -> Dict[str, Any]:
        return {}

    @classmethod
    def Descriptor(cls, version:str, author:str, description:str=None, name:str=None, options: Dict[str, Any] = None) -> PluginDescriptor:
        all_options = cls.default_options().copy()
        all_options.update(options or {})


        return PluginDescriptor(
            version=version,
            author=author,
            options=all_options,
            name= camel_to_kebab(name or cls.__name__),
            package=f"{cls.__module__}",
            class_name=cls.__name__,
            description=description or f"{cls.__doc__}",
            
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
    
    
    def main(self): pass
    
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
        """Stop the plugin."""
        
        self._worker.stop()

    def topic_subscribe(self, topic:str, callback:str) -> None:
        """Subscribe to a data topic or environment variable."""
        self._worker.topic_subscribe(topic, callback)

    def topic_publish(self, topic:str, data:Any) -> None:
        """Publish data to a topic."""
        self._worker.topic_publish(topic, data)

    def env_set(self, name:str, value:Any, wait: bool = True) -> None:
        """Set an environment variable."""
        self._worker.env_set(name, value, wait=wait)

    def env_get(self, name:str=None, default:Any=None) -> dict|Any:
        """Get an environment variable."""
        if name is None:
            return self._worker.env_get_all() 

        return self._worker.env_get(name) or default
    
    def env_subscribe(self, name:str, callback:str) -> None:
        """Subscribe to changes in an environment variable."""
        self._worker.env_subscribe(name, callback)
    
    def plugin_call(self, plugin_id: str, method_name: str, *args, **kwargs) -> Any:
        """Call a method provided by the manager or another plugin."""
        return self._worker.plugin_call(plugin_id, method_name, *args, **kwargs)