"""
Plugin Manager: Lightweight multiprocessing-based plugin orchestration.

Manages plugin lifecycle, pub/sub messaging, environment variables, and services.
Uses simplified request/response protocol via Channel.
"""

from dataclasses import dataclass
import logging
import sys
import threading
import torch.multiprocessing as mp
import time
from typing import Any, Dict, List, Optional

from parallelazy.utils import is_debugging_enabled, is_debugpy_attached, process_start_spawn
from parallelazy.plugin import PluginDescriptor
from parallelazy.plugin_rpc import PluginRPC, RequestEnvGetAll, RequestEnvUnsubscribe, RequestTopicAnnounce
from parallelazy.plugin_worker import PluginWorker, Queue, RequestPluginCall, RequestTopicSubscribe, RequestTopicUnsubscribe, setup_worker_debugging_env
from parallelazy.plugin_data import  CommandType, ProcessStatus, RequestEnvGet, RequestEnvSet, RequestEnvSubscribe, PluginStatus, Subscriber
from parallelazy.environment_manager import EnvironmentManager


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    
process_start_spawn()
    

@dataclass
class PluginInfo:
    plugin_id: str
    process: mp.Process
    rpc: PluginRPC
    descriptor: PluginDescriptor
    options: Dict[str, Any]
    stop_confirmed: bool = False

class PluginManager:
    """
    Lightweight manager for multiprocessing-based plugin orchestration.
    
    Responsibilities:
    - Plugin lifecycle (load, start, stop, unload)
    - Pub/sub topic routing
    - Environment variable management and subscriptions
    - Service request/response dispatch
    """
    
    def __init__(
        self,
        timeout_request: float = 5.0,
        timeout_start: float = 10.0,
        timeout_start_debug: float = 30.0
    ):
        """
        Initialize plugin manager.
        
        Args:
            timeout_request: Default timeout for sync requests (seconds)
            timeout_start: Timeout for start command (seconds)
            timeout_start_debug: Timeout for start command in debug mode (seconds)
        """
        self._plugins: Dict[str, PluginInfo] = {}  # plugin_id -> PluginInfo
        self._plugins_lock = threading.Lock()  # For thread-safe plugin management

        self._topic_publishers: Dict[str, str] = {}       # topic -> plugin_id
        self._topic_subscribers: Dict[str, Dict[str, Subscriber]] = {}        # topic -> [queue, queue, ...]
        self._env_subscribers: Dict[str, List[str]] = {}  # env_key -> [plugin_id, ...]
        
        self._env = EnvironmentManager()
        
        self.timeout_request = timeout_request
        self.worker_pydebug = is_debugpy_attached()
        self.timeout_start = timeout_start if not self.worker_pydebug else timeout_start_debug


        if self.worker_pydebug:
            setup_worker_debugging_env()


    def load_plugins(self, descriptors: List[PluginDescriptor], parallel: bool = True) -> None:

        """
        Load and initialize multiple plugins.
        
        Args:
            descriptors: List of PluginDescriptor objects
            parallel: Whether to load plugins in parallel
        """

        if parallel:
            barrier = mp.Barrier(len(descriptors) + 1)  # +1 for the main process
            for descriptor in descriptors:
                thread = threading.Thread(target=self._load_plugin_thread, args=(descriptor, barrier))
                thread.start()
            barrier.wait()  # Wait for all plugins to be loaded
        else:
            for descriptor in descriptors:
                self.load_plugin(descriptor)

    def _load_plugin_thread(self, descriptor: PluginDescriptor, barrier: 'mp.Barrier') -> None:
        """
        Thread target for loading a plugin and waiting at the barrier.
        
        Args:
            descriptor: PluginDescriptor object
            barrier: Multiprocessing barrier for synchronization
        """
        self.load_plugin(descriptor)
        barrier.wait()  # Wait for all plugins to be loaded

    def load_plugin(
        self,
        descriptor: PluginDescriptor,
        options: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Load and initialize a plugin (spawn subprocess).
        
        Args:
            descriptor: Plugin descriptor
            options: Initial options for plugin (passed to plugin on start)
        
        Returns:
            True if loaded successfully
        """
        plugin_id = descriptor.name
        options = options or descriptor.options or {}
        
        try:
            plugin_rpc = PluginRPC(plugin_id)
            self._init_plugin_rpc(plugin_rpc)
            worker_channel = plugin_rpc.channel.remote_descriptor()  # For worker to manager communication
            initialized = mp.Event()
            initialized.clear()  # Ensure not set

            # Spawn worker process
            process = mp.Process(
                target=PluginWorker.run,
                args=(plugin_id, descriptor.to_dict(), options, worker_channel, initialized), 
                name=f"plugin-{plugin_id}"
            )
            
            
            self._plugins[plugin_id] = PluginInfo(
                plugin_id=plugin_id,
                process=process,
                rpc=plugin_rpc,
                descriptor=descriptor,
                options=options
            )
            
            process.start()
            is_ready = False
            while process.is_alive():
                is_ready = initialized.wait(timeout=self.timeout_start)
                if is_ready: break
                # If we get here, wait() timed out but the process is still alive
                logger.info(f"Plugin {plugin_id} is still initializing (waiting...)")

            # If the loop exited and is_ready is False, the process died before setting the event
            if not is_ready:
                logger.error(f"Plugin {plugin_id} failed to initialize (process terminated unexpectedly).")
                return False

            logger.info(f"Plugin {plugin_id} loaded")
            return True
            
        except Exception as e:
            logger.error(f"Error loading plugin {descriptor}: {e}")
            return False
    
    def start_plugin(self, plugin_id: str) -> bool:
        """
        Start a loaded plugin and register subscriptions.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            True if started successfully
        """
        if plugin_id not in self._plugins:
            logger.error(f"Plugin {plugin_id} not loaded")
            return False
        
        try:
            info = self._plugins[plugin_id]
            pending = info.rpc.plugin_start()
            res = pending.wait(timeout=self.timeout_start)
            if not res:
                logger.warning(f"Plugin {plugin_id} failed to start within {self.timeout_start} seconds")
                return False
            if res.error:
                logger.error(f"Plugin {plugin_id} start error: {res.error}")
                return False

            return res.result
        except Exception as e:
            logger.error(f"Error starting plugin {plugin_id}: {e}")
            return False
        
    
    def stop_plugin(self, plugin_id: str) -> bool:
        """
        Stop a running plugin.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            True if stopped successfully
        """
        if plugin_id not in self._plugins:
            return False
        
        try:
            info = self._plugins[plugin_id]
            if not info.process.is_alive(): return True  # Already stopped


            pending = info.rpc.plugin_stop()
            res = pending.wait(timeout=self.timeout_start)
            if not res:
                logger.warning(f"Plugin {plugin_id} failed to stop within {self.timeout_start} seconds")
                return False
            if res.error:
                logger.error(f"Plugin {plugin_id} stop error: {res.error}")
                return False
            
            logger.info(f"Plugin {plugin_id} stop: {res.result}")
            if res.result:
                info.stop_confirmed = True
            return res.result
        except Exception as e:
            logger.error(f"Error stopping plugin {plugin_id}: {e}")
            return False
    
    def unload_plugin(self, plugin_id: str) -> bool:
        """
        Unload a plugin and terminate its process.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            True if unloaded successfully
        """
        try:
            if plugin_id in self._plugins:
                info = self._plugins[plugin_id]
                if info.process.is_alive() and not info.stop_confirmed:
                    self.stop_plugin(plugin_id)
            
            if plugin_id in self._plugins:
                info = self._plugins[plugin_id]
                process = info.process
                
                info.rpc.shutdown()  # Clean up RPC resources

                process.join(timeout=self.timeout_start)

                if process.is_alive():
                    process.terminate()
                    process.join(timeout=self.timeout_start)
                    
                if process.is_alive():
                    process.kill()
                
                # Clean up environment subscriptions
                for env, plugins in list(self._env_subscribers.items()):
                    if plugin_id in plugins:
                        plugins.remove(plugin_id)
                    if len(plugins) == 0:
                        del self._env_subscribers[env]

                # Clean up topic subscriptions
                for topic, plugins in list(self._topic_subscribers.items()):
                    if plugin_id in plugins.keys():
                        del plugins[plugin_id] 

                # Clean up topic publishers
                for topic, publisher in list(self._topic_publishers.items()):
                    if publisher == plugin_id:
                        del self._topic_publishers[topic]
                
                del self._plugins[plugin_id]
            
            logger.info(f"Plugin {plugin_id} unloaded")
            return True
            
        except Exception as e:
            import traceback
            dump = traceback.print_exc()
            logger.error(f"Error unloading plugin {plugin_id}: {e}\n{dump}", stack_info=True)
            return False
        
    def start_all_plugins(self) -> None:
        """Start all loaded plugins."""
        for plugin_id in list(self._plugins.keys()):
            self.start_plugin(plugin_id)
    
    def stop_all_plugins(self) -> None:
        """Stop all running plugins."""
        for plugin_id in list(self._plugins.keys()):
            self.stop_plugin(plugin_id)

    
    def unload_all_plugins(self) -> None:
        """Shut down the plugin manager and all plugins."""
        self.stop_all_plugins()
        for plugin_id in list(self._plugins.keys()):
            self.unload_plugin(plugin_id)

    # ========================================================================
    # Utility Methods for Plugin Management
    # ========================================================================

    
     
    def plugin_status(self, plugin_id: str) -> Optional[ProcessStatus]:
        """
        Get status of a plugin.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            ProcessStatus object, or None if not found
        """
        if plugin_id not in self._plugins:
            return None
        
        info = self._plugins[plugin_id]
        process = info.process
        alive = process.is_alive()
        pid = process.pid
        worker_status = None
        
        if alive:
            try:
                pending = info.rpc.plugin_status()
                status_response = pending.wait(timeout=1.0)
                if status_response is None:
                    logger.debug(f"[{plugin_id}] No status received")
                elif status_response.error:
                    logger.debug(f"[{plugin_id}] Error getting status: {status_response.error}")
                else:
                    worker_status = status_response.result
            except Exception as e:
                logger.debug(f"[{plugin_id}] Failed to get status: {e}")
        
        return ProcessStatus(
            plugin_id=plugin_id,
            process_alive=alive,
            process_pid=pid,
            worker_status=worker_status
        )
    
    def plugin_status_all(self) -> Dict[str, ProcessStatus]:
        """
        Get status of all plugins.
        
        Returns:
            Dictionary of plugin_id -> ProcessStatus
        """
        return {
            plugin_id: self.plugin_status(plugin_id)
            for plugin_id in self._plugins.keys()
        }
    
    def plugin_list(self) -> List[str]:
        """List all loaded plugin IDs."""
        return list(self._plugins.keys())
    
    
    
    

    # ========================================================================
    # Environment Variable Management
    # ========================================================================

    def env_set(self, name:str, value:Any) -> None:
        self._env.set(name, value)
        threading.Thread(target=self._notify_env_update, args=(name,), daemon=True).start()
        return True

    def _notify_env_update(self, name:str) -> None:
        # Notify subscribed plugins
        if name in self._env_subscribers:
            for plugin_id in self._env_subscribers[name]:
                if plugin_id in self._plugins:
                    rpc = self._plugins[plugin_id].rpc
                    rpc.env_update(name)

    def env_get(self, name:str=None, default:Any=None) -> Any:
        if name is None:
            return self._env.get_all()
        return self._env.get(name, default)
    

    def env_subscribe(self, plugin_id: str, var_name: str) -> bool:
        if plugin_id not in self._plugins:
            return False

        if var_name not in self._env_subscribers:
            self._env_subscribers[var_name] = []

        if plugin_id not in self._env_subscribers[var_name]:
            self._env_subscribers[var_name].append(plugin_id)

        return True

    def env_unsubscribe(self, plugin_id: str, var_name: str) -> bool:   
        if plugin_id not in self._plugins:
            return False

        if var_name in self._env_subscribers and plugin_id in self._env_subscribers[var_name]:
            self._env_subscribers[var_name].remove(plugin_id)
            if not self._env_subscribers[var_name]:
                del self._env_subscribers[var_name]
        return True
    
    # ========================================================================
    # Service Management (Optional, for future extension)
    # ========================================================================

    def topic_publish(self, topic: str, data: Any):
        """
        Publish data to a topic.
        
        Args:
            topic: Topic name
            data: Message data
        
        Returns:
            Number of subscribers that received the message
        """
        
        if topic not in self._topic_subscribers: return True
    
        for subscriber in self._topic_subscribers[topic].values():
            try:
                subscriber.queue.put_nowait(data)
            except Exception as e:
                logger.debug(f"Failed to deliver to topic '{topic}': {e}")
        
        return True

    def topic_list(self) -> List[str]:
        return list(self._topic_subscribers.keys())

    # ========================================================================
    # PLUGIN Lifecycle
    # ========================================================================



    # ========================================================================
    # RPC Responder: Handle requests from plugins
    # ========================================================================
    
    def _init_plugin_rpc(self, plugin_rpc: PluginRPC) -> None:
        """Initialize RPC callbacks for a master.

        #PLUGIN_START = "plugin_start"
        #PLUGIN_STOP = "plugin_stop"
        #PLUGIN_STATUS = "plugin_status"
        #PLUGIN_STATUS_ALL = "plugin_status_all"
        #PLUGIN_ERRORS = "plugin_errors"
        PLUGIN_CALL = "plugin_call"

        ENV_SET = "env_set"
        ENV_GET = "env_get"
        ENV_GET_ALL = "env_get_all"
        ENV_SUBSCRIBE = "env_subscribe"
        ENV_UNSUBSCRIBE = "env_unsubscribe"
        #ENV_UPDATE = "env_update"

        TOPIC_SUBSCRIBE = "topic_subscribe"
        TOPIC_UNSUBSCRIBE = "topic_unsubscribe"
        TOPIC_PUBLISH = "topic_publish"
        TOPIC_LIST = "topic_list"
        """

        #TODO: Handy but later
        #plugin_rpc.on_request(CommandType.PLUGIN_START, self._handle_plugin_start)
        #plugin_rpc.on_request(CommandType.PLUGIN_STOP, self._handle_plugin_stop)
        plugin_rpc.on_request(CommandType.PLUGIN_CALL, self._respond_call)  # forward
        #plugin_rpc.on_request(CommandType.PLUGIN_STATUS, self.get_plugin_status) 
        #plugin_rpc.on_request(CommandType.PLUGIN_STATUS_ALL, self.get_all_plugin_status)

        plugin_rpc.on_request(CommandType.ENV_SET, self._respond_env_set)
        plugin_rpc.on_request(CommandType.ENV_GET, self._respond_env_get)
        plugin_rpc.on_request(CommandType.ENV_GET_ALL, self._respond_env_get_all)
        plugin_rpc.on_request(CommandType.ENV_SUBSCRIBE, self._respond_env_subscribe) 
        plugin_rpc.on_request(CommandType.ENV_UNSUBSCRIBE, self._respond_env_unsubscribe)
        
        plugin_rpc.on_request(CommandType.TOPIC_SUBSCRIBE, self._respond_topic_subscribe)
        plugin_rpc.on_request(CommandType.TOPIC_UNSUBSCRIBE, self._respond_topic_unsubscribe)
        plugin_rpc.on_request(CommandType.TOPIC_ANNOUNCE, self._respond_topic_announce)
        plugin_rpc.on_request(CommandType.TOPIC_LIST, self._respond_topic_list)

    # ========================================================================
    # LIFECYCLE: 
    # ========================================================================



    def _respond_call(self, rpc: PluginRPC, req: RequestPluginCall) -> Any:
        
        if req.plugin_id not in self._plugins:
            raise ValueError(f"Plugin {req.plugin_id} not found")
        
        dst_rpc = self._plugins[req.plugin_id].rpc
        remote_result = dst_rpc.forward(req)
        
        return remote_result
    
    # ========================================================================
    # ENVIRONMENT: Variables and Subscriptions
    # ========================================================================
    
    def _respond_env_set(self, rpc: PluginRPC, req: RequestEnvSet) -> None:
        return self.env_set(req.name, req.value)
    
    def _respond_env_get(self, rpc: PluginRPC, req: RequestEnvGet) -> Any:
        return self.env_get(req.name)
    
    def _respond_env_get_all(self, rpc: PluginRPC, req: RequestEnvGetAll) -> Dict[str, Any]:
        return self.env_get()
    
    def _respond_env_subscribe(self, rpc: PluginRPC, req: RequestEnvSubscribe) -> bool:
        return self.env_subscribe(req.sender, req.name)

    def _respond_env_unsubscribe(self, rpc: PluginRPC, req: RequestEnvUnsubscribe) -> bool:
        return self.env_unsubscribe(req.sender, req.name)

    # ========================================================================
    # TOPIC PUB/SUB
    # ========================================================================
    

    def _respond_topic_subscribe(self, rpc: PluginRPC, req: RequestTopicSubscribe) -> bool:
        """
        Subscribe a plugin to a topic.
        Allow subscription also for non (yet, hopefully) existing topics.
        """
        
        sub  = req.subscriber
        topic = sub.topic
        plugin_sub = req.sender

        if plugin_sub not in self._plugins: return False

        plugin_pub = self._topic_publishers.get(topic,None)
        if plugin_pub is not None and plugin_pub not in self._plugins: return False

        if topic not in self._topic_subscribers:
            self._topic_subscribers[topic] = {}

        if plugin_sub not in self._topic_subscribers[topic]:
            self._topic_subscribers[topic][plugin_sub] = sub

        remote_result = True  # Default to success

        if plugin_pub is not None:
            forward_rpc = self._plugins[plugin_pub].rpc
            remote_result = forward_rpc.forward(req)
            logger.info("MANAGER: topic_subscribe: remote_result: %s", remote_result)

        if not remote_result: return False


        
        return True


    def _respond_topic_unsubscribe(self, rpc: PluginRPC, req: RequestTopicUnsubscribe) -> bool:
        """
        Unsubscribe a plugin from a topic.
        
        Args:
            plugin_id: Plugin identifier
            topic: Topic name
        
        Returns:
            True if unsubscription successful
        """
        sub  = req.subscriber
        topic = sub.topic
        src_id = sub.plugin_id

        if src_id not in self._plugins: return False
        if topic not in self._topic_publishers: return False

        dst_id = self._topic_publishers[topic]

        forward_rpc = self._plugins[dst_id].rpc
        remote_result = forward_rpc.forward(req)

        if not remote_result: return False
        

        if src_id in self._topic_subscribers[topic]:
            del self._topic_subscribers[topic][src_id]
            if not self._topic_subscribers[topic]:
                del self._topic_subscribers[topic]


        return True

    def _respond_topic_announce(self, rpc: PluginRPC, req: RequestTopicAnnounce) -> Dict[str, Subscriber]:
        """
        Announce a new topic.
        
        """
        self._topic_publishers[req.topic] = req.sender

        if req.topic not in self._topic_subscribers:
            self._topic_subscribers[req.topic] = {}
        subs = self._topic_subscribers[req.topic]

        return subs

    def _respond_topic_list(self) -> List[str]:
        return self.topic_list()