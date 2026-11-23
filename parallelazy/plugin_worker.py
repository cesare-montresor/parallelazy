"""
Plugin Worker: Subprocess handler for plugin execution.

PluginWorker.run() is the entry point for each plugin subprocess.
It creates plugin instance, handles incoming requests, and routes to handlers.

Command dispatch:
  - All incoming requests routed via @command_handler decorator
  - Services invoked via reflection (self.plugin.method_name(*args, **kwargs))
  - No manual if/elif chains
"""


import logging
import importlib
import threading
from multiprocessing.connection import wait as conn_wait
from multiprocessing.reduction import ForkingPickler
from multiprocessing.sharedctypes import Synchronized
import torch

from parallelazy.utils import init_debugger, is_debugging_enabled
from parallelazy.plugin_data import (
    Channel,
    CommandType,
    PluginStartupPhase,
    ProcessStatus,
    Queue,
    QueueDescriptor,
    PendingRequest,
    PluginStatus,
    RequestPluginErrors,
    RequestPluginStart,
    RequestPluginStatus,
    RequestPluginStop,
    RequestTopicList,
    Subscriber,
    Subscription,
    _TOPIC_SHUTDOWN_SENTINEL,
    _TopicShutdownSentinel,
)
from parallelazy.plugin import Plugin, PluginDescriptor, PluginState
from parallelazy.plugin_logging import (
    LoggingSession,
    PluginRuntimeSettings,
    configure_worker_logging,
    configure_worker_output,
    default_runtime_settings,
)

from parallelazy.plugin_rpc import  PluginRPC, RequestEnvUpdate, RequestPluginCall, RequestTopicList, RequestTopicSubscribe, RequestTopicUnsubscribe, sanitize_for_ipc
from multiprocessing.synchronize import Event


#typechecking imports
from typing import Any, Callable, Dict,  Optional, TYPE_CHECKING



logger = logging.getLogger(__name__)


def _set_startup_phase(
    startup_phase: Optional[Synchronized],
    phase: PluginStartupPhase,
) -> None:
    if startup_phase is not None:
        startup_phase.value = int(phase)


def _contains_torch_tensor(obj: Any, _seen: Optional[set] = None) -> bool:
    if isinstance(obj, torch.Tensor):
        return True
    if isinstance(obj, (str, bytes, int, float, bool, type(None))):
        return False
    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if obj_id in _seen:
        return False
    if isinstance(obj, dict):
        _seen.add(obj_id)
        return any(
            _contains_torch_tensor(k, _seen) or _contains_torch_tensor(v, _seen)
            for k, v in obj.items()
        )
    if isinstance(obj, (list, tuple, set, frozenset)):
        _seen.add(obj_id)
        return any(_contains_torch_tensor(item, _seen) for item in obj)
    return False


def _deliver_to_subscribers(
    topic: str,
    data: Any,
    subscribers: list,
    log: logging.Logger,
) -> tuple[int, list[str]]:
    """Fan out a payload to subscribers, isolating failures and reporting dead IDs."""
    payload_bytes = None
    if len(subscribers) > 1 and not _contains_torch_tensor(data):
        try:
            payload_bytes = bytes(ForkingPickler.dumps(data))
        except Exception:
            payload_bytes = None

    delivered = 0
    dead_ids: list[str] = []
    for sub_id, subscriber in subscribers:
        try:
            if payload_bytes is not None:
                subscriber.queue.send_bytes(payload_bytes)
            else:
                subscriber.queue.put(data)
            delivered += 1
        except (BrokenPipeError, EOFError, OSError, ValueError) as e:
            log.warning(
                "Subscriber '%s' on topic '%s' is unavailable (%s); evicting",
                sub_id,
                topic,
                e,
            )
            dead_ids.append(sub_id)
            subscriber.close()
        except Exception as e:
            log.error(
                "Failed to publish to subscriber '%s' on topic '%s': %s",
                sub_id,
                topic,
                e,
                exc_info=True,
            )
    return delivered, dead_ids


# ============================================================================
# PLUGIN WORKER
# ============================================================================

class PluginWorker:
    """
    Subprocess worker for running a plugin.
    
    PluginWorker.run() is called in a new subprocess and:
    1. Instantiates the plugin
    2. Runs an event loop waiting for requests
    3. Routes requests to command handlers
    4. Sends responses back
    """
    
    @staticmethod
    def run(
        plugin_id: str,
        descriptor_dict: Dict[str, Any],
        options: Dict[str, Any],
        queue_descriptor: QueueDescriptor,
        initialized: Event,
        runtime_settings: Optional[PluginRuntimeSettings] = None,
        startup_phase: Optional[Synchronized] = None,
    ) -> None:
        """
        Entry point for plugin subprocess.
        
        Args:
            plugin_id: Plugin instance identifier
            descriptor_dict: Serialized plugin descriptor
            options: Plugin options
            queue_descriptor: Queue descriptor for communication
            initialized: Event to signal initialization completion
            runtime_settings: Resolved paths and logging settings for this worker
            startup_phase: Shared startup phase state for manager visibility
        """
        _set_startup_phase(startup_phase, PluginStartupPhase.PROCESS_STARTED)
        logging_session = None
        output_capture = None
        try:
            runtime_settings = runtime_settings or default_runtime_settings()
            logging_session = configure_worker_logging(runtime_settings, plugin_id)
            if is_debugging_enabled():
                _set_startup_phase(startup_phase, PluginStartupPhase.WAITING_FOR_DEBUGGER)
                init_debugger(plugin_id)
            _set_startup_phase(startup_phase, PluginStartupPhase.INITIALIZING)
            output_capture = configure_worker_output(
                logging.getLogger(),
                display_logs=runtime_settings.display_logs,
            )

            worker = PluginWorker(
                plugin_id,
                descriptor_dict,
                options,
                queue_descriptor,
                initialized,
                runtime_settings=runtime_settings,
                logging_session=logging_session,
                startup_phase=startup_phase,
            )
            worker.main()
        except Exception:
            if (
                startup_phase is not None
                and startup_phase.value < PluginStartupPhase.READY
            ):
                _set_startup_phase(startup_phase, PluginStartupPhase.FAILED)
            raise
        finally:
            if output_capture is not None:
                output_capture.close()
            if logging_session is not None:
                logging_session.close()
    
    def __init__(
        self,
        plugin_id: str,
        descriptor_dict: Dict[str, Any],
        options: Dict[str, Any],
        queue_descriptor: QueueDescriptor,
        initialized: Event,
        runtime_settings: Optional[PluginRuntimeSettings] = None,
        logging_session: Optional[LoggingSession] = None,
        startup_phase: Optional[Synchronized] = None,
    ):
        """Initialize worker."""

        runtime_settings = runtime_settings or default_runtime_settings()
        owns_logging = logging_session is None
        if logging_session is None:
            logging_session = configure_worker_logging(runtime_settings, plugin_id)

        self.plugin_id = plugin_id
        self.descriptor = PluginDescriptor.from_dict(descriptor_dict)
        self.options = options
        self.work_dir = runtime_settings.work_dir
        self.log_dir = runtime_settings.log_dir
        self.timeout_request = getattr(runtime_settings, "timeout_request", 5.0)
        self._is_running = False
        self._initialized: Event = initialized
        self._startup_phase = startup_phase
        self._logging = logging_session
        self._owns_logging = owns_logging
        
        self.did_start = threading.Event()
        self.did_stop = threading.Event()
        self._stop_requested = threading.Event()
        self._lifecycle = threading.Condition()
        self._start_in_progress = False
        self._start_complete = False
        self._active_callbacks = 0
        
        # Topic subscriptions: topic_name -> queue
        
        self.topic_published = set()
        self.topic_subscriptions: Dict[str, Subscription] = {}
        self.topic_subscribers: Dict[str, Dict[str,Subscriber]] = {}
        self._topic_subscribers_lock = threading.RLock()
        # Topic subscriptions: topic_name -> queue
        self.env_subscriptions: Dict[str, Callable] = {}
        
        # Error log
        self.error_log: list = []
        # Instantiate the plugin
        self.plugin: Optional[Plugin] = None
        
        
        self.main_thread: Optional[threading.Thread] = None

        # Create manager proxy for direct manager access
        channel = Channel(queue_descriptor)
        self.rpc = PluginRPC(
            self.plugin_id,
            channel,
            timeout=self.timeout_request,
            start_listener=False,
            on_disconnect=self._request_stop,
        )

        try:
            self._init_rpc()
            self._init_plugin()
        except Exception:
            if self._owns_logging:
                self._logging.close()
            raise
        
        
        self.rpc.start_listener()  # Start the RPC listener after initialization

    def set_log_formats(
        self,
        file_format: Optional[str] = None,
        console_format: Optional[str] = None,
    ) -> None:
        """Change this worker's file and/or console log format."""
        self._logging.set_formats(file_format, console_format)


    ## ========================================================================
    # Worker Main Loop
    # ========================================================================

    def main(self) -> None:
        """Run plugin work on the process main thread, then await shutdown."""
        logger.info("Worker started")
        cmds = list(self.rpc.command_handlers.keys())
        logger.debug("Registered commands: %s", cmds)

        self.main_thread = threading.current_thread()
        self._is_running = True
        _set_startup_phase(self._startup_phase, PluginStartupPhase.READY)
        self._initialized.set()  # Signal to manager that initialization is complete
        try:
            with self._lifecycle:
                self._lifecycle.wait_for(
                    lambda: self._start_complete or self._stop_requested.is_set()
                )
                run_main = self._start_complete and not self._stop_requested.is_set()

            if run_main:
                logger.debug("Plugin main started")
                try:
                    self.plugin.main()
                except KeyboardInterrupt:
                    logger.info("Plugin interrupted")
                    self._request_stop()
                except Exception as e:
                    logger.error("Plugin error in main: %s", e, exc_info=True)
                    self.error_log.append(str(e))

            self._stop_requested.wait()
            self._finish_stop()
        finally:
            logger.info("Worker stopped")
            self.shutdown()

    def wait_for_stop(self, timeout: Optional[float] = None) -> bool:
        """Wait until a stop has been requested; useful inside a blocking main()."""
        return self._stop_requested.wait(timeout)

    def _request_stop(self) -> None:
        with self._lifecycle:
            first_request = not self._stop_requested.is_set()
            self._stop_requested.set()
            self._is_running = False
            if self.plugin is not None:
                self.plugin._is_running = False
            self._lifecycle.notify_all()

        if first_request:
            self.rpc.stop_accepting_commands()
            self.did_start.set()

    def _finish_stop(self) -> None:
        with self._lifecycle:
            self._lifecycle.wait_for(lambda: not self._start_in_progress)

        self.rpc.wait_for_commands(exclude=(CommandType.PLUGIN_STOP,))
        self._wake_topic_listeners()
        with self._lifecycle:
            self._lifecycle.wait_for(lambda: self._active_callbacks == 0)

        try:
            self.plugin.on_stop()
        except Exception as e:
            logger.error(f"Failed to stop plugin: {e}", exc_info=True)

        with self._lifecycle:
            self.plugin._is_running = False
            self.plugin.state = PluginState.STOPPED
            self.did_stop.set()
            self._lifecycle.notify_all()

        # Let stop handlers send their responses before closing the channel.
        self.rpc.drain_commands()

    def _wake_topic_listeners(self) -> None:
        for topic, subscription in list(self.topic_subscriptions.items()):
            try:
                if hasattr(subscription, "signal_stop"):
                    subscription.signal_stop()
                else:
                    subscription.queue.put(_TOPIC_SHUTDOWN_SENTINEL)
            except Exception as e:
                logger.error(f"Error waking topic listener for '{topic}': {e}")

    def start(self) -> bool:
        """Start the plugin once; the worker's main thread invokes plugin.main()."""
        with self._lifecycle:
            if self._stop_requested.is_set():
                return False
            if self._start_complete:
                return True
            if self._start_in_progress:
                raise RuntimeError(f"Plugin {self.plugin_id} is already starting")
            self._start_in_progress = True

        try:
            self.plugin.on_start()
        except Exception as e:
            logger.error(f"Failed to start plugin: {e}", exc_info=True)
            with self._lifecycle:
                self._start_in_progress = False
                self._lifecycle.notify_all()
            raise

        with self._lifecycle:
            self._start_in_progress = False
            if not self._stop_requested.is_set():
                self._start_complete = True
                self.plugin._is_running = True
                self.plugin.state = PluginState.RUNNING
            self.did_start.set()
            self._lifecycle.notify_all()
            return self._start_complete

    def stop(self, wait: bool = False) -> bool:
        """Request plugin shutdown, optionally waiting for on_stop() to finish."""
        self._request_stop()
        if wait and threading.current_thread() is not self.main_thread:
            self.did_stop.wait()
        return True

    def shutdown(self) -> None:
        """Clean up resources after lifecycle hooks and accepted RPCs complete."""
        logger.info("Worker shutting down")
        self._is_running = False
        if self.plugin is not None:
            self.plugin._is_running = False
            self.plugin.state = PluginState.STOPPED
        try:
            self.rpc.shutdown()
        except Exception as e:
            logger.error(f"Error closing RPC: {e}", exc_info=True)
        finally:
            if getattr(self, "_owns_logging", False):
                self._logging.close()
    ## ========================================================================
    # Initialization
    # ========================================================================

    def _init_plugin(self) -> None:
        """Instantiate plugin from descriptor."""
        try:
            # Import plugin class
            
            module = importlib.import_module(self.descriptor.package)
            plugin_class: Plugin = getattr(module, self.descriptor.class_name)
            

            # Instantiate plugin with proxy
            self.plugin = plugin_class(self, self.plugin_id, self.options )
            logger.info("Plugin instantiated")
        except Exception as e:
            logger.error("Failed to initialize plugin: %s", e)
            raise

    def _init_rpc(self) -> None:
        """Initialize command handlers from plugin methods.

            + on request
            > method
        

            + PLUGIN_LOAD = "plugin_load"
            + PLUGIN_START = "plugin_start"
            + PLUGIN_STOP = "plugin_stop"
            + PLUGIN_UNLOAD = "plugin_unload"
            + PLUGIN_STATUS = "plugin_status"
            - PLUGIN_STATUS_ALL = "plugin_status_all"
            + PLUGIN_ERRORS = "plugin_errors"
            > PLUGIN_CALL = "plugin_call"

            > ENV_SET = "env_set"
            > ENV_GET = "env_get"
            > ENV_GET_ALL = "env_get_all"
            > ENV_SUBSCRIBE = "env_subscribe"
            > ENV_UNSUBSCRIBE = "env_unsubscribe"
            + ENV_UPDATE = "env_update"

            * TOPIC_SUBSCRIBE = "topic_subscribe"
            * TOPIC_UNSUBSCRIBE = "topic_unsubscribe"
            * TOPIC_PUBLISH = "topic_publish"
            * TOPIC_LIST = "topic_list"
        """
        try:
            self.rpc.on_request(CommandType.PLUGIN_START, self._respond_start)
            self.rpc.on_request(CommandType.PLUGIN_STOP, self._respond_stop)
            self.rpc.on_request(CommandType.PLUGIN_CALL, self._respond_call)
            self.rpc.on_request(CommandType.PLUGIN_STATUS, self._respond_status)
            self.rpc.on_request(CommandType.PLUGIN_ERRORS, self._respond_errors)
            
            self.rpc.on_request(CommandType.ENV_UPDATE, self._respond_env_update)
            
            self.rpc.on_request(CommandType.TOPIC_SUBSCRIBE, self._respond_topic_subscribe)
            self.rpc.on_request(CommandType.TOPIC_UNSUBSCRIBE, self._respond_topic_unsubscribe)
            self.rpc.on_request(CommandType.TOPIC_LIST, self._request_topic_list)

        except Exception as e:
            logger.error(f"Failed to init RPC: {e}")
            raise
    
    
    

    # ========================================================================
    # Commands to send
    # ========================================================================
    
    """Commands that plugin can send to manager or other plugins.
        + PLUGIN_LOAD = "plugin_load"
        + PLUGIN_START = "plugin_start"
        + PLUGIN_STOP = "plugin_stop"
        + PLUGIN_UNLOAD = "plugin_unload"
        + PLUGIN_STATUS = "plugin_status"
        - PLUGIN_STATUS_ALL = "plugin_status_all"
        + PLUGIN_ERRORS = "plugin_errors"
        > PLUGIN_CALL = "plugin_call"
    """
    def _resolve_timeout(self, timeout: Optional[float]) -> Optional[float]:
        if timeout is not None:
            return timeout
        return getattr(self, "timeout_request", 5.0)

    def plugin_call(
        self,
        plugin_id: str,
        method_name: str,
        *args,
        wait: bool = True,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> PendingRequest | Any:
        """Invoke a method on the plugin via RPC."""
        pending: PendingRequest = self.rpc.plugin_call(plugin_id, method_name, *args, **kwargs)
        if not wait:
            return pending
        
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out calling plugin method '{method_name}' on '{plugin_id}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return None
        if response.error:
            logger.error(f"Error calling plugin method '{method_name}': {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result

    def plugin_load(
        self,
        descriptor: PluginDescriptor,
        options: Dict[str, Any] = None,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | bool:
        """Ask the manager to load a plugin instance described by descriptor."""
        pending = self.rpc.plugin_load(descriptor, options)
        if not wait:
            return pending
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out loading plugin '{descriptor.name}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error loading plugin '{descriptor.name}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result

    def plugin_start(
        self,
        plugin_id: str,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | bool:
        """Ask the manager to start another plugin."""
        pending = self.rpc.plugin_start(plugin_id)
        if not wait:
            return pending
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out starting plugin '{plugin_id}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error starting plugin: {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result

    def plugin_stop(
        self,
        plugin_id: str,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | bool:
        """Ask the manager to stop another plugin."""
        pending = self.rpc.plugin_stop(plugin_id)
        if not wait:
            return pending
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out stopping plugin '{plugin_id}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error stopping plugin: {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result

    def plugin_unload(
        self,
        plugin_id: str,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | bool:
        """Ask the manager to unload another plugin."""
        pending = self.rpc.plugin_unload(plugin_id)
        if not wait:
            return pending
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out unloading plugin '{plugin_id}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error unloading plugin: {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result

    def plugin_status(
        self,
        plugin_id: str = None,
        timeout: Optional[float] = None,
    ) -> Optional[ProcessStatus]:
        """Get the status of this plugin or another loaded plugin."""
        pending: PendingRequest = self.rpc.plugin_status(plugin_id)
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out getting status for plugin '{plugin_id}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return None
        if response.error:
            logger.error(f"Error getting plugin status: {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result

    def plugin_status_all(self, timeout: Optional[float] = None) -> Dict[str, ProcessStatus]:
        """Get the status of all loaded plugins."""
        pending: PendingRequest = self.rpc.plugin_status_all()
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out getting all plugin statuses after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return None
        if response.error:
            logger.error(f"Error getting all plugin statuses: {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result


    """Commands that plugin can send to manager or other plugins.
        > ENV_SET = "env_set"
        > ENV_GET = "env_get"
        > ENV_GET_ALL = "env_get_all"
        > ENV_SUBSCRIBE = "env_subscribe"
        > ENV_UNSUBSCRIBE = "env_unsubscribe"
    """
    def env_set(
        self,
        var_name: str,
        value: Any,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | bool:
        """Set an environment variable."""
        pending: PendingRequest = self.rpc.env_set(var_name, value)
        if not wait:
            return pending
        
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out setting env variable '{var_name}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error setting env variable: {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result
    
    def env_get(
        self,
        var_name: str,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | Any:
        """Get an environment variable."""
        pending: PendingRequest = self.rpc.env_get(var_name)
        if not wait:
            return pending
        
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out getting env variable '{var_name}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return None
        if response.error:
            logger.error(f"Error getting env variable: {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result
    
    def env_get_all(
        self,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | dict:
        """Get all environment variables."""
        pending: PendingRequest = self.rpc.env_get_all()
        if not wait:
            return pending
        
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out getting all env variables after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return None
        if response.error:
            logger.error(f"Error getting all env variables: {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result
    
    def env_subscribe(
        self,
        var_name: str,
        callback: Callable,
        timeout: Optional[float] = None,
    ) -> bool:
        """Subscribe to environment updates and invoke callback with the value."""
        pending: PendingRequest = self.rpc.env_subscribe(var_name)
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out subscribing to env variable '{var_name}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error subscribing to env variable '{var_name}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        
        self.env_subscriptions[var_name] = callback

        initial = self.rpc.env_get(var_name).wait(timeout=effective_timeout)
        if initial is not None and not initial.error:
            callback(initial.result)
        else:
            callback(None)

        return True
    
    def env_unsubscribe(
        self,
        var_name: str,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | bool:
        """Unsubscribe from an environment variable."""
        pending: PendingRequest = self.rpc.env_unsubscribe(var_name)
        if not wait:
            return pending
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out unsubscribing from env variable '{var_name}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error unsubscribing from env variable '{var_name}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        
        if var_name in self.env_subscriptions:
            del self.env_subscriptions[var_name]
        return True
    

    """
    Commands that plugin can send to manager or other plugins.
        * TOPIC_SUBSCRIBE = "topic_subscribe"
        * TOPIC_UNSUBSCRIBE = "topic_unsubscribe"
        * TOPIC_ANNOUNCE = "topic_announce"
        * TOPIC_LIST = "topic_list"
    """

    def topic_subscribe(
        self,
        topic: str,
        callback: Callable,
        timeout: Optional[float] = None,
    ) -> bool:
        """Subscribe to a topic."""
        subscription = Subscription(topic, callback)

        desc = subscription.queue.send_descriptor()
        subscriber = Subscriber(self.plugin_id, topic, desc)

        pending: PendingRequest = self.rpc.topic_subscribe(subscriber)
        subscription.queue.close_sender()
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            subscription.close()
            msg = f"Timed out subscribing to topic '{topic}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            subscription.close()
            logger.error(f"Error subscribing to topic '{topic}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        if response.result:
            subscription.thread = threading.Thread(target=self._topic_listener, args=(topic,), daemon=True)
            self.topic_subscriptions[topic] = subscription
            subscription.thread.start()
        else:
            subscription.close()
        return response.result
    
    def topic_unsubscribe(
        self,
        topic: str,
        timeout: Optional[float] = None,
    ) -> bool:
        """Unsubscribe from a topic."""
        if topic not in self.topic_subscriptions:
            logger.warning("Not subscribed to topic '%s'", topic)
            return True
        
        subscription = self.topic_subscriptions[topic]
        subscriber = Subscriber(self.plugin_id, topic, QueueDescriptor())

        pending: PendingRequest = self.rpc.topic_unsubscribe(subscriber)
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out unsubscribing from topic '{topic}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error unsubscribing from topic '{topic}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        
        if response.result:
            del self.topic_subscriptions[topic]
            subscription.signal_stop()
            if subscription.thread is not None and subscription.thread.is_alive():
                subscription.thread.join(timeout=1.0)
            subscription.close()

        return response.result


    def topic_list(
        self,
        plugin_id: Optional[str] = None,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> PendingRequest | list:
        """List available topics."""
        pending: PendingRequest = self.rpc.topic_list(plugin_id=plugin_id)
        if not wait:
            return pending
        
        effective_timeout = self._resolve_timeout(timeout)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out listing topics after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return None
        if response.error:
            logger.error(f"Error listing topics: {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result
    

    def topic_announce(self, topic: str, timeout: Optional[float] = None) -> bool:
        """Announce a topic to manager and sync subscribers."""
        # first time publishing this topic
        if topic in self.topic_published:
            return True

        with self._topic_subscribers_lock:
            if topic not in self.topic_subscribers:
                self.topic_subscribers[topic] = {}
            topic_subscribers = self.topic_subscribers[topic]
        effective_timeout = self._resolve_timeout(timeout)
        response = self.rpc.topic_announce(topic).wait(timeout=effective_timeout)
        if response is None:
            msg = f"Timed out announcing topic '{topic}' after {effective_timeout}s"
            logger.error(msg)
            self.error_log.append(msg)
            return False
        if response.error:
            logger.error(f"Error registering topic '{topic}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        

        self.topic_published.add(topic)

        manager_subscribers: Dict[str, Subscriber] = response.result

        with self._topic_subscribers_lock:
            for sub in manager_subscribers.values():
                if sub.plugin_id not in topic_subscribers:
                    sub.queue = Queue(sub.queue_descriptor)
                    topic_subscribers[sub.plugin_id] = sub

        return True

    def topic_publish(self, topic: str, data: Any) -> int:
        data = sanitize_for_ipc(data)

        self.topic_announce(topic)
        
        with self._topic_subscribers_lock:
            subscribers = list(self.topic_subscribers.get(topic, {}).items())
        if not subscribers:
            return 0

        delivered, dead_ids = _deliver_to_subscribers(topic, data, subscribers, logger)

        if dead_ids:
            with self._topic_subscribers_lock:
                topic_subs = self.topic_subscribers.get(topic)
                if topic_subs is not None:
                    for sub_id in dead_ids:
                        removed = topic_subs.pop(sub_id, None)
                        if removed is not None:
                            removed.close()

        return delivered
    
    

    # ========================================================================
    # On Command Recievd
    # ========================================================================
    
    def _respond_start(self, rpc: PluginRPC, req: RequestPluginStart) -> bool:
        """Start plugin."""
        return self.start()
    
    def _respond_stop(self, rpc: PluginRPC, req: RequestPluginStop) -> bool:
        """Stop plugin."""
        return self.stop(wait=True)
    
    
    def _respond_status(self, rpc: PluginRPC, req: RequestPluginStatus) -> Dict[str, Any]:
        """Get plugin status."""
        num_subscription = len(self.topic_subscriptions.keys())
        num_subscribers = sum(len(subs) for subs in self.topic_subscribers.values())
        num_published = len(self.topic_published)
        num_errors = len(self.error_log)

        status = PluginStatus(
            plugin_id=self.plugin_id,
            state=str(self.plugin.state),
            is_running=self.plugin.is_running,
            subscriptions=num_subscription,
            subscribers=num_subscribers,
            published=num_published,
            errors=num_errors
        )
        return status
    
    def _respond_errors(self, rpc: PluginRPC, req: RequestPluginErrors) -> list:
        """Get error log."""
        return self.error_log.copy()
    

    def _respond_call(self, rpc: PluginRPC, req: RequestPluginCall) -> Any:
        """Invoke service method on plugin via reflection."""
        if self._stop_requested.is_set():
            raise RuntimeError(f"Plugin {self.plugin_id} is stopping")
        method_name = req.method_name
        args = req.args
        kwargs = req.kwargs
        
        method = getattr(self.plugin, method_name)
        
        result = method(*args, **kwargs)
        result = sanitize_for_ipc(result)

        return result
            


    
    def _respond_env_update(self, rpc: PluginRPC, req: RequestEnvUpdate) -> None:
        """Receive environment variable change notification."""
        var_name = req.name
        callback = self.env_subscriptions.get(var_name)
        if callback is None:
            return True
        self._invoke_callback(callback, req.value)
        return True

    def _invoke_callback(self, callback: Callable, *args) -> None:
        with self._lifecycle:
            if self._stop_requested.is_set():
                return
            self._active_callbacks += 1
        try:
            callback(*args)
        finally:
            with self._lifecycle:
                self._active_callbacks -= 1
                self._lifecycle.notify_all()


    # ========================================================================
    # Topic Subscription Handling
    # ========================================================================
    
    def _topic_listener(self, topic: str) -> None:
        """Background thread: listen for messages from topic subscriptions. one thread each"""
        self.did_start.wait()  # Ensure plugin has started before processing messages
        with getattr(self, "_topic_subscribers_lock", threading.RLock()):
            registered = self.topic_subscriptions.get(topic)
        
        while True:
            
            if self._is_running is False:
                logger.info("Worker stopping, exiting topic listener for '%s'", topic)
                break

            # check is subscription still active (in case unsubscribed while waiting for data)
            with getattr(self, "_topic_subscribers_lock", threading.RLock()):
                cur = self.topic_subscriptions.get(topic,None)
            if cur is None or cur != registered: 
                logger.info(f"Subscription for topic '{topic}' no longer registered, stopping listener thread")
                break
            

            # pull data from subscription queue (blocking)
            try:
                wakeup_r = getattr(registered, "_wakeup_r", None)
                receiver = getattr(registered.queue, "receiver", None)
                if wakeup_r is not None and receiver is not None:
                    ready = conn_wait([receiver._conn, wakeup_r])
                    if wakeup_r in ready:
                        logger.info("Received shutdown signal for topic '%s'", topic)
                        break
                data = registered.queue.get()  # Blocking wait for new data
                if isinstance(data, _TopicShutdownSentinel):
                    logger.info("Received shutdown sentinel for topic '%s'", topic)
                    break
            except ValueError as e:
                if registered.queue.closed():
                    logger.info(f"Subscription for topic '{topic}' queue closed, stopping listener thread")
                    break
                else:
                    logger.error(f"Error getting data from topic '{topic}' queue: {e}")
                    self.error_log.append(str(e))
                    continue
            except (EOFError, OSError) as e:
                logger.info(
                    "Subscription for topic '%s' closed, stopping listener thread: %s",
                    topic,
                    e,
                )
                break
            except Exception as e:
                logger.error(f"Error in topic '{topic}' listener: {e}", exc_info=True)
                self.error_log.append(str(e))
                continue

            # Call the plugin's callback with the new data
            try:
                self._invoke_callback(registered.callback, data)
            except Exception as e:
                logger.error(
                    "Error in topic '%s' callback: %s",
                    topic,
                    e,
                    exc_info=True,
                )
                self.error_log.append(str(e))
                continue
    
    def _respond_topic_subscribe(self, rpc: PluginRPC, req: RequestTopicSubscribe) -> bool:
        """Handle topic subscribe request."""
        plugin_id = req.sender
        sub = req.subscriber
        topic = sub.topic
        sub.queue = Queue(req.subscriber.queue_descriptor)
        
        with self._topic_subscribers_lock:
            if topic not in self.topic_subscribers:
                self.topic_subscribers[topic] = {}
            self.topic_subscribers[topic][plugin_id] = sub

        # logger.info('_respond: topic_subscribers: %s', self.topic_subscribers)

        return True
    
    def _respond_topic_unsubscribe(self, rpc: PluginRPC, req: RequestTopicUnsubscribe) -> bool:
        """Handle topic unsubscribe request."""
        plugin_id = req.sender
        topic = req.subscriber.topic

        with self._topic_subscribers_lock:
            if topic not in self.topic_subscribers: return True
            if plugin_id not in self.topic_subscribers[topic]: return True
            sub = self.topic_subscribers[topic].pop(plugin_id)
        
        try:
            sub.close()
        except Exception as e:
            logger.info(f"Error closing queue for topic '{topic}': {e}", exc_info=True)

        return True
    
    def _request_topic_list(self, rpc: PluginRPC, req: RequestTopicList) -> list:
        """Handle topic list request."""
        return sorted(self.topic_published)
    
