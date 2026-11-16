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
from queue import Full
import threading


from parallelazy.utils import init_debugger, is_debugging_enabled, process_start_spawn, setup_worker_debugging_env
from parallelazy.plugin_data import  Channel, CommandType, Queue,QueueDescriptor, PendingRequest, PluginStatus,  RequestPluginErrors, RequestPluginStart, RequestPluginStatus, RequestPluginStop, RequestTopicList, Subscriber, Subscription
from parallelazy.plugin import Plugin, PluginDescriptor, PluginState

from parallelazy.plugin_rpc import  PluginRPC, RequestEnvUpdate, RequestPluginCall, RequestTopicList, RequestTopicSubscribe, RequestTopicUnsubscribe, sanitize_for_ipc
from multiprocessing.synchronize import Event


#typechecking imports
from typing import Any, Callable, Dict,  Optional, TYPE_CHECKING



logger = logging.getLogger(__name__)


# ============================================================================
# PLUGIN WORKER
# ============================================================================

process_start_spawn()




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
        initialized: Event
    ) -> None:
        """
        Entry point for plugin subprocess.
        
        Args:
            plugin_id: Plugin instance identifier
            descriptor_dict: Serialized plugin descriptor
            options: Plugin options
            queue_descriptor: Queue descriptor for communication
            initialized: Event to signal initialization completion
        """
        if is_debugging_enabled():
            logger.setLevel(logging.DEBUG)
            init_debugger(plugin_id)

            
        worker = PluginWorker(
            plugin_id,
            descriptor_dict,
            options,
            queue_descriptor,
            initialized
        )
        worker.main()
    
    def __init__(
        self,
        plugin_id: str,
        descriptor_dict: Dict[str, Any],
        options: Dict[str, Any],
        queue_descriptor: QueueDescriptor,
        initialized: Event,
        loglevel: int = logging.INFO
    ):
        """Initialize worker."""

        self.plugin_id = plugin_id
        self.descriptor = PluginDescriptor.from_dict(descriptor_dict)
        self.options = options
        self._is_running = False
        self._initialized: Event = initialized
        self.loglevel = loglevel
        logger.setLevel(loglevel)
        
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
        # Topic subscriptions: topic_name -> queue
        self.env_subscriptions: Dict[str, Callable] = {}
        
        # Error log
        self.error_log: list = []
        # Instantiate the plugin
        self.plugin: Optional[Plugin] = None
        
        
        self.main_thread: Optional[threading.Thread] = None

        # Create manager proxy for direct manager access+
        channel = Channel(queue_descriptor)
        self.rpc = PluginRPC(self.plugin_id, channel, start_listener=False)

        self._init_rpc()
        
        self._init_plugin()
        
        
        self.rpc.start_listener()  # Start the RPC listener after initialization


    ## ========================================================================
    # Worker Main Loop
    # ========================================================================

    def main(self) -> None:
        """Run plugin work on the process main thread, then await shutdown."""
        logger.info(f"Plugin {self.plugin_id} worker started")
        cmds = list(self.rpc.command_handlers.keys())
        logger.debug(f"Plugin {self.plugin_id} registered commands: {cmds}")

        self.main_thread = threading.current_thread()
        self._is_running = True
        self._initialized.set()  # Signal to manager that initialization is complete
        try:
            with self._lifecycle:
                self._lifecycle.wait_for(
                    lambda: self._start_complete or self._stop_requested.is_set()
                )
                run_main = self._start_complete and not self._stop_requested.is_set()

            if run_main:
                logger.debug(f"[{self.plugin_id}] Plugin main started")
                try:
                    self.plugin.main()
                except KeyboardInterrupt:
                    logger.info(f"Plugin {self.plugin_id} interrupted")
                    self._request_stop()
                except Exception as e:
                    logger.error(f"Plugin {self.plugin_id} error in main: {e}", exc_info=True)
                    self.error_log.append(str(e))

            self._stop_requested.wait()
            self._finish_stop()
        finally:
            logger.info(f"Plugin {self.plugin_id} worker stopped")
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
                subscription.queue.put(None)
            except Exception as e:
                logger.error(f"Error closing queue for topic '{topic}': {e}")

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
        logger.info(f"Plugin {self.plugin_id} shutting down")
        self._is_running = False
        if self.plugin is not None:
            self.plugin._is_running = False
            self.plugin.state = PluginState.STOPPED
        try:
            self.rpc.shutdown()
        except Exception as e:
            logger.error(f"Error closing RPC: {e}", exc_info=True)
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
            logger.info(f"Plugin {self.plugin_id} instantiated")
        except Exception as e:
            logger.error(f"Failed to init plugin {self.plugin_id}: {e}")
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
    def plugin_call(self, plugin_id: str, method_name: str, *args, wait: bool=True, **kwargs) -> PendingRequest|Any:
        """Invoke a method on the plugin via RPC."""
        pending: PendingRequest = self.rpc.plugin_call(plugin_id, method_name, *args, **kwargs)
        if not wait: return pending
        
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error calling plugin method '{method_name}': {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result

    def plugin_load(
        self,
        descriptor: PluginDescriptor,
        options: Dict[str, Any] = None,
        wait: bool = True
    ) -> PendingRequest | bool:
        """Ask the manager to load a plugin instance described by descriptor."""
        pending = self.rpc.plugin_load(descriptor, options)
        if not wait:
            return pending
        response = pending.wait()
        if response.error:
            logger.error(f"Error loading plugin '{descriptor.name}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result

    def plugin_start(self, plugin_id: str, wait: bool = True) -> PendingRequest | bool:
        """Ask the manager to start another plugin."""
        pending = self.rpc.plugin_start(plugin_id)
        if not wait:
            return pending
        response = pending.wait()
        if response.error:
            logger.error(f"Error starting plugin: {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result

    def plugin_stop(self, plugin_id: str, wait: bool = True) -> PendingRequest | bool:
        """Ask the manager to stop another plugin."""
        pending = self.rpc.plugin_stop(plugin_id)
        if not wait:
            return pending
        response = pending.wait()
        if response.error:
            logger.error(f"Error stopping plugin: {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result

    def plugin_unload(self, plugin_id: str, wait: bool = True) -> PendingRequest | bool:
        """Ask the manager to unload another plugin."""
        pending = self.rpc.plugin_unload(plugin_id)
        if not wait:
            return pending
        response = pending.wait()
        if response.error:
            logger.error(f"Error unloading plugin: {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result


    """Commands that plugin can send to manager or other plugins.
        > ENV_SET = "env_set"
        > ENV_GET = "env_get"
        > ENV_GET_ALL = "env_get_all"
        > ENV_SUBSCRIBE = "env_subscribe"
        > ENV_UNSUBSCRIBE = "env_unsubscribe"
    """
    def env_set(self, var_name: str, value: Any, wait: bool=True) -> PendingRequest|bool:
        """Set an environment variable."""
        pending: PendingRequest = self.rpc.env_set( var_name, value)
        if not wait: return pending
        
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error setting env variable: {response.error}")
            self.error_log.append(str(response.error))
            return False
        return response.result
    
    def env_get(self, var_name: str, wait: bool=True) -> PendingRequest|Any:
        """Get an environment variable."""
        pending: PendingRequest = self.rpc.env_get(var_name)
        if not wait: return pending
        
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error getting env variable: {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result
    
    def env_get_all(self, wait: bool=True) -> PendingRequest|dict:
        """Get all environment variables."""
        pending: PendingRequest = self.rpc.env_get_all()
        if not wait: return pending
        
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error getting all env variables: {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result
    
    def env_subscribe(self, var_name: str, callback: Callable) -> bool:
        """Get all environment variables."""

        pending: PendingRequest = self.rpc.env_subscribe(var_name)
        
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error getting all env variables: {response.error}")
            self.error_log.append(str(response.error))
            return False
        
        self.env_subscriptions[var_name] = callback

        
        callback(var_name)  # Trigger callback for initial value

        return True
    
    def env_unsubscribe(self, var_name: str, wait: bool=True) -> bool:
        """Get all environment variables."""
        pending: PendingRequest = self.rpc.env_unsubscribe(var_name)
        
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error getting all env variables: {response.error}")
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

    def topic_subscribe(self, topic: str, callback: Callable) -> bool:
        """Get all environment variables."""
        subscription = Subscription(topic, callback)

        desc = subscription.queue.descriptor()
        subscriber = Subscriber(self.plugin_id, topic, desc  )
        

        pending: PendingRequest = self.rpc.topic_subscribe(subscriber)
        subscriber.queue = Queue( desc )
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error subscribing to topic '{topic}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        if response.result:
            subscription.thread = threading.Thread(target=self._topic_listener, args=(topic,), daemon=True)
            self.topic_subscriptions[topic] = subscription
            subscription.thread.start()
        return response.result
    
    def topic_unsubscribe(self, topic: str) -> bool:
        """Get all environment variables."""
        if not topic in self.topic_subscriptions:
            logger.warning(f"Plugin {self.plugin_id} not subscribed to topic '{topic}'")
            return True
        
        subscription = self.topic_subscriptions[topic] 
        desc = subscription.queue.descriptor()
        subscriber = Subscriber(self.plugin_id, topic, desc  )


        pending: PendingRequest = self.rpc.topic_unsubscribe(subscriber)
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error unsubscribing to topic '{topic}': {response.error}")
            self.error_log.append(str(response.error))
            return False
        
        if response.result:
            del self.topic_subscriptions[topic]
            subscription.queue.close()
            subscription.thread.join(timeout=1.0)
            

        return response.result


    def topic_list(self, wait: bool=True) -> PendingRequest|list:
        """Get all environment variables."""
        pending: PendingRequest = self.rpc.topic_list()
        if not wait: return pending
        
        # Wait for response if no callback provided
        response = pending.wait()
        if response.error:
            logger.error(f"Error getting all env variables: {response.error}")
            self.error_log.append(str(response.error))
            return None
        return response.result
    

    def topic_announce(self, topic: str) -> None:
        """ Announce a topic to manager and sync subscribers."""
        # first time publishing this topic
        if topic in self.topic_published: return True

        if not topic in self.topic_subscribers:
            self.topic_subscribers[topic] = {}

        topic_subscribers: dict[str, Subscriber] = self.topic_subscribers.get(topic)
        reponse = self.rpc.topic_announce(topic).wait()
        if reponse.error:
            logger.error(f"Error registering topic '{topic}': {reponse.error}")
            self.error_log.append(str(reponse.error))
            return False
        

        self.topic_published.add(topic)

        manager_subscribers: Dict[str, Subscriber] = reponse.result


        diff_subs =  [] 
        for sub in manager_subscribers.values():
            if sub.plugin_id not in topic_subscribers:
                sub.queue = Queue(sub.queue_descriptor)
                diff_subs.append(sub)
                topic_subscribers[sub.plugin_id] = sub

        topic_subscribers.update(manager_subscribers)
        #print("topic_announce: manager_subscribers: ", self.topic_published, manager_subscribers )
        

        return True

    def topic_publish(self, topic: str, data: Any) -> None:
        data = sanitize_for_ipc(data)

        self.topic_announce(topic)
        
        topic_subscribers = self.topic_subscribers.get(topic, None) 
        if topic_subscribers is None: return
            

        #print("topic_publish: topic_subscribers: ",self.topic_subscribers)

        for subscriber in topic_subscribers.values():
            #print("----")

            try:
                subscriber.queue.put_nowait(data)
            except Full:
                logger.warning(f"Subscriber {subscriber.plugin_id} on topic '{topic}' is full — dropping message")
                return False
            except Exception as e:
                logger.error(f"[ERROR] Failed to publish to subscriber {subscriber.plugin_id} on topic '{topic}':", exc_info=True)
                logger.error(f"[ERROR]\n{e}\n---")
                return False
        
        return True
    
    

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
        callback = self.env_subscriptions[var_name]
        self._invoke_callback(callback, var_name)
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
        registered = self.topic_subscriptions.get(topic)
        
        while True:
            
            if self._is_running is False:
                logger.info(f"Plugin {self.plugin_id} worker stopping, exiting topic listener for '{topic}'")
                break

            # check is subscription still active (in case unsubscribed while waiting for data)
            cur = self.topic_subscriptions.get(topic,None)
            if cur is None or cur != registered: 
                logger.info(f"Subscription for topic '{topic}' no longer registered, stopping listener thread")
                break
            

            # pull data from subscription queue (blocking)
            try:
                data = registered.queue.get() # Blocking wait for new data
                if data is None:
                    logger.info(f"Plugin {self.plugin_id} received shutdown sentinel for topic '{topic}'")
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
                logger.info(f"Subscription for topic '{topic}' closed, stopping listener thread: {e}")
                break
            except Exception as e:
                logger.error(f"Error in topic '{topic}' listener: {e}", exc_info=True)
                self.error_log.append(str(e))
                continue

            # Call the plugin's callback with the new data
            try:
                self._invoke_callback(registered.callback, data)
            except Exception as e:
                logger.error(f"Error in topic '{topic}' callback: {e}", exc_info=True)
                self.error_log.append(str(e))
                continue
    
    def _respond_topic_subscribe(self, rpc: PluginRPC, req: RequestTopicSubscribe) -> bool:
        """Handle topic subscribe request."""
        plugin_id = req.sender
        sub = req.subscriber
        topic = sub.topic
        sub.queue = Queue(req.subscriber.queue_descriptor)
        
        if topic not in self.topic_subscribers:
            self.topic_subscribers[topic] = {}
        self.topic_subscribers[topic][plugin_id] = sub

        # logger.info('_respond: topic_subscribers: %s', self.topic_subscribers)

        return True
    
    def _respond_topic_unsubscribe(self, rpc: PluginRPC, req: RequestTopicUnsubscribe) -> bool:
        """Handle topic unsubscribe request."""
        plugin_id = req.sender
        topic = req.subscriber.topic

        if topic not in self.topic_subscribers: return True
        if plugin_id not in self.topic_subscribers[topic]: return True
        sub = self.topic_subscribers[topic].pop(plugin_id)
        
        try:
            sub.queue.close()
        except Exception as e:
            logger.info(f"Error closing queue for topic '{topic}': {e}", exc_info=True)

        return True
    
    def _request_topic_list(self, rpc: PluginRPC, req: RequestTopicList) -> list:
        """Handle topic list request."""
        return self.topic_published.copy()
    
