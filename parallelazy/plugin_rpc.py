

import itertools
import time 
from typing import Any, Callable, Dict, Optional
import logging
from torch import multiprocessing as mp
import threading
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.connection import wait
from parallelazy.plugin_data import Channel, CommandType, Message, PendingRequest, Queue, Request, RequestCallback, RequestEnvGet, RequestEnvGetAll, RequestEnvSet, RequestEnvSubscribe, RequestEnvUnsubscribe, RequestEnvUpdate, RequestPluginCall, RequestPluginErrors, RequestPluginStart, RequestPluginStatus, RequestPluginStatusAll, RequestPluginStop, RequestTopicAnnounce, RequestTopicList, RequestTopicSubscribe, RequestTopicUnsubscribe, Response, Subscriber
from parallelazy.plugin_transport import sanitize_for_ipc


logger = logging.getLogger(__name__)



class MessageUID:
    """Thread-safe unique ID generator."""
    def __init__(self, minimal=True):
        self._counter = itertools.count()
        self._lock = mp.Lock()
        self.minimal = minimal
        self._last = 0
    
    def __next__(self) -> str:
        with self._lock:
            num = next(self._counter)
            self._last = num
        uid = f"{threading.get_ident()}_{num}"
        if self.minimal: return uid
        return f"{threading.current_thread().name}_{uid}"
            
    def next(self) -> str: return self.__next__()
    def current(self) -> str: return self._last


class PluginCoreRPC:
    """
    Wraps bidirectional channel for communication between manager and plugin for a clean API interface:
    - Environment variable access
    - Pub/sub publishing
    - Service invocation
    - Manager state queries
    
    Usage in plugin:
        def on_start(self):
            model = self.manager.get_env("model")
            self.manager.set_env("training_status", "started")
            self.manager.publish("training_topic", {"epoch": 1})
    """

    
    def __init__(self, plugin_id: str, channel: Channel=None, timeout: float = 5.0):
        """
        Initialize manager proxy.
        
        Args:
            plugin_id: Plugin instance identifier
            channel: Bidirectional channel for communication with manager
            timeout: Request timeout in seconds
        """
        self.plugin_id = plugin_id
        self.channel = channel if channel is not None else Channel()
        self.timeout = timeout
        
        self.command_handlers: Dict[str, RequestCallback] = {}

        self.uid = MessageUID()
        self._running = True
        self._shutdown = Queue()

        # Request/response routing: cmd_id → Queue(maxsize=1)
        self._pending: Dict[str, PendingRequest] = {}
        self._pending_lock = threading.Lock()
        
        # Reader thread routes Responses to reply slots
        self._reader = threading.Thread(target=self._channel_listener, daemon=False)
        self._reader.start()

        # Persistent pool for executing incoming requests — avoids spawning
        # a fresh OS thread per RPC call (was the source of the stutter).
        self._executor = ThreadPoolExecutor(
            max_workers=8, thread_name_prefix=f"rpc-{plugin_id}"
        )

    def shutdown(self):
        """Clean up resources."""
        # Step 1: Signal reader to stop
        self._running = False
        self.signal_shutdown()
        
        
        # Step 3: Wait for reader thread to exit
        self._reader.join(timeout=2.0)
        if self._reader.is_alive():
            logger.warning("Reader thread did not exit cleanly")

        # Step 3b: Stop accepting new work; let in-flight commands finish.
        # wait=False keeps shutdown() from blocking indefinitely on a command
        # handler that's still running — the pending-request wait loop below
        # covers giving them time to complete.
        self._executor.shutdown(wait=False, cancel_futures=False)

        # Step 4: Wait for pending requests to be processed (with timeout)
        timeout_start = time.time()
        while self.has_pending_requests() and (time.time() - timeout_start) < 2.0:
            logger.debug(f"Plugin {self.plugin_id} waiting for pending requests to complete...")
            time.sleep(0.05)
        
        self.channel.shutdown()

    def signal_shutdown(self):
        """Wake the reader thread without polling."""
        try:
            self._shutdown.put(None)
        except Exception:
            pass
        
    
    def forward(self, req: Request, wait=True) -> Any:
        forward_req = req.copy()
        
        # Forward the call to the target plugin and wait for response
        remote_result = self.request(forward_req)
        if not wait: return remote_result
        
        remote_result = remote_result.wait()
        if remote_result.error:
            raise Exception(f"Error on forwarding {req}: {remote_result.error}")
        return remote_result.result 

    def request(self, req: Request) -> PendingRequest:
        """
        send a request
        """
        if req.message_id is None:
            req.message_id = self.uid.next()
        if req.sender is None:
            req.sender = self.plugin_id

        pending_request = PendingRequest(req.message_id,)
        with self._pending_lock:
            self._pending[req.message_id] = pending_request
        self.channel.send(req)
        return pending_request
    
    def respond(self, req: Request, result: Any = None, error: str = None) -> PendingRequest:
        """
        send a response
        """
        result = sanitize_for_ipc(result)
        error = sanitize_for_ipc(error)


        if not self._running:
            logger.debug(f"Plugin {self.plugin_id} RPC is shutting down, response discarded")
            return None
            
        error = str(error) if error else None

        resp = Response(result=result, error=error)
        resp.message_id = req.message_id
        resp.sender = self.plugin_id
        
        try:
            self.channel.send(resp)
        except OSError:
            # Channel is closed, likely during shutdown
            logger.debug(f"Plugin {self.plugin_id} could not send response (channel closed)")

    def has_pending_requests(self) -> bool:
        """Check if there are any pending requests."""
        with self._pending_lock:
            return len(self._pending) > 0
    

    def _channel_listener(self) -> None:
        """
        Background thread: drain incoming Responses and route to reply slots.
        
        Puts each Response into its pending request's queue.
        Warns on orphaned responses (request already timed out).
        """
        while self._running:
            try:
                ready = wait([self.channel._receive._conn, self._shutdown.receiver._conn])
                if self._shutdown.receiver._conn in ready:
                    self._shutdown.get()
                    logger.debug(f"Plugin {self.plugin_id} received shutdown sentinel on channel")
                    break
                message = self.channel.receive()
                if isinstance(message, Request):
                    self._handle_request(message)
                elif isinstance(message, Response):   
                    self._handle_response(message)
                else:
                    logger.error(f"Received unknown message type: {message}")
            except OSError:
                # Channel closed, exit cleanly
                break
            except Exception as e:
                if self._running:
                    logger.error(f"Response router error: {e}", exc_info=True)
                else:
                    # Expected during shutdown
                    break



    def _handle_request(self, req: Request) -> None:
        """Dispatch request to appropriate handler."""
        try:
            cmd = str(req.command)
            if cmd not in CommandType.commands():
                raise ValueError(f"Unknown command: {cmd}")
            if cmd not in self.command_handlers:
                raise ValueError(f"Command not registered: {cmd} (available: {list(self.command_handlers.keys())})")

            self._executor.submit(self._execute_command, req)
        except Exception as e:
            logger.error(f"Request handler error: {e}", exc_info=True)
            self.respond(req, error=e)
    
    def _handle_response(self, resp: Response) -> bool:
        """Route incoming response to pending request."""
        with self._pending_lock:
            pending_request = self._pending.pop(resp.message_id, None)
        
        if pending_request is None:
            logger.error(f"Received response for unknown message_id={resp.message_id} {resp.__dict__()}")
            return False
        
        pending_request.set_result(resp)
        return True
    
    def _execute_command(self, req: Request) -> Any:
        """
        Execute a command and wait for response.
        
        Args:
            command: Command name
            *args: Positional arguments
            **kwargs: Keyword arguments
        """
        cmd = str(req.command)
        if cmd not in self.command_handlers:
            raise ValueError(f"Command not registered: {cmd} (available: {list(self.command_handlers.keys())})")
        
        handler = self.command_handlers[cmd]
        result = None
        error = None
        try:
            result = handler(self, req)
        except Exception as e:
            import traceback
            stacktrace = traceback.format_exc()
            error = str(e)
            logger.error(f"Error occurred while executing command '{cmd}': {e}\n{stacktrace}", exc_info=True)

        self.respond(req, result=result, error=error)
        
    
    def on_request(self, command_name: CommandType, func: RequestCallback) -> None:
        """
        Register a command handler for an incoming message
        
        Args:
            command_name: Name of the command
            func: Function to handle the command
        """
        cmd = str(command_name)
        self.command_handlers[cmd] = func


class PluginLifeCycleRPC(PluginCoreRPC):
    """Convience class with manager-specific commands pre-registered.
    PLUGIN_START = "plugin_start"
    PLUGIN_STOP = "plugin_stop"
    PLUGIN_STATUS = "plugin_status"
    PLUGIN_STATUS_ALL = "plugin_status_all"
    PLUGIN_ERRORS = "plugin_errors"
    PLUGIN_CALL = "plugin_call"
    """
    def plugin_start(self):
        """
        Start the plugin process.
        """
        try:
            return self.request(RequestPluginStart())
        except Exception as e:
            logger.error(f"Failed to start plugin: {e}")
    

    def plugin_stop(self) :
        """
        Stop the plugin process.
        """
        try:
            return self.request(RequestPluginStop())
        except Exception as e:
            logger.error(f"Failed to stop plugin: {e}")
        
    def plugin_call(self, plugin_id: str, method_name: str, *args, **kwargs):
        """
        Call a method on the plugin.
        
        Args:
            plugin_id: ID of the plugin to call
            method_name: Name of the method to call
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            Method return value
        """
        try:
            return self.request(RequestPluginCall(plugin_id=plugin_id, method_name=method_name, args=args, kwargs=kwargs))
        except Exception as e:
            logger.error(f"Failed to call plugin method '{method_name}': {e}")
            return None
        
    
    # ========================================================================
    # STATUS: Query manager state
    # ========================================================================
    
    def plugin_status(self, plugin_id: str=None) -> PendingRequest | None:
        """
        Get status of another plugin.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            Status dict or None if not found
        """
        try:
            return self.request(RequestPluginStatus(plugin_id=plugin_id))
        except Exception as e:
            logger.debug(f"Failed to get status for {plugin_id}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    def plugin_status_all(self):
        """
        List all loaded plugins and their status.
        
        Returns:
            Dictionary of plugin_id -> status info
        """
        try:
            return self.request(RequestPluginStatusAll())
        except Exception as e:
            logger.error(f"Failed to list plugins: {e}")

    
    def plugin_errors(self, plugin_id:str = None):
        """
        Get any errors from the manager or other plugins.
        
        Returns:
            Dictionary of plugin_id -> error info
        """
        try:
            return self.request(RequestPluginErrors(plugin_id=plugin_id))
        except Exception as e:
            logger.error(f"Failed to get plugin errors: {e}")
            return {}

class PluginEnvRPC(PluginCoreRPC):
    """Convience class with plugin-specific commands pre-registered."""

    # ========================================================================
    # ENVIRONMENT VARIABLES
    # ========================================================================
    
    def env_get(self, var_name: str):
        """
        Get environment variable from manager.
        
        Args:
            key: Variable key
            default: Default value if not found
        
        Returns:
            Variable value
        """
        try:
            return self.request(RequestEnvGet(name=var_name))
        except Exception as e:
            logger.error(f"Failed to get env variable '{var_name}': {e}")

    
    def env_set(self, var_name: str, value: Any):
        """
        Set environment variable in manager (notifies subscribed plugins).
        
        Args:
            key: Variable key
            value: Variable value
        """
        try:
            value = sanitize_for_ipc(value)
            return self.request(RequestEnvSet(name=var_name, value=value))
        except Exception as e:
            logger.error(f"Failed to set env variable '{var_name}': {e}")
    
    def env_get_all(self):
        """
        Get all environment variables.
        
        Returns:
            Dictionary of all env variables
        """
        try:
            return self.request(RequestEnvGetAll())
        except Exception as e:
            logger.error(f"Failed to get all env variables: {e}")
            return {}
        
    def env_subscribe(self, var_name: str):
        """
            Subscribe to an environment variable.

        """
        try:
            return self.request(RequestEnvSubscribe(name=var_name))
        except Exception as e:
            logger.error(f"Failed to subscribe to env variable '{var_name}': {e}")
            return {}
        
    def env_unsubscribe(self, var_name: str):
        """
            Unsubscribe from an environment variable.

        """
        try:
            return self.request(RequestEnvUnsubscribe(name=var_name))
        except Exception as e:
            logger.error(f"Failed to unsubscribe from env variable '{var_name}': {e}")

    def env_update(self, var_name: str) :
        """
            Update an environment variable.

        """
        try:
            return self.request(RequestEnvUpdate(name=var_name))
        except Exception as e:
            logger.error(f"Failed to notify update env variable '{var_name}': {e}")

class PluginTopicRPC(PluginCoreRPC):
    # ========================================================================
    # PUB/SUB: Publishing
    # ========================================================================
    
    def topic_announce(self, topic: str):
        """
        Announce a topic.
        
        Args:
            topic: Topic name
        """
        try:
            return self.request(RequestTopicAnnounce(topic=topic))
        except Exception as e:
            logger.error(f"Failed to announce topic '{topic}': {e}")

    def topic_subscribe(self, subscriber: Subscriber):
        """
        Subscribe to a topic.
        
        Args:
            topic: Topic name
            callback: Function to call when a message is received
        """
        try:
            return self.request(RequestTopicSubscribe(subscriber=subscriber))
        except Exception as e:
            logger.error(f"Failed to subscribe to '{subscriber}': {e}")

    def topic_unsubscribe(self, subscriber: Subscriber):
        """
        Unsubscribe from a topic.
        
        Args:
            topic: Topic name
            callback: Function to remove from subscribers
        """
        try:
            return self.request(RequestTopicUnsubscribe(subscriber=subscriber))
        except Exception as e:
            logger.error(f"Failed to unsubscribe from '{subscriber}': {e}")

    def topic_list(self, plugin_id: str=None):
        """
        List all published topics.
        Returns:
            List of topic names
        """
        try:
            return self.request(RequestTopicList(plugin_id=plugin_id))
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
        
class PluginRPC(PluginLifeCycleRPC, PluginEnvRPC, PluginTopicRPC):
    """Convience class with all command categories pre-registered."""
    pass

