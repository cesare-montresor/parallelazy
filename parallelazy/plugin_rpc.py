

import itertools
import time 
from typing import Any, Callable, Dict, Optional
import logging
from torch import multiprocessing as mp
import threading
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.connection import wait
from parallelazy.plugin_data import Channel, CommandType, Message, PendingRequest, Queue, Request, RequestCallback, RequestEnvGet, RequestEnvGetAll, RequestEnvSet, RequestEnvSubscribe, RequestEnvUnsubscribe, RequestEnvUpdate, RequestPluginCall, RequestPluginErrors, RequestPluginLoad, RequestPluginStart, RequestPluginStatus, RequestPluginStatusAll, RequestPluginStop, RequestPluginUnload, RequestTopicAnnounce, RequestTopicList, RequestTopicSubscribe, RequestTopicUnsubscribe, Response, Subscriber
from parallelazy.plugin_transport import sanitize_for_ipc


logger = logging.getLogger(__name__)



class MessageUID:
    """Thread-safe unique ID generator."""
    def __init__(self, minimal=True):
        self._counter = itertools.count()
        self._lock = threading.Lock()
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

    
    def __init__(
        self,
        plugin_id: str,
        channel: Channel = None,
        timeout: float = 5.0,
        start_listener: bool = True,
        on_disconnect: Optional[Callable[[], None]] = None,
    ):
        """
        Initialize manager proxy.
        
        Args:
            plugin_id: Plugin instance identifier
            channel: Bidirectional channel for communication with manager
            timeout: Request timeout in seconds
            start_listener: Whether to start the background channel listener thread
            on_disconnect: Optional callback invoked when the peer disconnects unexpectedly
        """
        self.plugin_id = plugin_id
        self.channel = channel if channel is not None else Channel()
        self.timeout = timeout
        
        self.command_handlers: Dict[str, RequestCallback] = {}

        self.uid = MessageUID()
        self._running = True
        self._disconnected = False
        self._on_disconnect = on_disconnect
        self._shutdown = Queue()

        # Request/response routing: cmd_id → PendingRequest
        self._pending: Dict[str, PendingRequest] = {}
        self._pending_lock = threading.Lock()

        self._commands_condition = threading.Condition()
        self._accepting_commands = True
        self._active_commands: Dict[str, int] = {}
        
        # Reader thread routes Responses to reply slots
        self._reader = threading.Thread(target=self._channel_listener, daemon=True)
                

        # Persistent pool for executing incoming requests — avoids spawning
        # a fresh OS thread per RPC call (was the source of the stutter).
        self._executor = ThreadPoolExecutor(
            max_workers=8, thread_name_prefix=f"rpc-{plugin_id}"
        )
        # Dedicated single-worker executor for ENV_UPDATE so rapid updates
        # are always delivered to subscriber callbacks in strict FIFO order.
        self._env_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=f"env-{plugin_id}"
        )

        if start_listener:
            self.start_listener()

    def start_listener(self):
        if not self._reader.is_alive():
            self._reader.start()

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
        self._env_executor.shutdown(wait=False, cancel_futures=False)

        # Step 4: Wait for pending requests to be processed (with timeout)
        timeout_start = time.time()
        while self.has_pending_requests() and (time.time() - timeout_start) < 2.0:
            logger.debug(f"Plugin {self.plugin_id} waiting for pending requests to complete...")
            time.sleep(0.05)

        self._disconnected = True
        self._fail_pending_requests(
            ConnectionError(f"RPC shutdown for plugin '{self.plugin_id}'")
        )
        
        self.channel.shutdown()

    def signal_shutdown(self):
        """Wake the reader thread without polling."""
        try:
            self._shutdown.put(None)
        except Exception:
            pass

    def stop_accepting_commands(self) -> None:
        """Reject new incoming requests while keeping response routing active."""
        with self._commands_condition:
            self._accepting_commands = False

    def wait_for_commands(self, exclude=()) -> None:
        """Wait for accepted command handlers to finish, except excluded commands."""
        excluded = {str(command) for command in exclude}
        with self._commands_condition:
            self._commands_condition.wait_for(
                lambda: not any(
                    command not in excluded and count
                    for command, count in self._active_commands.items()
                )
            )

    def drain_commands(self) -> None:
        """Stop the reader and wait for accepted handlers to respond."""
        self.signal_shutdown()
        if self._reader.is_alive() and threading.current_thread() is not self._reader:
            self._reader.join()
        self._executor.shutdown(wait=True, cancel_futures=False)
        self._env_executor.shutdown(wait=True, cancel_futures=False)
        
    
    def _evict_pending(self, message_id: str) -> None:
        with self._pending_lock:
            self._pending.pop(message_id, None)

    def _fail_pending_requests(self, error: Any) -> None:
        with self._pending_lock:
            pending_items = list(self._pending.values())
            self._pending.clear()
        for pending in pending_items:
            pending.set_error(error)

    def _failed_request(self, error: Any, message_id: Optional[str] = None) -> PendingRequest:
        pending = PendingRequest(message_id or self.uid.next())
        pending.set_error(error)
        return pending

    def forward(self, req: Request, wait: bool = True, timeout: Optional[float] = None) -> Any:
        forward_req = req.copy()
        
        # Forward the call to the target plugin and wait for response
        pending = self.request(forward_req)
        if not wait:
            return pending
        
        effective_timeout = self.timeout if timeout is None else timeout
        remote_result = pending.wait(timeout=effective_timeout)
        if remote_result is None:
            raise TimeoutError(
                f"Timed out forwarding {req.command} to plugin '{self.plugin_id}' after {effective_timeout}s"
            )
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

        if not self._running or self._disconnected:
            return self._failed_request(
                ConnectionError(f"Peer disconnected for plugin '{self.plugin_id}'"),
                message_id=req.message_id,
            )

        pending_request = PendingRequest(req.message_id, on_timeout=self._evict_pending)
        with self._pending_lock:
            if not self._running or self._disconnected:
                pending_request.set_error(
                    ConnectionError(f"Peer disconnected for plugin '{self.plugin_id}'")
                )
                return pending_request
            self._pending[req.message_id] = pending_request

        try:
            self.channel.send(req)
        except Exception as e:
            self._evict_pending(req.message_id)
            if isinstance(e, (EOFError, OSError, BrokenPipeError, ValueError)):
                self._disconnected = True
            pending_request.set_error(e)
            return pending_request
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
            except (EOFError, OSError, ValueError):
                # Channel closed or peer disconnected
                self._disconnected = True
                self._fail_pending_requests(
                    ConnectionError(f"Peer disconnected for plugin '{self.plugin_id}'")
                )
                if self._running and self._on_disconnect is not None:
                    try:
                        self._on_disconnect()
                    except Exception:
                        logger.debug(
                            "Error in disconnect callback for plugin '%s'",
                            self.plugin_id,
                            exc_info=True,
                        )
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

            with self._commands_condition:
                if not self._accepting_commands:
                    logger.debug(
                        "Rejecting command '%s' for plugin '%s': RPC endpoint is stopping",
                        cmd,
                        self.plugin_id,
                    )
                    self.respond(req, error="RPC endpoint is stopping")
                    return
                self._active_commands[cmd] = self._active_commands.get(cmd, 0) + 1

            try:
                executor = (
                    self._env_executor
                    if cmd == CommandType.ENV_UPDATE
                    else self._executor
                )
                executor.submit(self._execute_command, req)
            except Exception:
                self._command_finished(cmd)
                raise
        except Exception as e:
            logger.error(f"Request handler error: {req} {e}", exc_info=True)
            self.respond(req, error=e)

    def _command_finished(self, command: str) -> None:
        with self._commands_condition:
            remaining = self._active_commands.get(command, 0) - 1
            if remaining > 0:
                self._active_commands[command] = remaining
            else:
                self._active_commands.pop(command, None)
            self._commands_condition.notify_all()
    
    def _handle_response(self, resp: Response) -> bool:
        """Route incoming response to pending request."""
        with self._pending_lock:
            pending_request = self._pending.pop(resp.message_id, None)
        
        if pending_request is None:
            logger.warning(f"Received response for unknown message_id={resp.message_id} {vars(resp)}")
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
        
        try:
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
        finally:
            self._command_finished(cmd)
        
    
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
    PLUGIN_LOAD = "plugin_load"
    PLUGIN_START = "plugin_start"
    PLUGIN_STOP = "plugin_stop"
    PLUGIN_UNLOAD = "plugin_unload"
    PLUGIN_STATUS = "plugin_status"
    PLUGIN_STATUS_ALL = "plugin_status_all"
    PLUGIN_ERRORS = "plugin_errors"
    PLUGIN_CALL = "plugin_call"
    """
    def plugin_load(self, descriptor: Any, options: Dict[str, Any] = None) -> PendingRequest:
        """Ask the manager to load an instance described by descriptor."""
        try:
            return self.request(RequestPluginLoad(descriptor=descriptor, options=options))
        except Exception as e:
            logger.error(f"Failed to load plugin '{descriptor}': {e}")
            return self._failed_request(e)

    def plugin_start(self, plugin_id: str = None) -> PendingRequest:
        """
        Start a plugin. A manager omits plugin_id when starting its worker.
        """
        try:
            return self.request(RequestPluginStart(plugin_id=plugin_id))
        except Exception as e:
            logger.error(f"Failed to start plugin: {e}")
            return self._failed_request(e)
    

    def plugin_stop(self, plugin_id: str = None) -> PendingRequest:
        """
        Stop a plugin. A manager omits plugin_id when stopping its worker.
        """
        try:
            return self.request(RequestPluginStop(plugin_id=plugin_id))
        except Exception as e:
            logger.error(f"Failed to stop plugin: {e}")
            return self._failed_request(e)

    def plugin_unload(self, plugin_id: str) -> PendingRequest:
        """Ask the manager to unload a plugin."""
        try:
            return self.request(RequestPluginUnload(plugin_id=plugin_id))
        except Exception as e:
            logger.error(f"Failed to unload plugin '{plugin_id}': {e}")
            return self._failed_request(e)
        
    def plugin_call(self, plugin_id: str, method_name: str, *args, **kwargs) -> PendingRequest:
        """
        Call a method on the plugin.
        
        Args:
            plugin_id: ID of the plugin to call
            method_name: Name of the method to call
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            PendingRequest resolving to the method return value
        """
        try:
            return self.request(RequestPluginCall(plugin_id=plugin_id, method_name=method_name, args=args, kwargs=kwargs))
        except Exception as e:
            logger.error(f"Failed to call plugin method '{method_name}': {e}")
            return self._failed_request(e)
        
    
    # ========================================================================
    # STATUS: Query manager state
    # ========================================================================
    
    def plugin_status(self, plugin_id: str=None) -> PendingRequest:
        """
        Get status of another plugin.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            PendingRequest resolving to status info
        """
        try:
            return self.request(RequestPluginStatus(plugin_id=plugin_id))
        except Exception as e:
            logger.debug(f"Failed to get status for {plugin_id}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return self._failed_request(e)
    
    def plugin_status_all(self) -> PendingRequest:
        """
        List all loaded plugins and their status.
        
        Returns:
            PendingRequest resolving to dictionary of plugin_id -> status info
        """
        try:
            return self.request(RequestPluginStatusAll())
        except Exception as e:
            logger.error(f"Failed to list plugins: {e}")
            return self._failed_request(e)

    
    def plugin_errors(self, plugin_id:str = None) -> PendingRequest:
        """
        Get any errors from the manager or other plugins.
        
        Returns:
            PendingRequest resolving to error list
        """
        try:
            return self.request(RequestPluginErrors(plugin_id=plugin_id))
        except Exception as e:
            logger.error(f"Failed to get plugin errors: {e}")
            return self._failed_request(e)

class PluginEnvRPC(PluginCoreRPC):
    """Convience class with plugin-specific commands pre-registered."""

    # ========================================================================
    # ENVIRONMENT VARIABLES
    # ========================================================================
    
    def env_get(self, var_name: str) -> PendingRequest:
        """
        Get environment variable from manager.
        
        Args:
            var_name: Variable key
        
        Returns:
            PendingRequest resolving to variable value
        """
        try:
            return self.request(RequestEnvGet(name=var_name))
        except Exception as e:
            logger.error(f"Failed to get env variable '{var_name}': {e}")
            return self._failed_request(e)

    
    def env_set(self, var_name: str, value: Any) -> PendingRequest:
        """
        Set environment variable in manager (notifies subscribed plugins).
        
        Args:
            var_name: Variable key
            value: Variable value
        """
        try:
            value = sanitize_for_ipc(value)
            return self.request(RequestEnvSet(name=var_name, value=value))
        except Exception as e:
            logger.error(f"Failed to set env variable '{var_name}': {e}")
            return self._failed_request(e)
    
    def env_get_all(self) -> PendingRequest:
        """
        Get all environment variables.
        
        Returns:
            PendingRequest resolving to dictionary of all env variables
        """
        try:
            return self.request(RequestEnvGetAll())
        except Exception as e:
            logger.error(f"Failed to get all env variables: {e}")
            return self._failed_request(e)
        
    def env_subscribe(self, var_name: str) -> PendingRequest:
        """
            Subscribe to an environment variable.

        """
        try:
            return self.request(RequestEnvSubscribe(name=var_name))
        except Exception as e:
            logger.error(f"Failed to subscribe to env variable '{var_name}': {e}")
            return self._failed_request(e)
        
    def env_unsubscribe(self, var_name: str) -> PendingRequest:
        """
            Unsubscribe from an environment variable.

        """
        try:
            return self.request(RequestEnvUnsubscribe(name=var_name))
        except Exception as e:
            logger.error(f"Failed to unsubscribe from env variable '{var_name}': {e}")
            return self._failed_request(e)

    def env_update(self, var_name: str, value: Any = None) -> PendingRequest:
        """
            Update an environment variable.

        """
        try:
            return self.request(RequestEnvUpdate(name=var_name, value=value))
        except Exception as e:
            logger.error(f"Failed to notify update env variable '{var_name}': {e}")
            return self._failed_request(e)

class PluginTopicRPC(PluginCoreRPC):
    # ========================================================================
    # PUB/SUB: Publishing
    # ========================================================================
    
    def topic_announce(self, topic: str) -> PendingRequest:
        """
        Announce a topic.
        
        Args:
            topic: Topic name
        """
        try:
            return self.request(RequestTopicAnnounce(topic=topic))
        except Exception as e:
            logger.error(f"Failed to announce topic '{topic}': {e}")
            return self._failed_request(e)

    def topic_subscribe(self, subscriber: Subscriber) -> PendingRequest:
        """
        Subscribe to a topic.
        
        Args:
            subscriber: Subscriber descriptor
        """
        try:
            return self.request(RequestTopicSubscribe(subscriber=subscriber))
        except Exception as e:
            logger.error(f"Failed to subscribe to '{subscriber}': {e}")
            return self._failed_request(e)

    def topic_unsubscribe(self, subscriber: Subscriber) -> PendingRequest:
        """
        Unsubscribe from a topic.
        
        Args:
            subscriber: Subscriber descriptor
        """
        try:
            return self.request(RequestTopicUnsubscribe(subscriber=subscriber))
        except Exception as e:
            logger.error(f"Failed to unsubscribe from '{subscriber}': {e}")
            return self._failed_request(e)

    def topic_list(self, plugin_id: str=None) -> PendingRequest:
        """
        List all published topics.
        Returns:
            PendingRequest resolving to list of topic names
        """
        try:
            return self.request(RequestTopicList(plugin_id=plugin_id))
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return self._failed_request(e)
        
class PluginRPC(PluginLifeCycleRPC, PluginEnvRPC, PluginTopicRPC):
    """Convience class with all command categories pre-registered."""
    pass

