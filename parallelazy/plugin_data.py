"""
Plugin System Message Protocol (Refactored)

Unified, lean messaging system for inter-process communication:

Core Concepts:
  1. Request/Response: Unified for ALL operations (lifecycle, services, status)
  2. DataExchange: Unified subscription model for pub/sub + env vars
  3. Channel: Proper async request/response routing with background thread
  
Message Flows:
  - Control: Manager sends Request("start"), Worker replies with Response
  - Services: Manager sends Request("call_service", ...), Worker replies
  - Pub/Sub: Manager.publish(topic, data) → direct to subscriber queues
  - Env Vars: Manager.set_env(key, value) → broadcasts EnvUpdate to subscribers

No Commands/Responses wrappers, no double indirection. Just Request and Response.
"""
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING
import logging
from concurrent.futures import Future
from torch import multiprocessing as mp
from dataclasses import asdict, dataclass, field, fields, replace
from enum import IntEnum
from multiprocessing.connection import Connection

import threading


class PluginStartupPhase(IntEnum):
    """Startup milestones reported by a plugin worker process."""

    NOT_STARTED = 0
    PROCESS_STARTED = 1
    WAITING_FOR_DEBUGGER = 2
    INITIALIZING = 3
    READY = 4
    FAILED = 5



if TYPE_CHECKING:
    from parallelazy.plugin_rpc import PluginRPC
    


logger = logging.getLogger(__name__)


RequestCallback = Callable[["PluginRPC","Request"], Any]
ResponseCallback = Callable[["PluginRPC","Response"], Any]



class _TopicShutdownSentinel:
    """Private sentinel used to stop topic listener threads cleanly."""
    pass


_TOPIC_SHUTDOWN_SENTINEL = _TopicShutdownSentinel()


class _EnvMissing:
    """Sentinel indicating an environment key is not set (distinct from stored None)."""
    pass


ENV_MISSING = _EnvMissing()


@dataclass
class ManagerOptions:
    """
    Runtime options for `parallelazy.PluginManager` execution.

    Kept intentionally separate from `PipelineConfig` (hyperparameters) so
    infrastructure/logging settings never pollute experiment configs or
    change `config.signature()`.
    """

    work_dir: Optional[str] = None
    log_dir: str = "logs"
    log_level: int = logging.INFO
    display_logs: bool = True
    write_logs: bool = True
    timeout_request: float = 5.0
    timeout_start: float = 10.0
    timeout_start_debug: float = 30.0
    timeout_initialize: float = 30.0
    timeout_debugger_attach: Optional[float] = None

    def to_manager_kwargs(self) -> Dict[str, Any]:
        """Return kwargs accepted directly by `parallelazy.PluginManager(...)`."""
        return asdict(self)

    def with_updates(self, **overrides: Any) -> "ManagerOptions":
        valid = {f.name for f in fields(self)}
        filtered = {k: v for k, v in overrides.items() if k in valid}
        return replace(self, **filtered)


class QueueDescriptor:
    """Descriptor for holding read/write connections representing a pipe-backed queue."""
    def __init__(
        self,
        send: Optional[Connection] = None,
        receive: Optional[Connection] = None,
        *,
        recieve: Optional[Connection] = None,
    ):
        self.send = send
        self.receive = receive if receive is not None else recieve
    
    def __str__(self):
        send_closed = self.send.closed if self.send is not None else None
        receive_closed = self.receive.closed if self.receive is not None else None
        return f"QueueDescriptor(send_closed={send_closed}, receive_closed={receive_closed})"
        


# ============================================================================
# CHANNEL: Bidirectional duplex IPC with proper async response routing
# ============================================================================
class Queue:
    
    def __init__(self, descriptor: QueueDescriptor = None):
        if descriptor is None:
            receive, send = mp.Pipe(duplex=False)
            self._send = QueueSend(send)
            self._receive = QueueReceive(receive)
        else:
            self._send = QueueSend(descriptor.send) if descriptor.send is not None else None
            self._receive = QueueReceive(descriptor.receive) if descriptor.receive is not None else None

    @property
    def sender(self):
        return self._send
    
    @property
    def receiver(self):
        return self._receive
    
    def closed(self):
        send_closed = self._send._conn.closed if self._send is not None else True
        recv_closed = self._receive._conn.closed if self._receive is not None else True
        if self._send is not None and self._receive is not None:
            return send_closed or recv_closed
        return send_closed and recv_closed
    
    def put(self, item):
        if self._send is None:
            raise ValueError("Queue has no send end")
        self._send.put(item)

    def put_nowait(self, item):
        self.put(item)

    def send_bytes(self, data: bytes):
        if self._send is None:
            raise ValueError("Queue has no send end")
        self._send.send_bytes(data)
    
    def get(self):
        if self._receive is None:
            raise ValueError("Queue has no receive end")
        return self._receive.get()
    
    def get_nowait(self):
        return self.get()

    def close_sender(self):
        if self._send is not None:
            try:
                self._send.close()
            except Exception:
                pass
            self._send = None

    def close(self):
        self.close_sender()
        if self._receive is not None:
            try:
                self._receive.close()
            except Exception:
                pass
            self._receive = None

    def descriptor(self):
        return QueueDescriptor(
            send=self._send._conn if self._send is not None else None,
            receive=self._receive._conn if self._receive is not None else None,
        )

    def send_descriptor(self):
        """Return a descriptor containing only the write end of the pipe."""
        return QueueDescriptor(
            send=self._send._conn if self._send is not None else None,
            receive=None,
        )


class QueueSend:
    def __init__(self, conn: Connection):
        self._conn = conn
        self._lock = threading.Lock()

    def put(self, item):
        with self._lock:
            self._conn.send(item)

    def send_bytes(self, data: bytes):
        with self._lock:
            self._conn.send_bytes(data)

    def close(self):
        self._conn.close()


class QueueReceive:
    def __init__(self, conn: Connection):
        self._conn = conn

    def get(self):
        return self._conn.recv()

    def close(self):
        self._conn.close()


# Backwards-compatible alias for the legacy misspelled name.
QueueRecive = QueueReceive

class Channel:
    """
    Duplex communication channel with request/response routing.
    Pass channel.reverse() to the other process — it's picklable.
    """

    def __init__(self, descriptor: QueueDescriptor = None ):
        if descriptor is None:
            r1, w1 = mp.Pipe(duplex=False)
            r2, w2 = mp.Pipe(duplex=False)
            self._send = QueueSend(w1)
            self._receive = QueueReceive(r2)
            self._remote_send = QueueSend(w2)
            self._remote_receive = QueueReceive(r1)
        else:
            self._send = QueueSend(descriptor.send)
            self._receive = QueueReceive(descriptor.receive)
            self._remote_send = None
            self._remote_receive = None

    
    def descriptor(self):
        return QueueDescriptor(self._send._conn, self._receive._conn)


    def remote_descriptor(self):
        return QueueDescriptor(self._remote_send._conn, self._remote_receive._conn)
    

    def send(self, message) -> None:
        self._send.put(message)

    def receive(self) -> Optional[object]:
        """Receive a message, blocking until one arrives or the pipe closes."""
        return self._receive.get()

    def close_remote(self) -> None:
        """Close the remote pipe ends after handing them to the child process."""
        if self._remote_send is not None:
            self._remote_send.close()
            self._remote_send = None
        if self._remote_receive is not None:
            self._remote_receive.close()
            self._remote_receive = None

    def shutdown(self):
        if self._send:
            self._send.close()
        if self._receive:
            self._receive.close()
        self.close_remote()


# ============================================================================
# CORE MESSAGE TYPES
# ============================================================================


class CommandType:
    """Standard commands for plugin control."""
    PLUGIN_LOAD = "plugin_load"
    PLUGIN_START = "plugin_start"
    PLUGIN_STOP = "plugin_stop"
    PLUGIN_UNLOAD = "plugin_unload"
    PLUGIN_STATUS = "plugin_status"
    PLUGIN_STATUS_ALL = "plugin_status_all"
    PLUGIN_ERRORS = "plugin_errors"
    PLUGIN_CALL = "plugin_call"

    ENV_SET = "env_set"
    ENV_GET = "env_get"
    ENV_GET_ALL = "env_get_all"
    ENV_SUBSCRIBE = "env_subscribe"
    ENV_UNSUBSCRIBE = "env_unsubscribe"
    ENV_UPDATE = "env_update"

    TOPIC_SUBSCRIBE = "topic_subscribe"
    TOPIC_UNSUBSCRIBE = "topic_unsubscribe"
    TOPIC_ANNOUNCE = "topic_announce"
    TOPIC_LIST = "topic_list"

    @classmethod
    def commands(cls):
        return iter([
            cls.PLUGIN_LOAD,
            cls.PLUGIN_START,
            cls.PLUGIN_STOP,
            cls.PLUGIN_UNLOAD,
            cls.PLUGIN_STATUS,
            cls.PLUGIN_STATUS_ALL,
            cls.PLUGIN_ERRORS,
            cls.PLUGIN_CALL,
            cls.ENV_SET,
            cls.ENV_GET,
            cls.ENV_GET_ALL,
            cls.ENV_SUBSCRIBE,
            cls.ENV_UNSUBSCRIBE,
            cls.ENV_UPDATE,
            cls.TOPIC_SUBSCRIBE,
            cls.TOPIC_UNSUBSCRIBE,
            cls.TOPIC_ANNOUNCE,
            cls.TOPIC_LIST
        ])


@dataclass
class Message:
    """Base message class for all communication."""
    sender: str = None
    message_id: str = None
    
    def copy(self):
        kwargs = {f.name: getattr(self, f.name) for f in fields(self)}
        kwargs['message_id'] = None
        return type(self)(**kwargs)

@dataclass
class Request(Message):
    """
    Unified request for all operations.ada
    Worker replies with Response(message_id, result=...) or Response(message_id, error=...)
    """
    command: CommandType = None

    def copy(self):
        """Create a copy of the message (for forwarding/modification)."""
        clone = type(self)(**self.__dict__)
        clone.message_id = None  # message_id are generated at request time.
        return clone

@dataclass
class Response(Message):
    """
    Unified response for all request results.
     - result: The return value of the request (if successful)
     - error: An error message (if an error occurred)
    """
    result: Any = None
    error: Optional[str] = None

class PendingRequest():
    def __init__(
        self,
        message_id: str,
        callback: Callable = None,
        on_timeout: Optional[Callable[[str], None]] = None,
    ):
        super().__init__()
        self.future = Future()
        self.message_id = message_id
        self._on_timeout = on_timeout
        if callback:
            self.future.add_done_callback(callback)

    def on_ready(self, callback: Callable):
        self.future.add_done_callback(callback)
    
    def set_result(self, result: Response|Any):
        if not isinstance(result, Response):
            result = Response(message_id=self.message_id, result=result)
        if not self.future.done():
            try:
                self.future.set_result(result)
            except Exception:
                pass

    def set_error(self, error: str|Exception|Any):
        if not self.future.done():
            try:
                self.future.set_result(Response(message_id=self.message_id, error=str(error)))
            except Exception:
                pass

    def wait(self, timeout=None) -> None | Response:
        try:
            result: Response = self.future.result(timeout=timeout)
            return result
        except TimeoutError:
            if self._on_timeout is not None:
                self._on_timeout(self.message_id)
            return None
        
    def __hash__(self):
        return hash(self.message_id)


class Subscription:
    """
    Represents a subscription to a topic or environment variable.
    """
    def __init__(self, topic: str, callback: Callable, queue: Queue=None, thread: Optional[threading.Thread] = None):
        self.topic = topic
        self.callback = callback
        self.queue = queue or Queue()  # For topic subscriptions, queue to receive data
        self.thread: Optional[threading.Thread] = thread  # Thread handling this subscription (if needed)
        self._wakeup_r, self._wakeup_w = mp.Pipe(duplex=False)
        self._wakeup_lock = threading.Lock()
        self._stopped = False

    def signal_stop(self) -> None:
        """Wake the listener thread without writing to the topic data pipe."""
        with self._wakeup_lock:
            if self._stopped:
                return
            self._stopped = True
            try:
                if not self._wakeup_w.closed:
                    self._wakeup_w.send_bytes(b"1")
            except Exception:
                pass

    def close(self) -> None:
        """Signal stop and close both wakeup and topic pipe handles."""
        self.signal_stop()
        try:
            self.queue.close()
        except Exception:
            pass
        for conn in (self._wakeup_w, self._wakeup_r):
            try:
                if not conn.closed:
                    conn.close()
            except Exception:
                pass

class Subscriber:
    """Represents a subscriber to a topic or environment variable."""
    def __init__(self, plugin_id: str, topic: str, queue_descriptor: QueueDescriptor, queue: Optional[Queue] = None):
        self.plugin_id: str = plugin_id
        self.topic: str = topic
        self.queue_descriptor: QueueDescriptor = queue_descriptor
        self._queue: Optional[Queue] = queue  # Will be initialized when subscription is active
    
    @property
    def queue(self) -> Queue:
        if self._queue is None:
            self._queue = Queue(self.queue_descriptor)
        return self._queue
    
    @queue.setter
    def queue(self, value: Queue):
        self._queue = value

    def close(self) -> None:
        if self._queue is not None:
            try:
                self._queue.close()
            except Exception:
                pass
        elif self.queue_descriptor is not None:
            for conn in (self.queue_descriptor.send, self.queue_descriptor.receive):
                if conn is not None:
                    try:
                        if not conn.closed:
                            conn.close()
                    except Exception:
                        pass
    

    def __str__(self):
        return f"Subscriber(plugin_id={self.plugin_id}, topic={self.topic}, queue_descriptor={self.queue_descriptor})"

# ============================================================================
# STATUS DATA STRUCTURES
# ============================================================================

@dataclass
class PluginStatus:
    """Status snapshot from plugin worker."""
    plugin_id: str
    state: str  # "initialized", "loaded", "running", "stopped", "error"
    is_running: bool
    subscriptions: int      # Number of data exchange subscriptions
    subscribers: int         # Number of subscribers to this plugin's topics
    published: int            # Number of topics published
    errors: int

    def to_dict(self):
        return {
            "plugin_id": self.plugin_id,
            "state": self.state,
            "is_running": self.is_running,
            "subscriptions": self.subscriptions,
            "subscribers": self.subscribers,
            "published": self.published,
            "errors": self.errors
        }


@dataclass
class ProcessStatus:
    """Manager-side process state, including the worker's startup phase."""
    plugin_id: str
    process_alive: bool
    process_pid: Optional[int]
    worker_status: Optional[PluginStatus] = None
    startup_phase: Optional[str] = None



"""
Classes for each request command:
    PLUGIN_LOAD = "plugin_load"
    PLUGIN_START = "plugin_start"
    PLUGIN_STOP = "plugin_stop"
    PLUGIN_UNLOAD = "plugin_unload"
    PLUGIN_STATUS = "plugin_status"
    PLUGIN_STATUS_ALL = "plugin_status_all"
    PLUGIN_ERRORS = "plugin_errors"
    PLUGIN_CALL = "plugin_call"

    ENV_SET = "env_set"
    ENV_GET = "env_get"
    ENV_GET_ALL = "env_get_all"
    ENV_SUBSCRIBE = "env_subscribe"
    ENV_UNSUBSCRIBE = "env_unsubscribe"
    ENV_UPDATE = "env_update"

    TOPIC_SUBSCRIBE = "topic_subscribe"
    TOPIC_UNSUBSCRIBE = "topic_unsubscribe"
    TOPIC_PUBLISH = "topic_publish"
    TOPIC_LIST = "topic_list"

"""



### Plugin Lifecycle Requests

@dataclass
class RequestPluginLoad(Request):
    command: CommandType = field(default=CommandType.PLUGIN_LOAD, init=False)
    descriptor: Any = None
    options: Optional[dict] = None

@dataclass
class RequestPluginStart(Request):
    command: CommandType = field(default=CommandType.PLUGIN_START, init=False)
    plugin_id: Optional[str] = None

@dataclass
class RequestPluginStop(Request):
    command: CommandType = field(default=CommandType.PLUGIN_STOP, init=False)
    plugin_id: Optional[str] = None

@dataclass
class RequestPluginUnload(Request):
    command: CommandType = field(default=CommandType.PLUGIN_UNLOAD, init=False)
    plugin_id: Optional[str] = None

@dataclass
class RequestPluginStatus(Request):
    command: CommandType = field(default=CommandType.PLUGIN_STATUS, init=False)
    plugin_id: Optional[str] = None

@dataclass
class RequestPluginStatusAll(Request):
    command: CommandType = field(default=CommandType.PLUGIN_STATUS_ALL, init=False)

@dataclass
class RequestPluginErrors(Request):
    command: CommandType = field(default=CommandType.PLUGIN_ERRORS, init=False)
    plugin_id: Optional[str] = None

@dataclass
class RequestPluginCall(Request):
    command: CommandType = field(default=CommandType.PLUGIN_CALL, init=False)
    plugin_id: str = None
    method_name: str = None
    args: tuple = None
    kwargs: dict = None

@dataclass
class RequestEnvSet(Request):
    command: CommandType = field(default=CommandType.ENV_SET, init=False)
    name: str = None
    value: Any = None

@dataclass
class RequestEnvGet(Request):
    command: CommandType = field(default=CommandType.ENV_GET, init=False)
    name: str = None

@dataclass
class RequestEnvGetAll(Request):    
    command: CommandType = field(default=CommandType.ENV_GET_ALL, init=False)


@dataclass
class RequestEnvSubscribe(Request):
    command: CommandType = field(default=CommandType.ENV_SUBSCRIBE, init=False)
    name: str = None


@dataclass
class RequestEnvUnsubscribe(Request):
    command: CommandType = field(default=CommandType.ENV_UNSUBSCRIBE, init=False)
    name: str = None

@dataclass
class RequestEnvUpdate(Request):
    command: CommandType = field(default=CommandType.ENV_UPDATE, init=False)
    name: str = None
    value: Any = None

### Pub/Sub Requests

@dataclass
class RequestTopicSubscribe(Request):
    command: CommandType = field(default=CommandType.TOPIC_SUBSCRIBE, init=False)
    subscriber: Subscriber = None

@dataclass
class RequestTopicUnsubscribe(Request):
    command: CommandType = field(default=CommandType.TOPIC_UNSUBSCRIBE, init=False)
    subscriber: Subscriber = None


@dataclass
class RequestTopicAnnounce(Request):
    command: CommandType = field(default=CommandType.TOPIC_ANNOUNCE, init=False)
    topic: str = None


@dataclass
class RequestTopicList(Request):
    command: CommandType = field(default=CommandType.TOPIC_LIST, init=False)
    plugin_id: Optional[str] = None

