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
from typing import Any, Callable, Optional, TYPE_CHECKING
import logging
from concurrent.futures import Future
from torch import multiprocessing as mp
from dataclasses import dataclass, field, fields
from multiprocessing.connection import Connection

import threading



if TYPE_CHECKING:
    from parallelazy.plugin_rpc import PluginRPC
    


logger = logging.getLogger(__name__)


RequestCallback = Callable[["PluginRPC","Request"], Any]
ResponseCallback = Callable[["PluginRPC","Response"], Any]



class QueueDescriptor:
    """Descriptor for holding 2 connections, read/write repesenting a queue made with pipes."""
    def __init__(self, send: Connection, recieve: Connection):
        self.send = send
        self.receive = recieve
    
    def __str__(self): return f"QueueDescriptor(send_closed={self.send.closed()}, receive_closed={self.receive.closed()})"
        


# ============================================================================
# CHANNEL: Bidirectional duplex IPC with proper async response routing
# ============================================================================
class Queue:
    
    def __init__(self, descriptor: QueueDescriptor = None):
        if descriptor is None:
            receive, send = mp.Pipe(duplex=False)
        else:
            receive, send = descriptor.receive, descriptor.send

        self._send = QueueSend(send)
        self._receive = QueueRecive(receive)

    @property
    def sender(self):
        return self._send
    
    @property
    def receiver(self):
        return self._receive
    
    def closed(self):
        return self._send._conn.closed or self._receive._conn.closed
    
    def put(self, item):
        self._send.put(item)

    def put_nowait(self, item):
        self._send.put(item)
    
    def get(self):
        return self._receive.get()
    
    def get_nowait(self):
        return self._receive.get()

    def close(self):
        self._send.put(None)  # Send a sentinel to indicate closure
        self._send.close()
        self._receive.close()

    def descriptor(self):
        return QueueDescriptor(self._send._conn, self._receive._conn)


class QueueSend:
    def __init__(self, conn: Connection):
        self._conn = conn
        self._lock = threading.Lock()

    def put(self, item):
        with self._lock:
            self._conn.send(item)

    def close(self):
        self._conn.close()


class QueueRecive:
    def __init__(self, conn: Connection):
        self._conn = conn

    def get(self):
        return self._conn.recv()

    def close(self):
        self._conn.close()

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
            self._receive = QueueRecive(r2)
            self._remote_send = QueueSend(w2)
            self._remote_receive = QueueRecive(r1)
        else:
            self._send = QueueSend(descriptor.send)
            self._receive = QueueRecive(descriptor.receive)
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

    def shutdown(self):
        if self._send:
            self._send.close()
        if self._receive:
            self._receive.close()
        if self._remote_send:
            self._remote_send.close()
        if self._remote_receive:
            self._remote_receive.close()


# ============================================================================
# CORE MESSAGE TYPES
# ============================================================================


class CommandType:
    """Standard commands for plugin control."""
    PLUGIN_START = "plugin_start"
    PLUGIN_STOP = "plugin_stop"
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
            cls.PLUGIN_START,
            cls.PLUGIN_STOP,
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
    def __init__(self, message_id: str, callback: Callable=None):
        super().__init__()
        self.future = Future()
        self.message_id = message_id
        if callback:
            self.future.add_done_callback(callback)

    def on_ready(self, callback: Callable):
        self.future.add_done_callback(callback)
    
    def set_result(self, result: Response|Any):
        if not isinstance(result, Response):
            result = Response(message_id=self.message_id, result=result)
        self.future.set_result(result)

    def set_error(self, error: str|Exception|Any):
        self.future.set_result(Response(message_id=self.message_id, error=str(error)))

    def wait(self, timeout=None) -> None | Response:
        try:
            result: Response = self.future.result(timeout=timeout)
            return result
        except TimeoutError:
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
    """Status of a plugin from manager perspective."""
    plugin_id: str
    process_alive: bool
    process_pid: Optional[int]
    worker_status: Optional[PluginStatus] = None



"""
Classes for each request command:
    PLUGIN_START = "plugin_start"
    PLUGIN_STOP = "plugin_stop"
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
class RequestPluginStart(Request):
    command: CommandType = field(default=CommandType.PLUGIN_START, init=False)
    plugin_id: Optional[str] = None

@dataclass
class RequestPluginStop(Request):
    command: CommandType = field(default=CommandType.PLUGIN_STOP, init=False)
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

