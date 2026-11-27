import copy
import dataclasses
import gc
import logging
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from multiprocessing.reduction import ForkingPickler
from typing import Any

import torch
import torch.nn as nn
from torch import cuda
from torch.multiprocessing.reductions import reduce_tensor

logger = logging.getLogger(__name__)


def release_cuda_ipc() -> None:
    """Release cached CUDA IPC handles if CUDA has been initialized in this process."""
    if cuda.is_initialized():
        gc.collect()
        cuda.ipc_collect()


def mark_graph_output(t: torch.Tensor) -> torch.Tensor:
    """Call this immediately after a reduce-overhead compiled fn returns.
    PT2 compiled graphs sometimes pool memory in ways that shouldn't cross IPC."""
    if isinstance(t, torch.Tensor):
        t._from_graph_pool = True  # type: ignore[attr-defined]
    return t


def _is_ipc_received(t: torch.Tensor) -> bool:
    """True if `t` sits on a CUDA storage that was itself received via IPC
    from another process (and therefore can't be re-shared as-is), OR if
    it was pooled by a compiled graph.

    Note: PyTorch does not expose a dedicated Python flag for CUDA storages
    opened via IPC handles, so for CUDA tensors we probe via `reduce_tensor(t)`
    (CPU and graph-pooled tensors take the fast path above).
    """
    if getattr(t, "_from_graph_pool", False):
        return True

    if not t.is_cuda:
        return False

    try:
        reduce_tensor(t)
        return False
    except RuntimeError as e:
        if "received from another process" in str(e):
            return True
        raise


def _module_needs_clone(module: nn.Module) -> bool:
    return any(
        _is_ipc_received(t)
        for t in (*module.parameters(recurse=True), *module.buffers(recurse=True))
    )


def _deepcopy_without_hooks(module: nn.Module) -> nn.Module:
    """deepcopy(module) will choke if any hook closure holds state that
    isn't deepcopy-able (a Queue, a Channel, a CUDA event, a lock...).
    Hooks are meaningless once the module crosses a process boundary
    anyway, so strip *all* hook registries for the duration of the copy
    and restore them on the original afterward. We iterate by attribute
    name rather than hardcoding _forward_hooks/_forward_pre_hooks/
    _backward_hooks, since torch has added more hook dicts over time
    (_forward_hooks_with_kwargs, _backward_pre_hooks, etc.) and any of
    them can hold non-copyable closures."""
    saved = []
    for m in module.modules():
        hook_attrs = {
            name: val for name, val in vars(m).items()
            if "hook" in name and isinstance(val, OrderedDict)
        }
        saved.append((m, hook_attrs))
        for name in hook_attrs:
            setattr(m, name, OrderedDict())

    try:
        return copy.deepcopy(module)
    except Exception:
        logger.error(
            "deepcopy failed while sanitizing module %r for IPC even after "
            "stripping hooks -- likely a non-hook attribute holding a "
            "non-copyable object (lock, open handle, CUDA event, etc.)",
            type(module).__name__,
            exc_info=True,
        )
        raise
    finally:
        for m, hook_attrs in saved:
            for name, val in hook_attrs.items():
                setattr(m, name, val)


def _sanitize_recursive(obj: Any, memo: dict[int, Any], has_tensor: list[bool]) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str, bytes, bytearray)):
        return obj

    if isinstance(obj, torch.Tensor):
        has_tensor[0] = True
        return obj.clone() if _is_ipc_received(obj) else obj

    if isinstance(obj, nn.Module):
        has_tensor[0] = True
        return _deepcopy_without_hooks(obj) if _module_needs_clone(obj) else obj

    obj_id = id(obj)
    if obj_id in memo:
        return memo[obj_id]

    if isinstance(obj, dict):
        memo[obj_id] = obj
        changed = False
        items = []
        for k, v in obj.items():
            new_v = _sanitize_recursive(v, memo, has_tensor)
            if new_v is not v:
                changed = True
            items.append((k, new_v))
        if not changed:
            return obj
        rebuilt_dict: dict[Any, Any]
        if isinstance(obj, OrderedDict):
            rebuilt_dict = OrderedDict()
        elif isinstance(obj, defaultdict):
            rebuilt_dict = defaultdict(obj.default_factory)
        else:
            rebuilt_dict = {}
        memo[obj_id] = rebuilt_dict
        for k, new_v in items:
            rebuilt_dict[k] = rebuilt_dict if new_v is obj else new_v
        return rebuilt_dict

    if isinstance(obj, list):
        memo[obj_id] = obj
        changed = False
        items = []
        for item in obj:
            new_item = _sanitize_recursive(item, memo, has_tensor)
            if new_item is not item:
                changed = True
            items.append(new_item)
        if not changed:
            return obj
        rebuilt_list: list[Any] = [None] * len(items)
        memo[obj_id] = rebuilt_list
        for idx, new_item in enumerate(items):
            rebuilt_list[idx] = rebuilt_list if new_item is obj else new_item
        return rebuilt_list

    if isinstance(obj, tuple):
        memo[obj_id] = obj
        changed = False
        items = []
        for v in obj:
            new_v = _sanitize_recursive(v, memo, has_tensor)
            if new_v is not v:
                changed = True
            items.append(new_v)
        if not changed:
            return obj
        rebuilt_tuple = type(obj)(*items) if hasattr(obj, "_fields") else type(obj)(items)
        memo[obj_id] = rebuilt_tuple
        return rebuilt_tuple

    if isinstance(obj, (set, frozenset)):
        changed = False
        items = []
        for v in obj:
            new_v = _sanitize_recursive(v, memo, has_tensor)
            if new_v is not v:
                changed = True
            items.append(new_v)
        if not changed:
            return obj
        return type(obj)(items)

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        memo[obj_id] = obj
        changed_fields: dict[str, Any] = {}
        for f in dataclasses.fields(obj):
            old_val = getattr(obj, f.name)
            new_val = _sanitize_recursive(old_val, memo, has_tensor)
            if new_val is not old_val:
                changed_fields[f.name] = new_val
        if not changed_fields:
            return obj
        rebuilt_dc: Any
        try:
            init_changes = {
                f.name: changed_fields[f.name]
                for f in dataclasses.fields(obj)
                if f.name in changed_fields and f.init
            }
            rebuilt_dc = dataclasses.replace(obj, **init_changes)
            for f in dataclasses.fields(obj):
                if f.name in changed_fields and not f.init:
                    object.__setattr__(rebuilt_dc, f.name, changed_fields[f.name])
        except Exception:
            rebuilt_dc = copy.copy(obj)
            for name, val in changed_fields.items():
                object.__setattr__(rebuilt_dc, name, val)
        memo[obj_id] = rebuilt_dc
        return rebuilt_dc

    return obj


def sanitize_for_ipc_with_info(obj: Any) -> tuple[Any, bool]:
    """
    Like `sanitize_for_ipc(obj)`, but returns `(sanitized_obj, has_tensor)` in a
    single traversal so callers can avoid a second walk to detect tensors.
    """
    has_tensor = [False]
    return _sanitize_recursive(obj, {}, has_tensor), has_tensor[0]


def sanitize_for_ipc(obj: Any) -> Any:
    """
    Recursively return a version of `obj` safe to put on a
    torch.multiprocessing Pipe/Queue, cloning only when necessary.

      - torch.Tensor -> obj.clone() if IPC-received OR compiled-graph output
      - nn.Module    -> deepcopy(obj) (with hooks stripped) if any param/buffer needs cloning
      - dict / OrderedDict / defaultdict / list / tuple / namedtuple / set / frozenset / dataclass
        -> traversed recursively with cycle detection, copy-on-write, preserving container types
      - anything else -> returned unchanged
    """
    return _sanitize_recursive(obj, {}, [False])


def _deliver_to_subscribers(
    topic: str,
    data: Any,
    subscribers: list,
    log: logging.Logger,
    has_tensor: bool = False,
) -> tuple[int, list[str]]:
    """Fan out a payload to subscribers, isolating failures and reporting dead IDs."""
    payload_bytes = None
    if len(subscribers) > 1 and not has_tensor:
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


# =====================================================================
# IPC event pool for cross-process CUDA synchronization.
# =====================================================================

@dataclass
class IpcEventCuda:
    ready: cuda.Event
    release: cuda.Event
    _ack_seq: torch.Tensor | None = None
    _seq: int = 0

    def record(self) -> None:
        """Record the ready event on the producer's current CUDA stream."""
        self.ready.record()

    def wait(self) -> None:
        """Block until the producer's ready event has completed."""
        self.ready.synchronize()

    def clear(self) -> None:
        """Signal that the consumer has finished reading the data."""
        self.release.record()
        if self._ack_seq is not None:
            self._ack_seq[0] = self._seq

    def is_cleared(self) -> bool:
        """Return True once the consumer has called clear() for this generation and GPU work completed."""
        if self._ack_seq is not None and int(self._ack_seq[0].item()) < self._seq:
            return False
        return self.release.query()


class _IpcEventSlot:
    """
    One reusable slot: a paired (producer, consumer) interprocess CUDA event
    backed by a shared monotonic sequence counter.

    CUDA events are level-triggered (`event.query()` is True before any
    `record()` and stays True after completion). Pairing `consumed_event`
    with a shared CPU sequence tensor ensures the producer only considers a
    slot acknowledged once the consumer has called `clear()` for the current
    generation AND the consumer's GPU stream has completed `consumed_event`.
    """
    __slots__ = ("produced_event", "consumed_event", "ack_seq", "expected_seq", "produced_at")

    def __init__(self):
        self.produced_event = cuda.Event(interprocess=True)
        self.consumed_event = cuda.Event(interprocess=True)
        self.ack_seq = torch.zeros(1, dtype=torch.int64).share_memory_()
        self.expected_seq: int = 0
        self.produced_at: float | None = None  # time.monotonic() of last use; None = never used

    def is_free(self) -> bool:
        if self.produced_at is None:
            return True
        if int(self.ack_seq[0].item()) < self.expected_seq:
            return False
        return self.consumed_event.query()

    def acquire(self, now: float) -> IpcEventCuda:
        self.expected_seq += 1
        self.produced_at = now
        return IpcEventCuda(
            ready=self.produced_event,
            release=self.consumed_event,
            _ack_seq=self.ack_seq,
            _seq=self.expected_seq,
        )


class IpcEventPoolManaged:
    """
    Fixed-size, preallocated pool of paired producer/consumer event slots for
    a single IPC-published CUDA stream.

    A slot is safe to reuse once the consumer has acknowledged the current
    generation via `event.clear()` AND `consumed_event.query()` is True.

    FAILSAFE: if a consumer crashes, hangs, or skips calling `clear()`, each
    slot tracks `produced_at`. If a slot remains unacknowledged past
    `stale_after_s`, the pool reclaims it and logs a warning so a dead
    consumer cannot permanently wedge the pool.
    """
    def __init__(self, size: int = 4, stale_after_s: float = 30.0):
        self.size = size
        self.stale_after_s = stale_after_s
        self._slots: list[_IpcEventSlot] = [_IpcEventSlot() for _ in range(size)]
        self._rr_idx = 0

    def acquire(self, stream_id: str = "") -> IpcEventCuda:
        now = time.monotonic()

        # 1. Prefer a slot that has never been used or whose consumer has acked
        # the current generation and finished GPU work on consumed_event.
        for slot in self._slots:
            if slot.is_free():
                return slot.acquire(now)

        # 2. Nothing acked yet: reclaim the oldest slot if it is past the
        # staleness timeout.
        oldest = min(self._slots, key=lambda s: s.produced_at or 0.0)
        age = now - (oldest.produced_at or 0.0)
        if age >= self.stale_after_s:
            logger.warning(
                f"IPC event slot for {stream_id!r} reclaimed after "
                f"{age:.1f}s without a consumer ack (stale_after_s="
                f"{self.stale_after_s}). Consumer may be stuck, crashed, or "
                f"not calling event.clear() -- check the subscriber."
            )
            return oldest.acquire(now)

        # 3. Everything is busy within the staleness window: fall back to
        # round-robin reuse so the producer never blocks or grows past `size`.
        logger.warning(
            f"IPC event pool for {stream_id!r} exhausted (size={self.size}); "
            f"reusing slot {self._rr_idx} without an ack. Consumer is "
            f"falling behind."
        )
        slot = self._slots[self._rr_idx]
        self._rr_idx = (self._rr_idx + 1) % self.size
        return slot.acquire(now)

    def has_busy(self) -> bool:
        return any(not s.is_free() for s in self._slots)

    def wait_all_free(self, timeout: float = 30.0, poll_s: float = 0.005) -> bool:
        """Wait for consumer acknowledgement on all slots. Does not reclaim stale slots."""
        deadline = time.monotonic() + timeout
        while self.has_busy():
            if time.monotonic() > deadline:
                busy = sum(1 for s in self._slots if not s.is_free())
                logger.warning(
                    "IPC event pool drain timed out: %d/%d slots still unacknowledged",
                    busy,
                    self.size,
                )
                return False
            time.sleep(poll_s)
        return True