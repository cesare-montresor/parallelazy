import copy
from dataclasses import dataclass
import logging
from collections import OrderedDict
import time
from typing import List, Optional

import torch
from torch import cuda
import torch.nn as nn
from torch.multiprocessing.reductions import reduce_tensor

logger = logging.getLogger(__name__)


def mark_graph_output(t: torch.Tensor) -> torch.Tensor:
    """Call this immediately after a reduce-overhead compiled fn returns.
    PT2 compiled graphs sometimes pool memory in ways that shouldn't cross IPC."""
    if isinstance(t, torch.Tensor):
        t._from_graph_pool = True
    return t


def _is_ipc_received(t: torch.Tensor) -> bool:
    """True if `t` sits on a CUDA storage that was itself received via IPC
    from another process (and therefore can't be re-shared as-is), OR if
    it was pooled by a compiled graph."""
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


def sanitize_for_ipc(obj):
    """
    Recursively return a version of `obj` safe to put on a
    torch.multiprocessing Pipe/Queue, cloning only when necessary.

      - torch.Tensor -> obj.clone() if IPC-received OR compiled-graph output
      - nn.Module    -> deepcopy(obj) if any param/buffer needs cloning
      - dict/list/tuple -> rebuilt recursively (payloads are usually dicts
        of tensors)
      - anything else -> returned unchanged
    """
    if obj is None:
        return None

    if isinstance(obj, torch.Tensor):
        return obj.clone() if _is_ipc_received(obj) else obj

    if isinstance(obj, nn.Module):
        return _deepcopy_without_hooks(obj) if _module_needs_clone(obj) else obj

    if isinstance(obj, dict):
        return {k: sanitize_for_ipc(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        cloned = [sanitize_for_ipc(v) for v in obj]
        return type(obj)(cloned) if isinstance(obj, tuple) else cloned

    return obj




class _IpcEventRingBuffer:
    """
    Fixed-size, round-robin pool of interprocess CUDA events for a single layer.
 
    Why a ring buffer instead of one reused event: a single shared
    torch.cuda.Event(interprocess=True) can get re-armed by record() before
    the downstream consumer (a different process, reading through the IPC
    handle) has finished waiting on / reading the data associated with the
    previous record() call. That's a race, not just a style issue.
 
    A ring buffer of N events gives the consumer up to N-1 steps of slack:
    while we're recording event k, the consumer can still be draining event
    k-1, k-2, ... To avoid unbounded growth (the original leaked-IPC-handle
    bug), the buffer is preallocated once per layer and reused forever --
    never grows past `size`.
 
    size=3 is a reasonable default: it tolerates a consumer that's briefly
    one or two steps behind without giving it so much slack that the
    dashboard reads badly stale activations. Raise it if the consumer proves
    to be reliably more than 1-2 steps behind; lower it to 2 (the minimum
    that's still safe) to reduce IPC handle count if fewer processes read
    less frequently.
    """
    def __init__(self, size: int = 3):
        self.size = size
        self._events: List[cuda.Event] = [cuda.Event(interprocess=True) for _ in range(size)]
        self._idx = 0
 
    def next(self) -> cuda.Event:
        event = self._events[self._idx]
        self._idx = (self._idx + 1) % self.size
        return event




class IpcEventPool:
    """
    Demand-grown, capped pool of interprocess CUDA events for a single layer.
 
    Why not a single reused event: a shared torch.cuda.Event(interprocess=True)
    can get re-armed by record() before the downstream consumer (a different
    process, reading through the IPC handle) has finished waiting on / reading
    the data tied to the previous record() call. That's a race, not just a
    style issue.
 
    Why not mint one per call: that's the original leaked-IPC-handle bug --
    unbounded handle growth, thousands of live handles per epoch.
 
    This pool grows lazily instead of preallocating a fixed size: on each
    next() call it looks for an existing slot whose recorded GPU work has
    already completed (event.query() == True) and reuses that one; if none
    are free it allocates a new event, up to `max_size`. Once `max_size` is
    hit it falls back to round-robin reuse of the oldest slot and logs a
    warning, rather than growing further (that would reintroduce the leak).
 
    IMPORTANT CAVEAT: event.query() only reports whether the *producer-side*
    GPU work recorded up to that event has finished executing (i.e. `scores`
    is fully computed) -- it does NOT confirm the downstream consumer has
    finished reading the data through the IPC handle. There is no ack channel
    here from the consumer back to the producer. So this is a heuristic that
    avoids the worst case (re-arming work that isn't even done yet) but is
    not a strict guarantee against a slow consumer racing a reused event. If
    that matters for correctness (not just staleness), add an explicit
    consumer -> producer ack (e.g. publish "consumed layer_id/step_id" and
    only reuse an event once its consumption has been acked) instead of
    relying on query() alone.
    """
    def __init__(self, max_size: int = 8):
        self.max_size = max_size
        self._events: List[cuda.Event] = []
        self._rr_idx = 0  # round-robin cursor used only once max_size is hit
 
    def next(self, layer_id: str = "") -> cuda.Event:
        # 1. Reuse a slot whose previously recorded work has completed.
        for event in self._events:
            if event.query():
                return event
 
        # 2. No free slot -- grow if we're under the cap.
        if len(self._events) < self.max_size:
            event = cuda.Event(interprocess=True)
            self._events.append(event)
            return event
 
        # 3. At the cap and everything still appears busy: fall back to
        # round-robin reuse of the oldest slot rather than growing further
        # or blocking. This trades a bit of staleness risk for a hard bound
        # on IPC handle count.
        logger.warning(
            f"IPC event pool for layer {layer_id!r} exhausted at max_size="
            f"{self.max_size} with no free slots; reusing oldest event. "
            f"Consider raising max_size or checking whether the consumer "
            f"is falling behind."
        )
        event = self._events[self._rr_idx]
        self._rr_idx = (self._rr_idx + 1) % self.max_size
        return event




# =====================================================================
# IPC event pool for the score-publishing pipeline.
#
# NOTE: earlier iterations of this file defined three different versions
# of this idea in sequence (_IpcEventRingBuffer, a demand-grown
# _IpcEventPool, then this paired-slot _IpcEventPool) without removing the
# previous ones. Because the last two both used the class name
# `_IpcEventPool`, the demand-grown version was silently shadowed and dead
# -- the same failure mode as the duplicate `runtime_map` def caught
# earlier in modularobserver.py. Only this final version remains below;
# the other two were deleted rather than left as unused dead code.
# =====================================================================

@dataclass
class IpcEventCuda:
    ready: cuda.Event
    release: cuda.Event

    def record(self):
        """Record the ready event on the producer's stream."""
        self.ready.record()

    def wait(self):
        """Blocks until the producer has recorded the ready event."""
        self.ready.synchronize()

    def clear(self):
        """Signals that the consumer has finished reading the data."""
        self.release.record()


class _IpcEventSlot:
    """
    One reusable slot: a paired (producer, consumer) interprocess CUDA event.
 
    - produced_event: recorded by the producer right after `scores` is
      computed. The consumer waits on this before reading.
    - consumed_event: recorded by the CONSUMER, on its own stream, once it
      has actually finished reading/using the data from this slot. This is
      the piece a single-event design is missing: it's a real signal of
      consumer progress instead of a proxy (query() on the producer's own
      event only ever reflects producer-side GPU completion, which happens
      almost immediately and tells us nothing about the consumer).
    """
    __slots__ = ("produced_event", "consumed_event", "produced_at")
 
    def __init__(self):
        self.produced_event = cuda.Event(interprocess=True)
        self.consumed_event = cuda.Event(interprocess=True)
        self.produced_at: Optional[float] = None  # time.monotonic() of last use; None = never used

    def to_event(self) -> IpcEventCuda:
        """Return a simple pair of events for the producer to publish."""
        return IpcEventCuda(ready=self.produced_event, release=self.consumed_event)
 
class IpcEventPoolManaged:
    """
    Fixed-size, preallocated pool of paired producer/consumer event slots for
    a single layer (or any single IPC-published stream that needs one).
 
    A slot is safe to reuse once `consumed_event.query()` is True -- meaning
    the consumer has actually recorded it after finishing with the data, not
    merely that our own GPU work finished. query()-ing only the producer's
    own event can never tell you anything about the consumer, since GPU
    compute for the published tensor finishes long before any cross-process
    consumer has even opened the IPC handle -- that's why the ack exists.
 
    FAILSAFE: if a consumer crashes, hangs, or has a bug that skips recording
    consumed_event, that slot's consumed_event.query() would never return
    True and a naive pool would strand it forever -- with enough stuck slots
    the pool starves completely. To guard against that, each slot tracks
    produced_at (wall-clock time of its last use). If a slot is still unacked
    after `stale_after_s` seconds, the pool reclaims it anyway and logs a
    warning, so a consumer that forgets (or dies) can't permanently wedge the
    pool. This is a bounded risk, not a guarantee: a genuinely slow but
    still-alive consumer could theoretically be raced if it takes longer than
    stale_after_s. Set stale_after_s comfortably above the consumer's
    worst-case processing time.
    """
    def __init__(self, size: int = 4, stale_after_s: float = 30.0):
        self.size = size
        self.stale_after_s = stale_after_s
        self._slots: List[_IpcEventSlot] = [_IpcEventSlot() for _ in range(size)]
        self._rr_idx = 0
 
    def acquire(self, stream_id: str = "") -> IpcEventCuda:
        now = time.monotonic()
 
        # 1. Prefer a slot that's either never been used, or whose consumer
        # has genuinely finished with it (real ack, not a GPU-completion proxy).
        for slot in self._slots:
            if slot.produced_at is None or slot.consumed_event.query():
                slot.produced_at = now
                return slot.to_event()
 
        # 2. Nothing acked yet: reclaim the oldest slot if it's past the
        # staleness timeout, even without an ack. This is the failsafe --
        # it bounds how long a stuck/crashed consumer can hold a slot
        # hostage, at the cost of possibly racing a live-but-slow consumer.
        oldest = min(self._slots, key=lambda s: s.produced_at or 0.0)
        age = now - (oldest.produced_at or 0.0)
        if age >= self.stale_after_s:
            logger.warning(
                f"IPC event slot for {stream_id!r} reclaimed after "
                f"{age:.1f}s without a consumer ack (stale_after_s="
                f"{self.stale_after_s}). Consumer may be stuck, crashed, or "
                f"not recording consumed_event -- check the subscriber."
            )
            oldest.produced_at = now
            return oldest.to_event()
 
        # 3. Everything is busy and still within the staleness window: fall
        # back to round-robin reuse without an ack. This can race a live but
        # slow consumer; it exists only so we never block or grow past
        # `size`. If this fires often, raise `size` rather than lowering
        # stale_after_s.
        logger.warning(
            f"IPC event pool for {stream_id!r} exhausted (size={self.size}); "
            f"reusing slot {self._rr_idx} without an ack. Consumer is "
            f"falling behind."
        )
        slot = self._slots[self._rr_idx]
        self._rr_idx = (self._rr_idx + 1) % self.size
        slot.produced_at = now
        return slot.to_event()
 
 