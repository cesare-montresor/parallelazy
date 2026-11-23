# Implementation Plan

This plan turns the findings in `bug_analysys.md` into staged changes. It is
for review; no implementation choices beyond those stated here are assumed to
be final.

## Goals

- Ensure plugin failures, peer shutdown, and startup errors are visible rather
  than leaving callers blocked indefinitely.
- Make pub/sub and RPC cleanup reliable when plugins exit unexpectedly.
- Preserve the current debugger workflow, including time to attach to a worker.
- Address confirmed behavior defects before broader API and packaging changes.

## Phase 1: Startup, lifecycle, and RPC failure handling

1. **Represent worker startup phases explicitly**
   - Distinguish at least: process started, waiting for debugger, initializing
     worker/plugin, and ready.
   - Surface the current phase to the manager so a debugger pause is not
     mistaken for a hung initializer.
2. **Use separate timeout policies**
   - Keep debugger-attach waiting independent from worker initialization.
   - Debugger attachment is unbounded by default; `timeout_debugger_attach`
     can set a finite deadline.
   - `timeout_initialize` bounds worker initialization, defaults to 30 seconds,
     and starts separately after debugger attachment.
   - Report the phase and plugin ID on timeout.
   - Do not replace the current repeated wait with one hard deadline that
     includes the debugger pause.
3. **Replace startup barriers with per-plugin outcomes**
   - Collect each load/start result independently and return or expose a
     plugin-ID-to-result mapping.
   - Keep all plugin loads/starts concurrent; collect each completion without
     allowing a failed worker thread to strand the others.
   - Keep synchronous waiting behavior explicit; do not silently return
     `None` when some plugins fail.
4. **Handle RPC peer death**
   - Treat `EOFError` and `OSError` as channel closure in the RPC listener.
   - Mark the endpoint disconnected and fail all pending requests with a clear
     peer-disconnected error.
   - Remove timed-out requests from the pending map.
   - Add configurable bounded waits to synchronous RPC call sites, without
     breaking callers that intentionally request asynchronous results.
5. **Validate lifecycle cleanup**
   - Confirm worker listener threads and RPC executors stop after shutdown or
     peer death.
   - On failed initialization, terminate/join the worker and clean up its RPC
     channels and manager registration.

## Phase 2: Pub/sub reliability

1. **Clarify pipe ownership**
   - Document which process owns each pipe endpoint.
   - Close inherited or duplicated endpoints that a process does not own.
   - Verify that publisher and subscriber death produces detectable EOF or
     broken-pipe errors.
2. **Make fan-out resilient**
   - Handle closed subscribers individually, evict them from registrations,
     and continue attempting delivery to other subscribers.
   - Return a documented delivery result (for example, successful delivery
     count or structured per-subscriber outcomes), not an unconditional
     success value.
   - Define whether delivery is blocking and what backpressure/drop behavior
     callers can expect; do not treat a blocking pipe send as non-blocking.
3. **Make subscription changes transactional**
   - Roll back manager registration when forwarding subscription setup fails.
   - Support unsubscribe when no publisher is currently registered.
   - Ensure manager and worker state are both cleaned up on unsubscribe,
     plugin exit, and unload.
4. **Align topic listing**
   - Fix the manager topic-list RPC handler signature.
   - Define whether topic listing means published topics, subscribed topics,
     or both, and make manager and worker APIs consistent.

## Phase 3: Plugin APIs and state/error reporting

1. Add public `Plugin` wrappers for `topic_unsubscribe`, `env_unsubscribe`,
   `topic_list`, and `plugin_errors`, following existing API conventions.
2. On initialization, start, or `main()` failure:
   - Record the error and set `PluginState.ERROR` where appropriate.
   - Invoke `on_error()` according to a documented policy.
   - Return the failure to the manager with plugin ID, phase, message, and
     traceback where practical.
3. Correct smaller confirmed behavior issues:
   - `EnvironmentManager.clear(key)` should leave other keys untouched when
     `key` is missing; define `clear()` with no key as clear-all.
   - Avoid producing the literal description `"None"` for plugins without a
     docstring.
   - Make plugin subclass registration behavior safe for reload/notebook
     redefinition, or document and provide an explicit replacement mechanism.
4. Fix secondary error paths, including `QueueDescriptor.__str__()` and
   unknown/late-response logging.

## Phase 4: Thread safety and environment semantics

1. Protect manager plugin, topic, and environment subscription registries
   consistently, or serialize their mutations through one execution path.
2. Avoid iterating mutable worker subscriber dictionaries while RPC handlers
   can change them.
3. Preserve environment notification order, preferably with a single ordered
   dispatcher rather than one new thread per update.
4. Environment notifications receive the new value; `None` remains a valid
   stored value. Initial subscription callbacks receive the current value,
   or `None` if the key is absent.

## Phase 5: Logging, imports, and compatibility

1. `display_logs=False` suppresses worker `print()` terminal output while
   preserving file capture when `write_logs=True`.
2. `write_logs` is available on the manager and plugin descriptors and disables
   manager/worker file handlers when false.
3. Process start-method setup runs explicitly during `PluginManager`
   initialization instead of at module import.
4. The declared minimum Python version is 3.11, matching `typing.Self` and
   supported generic syntax.
5. `fcntl` is imported lazily by Linux pipe monitoring and is not imported
   while importing the package on Windows.
6. `debugpy` is not packaged; the VS Code integration reuses VS Code's loaded
   version. `torch` and `numpy` remain mandatory for the core IPC implementation.

## Phase 6: IPC sanitization and CUDA-specific review

1. Add cycle detection to `sanitize_for_ipc()` and preserve supported
   container types, including namedtuples and ordered/default dictionaries.
2. Add focused CPU tests for nested containers, cycles, and unsupported
   objects.
3. Keep CUDA transport changes separate from generic RPC changes.
4. Test CUDA IPC event reuse and tensor lifetime with real producer/consumer
   processes before changing event-pool behavior. Treat the current analysis
   as source-based until those tests confirm it.

## Validation strategy

- Add focused tests for each fix before or alongside implementation.
- Test manager, publisher, subscriber, and RPC callee termination; late
  responses; request timeouts; slow consumers; and concurrent
  subscribe/publish/unload.
- Test startup with:
  - debugger disabled;
  - debugger attached promptly;
  - debugger attachment delayed beyond the normal initialization timeout;
  - debugger attachment timeout configured, if supported;
  - plugin initialization failure and process exit before readiness.
- Verify aggregate load/start methods report each plugin's outcome when one
  plugin fails or is slow.
- Run relevant `unittest` modules in the project virtual environment, then
  the full suite. Run CUDA tests separately and record GPU availability and
  any shared-tensor lifecycle warnings.

## Suggested implementation order

1. Startup phase reporting and debugger-aware timeout semantics.
2. RPC disconnect handling and pending-request cleanup.
3. Pipe ownership and dead-subscriber cleanup.
4. Subscription rollback and topic-list consistency.
5. Error/state reporting and the small confirmed correctness fixes.
6. Thread-safety and environment semantics.
7. Logging, compatibility, import side effects, and packaging.
8. Sanitization and CUDA event-pool work.

## Decisions for review

- **Resolved:** debugger attachment waits indefinitely by default; callers can
  set a finite `timeout_debugger_attach`.
- **Resolved:** aggregate load/start methods return a plugin-ID-to-boolean
  mapping.
- **Resolved:** parallel loading and starting remain concurrent, and aggregate
  calls still wait for all submitted operations. Per-plugin futures collect
  outcomes without a barrier deadlocking when a worker thread fails.
- What should topic publish return: successful count, boolean, or per-subscriber
  outcomes?
- Should `display_logs=False` suppress only terminal output, or should logging
  have a separate file-writing switch?
