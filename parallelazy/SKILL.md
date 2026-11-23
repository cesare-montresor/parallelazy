---
name: parallelazy
description: Build and debug multiprocess Python pipelines using the parallelazy framework (spawn-isolated plugins, topic pub/sub, shared environment, plugin RPC, PyTorch/CUDA IPC, logging, and monitoring). Use whenever writing, modifying, or troubleshooting code that imports parallelazy.
---

# parallelazy — Agent Usage Guide

`parallelazy` is a lightweight Python (3.11+) multiprocessing plugin framework built on `torch.multiprocessing` (`spawn`), pipe-backed topic pub/sub, a manager-owned shared environment, plugin RPC, and PyTorch/CUDA-aware IPC.

## Minimal Runnable Template

```python
import time
from parallelazy import Plugin, PluginManager


class Worker(Plugin):
    @classmethod
    def default_options(cls) -> dict:
        return {"greeting": "hello"}

    def on_start(self):
        self.topic_subscribe("jobs", self.on_job)
        self.env_set("worker.ready", True)

    def on_job(self, payload):
        self.logger.info("%s: %s", self.options["greeting"], payload)

    def main(self):
        # Optional: return immediately for event-driven plugins,
        # or loop cooperatively with self.wait_for_stop(timeout=0.5).
        pass

    def on_stop(self):
        self.env_set("worker.ready", False)


def main():
    with PluginManager() as manager:
        desc = Worker.Descriptor(
            version="1.0.0",
            author="team",
            description="Example worker",
            options={"greeting": "hi"},
        )
        if not manager.load_plugin(desc):
            raise RuntimeError(f"Failed to load {desc.name}")
        if not manager.start_plugin(desc.name):
            raise RuntimeError(f"Failed to start {desc.name}")

        time.sleep(0.1)
        manager.topic_publish("jobs", {"id": 1})


if __name__ == "__main__":
    main()
```

## Core Rules & Invariants

1. **Always guard entry points with `if __name__ == "__main__":`**
   Workers are started with `spawn`. Plugin subclasses **must** be defined at module top level in an importable module (not nested inside functions or `__main__` scripts run without an importable module path). Run module scripts as `python -m pkg.module`.
2. **Always tear down `PluginManager`**
   Use `with PluginManager() as manager:` or call `manager.close()` / `manager.unload_all_plugins()` in a `finally` block so worker processes, monitoring threads, and log handlers exit cleanly.
3. **Check `load_plugin()` and `start_plugin()` return values**
   Both return `bool` (`load_plugins()` / `start_plugins()` / `start_all_plugins()` return `dict[str, bool]`). Never assume success without checking.
4. **Separate lifecycle stages**
   - `load_plugin(descriptor, options=None)` spawns and initializes the child worker.
   - `start_plugin(plugin_id)` runs `on_start()`, marks the plugin running, then runs `main()` once on the worker's main thread.
   - `stop_plugin(plugin_id)` requests cooperative shutdown, waits for `main()`, accepted RPC handlers, and active callbacks to finish, then runs `on_stop()`.
   - `unload_plugin(plugin_id)` stops (if needed) and removes manager state. A stopped worker cannot be restarted directly—unload and load again.
5. **Event-driven vs. looping `main()`**
   - Returning from `main()` does **not** stop the worker; it stays alive handling topics, env updates, and RPC until stopped.
   - A long-running `main()` **must** check `self.wait_for_stop(timeout=...)` so shutdown doesn't hang.
   - From inside a plugin, call `self.stop()` to request its own shutdown (never `self.plugin_stop(self.plugin_id)`).
6. **Thread safety inside plugins**
   Topic callbacks, environment callbacks, and RPC methods execute on worker threads concurrently with `main()` and with each other. Protect any shared mutable attributes on `self` with a `threading.Lock`.
7. **Descriptors & unique names**
   - `MyPlugin.Descriptor(version=..., author=..., description=None, name=None, options=None, work_dir=None, log_dir=None, log_level=None, display_logs=None, write_logs=None)`
   - `version` and `author` are required.
   - `name` defaults to kebab-case of the class name (`MyPlugin` → `my-plugin`). When running multiple instances of the same class, pass a unique `name` for each.
   - `options` passed to `Descriptor()` or `load_plugin()` merge over `default_options()`.
   - Descriptors serialize cleanly via `to_dict()` / `to_json()` / `from_dict()` / `from_json()`.

## Communication Primitives

### Topics (Live Fan-Out Pub/Sub)
- Plugin: `self.topic_subscribe(topic, callback) -> bool`, `self.topic_unsubscribe(topic) -> bool`, `self.topic_publish(topic, data) -> int`, `self.topic_list(plugin_id=None) -> list[str]`
- Manager: `manager.topic_publish(topic, data) -> int`, `manager.topic_list(plugin_id=None) -> list[str]`
- **No backlog or replay**: messages published before a subscriber registers are dropped. Register subscribers in `on_start()` (and coordinate readiness via `env_set` / `env_subscribe` or start subscribers before publishers) if the first message matters.
- Callbacks take a single argument `(self, payload)`.

### Shared Environment (Manager-Owned Key/Value Store)
- Plugin: `self.env_set(name, value, wait=True) -> bool`, `self.env_get(name=None, default=None)`, `self.env_subscribe(name, callback) -> bool`, `self.env_unsubscribe(name) -> bool`
- Manager: `manager.env_set(name, value)`, `manager.env_get(name=None, default=None)`
- `env_get()` with no key returns the full dict; with a key it distinguishes a stored `None` from an unset key (returns `default` only when unset).
- `env_subscribe()` delivers updates in FIFO order and delivers the current value first before subsequent updates.

### Plugin RPC & Dynamic Orchestration
- Call any public method on another plugin:
  - From a plugin: `self.plugin_call(target_id, "method_name", *args, **kwargs)` (pass `wait=False` for async `PendingRequest`).
  - From the manager: `manager.plugin_call(target_id, "method_name", *args, timeout=None, **kwargs)`.
- Inspect health/errors:
  - `plugin_status(plugin_id)`, `plugin_status_all()`, `plugin_errors(plugin_id)` (on both `PluginManager` and `Plugin`).
  - `ProcessStatus.startup_phase` reports: `not_started`, `process_started`, `waiting_for_debugger`, `initializing`, `ready`, or `failed`.
- Dynamic lifecycle from a plugin (for other plugins only, never itself):
  - `self.plugin_load(descriptor, options=None) -> bool`
  - `self.plugin_start(plugin_id) -> bool`
  - `self.plugin_stop(plugin_id) -> bool`
  - `self.plugin_unload(plugin_id) -> bool`

## PyTorch & CUDA IPC

- Plain Python values, nested containers, NumPy arrays, and PyTorch CPU/CUDA tensors can be sent over topics, env, and RPC.
- `parallelazy` automatically sanitizes payloads via `sanitize_for_ipc()`, which clones tensors received over IPC or backed by CUDA graph pools before re-sharing.
- If a plugin produces tensors from a CUDA graph or `torch.compile` graph pool and publishes/returns them, mark them with `mark_graph_output(tensor)` so IPC sanitization clones them safely before cross-process transfer.

## Logging, Directories, and Monitoring

- `PluginManager(work_dir=None, log_dir="logs", log_level=logging.INFO, display_logs=True, write_logs=True, timeout_request=5.0, timeout_start=10.0, timeout_initialize=30.0, timeout_debugger_attach=None)`
- Set `manager.work_dir` / `manager.log_dir` **before** loading any plugin; changes are rejected once loading starts.
- Set `write_logs=False` to disable all log directory/file creation (useful in tests or ephemeral runs).
- Plugins read their effective directory via `self.work_dir` and log via `self.logger`.
- Optional read-only monitoring (never consumes topic payloads; reports pending pipe bytes on Linux):
  ```python
  manager.enable_monitoring(sample_interval=1.0, report_file="monitor.txt", report_hotkey="m")
  snapshot = manager.get_monitoring_snapshot()
  manager.print_monitoring_report()
  # Toggle outputs independently at any time:
  manager.enable_report_file("monitor.txt") / manager.disable_report_file()
  manager.enable_report_hotkey("m") / manager.disable_report_hotkey()
  manager.disable_monitoring()
  ```
  Process/GPU metrics use optional extras: `pip install "parallelazy[monitoring]"` (`psutil`, `nvidia-ml-py`).

## Common Pitfalls Checklist

- **Spawn import errors**: Plugin class defined in `__main__` or inside a function → move it to an importable module and run with `python -m`.
- **Missing first topic messages**: Publisher started/published before subscriber subscribed → start subscribers first or synchronize via `env_set`/`env_subscribe`.
- **Shutdown hangs**: Blocking loop in `main()` ignoring stop signal → poll `self.wait_for_stop(timeout=...)`.
- **Race conditions**: Mutating `self.*` from `main()` and a topic/env/RPC callback without a lock → add a `threading.Lock`.
- **Re-starting a stopped plugin**: Calling `start_plugin()` after `stop_plugin()` → call `unload_plugin()` then `load_plugin()` and `start_plugin()`.
- **Self-targeting lifecycle RPC**: Calling `self.plugin_stop(self.plugin_id)` → call `self.stop()` instead.
