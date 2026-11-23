# API reference

This is a task-oriented reference for the public API. Implementation details
in `plugin_data.py`, `plugin_rpc.py`, and `plugin_transport.py` are internal
transport machinery unless an application is building a custom integration.

## `parallelazy.plugin.Plugin`

### Class methods

- `Descriptor(version, author, description=None, name=None, options=None, work_dir=None, log_dir=None, log_level=None, display_logs=None, write_logs=None)`
  returns a `PluginDescriptor`.
- `default_options()` returns a dictionary of default plugin options.

`work_dir` is available as a read-only `self.work_dir` path. `self.logger`
provides a standard Python logger. `set_log_formats(file_format=None,
console_format=None)` changes the formats for this plugin worker only.

### Lifecycle methods

- `on_start()` initializes resources and subscriptions.
- `main()` runs once on the worker process's main thread. Returning from it
  does not stop an event-driven plugin; the worker continues handling events.
- `on_stop()` runs after `main()` exits and accepted RPC handlers and callbacks
  finish.
- `on_error(error, source=None)`
- `stop()` requests shutdown; a blocking `main()` should observe it with
  `wait_for_stop()`.
- `wait_for_stop(timeout=None)` waits for a cooperative stop request.

### Communication methods

- `topic_subscribe(topic, callback) -> bool`
- `topic_unsubscribe(topic) -> bool`
- `topic_publish(topic, data) -> int` (returns the number of subscribers that received the payload)
- `topic_list(plugin_id=None) -> list[str]` (sorted list of all known topics, or topics published by `plugin_id`)
- `env_set(name, value, wait=True) -> bool`
- `env_get(name=None, default=None)` (distinguishes stored `None` from unset keys)
- `env_subscribe(name, callback) -> bool`
- `env_unsubscribe(name) -> bool`
- `plugin_call(plugin_id, method_name, *args, **kwargs)`
- `plugin_errors(plugin_id=None) -> list[str]`
- `plugin_load(descriptor, options=None) -> bool` loads a new plugin instance.
- `plugin_start(plugin_id) -> bool` starts another loaded plugin.
- `plugin_stop(plugin_id) -> bool` stops another loaded plugin.
- `plugin_unload(plugin_id) -> bool` stops and removes another plugin.

Callbacks are normally bound methods and must accept the delivered payload
(one argument for topic and environment callbacks).

Lifecycle requests are handled by the manager and return `bool`. A plugin
cannot target itself with these RPC methods; use `Plugin.stop()` to stop the
calling plugin. `plugin_load` takes a `PluginDescriptor` directly; each
instance must have a unique descriptor name. Loading initializes the worker
but does not start it, so call `plugin_start(descriptor.name)` separately.
The manager forwards the descriptor and options to the new worker process;
with the spawn start method, the descriptor's package and class must be
importable in that process. Stopping a worker ends that worker process; to run
it again, unload it and load its descriptor again under the same name.

For example, a plugin can create separately configured instances:

```python
for layer_name in layer_names:
    descriptor = LayerPlugin.Descriptor(
        name=f"layer-{layer_name}",
        options={"layer": layer_name},
    )
    if self.plugin_load(descriptor):
        self.plugin_start(descriptor.name)
```

## Pipeline monitoring

Monitoring is optional and read-only. It does not read topic payloads or
change queue delivery.

```python
monitor = manager.enable_monitoring(sample_interval=1.0)
snapshot = manager.get_monitoring_snapshot()
manager.print_monitoring_report()
manager.disable_monitoring()
```

`enable_monitoring()` starts one background sampler. `disable_monitoring()`
stops it synchronously. The sampler remains active until disabled even if no
report is currently printed. `get_monitoring_snapshot()` returns the latest
sample while enabled and performs a one-off sample when disabled.

For an interactive terminal, a hotkey can print the latest snapshot:

```python
manager.enable_report_hotkey("m")
# Later:
manager.disable_report_hotkey()
```

Hotkey capture is opt-in and only available for an interactive TTY.
`MonitoringSnapshot` contains topic, plugin process, environment, and backend
availability records. Pending pipe bytes are the primary backlog measurement;
message counts are not estimated.

Process metrics require `psutil` and cover the worker plus live descendants.
NVIDIA GPU metrics require `nvidia-ml-py`. Missing optional dependencies are
reported with the package-specific installation command. Environment reports
show key names, value types, shallow stored sizes, subscriber counts, write
rates, and last-write age; values themselves are not included.

## `parallelazy.plugin.PluginDescriptor`

Dataclass fields:

| Field | Meaning |
| --- | --- |
| `name` | Unique manager/plugin instance identifier |
| `version` | Plugin version string |
| `package` | Importable Python package/module |
| `class_name` | Plugin class to instantiate |
| `description` | Human-readable description |
| `author` | Plugin author |
| `options` | Default configuration dictionary |
| `dependencies` | Declared dependency names |
| `work_dir` | Optional plugin working directory; relative paths resolve under the manager's work directory |
| `log_dir` | Optional plugin log directory; relative paths resolve under the plugin's effective working directory |
| `log_level` | Optional integer Python logging level; defaults to the manager's level |
| `display_logs` | Optional console logging flag; defaults to the manager's setting |
| `write_logs` | Optional file logging flag; defaults to the manager's setting |

Serialization helpers are `to_dict()`, `to_json()`, `from_dict()`, and
`from_json()`.

## `parallelazy.plugin_manager.PluginManager`

Constructor:

```python
import logging

PluginManager(
    timeout_request=5.0,
    timeout_start=10.0,
    timeout_start_debug=30.0,
    work_dir=None,
    log_dir="logs",
    log_level=logging.INFO,
    display_logs=True,
    write_logs=True,
    timeout_initialize=30.0,
    timeout_debugger_attach=None,
)
```

`timeout_initialize` bounds worker initialization and defaults to 30 seconds.
`timeout_debugger_attach` is a separate optional deadline; `None` means a
debugger may take as long as needed to attach. These settings do not change
`timeout_start` or `timeout_start_debug`, which bound synchronous lifecycle
requests.

The manager resolves `work_dir` to the current directory when omitted and
resolves a relative `log_dir` under it. A unique run folder is created below
the log directory. It contains `manager.log` and one log file per plugin.
Descriptor values override the corresponding manager defaults for that plugin.
Set `write_logs=False` to disable manager and plugin file handlers (no log files
or log directories are created). Set `display_logs=False` separately to suppress
worker and logger terminal output; captured worker `print()` calls may still be
written to plugin files when `write_logs=True`.
File and console output use the effective log level; console output is enabled
by default. The manager and plugins can independently change their process's
formats with `set_log_formats(file_format=None, console_format=None)`.
The manager's `work_dir` and `log_dir` properties can be changed before plugin
loading starts. Changing either redirects manager logs and affects the defaults
used for subsequently loaded plugins; both settings are fixed once loading
begins.

### Lifecycle

| Method | Result |
| --- | --- |
| `load_plugin(descriptor, options=None)` | `bool` |
| `load_plugins(descriptors, parallel=True)` | `dict[str, bool]` |
| `start_plugin(plugin_id)` | `bool` |
| `start_plugins(plugin_ids, parallel=True)` | `dict[str, bool]` |
| `stop_plugin(plugin_id)` | `bool` |
| `unload_plugin(plugin_id)` | `bool` |
| `start_all_plugins()` | `dict[str, bool]` |
| `stop_all_plugins()` | `None` |
| `unload_all_plugins()` | `None` |
| `close()` | `None` |

`PluginManager` also supports the context-manager protocol (`with PluginManager() as manager:`), calling `close()` on exit to stop and unload all plugins, stop monitoring, and close log handlers.

### Inspection and RPC

- `plugin_list()` returns loaded plugin IDs.
- `plugin_status(plugin_id)` returns a `ProcessStatus` or `None`.
- `plugin_status_all()` returns a mapping of IDs to `ProcessStatus`.
- `plugin_call(plugin_id, method_name, *args, timeout=None, **kwargs)` invokes a method on a loaded plugin via RPC.
- `plugin_errors(plugin_id, timeout=None)` returns recorded error messages for a loaded plugin.
- `ProcessStatus.startup_phase` reports the worker milestone as a string:
  `not_started`, `process_started`, `waiting_for_debugger`, `initializing`,
  `ready`, or `failed`.

While a worker waits for a debugger, its phase remains
`waiting_for_debugger`. `load_plugin()` continues waiting by default and
reports the phase at its polling interval; set `timeout_debugger_attach` to
configure a finite attach deadline. Parallel load/start methods still execute
plugins concurrently and return a success flag for each plugin ID.

Plugin code can also call `self.plugin_status(plugin_id=None)` to inspect its
own status (or another loaded plugin's status), and `self.plugin_status_all()`
to inspect all loaded plugins.

### Environment and topics

- `env_set(name, value)`
- `env_get(name=None, default=None)`
- `env_subscribe(plugin_id, var_name)`
- `env_unsubscribe(plugin_id, var_name)`
- `topic_publish(topic, data) -> int`
- `topic_list(plugin_id=None) -> list[str]`

Manager-side subscriptions are useful for registering routing state; plugin
code normally uses the corresponding methods on `Plugin`.

## `parallelazy.environment_manager.EnvironmentManager`

The thread-safe local environment implementation exposes:

- `get(key, default=None)`
- `set(key, value)`
- `get_all()`
- `set_all(env_dict)`
- `clear(key=None)`
- `key in environment_manager`

Use it directly for local application state; use `PluginManager` or `Plugin`
for cross-process state.

## Data types

`PluginStatus` and `ProcessStatus` are returned by status operations.
`Request`, `Response`, `PendingRequest`, `Queue`, and `Channel` implement the
internal pipe/RPC protocol and are primarily useful when extending the
framework itself.
