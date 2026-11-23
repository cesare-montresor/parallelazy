# Core concepts

## Process model

`PluginManager.load_plugin()` creates a child process using the descriptor's
module and class name. The child creates a `PluginWorker`, instantiates the
plugin, and communicates with the manager over pipe-backed channels.

The lifecycle is deliberately separate:

1. **Load**: create and initialize the worker process.
2. **Start**: call `on_start()`, mark the plugin running, and call `main()`
   once on the worker process's main thread.
3. **Stop**: request cooperative shutdown, call `on_stop()` on the worker's
   main thread, and close worker communication.
4. **Unload**: remove the process and manager bookkeeping.

`load_plugin()` and `start_plugin()` return `True` on success and `False` on a
reported failure. Check those values instead of assuming that a process is
ready.

## Writing a plugin

Subclass `Plugin` and use these hooks:

- `on_start()`: initialize resources and register subscriptions;
- `main()`: optional synchronous work, called once on the worker's main thread;
- `on_stop()`: release resources and publish final state;
- `on_error(error, source=None)`: optional application error hook.

An event-driven plugin can return from `main()` immediately; the worker stays
alive to process topic and environment callbacks until stopped. A plugin that
does ongoing work in `main()` should periodically check
`self.wait_for_stop(timeout)` or otherwise return when shutdown is requested.
The worker calls `on_stop()` after `main()` exits and accepted RPC handlers and
callbacks have finished.

RPC methods and topic/environment callbacks run on worker threads and may run
concurrently with `main()` and each other. Accepted handlers and callbacks are
drained before `on_stop()` runs. Synchronize shared plugin state when it is
accessed from more than one context.

## Descriptors and options

Create descriptors with the class method `Plugin.Descriptor()`:

```python
descriptor = Greeter.Descriptor(options={"language": "en"})
```

The descriptor derives a kebab-case name from the class name (for example,
`MyPlugin` becomes `my-plugin`) and stores the import package, class name,
version, author, description, options, and dependencies.

The plugin constructor receives `(worker, plugin_id, options)`. Options are
merged with `default_options()` when a plugin is instantiated. A descriptor
can be serialized for configuration files or transport:

```python
payload = descriptor.to_json()
restored = PluginDescriptor.from_json(payload)
```

If multiple instances of one class are needed, give each descriptor a unique
`name` before loading it.

## Working directories and logs

`PluginManager` accepts `work_dir`, `log_dir`, `log_level`, and
`display_logs` settings. By default, `work_dir` is the current directory and
`log_dir` is `logs` beneath it. Each manager creates a unique run folder under
the log directory:

```text
<work_dir>/logs/<timestamp>_<run-id>/
    manager.log
    <plugin-id>-<suffix>.log
```

You can change `manager.work_dir` and `manager.log_dir` before loading plugins.
Relative manager log directories are resolved under the current working
directory, and changing either setting redirects manager logging to the
corresponding run's `manager.log`. Neither setting can be changed after plugin
loading starts.

An individual `PluginDescriptor` can override these defaults with
`work_dir`, `log_dir`, `log_level`, or `display_logs`. Relative plugin working
directories resolve under the manager's working directory; a relative plugin
log directory resolves under that plugin's effective working directory.
Plugins can access their effective working directory as `self.work_dir`.

File records use elapsed time relative to that process's logging setup; each
file begins with a UTC open-time header. Console records include a short UTC
wall-clock time and the manager or plugin ID. Console output is enabled by
default, and the effective log level filters both console and file output.

Plugins can log with `self.logger` or another standard Python logger. The
manager and each plugin can customize their own file and console formats with
`set_log_formats(file_format=None, console_format=None)`. These changes are
process-local and do not propagate to other workers. If a log file or its
directory is removed while running, the next log write recreates the path and
continues in a new file.

Worker standard output and error are mirrored to both the terminal and the
plugin log. Newline- and carriage-return-delimited writes are logged as
`[stdout]` or `[stderr]` records and flushed promptly to the terminal. Libraries
such as `tqdm` may suppress progress bars when their output stream is not a
TTY; pass `disable=False` when you want progress output in a non-TTY terminal
or IDE console.

## Shared environment

The manager owns one in-memory environment shared by all loaded plugins.
From a plugin:

```python
self.env_set("model.status", "ready")
status = self.env_get("model.status", "unknown")
all_values = self.env_get()
self.env_subscribe("model.status", self.on_status)
```

An environment callback receives the new value. Names are application-defined;
prefixes such as `pipeline.` or `worker.` help avoid collisions. Calls with
`wait=False` enqueue the update without waiting for the manager response.

From the manager, use `env_set`, `env_get`, `env_subscribe`, and
`env_unsubscribe`. `EnvironmentManager` itself is the thread-safe local
key/value implementation.

## Topics

Topics are fan-out channels. A publisher sends one payload and every current
subscriber receives it:

```python
# subscriber
def on_message(self, payload):
    print(payload)

def on_start(self):
    self.topic_subscribe("events", self.on_message)

# publisher
def main(self):
    self.topic_publish("events", {"kind": "tick"})
```

Topic payloads must be suitable for multiprocessing IPC. Plain Python values,
containers, and PyTorch tensors are supported by the transport sanitization
logic. Topic delivery is not a durable queue; messages published before a
subscriber is registered are not replayed.

## RPC

Any public plugin method can be called from another plugin:

```python
result = self.plugin_call("data-provider", "get_item", 2)
```

The call waits for the response by default. Pass `wait=False` to work with
the returned pending request. Remote exceptions are returned as RPC errors
and surfaced by the caller, so wrap calls when the target may be unavailable
or may reject input.

## Status and error handling

The manager provides `plugin_status(plugin_id)`,
`plugin_status_all()`, and `plugin_list()`. A status includes process
identity/aliveness, the worker-reported status when available, and the
worker's startup phase. The phase distinguishes debugger attachment from
worker initialization; a worker waiting for a debugger is reported as
`waiting_for_debugger`.

Use manager logs and plugin-specific logging for diagnostics. Exceptions in
`on_start()`, `main()`, `on_stop()`, topic/environment callbacks, and RPC
handlers are recorded in the worker's error log (inspectable via
`plugin_errors()`), invoke `on_error(error, source=...)`, and transition
lifecycle failures (`on_start` / `main`) to `PluginState.ERROR`.
