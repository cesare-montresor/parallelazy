# Core concepts

## Process model

`PluginManager.load_plugin()` creates a child process using the descriptor's
module and class name. The child creates a `PluginWorker`, instantiates the
plugin, and communicates with the manager over pipe-backed channels.

The lifecycle is deliberately separate:

1. **Load**: create and initialize the worker process.
2. **Start**: call `on_start()`, mark the plugin running, and begin repeated
   calls to `main()`.
3. **Stop**: call `on_stop()`, stop the loop, and close worker communication.
4. **Unload**: remove the process and manager bookkeeping.

`load_plugin()` and `start_plugin()` return `True` on success and `False` on a
reported failure. Check those values instead of assuming that a process is
ready.

## Writing a plugin

Subclass `Plugin` and use these hooks:

- `on_start()`: initialize resources and register subscriptions;
- `main()`: repeated work loop; it may be empty for event-driven plugins;
- `on_stop()`: release resources and publish final state;
- `on_error(error, source=None)`: optional application error hook.

The worker calls `main()` continuously while the plugin is running. Event
callbacks for topics and environment changes are dispatched by the worker, so
an event-driven plugin can use `pass` in `main()`.

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
identity/aliveness and the worker-reported status when available.

Use manager logs and plugin-specific logging for diagnostics. A worker records
exceptions raised by its loop in its error log; the lifecycle and RPC methods
also report failures through their boolean/result contracts.
