# API reference

This is a task-oriented reference for the public API. Implementation details
in `plugin_data.py`, `plugin_rpc.py`, and `plugin_transport.py` are internal
transport machinery unless an application is building a custom integration.

## `parallelazy.plugin.Plugin`

### Class methods

- `Descriptor(version, author, description=None, name=None, options=None)`
  returns a `PluginDescriptor`.
- `default_options()` returns a dictionary of default plugin options.

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

- `topic_subscribe(topic, callback)`
- `topic_publish(topic, data)`
- `env_set(name, value, wait=True)`
- `env_get(name=None, default=None)`
- `env_subscribe(name, callback)`
- `plugin_call(plugin_id, method_name, *args, **kwargs)`
- `plugin_load(descriptor, options=None)` loads a new plugin instance.
- `plugin_start(plugin_id)` starts another loaded plugin.
- `plugin_stop(plugin_id)` stops another loaded plugin.
- `plugin_unload(plugin_id)` stops and removes another plugin.

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

Serialization helpers are `to_dict()`, `to_json()`, `from_dict()`, and
`from_json()`.

## `parallelazy.plugin_manager.PluginManager`

Constructor:

```python
PluginManager(
    timeout_request=5.0,
    timeout_start=5.0,
    worker_pydebug=False,
)
```

### Lifecycle

| Method | Result |
| --- | --- |
| `load_plugin(descriptor, options=None)` | `bool` |
| `load_plugins(descriptors, parallel=True)` | `None` |
| `start_plugin(plugin_id)` | `bool` |
| `stop_plugin(plugin_id)` | `bool` |
| `unload_plugin(plugin_id)` | `bool` |
| `start_all_plugins()` | `None` |
| `stop_all_plugins()` | `None` |
| `unload_all_plugins()` | `None` |

### Inspection

- `plugin_list()` returns loaded plugin IDs.
- `plugin_status(plugin_id)` returns a `ProcessStatus` or `None`.
- `plugin_status_all()` returns a mapping of IDs to `ProcessStatus`.

Plugin code can also call `self.plugin_status(plugin_id=None)` to inspect its
own status (or another loaded plugin's status), and `self.plugin_status_all()`
to inspect all loaded plugins.

### Environment and topics

- `env_set(name, value)`
- `env_get(name=None, default=None)`
- `env_subscribe(plugin_id, var_name)`
- `env_unsubscribe(plugin_id, var_name)`
- `topic_publish(topic, data)`
- `topic_list()`

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
