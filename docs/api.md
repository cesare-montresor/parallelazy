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

- `on_start()`
- `main()`
- `on_stop()`
- `on_error(error, source=None)`
- `stop()`

### Communication methods

- `topic_subscribe(topic, callback)`
- `topic_publish(topic, data)`
- `env_set(name, value, wait=True)`
- `env_get(name=None, default=None)`
- `env_subscribe(name, callback)`
- `plugin_call(plugin_id, method_name, *args, **kwargs)`

Callbacks are normally bound methods and must accept the delivered payload
(one argument for topic and environment callbacks).

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
