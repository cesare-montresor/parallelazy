# Examples and usage patterns

All examples live in [examples/](../examples/) and
can be run from the repository root with `python -m`.

| Module | Demonstrates |
| --- | --- |
| `example_count` | Minimal lifecycle and a plugin loop updating shared state |
| `example_graceful_shutdown` | `on_start`, `on_stop`, and clean unloading |
| `example_pubsub` | One publisher and one subscriber on a topic |
| `example_env_variables` | Shared configuration and environment callbacks |
| `example_configuration` | Descriptor options and runtime configuration |
| `example_rpc_calls` | Calling methods between plugin processes |
| `example_data_pipeline` | Three-stage topic pipeline with aggregate metrics |
| `example_worker_pool` | Multiple instances consuming the same topic |
| `example_error_handling` | Worker/plugin error reporting |

## Lifecycle

```bash
python -m examples.example_count
python -m examples.example_graceful_shutdown
```

Use the first example as a minimal template. Use the shutdown example when
adding resources such as files, sockets, model handles, or CUDA objects:
release those resources in `on_stop()`.

## Pub/sub

```bash
python -m examples.example_pubsub
python -m examples.example_data_pipeline
```

The pipeline example illustrates a useful convention: publish structured
dictionaries on topics and use environment keys for progress and final
metrics. Each stage subscribes in `on_start()` before the source begins
publishing.

## Shared configuration

```bash
python -m examples.example_env_variables
python -m examples.example_configuration
```

Configuration values are ordinary Python values, so keep them serializable
and use stable, namespaced keys. `example_configuration` also shows how
descriptor options are merged and consumed by the plugin constructor.

## RPC

```bash
python -m examples.example_rpc_calls
```

The provider exposes ordinary methods (`get_data`, `get_item`, and
`add_data`). The consumer calls them with `plugin_call`. This is a
request/response operation, not a topic broadcast, and should be guarded with
error handling when a target plugin can stop independently.

## Worker pools

```bash
python -m examples.example_worker_pool
```

The manager can load several descriptors for the same class. Give each
descriptor a unique name, then track per-instance metrics with namespaced
environment keys.

## Error handling

```bash
python -m examples.example_error_handling
```

Use the example to see how exceptions in plugin work are logged and how
application code can inspect plugin state. Errors in an RPC target should be
handled by the calling plugin rather than silently ignored.

## Reusing an example

Copy the smallest relevant example and change:

1. the plugin class and its descriptor metadata;
2. `on_start()` subscriptions and resource initialization;
3. `main()` for periodic work, or leave it empty for callback-driven work;
4. `on_stop()` cleanup and final metrics; and
5. the manager's load/start/stop/unload sequence.

Keep application startup under a `main()` function and protect it with the
standard `if __name__ == "__main__"` guard because workers use multiprocessing
spawn.
