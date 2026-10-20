# Troubleshooting and operational notes

## A plugin does not load

Check the boolean result from `load_plugin()` and inspect logs. Common causes:

- the descriptor package is not importable by a spawned child;
- `class_name` does not exist in the package;
- the plugin constructor raises;
- two loaded plugins use the same descriptor name.

Run the application from an environment where the project/package is
installed, or use an editable install during development.

## A plugin starts but does no work

Confirm that:

1. `start_plugin(descriptor.name)` was called after loading;
2. `on_start()` completed without raising;
3. work is in `main()` or a registered callback; and
4. the process is alive with `plugin_status()`.

An event-driven plugin is expected to have an empty `main()`.

## Topics do not arrive

Register subscriptions in `on_start()` before the publisher starts. Check the
exact topic string and callback signature. Topic delivery is live fan-out:
there is no replay for messages sent before subscription and no durable
broker.

## Environment callbacks do not arrive

Subscribe with `env_subscribe(name, callback)` from the plugin, and make sure
the writer uses the same exact key. The callback receives the new value. Use
`env_get()` for an initial snapshot because subscribing does not itself
replay the current value.

## RPC calls fail or time out

Verify the target plugin ID with `manager.plugin_list()`, start the target
before issuing calls, and make sure the method name and arguments are valid.
Remote exceptions are surfaced to the caller. Use `try/except` around calls
to plugins that may be stopped or unloaded.

## Multiprocessing and CUDA

The package requests the `spawn` start method. Always protect application
startup with `if __name__ == "__main__":`. Do not create manager/plugin
processes as an import-time side effect.

For worker debugging, construct the manager with
`PluginManager(worker_pydebug=True)`. The worker uses `debugpy` and waits for
a debugger client, so only enable this when a debugger is ready to attach.

## Shutdown hangs or leaves processes

Prefer this order:

```python
manager.stop_plugin(plugin_id)
manager.unload_plugin(plugin_id)
```

For application-wide cleanup, call `unload_all_plugins()` in a `finally`
block. Keep `on_stop()` bounded: long-running cleanup delays the manager's
configured `timeout_start` wait.
