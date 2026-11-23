# Examples and usage patterns

All examples live in [examples/](../examples/) and can be run from the
repository root with `python -m`. Each example is self-contained (stdlib +
`numpy` + `torch`), uses `with PluginManager(...) as manager:`, and prints a
summary line after verifying its assertions.

| Module | Demonstrates |
| --- | --- |
| `example_quickstart` | Lifecycle, `work_dir`, custom log formats, and cooperative `wait_for_stop()` |
| `example_tensor_pipeline` | 3-stage PyTorch tensor pub/sub pipeline, `topic_unsubscribe`, `topic_list`, and live monitoring |
| `example_live_config` | Shared environment flags, stored `None` vs unset keys, FIFO `env_subscribe`, and `env_unsubscribe` |
| `example_dynamic_orchestrator` | Supervisor plugin spawning workers dynamically with async RPC sharding (`wait=False`) |
| `example_resilience_and_errors` | `on_error(error, source)`, `PluginState.ERROR`, `plugin_errors()`, retries, and reload recovery |
| `example_cuda_zero_copy` | Zero-copy CUDA tensor streaming with `IpcEventPoolManaged`, `is_cleared()`, and `mark_graph_output()` |

## Quickstart & lifecycle

```bash
python -m examples.example_quickstart
```

Starts a `TelemetryAgent` plugin that emits periodic heartbeats to shared
environment state, customizes its console log format, and writes a JSON summary
into `self.work_dir` during `on_stop()`.

## Multi-stage PyTorch tensor pipeline + monitoring

```bash
python -m examples.example_tensor_pipeline
```

Runs a 3-stage pipeline (`FrameSource` -> `FeatureExtractor` ->
`AnomalyDetector`) using `load_plugins(parallel=True)`, `topic_publish` /
`topic_subscribe` / `topic_unsubscribe`, `topic_list`, and live
`enable_monitoring()` / `print_monitoring_report()`.

## Live configuration control plane

```bash
python -m examples.example_live_config
```

Demonstrates live hyperparameter updates via `env_set` and `env_subscribe`,
distinguishing an explicitly stored `None` from an unset key in `env_get`, and
cleanly detaching an auditor subscriber via `env_unsubscribe`.

## Dynamic orchestrator & async RPC sharding

```bash
python -m examples.example_dynamic_orchestrator
```

Shows a supervisor plugin (`Orchestrator`) dynamically loading and starting
multiple `ComputeWorker` instances, sharding work across them with async
`plugin_call(..., wait=False)`, inspecting `plugin_status_all()`, and unloading
temporary workers on completion.

## Resilience, `on_error`, and recovery

```bash
python -m examples.example_resilience_and_errors
```

Exercises transient RPC retries, circuit breaking, `Plugin.on_error(error,
source)`, `PluginState.ERROR` on startup failure, `plugin_errors()`, and
unloading/reloading a fresh plugin instance.

## Zero-copy CUDA tensor streaming

```bash
python -m examples.example_cuda_zero_copy
```

Streams CUDA tensors between `GpuProducer` and `GpuConsumer` using
`IpcEventPoolManaged` (`acquire`, `record`, `wait`, `clear`, `is_cleared`,
`wait_all_free`) and `mark_graph_output()`. Skips cleanly on CPU-only hosts.
