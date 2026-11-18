# parallelazy

[PyPi parallelazy](https://pypi.org/project/parallelazy/)

A lightweight Python plugin/worker framework with:

- isolated plugin processes started with `spawn`;
- topic-based publish/subscribe;
- shared environment values and change notifications;
- plugin-to-plugin RPC; and
- PyTorch/CUDA-aware multiprocessing support.

## Install

```bash
pip install parallelazy
```

*for development*
```bash
pip install -e .
```

## Debug in VSCode

Due to a known issue with debugpy and spawn ( as opposite to fork ) processes, a small companion vscode extension has been developed in order to enable debugging.

https://marketplace.visualstudio.com/items?itemName=CesareMontresor.parallelazy-vscode

## Quick start

```python
import time

from parallelazy import Plugin, PluginManager


class Worker(Plugin):
    def on_start(self):
        self.env_set("worker.ready", True)

    def main(self):
        pass


def main():
    manager = PluginManager()
    descriptor = Worker.Descriptor(
        version="1.0.0",
        author="Application team",
        description="Example worker",
    )
    if not manager.load_plugin(descriptor):
        raise RuntimeError("Plugin failed to load")
    if not manager.start_plugin(descriptor.name):
        raise RuntimeError("Plugin failed to start")
    try:
        time.sleep(0.1)
        print(manager.env_get("worker.ready"))
    finally:
        manager.unload_plugin(descriptor.name)


if __name__ == "__main__":
    main()
```

## Pipeline monitoring

Enable an optional read-only view of topic backlog, plugin process resources,
and shared environment activity:

```python
manager.enable_monitoring(sample_interval=1.0)
snapshot = manager.get_monitoring_snapshot()
manager.print_monitoring_report()
manager.disable_monitoring()
```

The monitor never reads or changes topic payloads. Topic backlog is reported as
pending pipe bytes. Process metrics use `psutil` when installed, and NVIDIA GPU
metrics use `nvidia-ml-py`; missing optional dependencies are identified with
their install command.

## Documentation

Read the [documentation index](docs/index.md) for installation, concepts,
API reference, usage patterns, and troubleshooting.

The complete runnable examples are in
[examples/](examples/). For example:

```bash
python -m examples.example_pubsub
python -m examples.example_rpc_calls
```
