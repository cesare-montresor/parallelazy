# Installation and quick start

## Requirements

- Python 3.8 or newer
- PyTorch
- `debugpy` (installed as a runtime dependency)

PyTorch is a required dependency because the process and IPC implementation
uses `torch.multiprocessing`. CUDA is optional, but applications that use CUDA
should keep the default `spawn` start method.

## Install

From a checkout:

```bash
python -m pip install .
```

For editable development installation:

```bash
python -m pip install -e .
```

The package metadata installs `torch` and `debugpy` automatically. Select the
PyTorch distribution appropriate for your platform and CUDA version if your
environment requires a custom PyTorch index.

## Minimal plugin

Put plugin classes in an importable module. A descriptor records the module
and class that the worker must import, so the class must be importable by a
spawned child process.

```python
# my_plugins.py
from parallelazy.plugin import Plugin, PluginDescriptor
from parallelazy.plugin_worker import PluginWorker


class Greeter(Plugin):
    @classmethod
    def Descriptor(cls, options=None) -> PluginDescriptor:
        return super().Descriptor(
            version="1.0.0",
            author="Application team",
            description="Publishes a greeting",
            options=options or {},
        )

    def on_start(self) -> None:
        self.env_set("greeter.ready", True)

    def main(self) -> None:
        # main() is called repeatedly while the plugin is running.
        pass

    def on_stop(self) -> None:
        self.env_set("greeter.ready", False)
```

Start it from a guarded application entry point:

```python
# run_greeter.py
import time

from my_plugins import Greeter
from parallelazy.plugin_manager import PluginManager


def main() -> None:
    manager = PluginManager()
    descriptor = Greeter.Descriptor()

    if not manager.load_plugin(descriptor):
        raise RuntimeError("Could not load greeter")
    if not manager.start_plugin(descriptor.name):
        raise RuntimeError("Could not start greeter")

    try:
        time.sleep(1)
        print(manager.env_get("greeter.ready"))
    finally:
        manager.stop_plugin(descriptor.name)
        manager.unload_plugin(descriptor.name)


if __name__ == "__main__":
    main()
```

The `if __name__ == "__main__"` guard is important: the `spawn` start method
imports the application module in child processes.

## Run the shipped examples

The examples are Python modules, so run them from the repository root:

```bash
python -m examples.example_count
python -m examples.example_pubsub
python -m examples.example_configuration
```

See [Examples and usage patterns](examples.md) for the complete catalog.

## Recommended shutdown pattern

For multiple plugins, stop them before unloading them. The convenience
methods make this explicit:

```python
try:
    manager.start_all_plugins()
    # application work
finally:
    manager.unload_all_plugins()
```

`unload_all_plugins()` stops running plugins and then removes their manager
records.
