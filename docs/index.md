# parallelazy documentation

`parallelazy` is a lightweight Python framework for running plugins in
separate processes and connecting them with:

- lifecycle management (`load`, `start`, `stop`, and `unload`);
- topic-based publish/subscribe messaging;
- shared environment values with change notifications; and
- plugin-to-plugin remote procedure calls (RPC).

The manager uses the `spawn` multiprocessing start method. This makes the
framework suitable for workloads that need process isolation and is
compatible with PyTorch and CUDA process-start requirements.

## Documentation

- [Installation and quick start](getting-started.md)
- [Core concepts and lifecycle](concepts.md)
- [API reference](api.md)
- [Examples and usage patterns](examples.md)
- [Troubleshooting and operational notes](troubleshooting.md)

## Source links

- [Project README](../README.md)
- [Example source files](../examples/)
- [Project metadata](../pyproject.toml)

## Scope and status

This documentation describes the current `0.0.1` implementation. The
framework is small and evolving; application code should treat return values
from manager operations (`bool` for lifecycle operations and `None` for
timeouts in status/RPC calls) as part of the operational contract and should
log or handle failures explicitly.
