## Keeping this file current
Last synced: 25dcb969bdeee3b31f1b2b1277e0e6d09f1285aa

At the start of every session:
1. Find commits since the last sync: `git log --oneline <Last synced hash>..HEAD`
   and `git diff --stat <Last synced hash>..HEAD`.
2. If those changes touched modules described below, check those sections against the
   code and fix anything stale. If nothing relevant changed, skip this.
3. Update `Last synced` only after a real review, not for unrelated edits.

Self-healing (any time during a session):
- If you find anything in this file that is wrong, outdated, or misleading, fix it
  immediately in the same session, even if unrelated to your current task.
- If you wasted effort rediscovering something a future agent would also have to
  rediscover (a gotcha, a hidden invariant, a command that needs special flags),
  add one concise line for it.
- Resolve `TODO(verify)` items when you can confirm them from code; otherwise leave
  them and ask the user.

At the end of every session:
- Review `git diff` of this session and propose edits to this file. Add only what
  meets the bar above. Remove stale or redundant lines. Prefer deleting or shortening
  over adding. Keep the file under ~150 lines.
- If public APIs, lifecycle rules, IPC behavior, logging, or monitoring changed, keep
  the packaged consumer guide `parallelazy/SKILL.md` updated and aligned in the same
  session so pip users and downstream agents stay in sync.
- Do not log individual changes; git history does that.
- Commit AGENTS.md changes in their own commit: "docs: sync AGENTS.md".

## Purpose
- `parallelazy` is a Python framework for isolated plugin processes, lifecycle management,
  topic pub/sub, shared environment values, and plugin RPC.
- It provides spawn-based worker coordination and PyTorch/CUDA-aware IPC transport.

## Role in the ecosystem
- This repository owns the Python framework, its IPC, logging, monitoring, and plugin APIs.
- No sibling Python package dependency is declared in `pyproject.toml`.
- The companion `parallelazy-vscode` extension is a small debugpy bridge: it receives
  worker attach notifications and asks VS Code to attach; it does not own framework behavior.
- `parallelazy` provides the worker-side notification and reuses the debugpy path loaded by
  VS Code, avoiding a separately pinned debugpy version for this integration.

## Directory map
- `parallelazy/`: public API plus manager, worker, protocol, transport, logging, and monitor.
- `tests/`: `unittest` coverage for lifecycle, logging, monitoring, and CUDA IPC.
- `examples/`: runnable end-to-end plugin examples; invoke as modules from repo root.
- `docs/`: user-facing setup, concepts, API, examples, monitoring, and troubleshooting.
- `changelogs/`: per-release notes (`vX.Y.Z.md`) and `changelog.sh` generator for root `CHANGELOG.md`.
- `.github/workflows/publish.yml`: release-branch build, changelog check, and PyPI publishing workflow.
- `.vscode/launch.json`: debugpy launch configurations for the examples.

## Key abstractions and invariants
- `PluginManager` uses `torch.multiprocessing` with `spawn`; guard application startup
  with `if __name__ == "__main__":` and keep plugin classes importable in child processes.
- The design goal is a lightweight, self-contained, high-performance multiprocessing
  pipeline framework that avoids requiring applications to manage queues directly.
- `Plugin.Descriptor()` records the module/class import path and merges default options
  with provided options. Plugin IDs derive to kebab-case by default and must be unique.
- Lifecycle is separate: load initializes the child; start calls `on_start()` then invokes
  `main()` once on the worker main thread; stop runs `on_stop()`; unload removes manager state.
- `main()` may return while the worker remains alive for events and shutdown. A blocking
  `main()` should observe `wait_for_stop()`. RPC and topic/environment callbacks can run
  concurrently on worker threads; synchronize shared plugin state.
- Stop waits for an in-progress start, accepted RPC handlers, and active callbacks before
  `on_stop()`. Preserve this ordering and response-drain behavior when changing lifecycle code.
- The manager owns the shared in-memory environment. Environment updates are dispatched
  asynchronously and delivered to worker subscribers in FIFO order; `env_subscribe()`
  delivers the initial value before subsequent updates, and `env_get()` distinguishes stored
  `None` from unset keys.
- Topics are live fan-out through per-subscriber pipe-backed queues with strict pipe
  ownership (subscriber owns read end; manager/publisher own write end) and single-publisher
  ownership per topic. No durable backlog or replay; register subscribers before publishing
  if the first message matters.
- `sanitize_for_ipc()` recursively handles containers (copy-on-write with cycle detection)
  and clones IPC-received or graph-pooled tensors before re-sharing; `mark_graph_output()`
  marks compiled-graph outputs.
- Monitoring is optional and read-only; topic backlog is measured as pending pipe bytes on
  the write end (`FIONREAD`), not by reading payloads. Process/GPU metrics depend on optional
  `monitoring` extras (`psutil`, `nvidia-ml-py`).
- Manager and plugin logs share a run ID. Set `work_dir`/`log_dir` before plugin loading;
  manager path changes are rejected after loading starts. `write_logs=False` creates no log
  directories or files.
- `pyproject.toml` requires Python 3.11+; PyTorch and NumPy are the declared runtime
  dependencies, with `monitoring` optional extras. `debugpy` is supplied by VS Code for the
  companion integration, not packaged.

## Commands
- Run example: `.venv/bin/python -m examples.example_quickstart` (verified).
- Test: `.venv/bin/python -m unittest discover -s tests` (verified; 85 tests passed).
- Build distributions: `.venv/bin/python -m build` (verified).
- Rebuild changelog: `./changelogs/changelog.sh` (`--check` verifies `CHANGELOG.md` is in sync).
- Lint: `ruff check parallelazy tests` (configured in `pyproject.toml`).
- Typecheck: `.venv/bin/mypy parallelazy` and `.venv/bin/pyright parallelazy` (PEP 561 `py.typed`, configured in `pyproject.toml`).
- Debug: select a `Debug Example: ...` configuration in `.vscode/launch.json`.
  Spawned-worker debugging also depends on the companion VS Code extension described in README.

## Conventions and gotchas
- Tests use the standard-library `unittest` framework; CUDA IPC coverage skips without CUDA.
- Run example modules from the repository root so spawned workers can import them.
- Use `with PluginManager() as manager:` or call `PluginManager.unload_all_plugins()` /
  `close()` in `finally` blocks to tear down workers, monitoring, and log handlers.
- The `pyproject.toml` metadata is the dependency/version source of truth.
- Changelogs: maintain one `changelogs/vX.Y.Z.md` per release (`## vX.Y.Z (YYYY-MM-DD)` with
  `### Added`/`Changed`/`Fixed` sections) and regenerate root `CHANGELOG.md` via
  `./changelogs/changelog.sh`. `.github/workflows/publish.yml` uses `changelogs/v<version>.md`
  when present and warns (falling back to auto-generated release notes) if missing or out of sync.

## Do not touch
- Do not hand-edit generated `CHANGELOG.md`, `parallelazy.egg-info/`, `build/`, `dist/`, or `__pycache__/`.
- Treat worker startup phases, shared pipe/channel direction, and shutdown/RPC draining as
  coupled protocol behavior; make lifecycle changes with focused tests.

## Docs
- Start at `docs/index.md`; detailed lifecycle and concurrency behavior is in `docs/concepts.md`.
- API contracts are in `docs/api.md`; operational notes are in `docs/troubleshooting.md`.
- `parallelazy/SKILL.md` is the pip-distributed consumer guide/Agent Skill (`python -m parallelazy agent-guide` / `install-skill`); keep it aligned with public API and usage invariants.
