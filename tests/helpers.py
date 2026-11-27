"""Shared helpers and top-level probe plugins for end-to-end parallelazy tests."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

from parallelazy import Plugin, PluginManager


def wait_until(
    predicate: Callable[[], Any],
    timeout: float = 5.0,
    interval: float = 0.02,
    message: str = "",
) -> None:
    """Poll predicate() until truthy or fail with AssertionError."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    detail = f": {message}" if message else ""
    raise AssertionError(f"Timed out after {timeout:.2f}s waiting for condition{detail}")


def is_pid_alive(pid: int | None) -> bool:
    """Return True if pid exists and is not a Linux zombie."""
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

    stat_path = f"/proc/{pid}/stat"
    if os.path.exists(stat_path):
        try:
            with open(stat_path, encoding="utf-8") as fh:
                fields = fh.read().rsplit(")", 1)[-1].split()
            if fields and fields[0] == "Z":
                return False
        except FileNotFoundError:
            return False
    return True


def make_manager(tmp_dir: str, **kwargs) -> PluginManager:
    """Create a PluginManager rooted in tmp_dir with fast deterministic defaults."""
    defaults = {
        "work_dir": tmp_dir,
        "display_logs": False,
        "write_logs": False,
        "timeout_request": 2.0,
        "timeout_start": 5.0,
        "timeout_initialize": 5.0,
    }
    defaults.update(kwargs)
    return PluginManager(**defaults)


class EchoPlugin(Plugin):
    """Lightweight event-driven plugin whose main() returns immediately."""

    @classmethod
    def default_options(cls) -> dict[str, Any]:
        return {"a": 1, "b": 2}

    def on_start(self) -> None:
        self.events = ["on_start"]
        self.env_set(f"{self.plugin_id}.started", True)

    def main(self) -> None:
        self.events.append("main")

    def on_stop(self) -> None:
        self.events.append("on_stop")
        self.env_set(f"{self.plugin_id}.stopped", True)

    def echo(self, value: Any, *, prefix: str = "") -> Any:
        return f"{prefix}{value}" if prefix else value

    def get_options(self) -> dict[str, Any]:
        return dict(self.options)

    def get_pid(self) -> int:
        return os.getpid()


class BlockingMainPlugin(Plugin):
    """Plugin whose main() blocks on wait_for_stop()."""

    def on_start(self) -> None:
        self.env_set(f"{self.plugin_id}.in_main", False)

    def main(self) -> None:
        self.env_set(f"{self.plugin_id}.in_main", True)
        self.wait_for_stop()
        self.env_set(f"{self.plugin_id}.main_exited", True)

    def on_stop(self) -> None:
        self.env_set(f"{self.plugin_id}.stopped", True)


class SlowStartPlugin(Plugin):
    """Plugin that sleeps in on_start() to test stop-during-start ordering."""

    @classmethod
    def default_options(cls) -> dict[str, Any]:
        return {"start_delay": 0.35}

    def on_start(self) -> None:
        self.order = ["on_start:enter"]
        self.env_set(f"{self.plugin_id}.start_entered", True)
        time.sleep(float(self.options["start_delay"]))
        self.order.append("on_start:exit")
        self.env_set(f"{self.plugin_id}.order", list(self.order))

    def main(self) -> None:
        self.order.append("main")
        self.env_set(f"{self.plugin_id}.order", list(self.order))

    def on_stop(self) -> None:
        self.order.append("on_stop")
        self.env_set(f"{self.plugin_id}.order", list(self.order))


class CrashInitPlugin(Plugin):
    """Plugin that raises inside __init__."""

    def __init__(self, worker, plugin_id: str, options=None):
        super().__init__(worker, plugin_id, options)
        raise RuntimeError("CrashInitPlugin intentional init failure")


class HangInitPlugin(Plugin):
    """Plugin that hangs inside __init__ to test timeout_initialize."""

    def __init__(self, worker, plugin_id: str, options=None):
        super().__init__(worker, plugin_id, options)
        time.sleep(30.0)


class CoordinatorPlugin(Plugin):
    """Plugin that exercises plugin-driven lifecycle RPCs."""

    def manage_child(self, child_name: str) -> dict[str, Any]:
        desc = EchoPlugin.Descriptor(version="1.0.0", author="tests", name=child_name)
        loaded = self.plugin_load(desc, options={"b": 99})
        started = self.plugin_start(child_name)
        status = self.plugin_status(child_name)
        echoed = self.plugin_call(child_name, "echo", "hi")
        stopped = self.plugin_stop(child_name)
        unloaded = self.plugin_unload(child_name)
        return {
            "loaded": loaded,
            "started": started,
            "alive_when_running": bool(status and status.process_alive),
            "echoed": echoed,
            "stopped": stopped,
            "unloaded": unloaded,
        }

    def attempt_self_lifecycle(self) -> dict[str, bool]:
        return {
            "start": self.plugin_start(self.plugin_id),
            "stop": self.plugin_stop(self.plugin_id),
            "unload": self.plugin_unload(self.plugin_id),
        }

    def main(self) -> None:
        self.wait_for_stop()


class SelfStoppingPlugin(Plugin):
    """Plugin that calls self.stop() from main() with optional on_stop() delay."""

    @classmethod
    def default_options(cls) -> dict[str, Any]:
        return {"stop_delay": 0.0}

    def main(self) -> None:
        self.stop()

    def on_stop(self) -> None:
        self.env_set(f"{self.plugin_id}.stopping", True)
        delay = float(self.options.get("stop_delay", 0.0))
        if delay > 0:
            time.sleep(delay)
        self.env_set(f"{self.plugin_id}.stopped", True)
