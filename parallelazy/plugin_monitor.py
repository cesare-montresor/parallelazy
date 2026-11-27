"""Read-only pipeline monitoring for PluginManager."""

import logging
import os
import struct
import sys
import tempfile
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from parallelazy.plugin_manager import PluginManager

if sys.platform.startswith("linux"):
    import fcntl
    import termios as _linux_termios
    _FIONREAD = getattr(_linux_termios, "FIONREAD", 0x541B)
else:
    fcntl = None
    _FIONREAD = 0x541B


@dataclass(frozen=True)
class CapabilityAvailability:
    capability: str
    available: bool
    reason: str | None = None
    package: str | None = None
    install_command: str | None = None


@dataclass(frozen=True)
class TopicMetric:
    topic: str
    publisher: str | None
    subscriber: str
    pending_bytes: int | None
    trend: str | None
    probe_available: bool
    probe_reason: str | None = None


@dataclass(frozen=True)
class ProcessMetric:
    plugin_id: str
    root_pid: int | None
    alive: bool
    worker_state: str | None
    descendant_count: int
    cpu_percent: float | None
    rss_bytes: int | None
    gpu_memory_bytes: int | None
    gpu_utilization_percent: float | None
    process_available: bool
    gpu_available: bool
    process_reason: str | None = None
    gpu_reason: str | None = None
    gpu_dependency_missing: bool = False


@dataclass(frozen=True)
class EnvironmentMetric:
    key: str
    value_type: str
    writes_per_second: float | None
    stored_size_bytes: int
    subscribers: int
    last_write_seconds_ago: float | None


@dataclass(frozen=True)
class MonitoringSnapshot:
    sampled_at: float
    topics: tuple[TopicMetric, ...]
    processes: tuple[ProcessMetric, ...]
    environment: tuple[EnvironmentMetric, ...]
    capabilities: tuple[CapabilityAvailability, ...]


class MonitoringAdapter:
    """Read-only, manager-specific view used by PipelineMonitor."""

    def __init__(self, manager: "PluginManager") -> None:
        self.manager = manager

    def snapshot_state(self) -> dict[str, Any]:
        manager = self.manager
        with manager._plugins_lock:
            publishers = dict(manager._topic_publishers)
            subscribers = {
                topic: dict(topic_subscribers)
                for topic, topic_subscribers in manager._topic_subscribers.items()
            }
            env_subscribers = {
                key: tuple(plugin_ids)
                for key, plugin_ids in manager._env_subscribers.items()
            }
            plugins = {}
            for plugin_id, info in manager._plugins.items():
                try:
                    pid = info.process.pid
                    alive = info.process.is_alive()
                except Exception:
                    pid = None
                    alive = False
                plugins[plugin_id] = {
                    "pid": pid,
                    "alive": alive,
                }

        environment = manager._env.monitoring_snapshot()
        return {
            "publishers": publishers,
            "subscribers": subscribers,
            "env_subscribers": env_subscribers,
            "environment": environment,
            "plugins": plugins,
        }


class PipelineMonitor:
    """Background sampler for pipeline health without consuming topic payloads."""

    def __init__(
        self,
        adapter: MonitoringAdapter,
        sample_interval: float = 1.0,
        logger: logging.Logger | None = None,
        report_file: str | os.PathLike | None = None,
    ) -> None:
        if isinstance(sample_interval, bool) or not isinstance(sample_interval, (int, float)):
            raise TypeError("sample_interval must be a number")
        if sample_interval <= 0:
            raise ValueError("sample_interval must be greater than zero")

        self.adapter = adapter
        self.sample_interval = float(sample_interval)
        self.logger = logger
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot_lock = threading.Lock()
        self._snapshot: MonitoringSnapshot | None = None
        self._previous_topic_bytes: dict[tuple[str, str], int] = {}
        self._previous_environment_counts: dict[str, int] = {}
        self._previous_sample_at: float | None = None
        self._previous_process_cpu: dict[int, float] = {}
        self._hotkey_thread: threading.Thread | None = None
        self._hotkey_stop: threading.Event | None = None
        self._terminal_settings: Any = None
        self._report_file_lock = threading.Lock()
        self._report_file: str | None = None
        if report_file is not None:
            self._report_file = self._normalize_report_file(report_file)

        self._psutil: Any = None
        self._psutil_status = CapabilityAvailability(
            "process metrics",
            False,
            "psutil is not installed.",
            "psutil",
            "pip install psutil",
        )
        self._pynvml: Any = None
        self._gpu_status = CapabilityAvailability(
            "GPU metrics",
            False,
            "NVML support is not installed.",
            "nvidia-ml-py",
            "pip install nvidia-ml-py",
        )

        try:
            import psutil  # type: ignore[import-untyped]  # pyright: ignore[reportMissingModuleSource]

            self._psutil = psutil
            self._psutil_status = CapabilityAvailability(
                "process metrics",
                True,
            )
        except ImportError:
            pass

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._sampling_loop,
            name="parallelazy-monitor",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._thread = None
        self.stop_hotkey()
        self.stop_report_file()
        if self._pynvml is not None:
            try:
                self._pynvml.nvmlShutdown()
            except Exception:
                pass
            self._pynvml = None

    def _sampling_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                snapshot = self.sample()
            except Exception as exc:
                if self.logger is not None:
                    self.logger.debug("Monitoring sample failed: %s", exc)
            else:
                try:
                    self._write_report_file(snapshot)
                except Exception as exc:
                    if self.logger is not None:
                        self.logger.debug("Monitoring report file write failed: %s", exc)
            self._stop_event.wait(self.sample_interval)

    def sample(self) -> MonitoringSnapshot:
        state = self.adapter.snapshot_state()
        sampled_at = time.monotonic()
        wall_now = time.time()
        elapsed = (
            None
            if self._previous_sample_at is None
            else max(0.0, sampled_at - self._previous_sample_at)
        )

        topic_metrics = self._sample_topics(state, elapsed)
        process_metrics = self._sample_processes(state)
        environment_metrics = self._sample_environment(
            state,
            wall_now,
            elapsed,
        )

        capabilities = [
            self._pipe_capability(),
            self._psutil_status,
            self._gpu_status,
        ]
        snapshot = MonitoringSnapshot(
            sampled_at=wall_now,
            topics=tuple(topic_metrics),
            processes=tuple(process_metrics),
            environment=tuple(environment_metrics),
            capabilities=tuple(capabilities),
        )
        with self._snapshot_lock:
            self._snapshot = snapshot
        self._previous_sample_at = sampled_at
        return snapshot

    def get_snapshot(self) -> MonitoringSnapshot:
        with self._snapshot_lock:
            if self._snapshot is not None:
                return self._snapshot
        return self.sample()

    def _pipe_capability(self) -> CapabilityAvailability:
        if sys.platform.startswith("linux"):
            return CapabilityAvailability("pipe backlog", True)
        return CapabilityAvailability(
            "pipe backlog",
            False,
            "Pipe byte inspection is unsupported on this platform.",
        )

    def _sample_topics(
        self,
        state: dict[str, Any],
        elapsed: float | None,
    ) -> list[TopicMetric]:
        del elapsed
        metrics = []
        publishers = state["publishers"]
        subscribers = state["subscribers"]

        if not sys.platform.startswith("linux"):
            reason = "Pipe byte inspection is unsupported on this platform."
            for topic, topic_subscribers in subscribers.items():
                publisher = publishers.get(topic)
                for subscriber_id in topic_subscribers:
                    metrics.append(
                        TopicMetric(
                            topic,
                            publisher,
                            subscriber_id,
                            None,
                            None,
                            False,
                            reason,
                        )
                    )
            return metrics

        next_topic_bytes: dict[tuple[str, str], int] = {}
        for topic, topic_subscribers in subscribers.items():
            publisher = publishers.get(topic)
            for subscriber_id, subscriber in topic_subscribers.items():
                pending = self._read_pipe_pending_bytes(subscriber)
                trend = None
                key = (topic, subscriber_id)
                if pending is not None:
                    previous = self._previous_topic_bytes.get(key)
                    if previous is not None:
                        if pending > previous:
                            trend = "growing"
                        elif pending < previous:
                            trend = "shrinking"
                        else:
                            trend = "steady"
                    next_topic_bytes[key] = pending

                metrics.append(
                    TopicMetric(
                        topic,
                        publisher,
                        subscriber_id,
                        pending,
                        trend,
                        pending is not None,
                        None if pending is not None else "Subscriber pipe is unavailable or closed.",
                    )
                )
        self._previous_topic_bytes = next_topic_bytes
        return metrics

    def _read_pipe_pending_bytes(self, subscriber: Any) -> int | None:
        if fcntl is None:
            return None
        try:
            descriptor = subscriber.queue_descriptor
            connection = descriptor.send if descriptor.send is not None else descriptor.receive
            if connection is None or connection.closed:
                return None
            pending = struct.pack("I", 0)
            result = fcntl.ioctl(connection.fileno(), _FIONREAD, pending, True)
            return struct.unpack("I", result)[0]
        except Exception:
            return None

    def _sample_processes(self, state: dict[str, Any]) -> list[ProcessMetric]:
        metrics = []
        current_cpu = {}
        now = time.monotonic()
        interval = (
            None
            if self._previous_sample_at is None
            else max(0.0, now - self._previous_sample_at)
        )

        plugin_rows = []
        all_discovered_pids = set()
        for plugin_id, plugin in state["plugins"].items():
            discovered_pids: set[int] = set()
            root_pid = plugin["pid"]
            alive = plugin["alive"]
            descendant_count = 0
            rss_bytes = None
            cpu_percent = None

            if root_pid is not None and self._psutil is not None:
                try:
                    root = self._psutil.Process(root_pid)
                    processes = [root] + root.children(recursive=True)
                    unique = {}
                    for process in processes:
                        unique[process.pid] = process
                    processes = list(unique.values())
                    discovered_pids.update(unique.keys())
                    all_discovered_pids.update(discovered_pids)
                    descendant_count = max(0, len(processes) - 1)

                    rss_bytes = 0
                    total_delta = 0.0
                    saw_previous = False
                    for process in processes:
                        try:
                            rss_bytes += process.memory_info().rss
                            times = process.cpu_times()
                            cumulative = float(times.user + times.system)
                            current_cpu[process.pid] = cumulative
                            previous = self._previous_process_cpu.get(process.pid)
                            if previous is not None and interval:
                                saw_previous = True
                                total_delta += max(0.0, cumulative - previous)
                        except (self._psutil.NoSuchProcess, self._psutil.AccessDenied, AttributeError):
                            continue
                    if interval and saw_previous:
                        cpu_percent = 100.0 * total_delta / interval
                except (self._psutil.NoSuchProcess, self._psutil.AccessDenied):
                    descendant_count = 0

            plugin_rows.append(
                (plugin_id, root_pid, alive, descendant_count, cpu_percent, rss_bytes, discovered_pids)
            )

        gpu_memory_by_pid = (
            self._snapshot_gpu_memory_by_pid() if all_discovered_pids else None
        )

        for plugin_id, root_pid, alive, descendant_count, cpu_percent, rss_bytes, discovered_pids in plugin_rows:
            gpu_memory = None
            gpu_utilization = None
            if gpu_memory_by_pid is not None and discovered_pids:
                matched = [
                    gpu_memory_by_pid[pid]
                    for pid in discovered_pids
                    if pid in gpu_memory_by_pid
                ]
                if matched:
                    gpu_memory = sum(matched)

            metrics.append(
                ProcessMetric(
                    plugin_id=plugin_id,
                    root_pid=root_pid,
                    alive=alive,
                    worker_state=None,
                    descendant_count=descendant_count,
                    cpu_percent=cpu_percent,
                    rss_bytes=rss_bytes,
                    gpu_memory_bytes=gpu_memory,
                    gpu_utilization_percent=gpu_utilization,
                    process_available=self._psutil is not None,
                    gpu_available=self._gpu_status.available,
                    process_reason=None if self._psutil is not None else self._psutil_status.reason,
                    gpu_reason=None if self._gpu_status.available else self._gpu_status.reason,
                    gpu_dependency_missing=self._gpu_status.install_command is not None,
                )
            )

        self._previous_process_cpu = current_cpu
        return metrics

    def _initialize_nvml(self) -> bool:
        if self._pynvml is not None:
            return True
        try:
            import pynvml  # type: ignore[import-untyped]

        except ImportError:
            self._gpu_status = CapabilityAvailability(
                "GPU metrics",
                False,
                "NVML support is not installed.",
                "nvidia-ml-py",
                "pip install nvidia-ml-py",
            )
            return False
        try:
            pynvml.nvmlInit()
        except Exception as exc:
            self._gpu_status = CapabilityAvailability(
                "GPU metrics",
                False,
                f"NVML could not initialize: {exc}",
            )
            return False

        self._pynvml = pynvml
        self._gpu_status = CapabilityAvailability("GPU metrics", True)
        return True

    def _snapshot_gpu_memory_by_pid(self) -> dict[int, int] | None:
        if not self._initialize_nvml():
            return None

        pynvml = self._pynvml
        if pynvml is None:
            return None
        try:
            gpu_memory_by_pid: dict[int, int] = {}
            for index in range(pynvml.nvmlDeviceGetCount()):
                handle = pynvml.nvmlDeviceGetHandleByIndex(index)
                try:
                    processes = pynvml.nvmlDeviceGetComputeRunningProcesses(handle)
                except Exception:
                    processes = []
                try:
                    graphics = pynvml.nvmlDeviceGetGraphicsRunningProcesses(handle)
                except Exception:
                    graphics = []

                for process in list(processes) + list(graphics):
                    pid = getattr(process, "pid", None)
                    used = getattr(process, "usedGpuMemory", None)
                    if isinstance(pid, int) and isinstance(used, int) and used >= 0:
                        gpu_memory_by_pid[pid] = gpu_memory_by_pid.get(pid, 0) + used

            return gpu_memory_by_pid
        except Exception as exc:
            self._gpu_status = CapabilityAvailability(
                "GPU metrics",
                False,
                f"NVML could not collect GPU metrics: {exc}",
            )
            return None

    def _sample_gpu(
        self,
        pids: Iterable[int],
    ) -> tuple[int | None, float | None]:
        pid_set = set(pids)
        if not pid_set:
            return None, None
        gpu_memory_by_pid = self._snapshot_gpu_memory_by_pid()
        if gpu_memory_by_pid is None:
            return None, None
        matched = [
            gpu_memory_by_pid[pid]
            for pid in pid_set
            if pid in gpu_memory_by_pid
        ]
        if matched:
            return sum(matched), None
        return None, None

    def _sample_environment(
        self,
        state: dict[str, Any],
        wall_now: float,
        elapsed: float | None,
    ) -> list[EnvironmentMetric]:
        metrics = []
        for key, metadata in state["environment"].items():
            count = int(metadata["write_count"])
            previous_count = self._previous_environment_counts.get(key)
            writes_per_second = None
            if previous_count is not None and elapsed:
                delta = max(0, count - previous_count)
                writes_per_second = delta / elapsed

            last_write = metadata["last_write"]
            last_write_seconds_ago = (
                None if last_write is None else max(0.0, time.monotonic() - last_write)
            )
            metrics.append(
                EnvironmentMetric(
                    key=key,
                    value_type=metadata["type"],
                    writes_per_second=writes_per_second,
                    stored_size_bytes=int(metadata["stored_size"]),
                    subscribers=len(state["env_subscribers"].get(key, ())),
                    last_write_seconds_ago=last_write_seconds_ago,
                )
            )
        self._previous_environment_counts = {
            key: int(metadata["write_count"])
            for key, metadata in state["environment"].items()
        }
        del wall_now
        return metrics

    def format_report(self, snapshot: MonitoringSnapshot | None = None) -> str:
        snapshot = snapshot or self.get_snapshot()
        updated_at = time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(snapshot.sampled_at),
        )
        lines = [f"Parallelazy pipeline monitor (updated {updated_at})", ""]

        topic_rows = []
        for metric in snapshot.topics:
            publisher = metric.publisher or "unknown"
            if metric.pending_bytes is None:
                pending = "-"
                trend = "-"
            else:
                pending = f"{metric.pending_bytes:d} B"
                trend = metric.trend or "-"
            topic_rows.append(
                (
                    metric.topic,
                    publisher,
                    metric.subscriber,
                    pending,
                    trend,
                )
            )
        lines.extend(
            self._format_table(
                "Topics",
                ("Topic", "Publisher", "Subscriber", "Pending bytes", "Trend"),
                topic_rows,
            )
        )
        lines.append("")

        process_rows = []
        for proc_metric in snapshot.processes:
            if proc_metric.process_available:
                pid = proc_metric.root_pid if proc_metric.root_pid is not None else "-"
                descendants = str(proc_metric.descendant_count)
                cpu = "-" if proc_metric.cpu_percent is None else f"{proc_metric.cpu_percent:.1f}%"
                rss = "-" if proc_metric.rss_bytes is None else self._format_bytes(proc_metric.rss_bytes)
            else:
                pid = "unavailable"
                descendants = "unavailable"
                cpu = "unavailable"
                rss = "unavailable"
            gpu_memory = (
                ("unavailable" if proc_metric.gpu_dependency_missing else "-")
                if proc_metric.gpu_memory_bytes is None
                else self._format_bytes(proc_metric.gpu_memory_bytes)
            )
            gpu_util = (
                ("unavailable" if proc_metric.gpu_dependency_missing else "-")
                if proc_metric.gpu_utilization_percent is None
                else f"{proc_metric.gpu_utilization_percent:.1f}%"
            )
            process_rows.append(
                (
                    proc_metric.plugin_id,
                    pid,
                    descendants,
                    cpu,
                    rss,
                    gpu_memory,
                    gpu_util,
                )
            )
        lines.extend(
            self._format_table(
                "Plugins",
                (
                    "Plugin",
                    "PID",
                    "Descendants",
                    "CPU",
                    "RSS",
                    "GPU memory",
                    "GPU util",
                ),
                process_rows,
            )
        )
        lines.append("")

        environment_rows = []
        for env_metric in snapshot.environment:
            writes = (
                "-"
                if env_metric.writes_per_second is None
                else f"{env_metric.writes_per_second:.2f}/s"
            )
            age = (
                "-"
                if env_metric.last_write_seconds_ago is None
                else f"{env_metric.last_write_seconds_ago:.1f}s ago"
            )
            environment_rows.append(
                (
                    env_metric.key,
                    env_metric.value_type,
                    writes,
                    self._format_bytes(env_metric.stored_size_bytes),
                    env_metric.subscribers,
                    age,
                )
            )
        lines.extend(
            self._format_table(
                "Environment",
                (
                    "Key",
                    "Type",
                    "Writes/s",
                    "Stored size",
                    "Subscribers",
                    "Last write",
                ),
                environment_rows,
            )
        )

        for capability in snapshot.capabilities:
            if not capability.available:
                lines.append("")
                state = "unavailable" if capability.install_command else "not collected"
                notice = f"{capability.capability} {state}"
                if capability.reason:
                    notice += f": {capability.reason}"
                if capability.install_command:
                    notice += f" Enable with: {capability.install_command}"
                lines.append(notice)

        return "\n".join(lines)

    def print_report(self, snapshot: MonitoringSnapshot | None = None) -> None:
        print(self.format_report(snapshot), flush=True)

    @staticmethod
    def _normalize_report_file(path: str | os.PathLike) -> str:
        try:
            normalized = os.fspath(path)
        except TypeError as exc:
            raise TypeError("report_file must be a path-like string") from exc
        if not isinstance(normalized, str) or not normalized.strip():
            raise ValueError("report_file must be a non-empty path")
        return os.path.abspath(normalized)

    def start_report_file(self, path: str | os.PathLike) -> str:
        normalized = self._normalize_report_file(path)
        with self._report_file_lock:
            self._report_file = normalized
        self._write_report_file(self.get_snapshot())
        return normalized

    def stop_report_file(self) -> None:
        with self._report_file_lock:
            self._report_file = None

    @property
    def report_file(self) -> str | None:
        with self._report_file_lock:
            return self._report_file

    def _write_report_file(self, snapshot: MonitoringSnapshot) -> bool:
        with self._report_file_lock:
            target = self._report_file
            if target is None:
                return False
            content = self.format_report(snapshot) + "\n"
            parent = os.path.dirname(target) or "."
            os.makedirs(parent, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(
                prefix=".parallelazy-monitor-",
                suffix=".tmp",
                dir=parent,
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(content)
                self._replace_report_file(tmp_path, target)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            return True

    @staticmethod
    def _replace_report_file(tmp_path: str, target: str) -> None:
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                os.replace(tmp_path, target)
                return
            except PermissionError:
                if attempt == max_attempts - 1:
                    raise
                time.sleep(0.02)

    @staticmethod
    def _format_table(
        title: str,
        headers: Iterable[str],
        rows: Iterable[Iterable[Any]],
    ) -> list[str]:
        headers = tuple(headers)
        row_tuples = [tuple(str(value) for value in row) for row in rows]
        if not row_tuples:
            row_tuples = [tuple("-" for _ in headers)]

        widths = [
            max(len(header), *(len(row[index]) for row in row_tuples))
            for index, header in enumerate(headers)
        ]
        separator = "+-" + "-+-".join("-" * width for width in widths) + "-+"

        def render_row(values: Iterable[str]) -> str:
            return "| " + " | ".join(
                value.center(width) if value == "-" else value.ljust(width)
                for value, width in zip(values, widths, strict=False)
            ) + " |"

        return [
            title + ":",
            separator,
            render_row(headers),
            separator,
            *(render_row(row) for row in row_tuples),
            separator,
        ]

    @staticmethod
    def _format_bytes(value: int) -> str:
        units = ("B", "KiB", "MiB", "GiB", "TiB")
        amount = float(value)
        for unit in units:
            if amount < 1024.0 or unit == units[-1]:
                if unit == "B":
                    return f"{int(amount):d} B"
                return f"{amount:.1f} {unit}"
            amount /= 1024.0
        return f"{amount:.1f} {units[-1]}"

    def start_hotkey(self, key: str = "m") -> bool:
        if len(key) != 1:
            raise ValueError("hotkey must be one character")
        if not sys.stdin or not sys.stdin.isatty():
            return False

        self.stop_hotkey()
        if sys.platform == "win32":
            return self._start_hotkey_windows(key)
        return self._start_hotkey_posix(key)

    def _start_hotkey_posix(self, key: str) -> bool:
        try:
            import termios
            import tty

            fd = sys.stdin.fileno()
            self._terminal_settings = termios.tcgetattr(fd)
            tty.setcbreak(fd)
        except Exception:
            self._terminal_settings = None
            return False

        stop_event = threading.Event()
        self._hotkey_stop = stop_event

        def listen() -> None:
            import select

            while not stop_event.is_set():
                try:
                    ready, _, _ = select.select([sys.stdin], [], [], 0.1)
                except Exception:
                    break
                if not ready:
                    continue
                if stop_event.is_set():
                    break
                try:
                    char = sys.stdin.read(1)
                except Exception:
                    break
                if not char:
                    break
                if char == key:
                    self.print_report()

        self._hotkey_thread = threading.Thread(
            target=listen,
            name="parallelazy-monitor-hotkey",
            daemon=True,
        )
        self._hotkey_thread.start()
        return True

    def _start_hotkey_windows(self, key: str) -> bool:
        try:
            import _thread
            import msvcrt as _msvcrt
        except ImportError:
            return False

        msvcrt: Any = _msvcrt
        stop_event = threading.Event()
        self._hotkey_stop = stop_event

        def listen() -> None:
            while not stop_event.is_set():
                try:
                    if not msvcrt.kbhit():
                        stop_event.wait(0.05)
                        continue
                    char = msvcrt.getwch()
                except Exception:
                    break
                if char in ("\x00", "\xe0"):
                    # Discard extended key scan code (arrows, function keys)
                    try:
                        if msvcrt.kbhit():
                            msvcrt.getwch()
                    except Exception:
                        break
                    continue
                if char == "\x03":
                    _thread.interrupt_main()
                    break
                if char == key:
                    self.print_report()

        self._hotkey_thread = threading.Thread(
            target=listen,
            name="parallelazy-monitor-hotkey",
            daemon=True,
        )
        self._hotkey_thread.start()
        return True

    def stop_hotkey(self) -> None:
        if self._hotkey_stop is not None:
            self._hotkey_stop.set()
        if self._hotkey_thread is not None:
            self._hotkey_thread.join(timeout=0.5)
        self._hotkey_thread = None
        self._hotkey_stop = None

        if self._terminal_settings is not None:
            try:
                import termios

                termios.tcsetattr(
                    sys.stdin.fileno(),
                    termios.TCSADRAIN,
                    self._terminal_settings,
                )
            except Exception:
                pass
        self._terminal_settings = None
