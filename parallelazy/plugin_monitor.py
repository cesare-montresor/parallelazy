"""Read-only pipeline monitoring for PluginManager."""

from dataclasses import dataclass
import os
import struct
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class CapabilityAvailability:
    capability: str
    available: bool
    reason: Optional[str] = None
    package: Optional[str] = None
    install_command: Optional[str] = None


@dataclass(frozen=True)
class TopicMetric:
    topic: str
    publisher: Optional[str]
    subscriber: str
    pending_bytes: Optional[int]
    trend: Optional[str]
    probe_available: bool
    probe_reason: Optional[str] = None


@dataclass(frozen=True)
class ProcessMetric:
    plugin_id: str
    root_pid: Optional[int]
    alive: bool
    worker_state: Optional[str]
    descendant_count: int
    cpu_percent: Optional[float]
    rss_bytes: Optional[int]
    gpu_memory_bytes: Optional[int]
    gpu_utilization_percent: Optional[float]
    process_available: bool
    gpu_available: bool
    process_reason: Optional[str] = None
    gpu_reason: Optional[str] = None
    gpu_dependency_missing: bool = False


@dataclass(frozen=True)
class EnvironmentMetric:
    key: str
    value_type: str
    writes_per_second: Optional[float]
    stored_size_bytes: int
    subscribers: int
    last_write_seconds_ago: Optional[float]


@dataclass(frozen=True)
class MonitoringSnapshot:
    sampled_at: float
    topics: Tuple[TopicMetric, ...]
    processes: Tuple[ProcessMetric, ...]
    environment: Tuple[EnvironmentMetric, ...]
    capabilities: Tuple[CapabilityAvailability, ...]


class MonitoringAdapter:
    """Read-only, manager-specific view used by PipelineMonitor."""

    def __init__(self, manager):
        self.manager = manager

    def snapshot_state(self) -> Dict[str, Any]:
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
        logger=None,
    ):
        if isinstance(sample_interval, bool) or not isinstance(sample_interval, (int, float)):
            raise TypeError("sample_interval must be a number")
        if sample_interval <= 0:
            raise ValueError("sample_interval must be greater than zero")

        self.adapter = adapter
        self.sample_interval = float(sample_interval)
        self.logger = logger
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._snapshot_lock = threading.Lock()
        self._snapshot: Optional[MonitoringSnapshot] = None
        self._previous_topic_bytes: Dict[Tuple[str, str], int] = {}
        self._previous_environment_counts: Dict[str, int] = {}
        self._previous_sample_at: Optional[float] = None
        self._previous_process_cpu: Dict[int, float] = {}
        self._hotkey_thread: Optional[threading.Thread] = None
        self._hotkey_stop: Optional[threading.Event] = None
        self._terminal_settings = None

        self._psutil = None
        self._psutil_status = CapabilityAvailability(
            "process metrics",
            False,
            "psutil is not installed.",
            "psutil",
            "pip install psutil",
        )
        self._pynvml = None
        self._gpu_status = CapabilityAvailability(
            "GPU metrics",
            False,
            "NVML support is not installed.",
            "nvidia-ml-py",
            "pip install nvidia-ml-py",
        )

        try:
            import psutil

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

    def _sampling_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.sample()
            except Exception as exc:
                if self.logger is not None:
                    self.logger.debug("Monitoring sample failed: %s", exc)
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
        state: Dict[str, Any],
        elapsed: Optional[float],
    ) -> List[TopicMetric]:
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
                    self._previous_topic_bytes[key] = pending

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
        return metrics

    def _read_pipe_pending_bytes(self, subscriber: Any) -> Optional[int]:
        try:
            import fcntl
            descriptor = subscriber.queue_descriptor
            connection = descriptor.send if descriptor.send is not None else descriptor.receive
            if connection is None or connection.closed:
                return None
            pending = struct.pack("I", 0)
            result = fcntl.ioctl(connection.fileno(), 0x541B, pending, True)
            return struct.unpack("I", result)[0]
        except Exception:
            return None

    def _sample_processes(self, state: Dict[str, Any]) -> List[ProcessMetric]:
        metrics = []
        current_cpu = {}
        now = time.monotonic()
        interval = (
            None
            if self._previous_sample_at is None
            else max(0.0, now - self._previous_sample_at)
        )

        for plugin_id, plugin in state["plugins"].items():
            discovered_pids = set()
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
                    descendant_count = max(0, len(processes) - 1)

                    rss_bytes = 0
                    total_delta = 0.0
                    for process in processes:
                        try:
                            rss_bytes += process.memory_info().rss
                            times = process.cpu_times()
                            cumulative = float(times.user + times.system)
                            current_cpu[process.pid] = cumulative
                            previous = self._previous_process_cpu.get(process.pid)
                            if previous is not None and interval:
                                total_delta += max(0.0, cumulative - previous)
                        except (self._psutil.NoSuchProcess, self._psutil.AccessDenied, AttributeError):
                            continue
                    if interval and total_delta > 0:
                        cpu_percent = 100.0 * total_delta / interval
                except (self._psutil.NoSuchProcess, self._psutil.AccessDenied):
                    descendant_count = 0

            gpu_memory, gpu_utilization = self._sample_gpu(discovered_pids)
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
            import pynvml

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
                "NVML could not initialize: %s" % exc,
            )
            return False

        self._pynvml = pynvml
        self._gpu_status = CapabilityAvailability("GPU metrics", True)
        return True

    def _sample_gpu(
        self,
        pids: Iterable[int],
    ) -> Tuple[Optional[int], Optional[float]]:
        if not pids or not self._initialize_nvml():
            return None, None

        pynvml = self._pynvml
        try:
            pid_set = set(pids)
            gpu_memory = 0
            found_memory = False
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
                    if pid in pid_set and isinstance(used, int) and used >= 0:
                        found_memory = True
                        gpu_memory += used

            if found_memory:
                return gpu_memory, None
            return None, None
        except Exception as exc:
            self._gpu_status = CapabilityAvailability(
                "GPU metrics",
                False,
                "NVML could not collect GPU metrics: %s" % exc,
            )
            return None, None

    def _sample_environment(
        self,
        state: Dict[str, Any],
        wall_now: float,
        elapsed: Optional[float],
    ) -> List[EnvironmentMetric]:
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

    def print_report(self, snapshot: Optional[MonitoringSnapshot] = None) -> None:
        snapshot = snapshot or self.get_snapshot()
        lines = ["Parallelazy pipeline monitor", ""]

        topic_rows = []
        for metric in snapshot.topics:
            publisher = metric.publisher or "unknown"
            if metric.pending_bytes is None:
                pending = "-"
                trend = "-"
            else:
                pending = "%d B" % metric.pending_bytes
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
        for metric in snapshot.processes:
            if metric.process_available:
                pid = metric.root_pid if metric.root_pid is not None else "-"
                descendants = str(metric.descendant_count)
                cpu = "-" if metric.cpu_percent is None else "%.1f%%" % metric.cpu_percent
                rss = "-" if metric.rss_bytes is None else self._format_bytes(metric.rss_bytes)
            else:
                pid = "unavailable"
                descendants = "unavailable"
                cpu = "unavailable"
                rss = "unavailable"
            gpu_memory = (
                ("unavailable" if metric.gpu_dependency_missing else "-")
                if metric.gpu_memory_bytes is None
                else self._format_bytes(metric.gpu_memory_bytes)
            )
            gpu_util = (
                ("unavailable" if metric.gpu_dependency_missing else "-")
                if metric.gpu_utilization_percent is None
                else "%.1f%%" % metric.gpu_utilization_percent
            )
            process_rows.append(
                (
                    metric.plugin_id,
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
        for metric in snapshot.environment:
            writes = (
                "-"
                if metric.writes_per_second is None
                else "%.2f/s" % metric.writes_per_second
            )
            age = (
                "-"
                if metric.last_write_seconds_ago is None
                else "%.1fs ago" % metric.last_write_seconds_ago
            )
            environment_rows.append(
                (
                    metric.key,
                    metric.value_type,
                    writes,
                    self._format_bytes(metric.stored_size_bytes),
                    metric.subscribers,
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
                notice = "%s %s" % (capability.capability, state)
                if capability.reason:
                    notice += ": %s" % capability.reason
                if capability.install_command:
                    notice += " Enable with: %s" % capability.install_command
                lines.append(notice)

        print("\n".join(lines), flush=True)

    @staticmethod
    def _format_table(
        title: str,
        headers: Iterable[str],
        rows: Iterable[Iterable[Any]],
    ) -> List[str]:
        headers = tuple(headers)
        rows = [tuple(str(value) for value in row) for row in rows]
        if not rows:
            rows = [tuple("-" for _ in headers)]

        widths = [
            max(len(header), *(len(row[index]) for row in rows))
            for index, header in enumerate(headers)
        ]
        separator = "+-" + "-+-".join("-" * width for width in widths) + "-+"

        def render_row(values: Iterable[str]) -> str:
            return "| " + " | ".join(
                value.center(width) if value == "-" else value.ljust(width)
                for value, width in zip(values, widths)
            ) + " |"

        return [
            title + ":",
            separator,
            render_row(headers),
            separator,
            *(render_row(row) for row in rows),
            separator,
        ]

    @staticmethod
    def _format_bytes(value: int) -> str:
        units = ("B", "KiB", "MiB", "GiB", "TiB")
        amount = float(value)
        for unit in units:
            if amount < 1024.0 or unit == units[-1]:
                if unit == "B":
                    return "%d B" % int(amount)
                return "%.1f %s" % (amount, unit)
            amount /= 1024.0

    def start_hotkey(self, key: str = "m") -> bool:
        if len(key) != 1:
            raise ValueError("hotkey must be one character")
        if not sys.stdin.isatty():
            return False

        self.stop_hotkey()
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

        def listen():
            import termios

            fd = sys.stdin.fileno()
            while not stop_event.is_set():
                try:
                    char = sys.stdin.read(1)
                except Exception:
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
            self._hotkey_thread.join(timeout=0.2)
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
