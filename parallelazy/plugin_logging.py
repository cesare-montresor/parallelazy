"""Logging setup shared by the manager and isolated plugin workers."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import logging
import os
import re
import sys
import threading
import time
from typing import List, Optional, TextIO


DEFAULT_FILE_LOG_FORMAT = "[%(elapsed)s] %(levelname)s %(message)s"
DEFAULT_CONSOLE_LOG_FORMAT = (
    "%(asctime)s.%(msecs)03dZ %(levelname)s [%(plugin_id)s] %(message)s"
)
CONSOLE_DATE_FORMAT = "%H:%M:%S"


@dataclass(frozen=True)
class PluginRuntimeSettings:
    work_dir: str
    log_dir: str
    log_level: int
    display_logs: bool
    write_logs: bool = True
    timeout_request: float = 5.0


def resolve_directory(path: str, base_dir: Optional[str] = None) -> str:
    """Return an absolute directory path, resolving relative paths against base_dir."""
    expanded_path = os.path.expanduser(os.fspath(path))
    if not os.path.isabs(expanded_path):
        expanded_path = os.path.join(base_dir or os.getcwd(), expanded_path)
    return os.path.abspath(expanded_path)


def create_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{os.urandom(6).hex()}"


def plugin_log_filename(plugin_id: str) -> str:
    readable_id = "".join(
        character if character.isascii() and (character.isalnum() or character in "-_.") else "_"
        for character in plugin_id
    ).strip("._-") or "plugin"
    suffix = hashlib.sha256(plugin_id.encode("utf-8")).hexdigest()[:12]
    return f"{readable_id}-{suffix}.log"


def default_runtime_settings() -> PluginRuntimeSettings:
    work_dir = resolve_directory(os.getcwd())
    log_dir = os.path.join(resolve_directory("logs", work_dir), create_run_id())
    os.makedirs(log_dir, exist_ok=True)
    return PluginRuntimeSettings(
        work_dir=work_dir,
        log_dir=log_dir,
        log_level=logging.INFO,
        display_logs=True,
        write_logs=True,
    )


class _CapturedStreamConsoleFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(record, "_parallelazy_captured_stream", False)


class _TeeTextStream:
    """Mirror worker stream writes to the original stream and its log file."""

    def __init__(
        self,
        stream: TextIO,
        logger: logging.Logger,
        label: str,
        mirror_to_terminal: bool = True,
    ):
        self._stream = stream
        self._logger = logger
        self._label = label
        self._mirror_to_terminal = mirror_to_terminal
        self._buffer = ""
        self._lock = threading.RLock()
        self._logging = threading.local()

    @property
    def encoding(self):
        return self._stream.encoding

    @property
    def errors(self):
        return self._stream.errors

    @property
    def buffer(self):
        return self._stream.buffer

    @property
    def name(self):
        return self._stream.name

    @property
    def closed(self):
        return self._stream.closed

    def isatty(self) -> bool:
        return self._stream.isatty()

    def fileno(self) -> int:
        return self._stream.fileno()

    def writable(self) -> bool:
        return self._stream.writable()

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError("write() argument must be str")
        with self._lock:
            written = self._stream.write(text) if self._mirror_to_terminal else len(text)
            if "\r" in text or "\n" in text:
                if self._mirror_to_terminal:
                    self._stream.flush()
            if not getattr(self._logging, "active", False):
                self._capture_complete_lines(text)
            return written

    def _capture_complete_lines(self, text: str) -> None:
        for index, character in enumerate(text):
            if character in ("\r", "\n"):
                if character == "\n" and index > 0 and text[index - 1] == "\r":
                    continue
                self._emit_buffer()
            else:
                self._buffer += character

    def _emit_buffer(self) -> None:
        line = self._buffer
        self._buffer = ""
        self._log_line(line)

    def _log_line(self, line: str) -> None:
        self._logging.active = True
        try:
            self._logger.info(
                "[%s] %s",
                self._label,
                line,
                extra={"_parallelazy_captured_stream": True},
            )
        finally:
            self._logging.active = False

    def flush(self) -> None:
        with self._lock:
            self._stream.flush()
            if self._buffer and not getattr(self._logging, "active", False):
                self._emit_buffer()

    def close(self) -> None:
        self.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)


class WorkerOutputCapture:
    """Restore worker streams after teeing their output into the worker log."""

    def __init__(self, logger: logging.Logger, display_logs: bool = True):
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        self.stdout = _TeeTextStream(
            self._original_stdout,
            logger,
            "stdout",
            mirror_to_terminal=display_logs,
        )
        self.stderr = _TeeTextStream(
            self._original_stderr,
            logger,
            "stderr",
            mirror_to_terminal=display_logs,
        )
        sys.stdout = self.stdout
        sys.stderr = self.stderr

    def close(self) -> None:
        try:
            self.stdout.flush()
            self.stderr.flush()
        finally:
            if sys.stdout is self.stdout:
                sys.stdout = self._original_stdout
            if sys.stderr is self.stderr:
                sys.stderr = self._original_stderr


def configure_worker_output(
    logger: logging.Logger,
    display_logs: bool = True,
) -> WorkerOutputCapture:
    """Mirror worker stdout and stderr to the plugin log and optionally the terminal."""
    return WorkerOutputCapture(logger, display_logs=display_logs)


def _format_elapsed(seconds: float) -> str:
    milliseconds = max(0, int(seconds * 1000))
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    whole_seconds, milliseconds = divmod(remainder, 1000)
    return f"+{hours:02}:{minutes:02}:{whole_seconds:02}.{milliseconds:03}"


class _LogContextFilter(logging.Filter):
    def __init__(self, plugin_id: str, started_at: float):
        super().__init__()
        self.plugin_id = plugin_id
        self.started_at = started_at

    def filter(self, record: logging.LogRecord) -> bool:
        record.plugin_id = self.plugin_id
        record.elapsed = _format_elapsed(time.monotonic() - self.started_at)
        return True


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class _RecoveringFileHandler(logging.Handler):
    """Append to a log file and recreate it if its path or parent is removed."""

    terminator = os.linesep

    def __init__(self, filename: str):
        super().__init__()
        self.base_filename = os.path.abspath(filename)
        self.stream: Optional[TextIO] = None
        self._open_stream()

    def _create_stream(self, filename: str) -> TextIO:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        stream = open(filename, "a", encoding="utf-8")
        try:
            stream.write(f"# Log opened: {_utc_timestamp()}{self.terminator}")
            stream.write(
                "# Elapsed time is relative to this process's logging setup."
                f"{self.terminator}"
            )
            stream.flush()
        except Exception:
            stream.close()
            raise
        return stream

    def _open_stream(self) -> None:
        self.stream = self._create_stream(self.base_filename)

    def set_filename(self, filename: str) -> None:
        filename = os.path.abspath(filename)
        if filename == self.base_filename:
            return

        stream = self._create_stream(filename)
        self.acquire()
        try:
            previous_stream = self.stream
            self.base_filename = filename
            self.stream = stream
            if previous_stream is not None:
                previous_stream.close()
        finally:
            self.release()

    def _close_stream(self) -> None:
        stream = self.stream
        self.stream = None
        if stream is not None:
            try:
                stream.flush()
            finally:
                stream.close()

    def _reopen_stream(self) -> None:
        self._close_stream()
        self._open_stream()

    def _ensure_current_stream(self) -> None:
        if self.stream is None:
            self._open_stream()
            return

        try:
            path_identity = os.stat(self.base_filename)
            stream_stat = os.fstat(self.stream.fileno())
        except FileNotFoundError:
            self._reopen_stream()
            return

        if (path_identity.st_dev, path_identity.st_ino) != (
            stream_stat.st_dev,
            stream_stat.st_ino,
        ):
            self._reopen_stream()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._ensure_current_stream()
            self.stream.write(self.format(record) + self.terminator)
            self.flush()
        except OSError:
            try:
                self._reopen_stream()
                self.stream.write(self.format(record) + self.terminator)
                self.flush()
            except Exception:
                self.handleError(record)
        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        if self.stream is not None:
            self.stream.flush()

    def close(self) -> None:
        try:
            self._close_stream()
        finally:
            super().close()


def _make_formatter(format_string: str, datefmt: Optional[str] = None) -> logging.Formatter:
    formatter = logging.Formatter(format_string, datefmt=datefmt)
    formatter.converter = time.gmtime

    record = logging.LogRecord(
        name="parallelazy",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg="validation",
        args=(),
        exc_info=None,
    )
    record.plugin_id = "plugin"
    record.elapsed = "+00:00:00.000"
    formatter.format(record)
    return formatter


class LoggingSession:
    """Owns the handlers and formatters for one process's logging output."""

    def __init__(
        self,
        logger: logging.Logger,
        log_file: str,
        plugin_id: str,
        log_level: int,
        display_logs: bool,
        replace_handlers: bool = False,
        write_logs: bool = True,
    ):
        if isinstance(log_level, bool) or not isinstance(log_level, int):
            raise TypeError("log_level must be an integer logging level")

        self.logger = logger
        self.file_format = DEFAULT_FILE_LOG_FORMAT
        self.console_format = DEFAULT_CONSOLE_LOG_FORMAT
        self._context_filter = _LogContextFilter(plugin_id, time.monotonic())
        self._handlers: List[logging.Handler] = []

        if replace_handlers:
            for handler in list(logger.handlers):
                logger.removeHandler(handler)
                handler.close()

        logger.setLevel(log_level)
        logger.propagate = False

        if write_logs:
            file_handler = _RecoveringFileHandler(log_file)
            file_handler.setLevel(log_level)
            file_handler.addFilter(self._context_filter)
            file_handler.setFormatter(_make_formatter(self.file_format))
            logger.addHandler(file_handler)
            self._handlers.append(file_handler)

        if display_logs:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            console_handler.addFilter(self._context_filter)
            console_handler.addFilter(_CapturedStreamConsoleFilter())
            console_handler.setFormatter(
                _make_formatter(self.console_format, CONSOLE_DATE_FORMAT)
            )
            logger.addHandler(console_handler)
            self._handlers.append(console_handler)

    def set_formats(
        self,
        file_format: Optional[str] = None,
        console_format: Optional[str] = None,
    ) -> None:
        new_file_format = self.file_format if file_format is None else file_format
        new_console_format = (
            self.console_format if console_format is None else console_format
        )
        file_formatter = _make_formatter(new_file_format)
        console_formatter = _make_formatter(new_console_format, CONSOLE_DATE_FORMAT)

        for handler in self._handlers:
            if isinstance(handler, _RecoveringFileHandler):
                handler.setFormatter(file_formatter)
            elif isinstance(handler, logging.StreamHandler):
                handler.setFormatter(console_formatter)

        self.file_format = new_file_format
        self.console_format = new_console_format

    def set_log_file(self, log_file: str) -> None:
        for handler in self._handlers:
            if isinstance(handler, _RecoveringFileHandler):
                handler.set_filename(log_file)

    def close(self) -> None:
        for handler in self._handlers:
            self.logger.removeHandler(handler)
            handler.close()
        self._handlers.clear()


def configure_worker_logging(
    settings: PluginRuntimeSettings,
    plugin_id: str,
) -> LoggingSession:
    root_logger = logging.getLogger()
    return LoggingSession(
        logger=root_logger,
        log_file=os.path.join(settings.log_dir, plugin_log_filename(plugin_id)),
        plugin_id=plugin_id,
        log_level=settings.log_level,
        display_logs=settings.display_logs,
        replace_handlers=True,
        write_logs=settings.write_logs,
    )


def configure_manager_logging(
    log_file: str,
    log_level: int,
    display_logs: bool,
    run_id: str,
    write_logs: bool = True,
) -> LoggingSession:
    manager_logger = logging.getLogger(f"parallelazy.manager.{run_id}")
    return LoggingSession(
        logger=manager_logger,
        log_file=log_file,
        plugin_id="manager",
        log_level=log_level,
        display_logs=display_logs,
        write_logs=write_logs,
    )
