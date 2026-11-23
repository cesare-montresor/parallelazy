"""
Environment State System: Manages shared environment variables across plugins.

Simple thread-safe system for reading/writing global environment variables
and broadcasting updates to interested plugins.
"""

from typing import Any, Dict
from threading import Lock
import logging
import time

logger = logging.getLogger(__name__)



class EnvironmentManager:
    """Thread-safe manager for shared environment variables."""
    
    def __init__(self):
        """Initialize environment manager."""
        self._env: Dict[str, Any] = {}
        self._lock = Lock()
        self._monitoring_enabled = False
        self._write_counts: Dict[str, int] = {}
        self._last_write_times: Dict[str, float] = {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get environment variable value."""
        with self._lock:
            return self._env.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set environment variable."""
        with self._lock:
            self._env[key] = value
            if self._monitoring_enabled:
                self._write_counts[key] = self._write_counts.get(key, 0) + 1
                self._last_write_times[key] = time.monotonic()

    def begin_monitoring(self) -> None:
        """Start collecting environment write counters."""
        with self._lock:
            self._monitoring_enabled = True
            self._write_counts = {key: 0 for key in self._env}
            self._last_write_times = {}

    def end_monitoring(self) -> None:
        """Stop collecting environment write counters."""
        with self._lock:
            self._monitoring_enabled = False
            self._write_counts = {}
            self._last_write_times = {}

    def monitoring_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Return environment metadata and write counters without values."""
        with self._lock:
            return {
                key: {
                    "type": type(value).__name__,
                    "stored_size": __import__("sys").getsizeof(value),
                    "write_count": self._write_counts.get(key, 0),
                    "last_write": self._last_write_times.get(key),
                }
                for key, value in self._env.items()
            }
    
    def get_all(self) -> Dict[str, Any]:
        """Get all environment variables."""
        with self._lock:
            return self._env.copy()
    
    def set_all(self, env_dict: Dict[str, Any]) -> None:
        """Set multiple environment variables."""
        for key, value in env_dict.items():
            self.set(key, value)
    
    def clear(self, key: str = None) -> None:
        """Clear a specific environment variable, or all variables when key is None."""
        with self._lock:
            if key is None:
                self._env.clear()
                self._write_counts.clear()
                self._last_write_times.clear()
            else:
                self._env.pop(key, None)
                self._write_counts.pop(key, None)
                self._last_write_times.pop(key, None)
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in environment."""
        with self._lock:
            return key in self._env
