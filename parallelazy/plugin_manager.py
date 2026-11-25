"""
Plugin Manager: Lightweight multiprocessing-based plugin orchestration.

Manages plugin lifecycle, pub/sub messaging, environment variables, and services.
Uses simplified request/response protocol via Channel.
"""

from dataclasses import dataclass
import logging
import math
import os
import threading
import torch.multiprocessing as mp
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue as WorkQueue
from multiprocessing.sharedctypes import Synchronized
from typing import Any, Dict, List, Optional, Union

from parallelazy.utils import is_debugpy_attached, process_start_spawn, setup_worker_debugging_env
from parallelazy.plugin import PluginDescriptor
from parallelazy.plugin_logging import (
    PluginRuntimeSettings,
    configure_manager_logging,
    create_run_id,
    resolve_directory,
)
from parallelazy.plugin_rpc import PluginRPC, RequestEnvGetAll, RequestEnvUnsubscribe, RequestTopicAnnounce
from parallelazy.plugin_transport import release_cuda_ipc, sanitize_for_ipc, sanitize_for_ipc_with_info
from parallelazy.plugin_worker import PluginWorker, Queue, RequestPluginCall, RequestTopicList, RequestTopicSubscribe, RequestTopicUnsubscribe
from parallelazy.plugin_data import  CommandType, ENV_MISSING, PluginStartupPhase, ProcessStatus, RequestEnvGet, RequestEnvSet, RequestEnvSubscribe, PluginStatus, RequestPluginErrors, RequestPluginLoad, RequestPluginStart, RequestPluginStatus, RequestPluginStatusAll, RequestPluginStop, RequestPluginUnload, Subscriber
from parallelazy.environment_manager import EnvironmentManager


@dataclass
class PluginInfo:
    plugin_id: str
    process: mp.Process
    rpc: PluginRPC
    descriptor: PluginDescriptor
    options: Dict[str, Any]
    stop_confirmed: bool = False
    startup_phase: Optional[Synchronized] = None
    stopped_event: Optional[Any] = None

class PluginManager:
    """
    Lightweight manager for multiprocessing-based plugin orchestration.
    
    Responsibilities:
    - Plugin lifecycle (load, start, stop, unload)
    - Pub/sub topic routing
    - Environment variable management and subscriptions
    - Service request/response dispatch
    """
    
    def __init__(
        self,
        timeout_request: float = 5.0,
        timeout_start: float = 10.0,
        timeout_start_debug: float = 30.0,
        work_dir: Optional[str] = None,
        log_dir: str = "logs",
        log_level: int = logging.INFO,
        display_logs: bool = True,
        timeout_initialize: float = 30.0,
        timeout_debugger_attach: Optional[float] = None,
        write_logs: bool = True,
    ):
        """
        Initialize plugin manager.
        
        Args:
            timeout_request: Default timeout for sync requests (seconds)
            timeout_start: Timeout for start command (seconds)
            timeout_start_debug: Timeout for start command in debug mode (seconds)
            timeout_initialize: Timeout for worker initialization (seconds)
            timeout_debugger_attach: Optional timeout for debugger attachment
            write_logs: Whether to write manager and plugin log files
            work_dir: Base working directory for the manager and plugins
            log_dir: Log directory, relative to work_dir unless absolute
            log_level: Default logging level for the manager and plugins
            display_logs: Whether to also emit logs to the terminal
        """
        if not isinstance(display_logs, bool):
            raise TypeError("display_logs must be a bool")
        if not isinstance(write_logs, bool):
            raise TypeError("write_logs must be a bool")
        if isinstance(log_level, bool) or not isinstance(log_level, int):
            raise TypeError("log_level must be an integer logging level")
        if (
            isinstance(timeout_initialize, bool)
            or not isinstance(timeout_initialize, (int, float))
        ):
            raise TypeError("timeout_initialize must be a number")
        if not math.isfinite(timeout_initialize) or timeout_initialize <= 0:
            raise ValueError("timeout_initialize must be finite and greater than zero")
        if timeout_debugger_attach is not None:
            if (
                isinstance(timeout_debugger_attach, bool)
                or not isinstance(timeout_debugger_attach, (int, float))
            ):
                raise TypeError("timeout_debugger_attach must be a number or None")
            if not math.isfinite(timeout_debugger_attach) or timeout_debugger_attach <= 0:
                raise ValueError(
                    "timeout_debugger_attach must be finite and greater than zero"
                )

        self._work_dir = resolve_directory(work_dir or os.getcwd())
        self._log_dir_setting = log_dir
        self._log_dir = resolve_directory(log_dir, self._work_dir)
        self.log_level = log_level
        self.display_logs = display_logs
        self.run_id = create_run_id()
        self.run_log_dir = os.path.join(self.log_dir, self.run_id)
        os.makedirs(self._work_dir, exist_ok=True)
        if write_logs:
            os.makedirs(self.run_log_dir, exist_ok=True)

        self._logging = configure_manager_logging(
            os.path.join(self.run_log_dir, "manager.log"),
            log_level,
            display_logs,
            self.run_id,
            write_logs,
        )
        self.logger = self._logging.logger

        process_start_spawn()

        self._plugins: Dict[str, PluginInfo] = {}  # plugin_id -> PluginInfo
        self._plugins_lock = threading.RLock()  # For nested registry operations
        self._loading_plugins = set()

        self._topic_publishers: Dict[str, str] = {}       # topic -> plugin_id
        self._topic_subscribers: Dict[str, Dict[str, Subscriber]] = {}        # topic -> [queue, queue, ...]
        self._env_subscribers: Dict[str, List[str]] = {}  # env_key -> [plugin_id, ...]
        
        self._env = EnvironmentManager()
        self._monitor_lock = threading.Lock()
        self._monitor = None
        
        self.timeout_request = timeout_request
        self.worker_pydebug = is_debugpy_attached()
        self.timeout_start = timeout_start if not self.worker_pydebug else timeout_start_debug
        self.timeout_initialize = timeout_initialize
        self.timeout_debugger_attach = timeout_debugger_attach
        self.write_logs = write_logs
        self._env_update_queue: WorkQueue = WorkQueue()
        self._env_dispatcher_stop = object()
        self._env_dispatcher = threading.Thread(
            target=self._dispatch_environment_updates,
            name="plugin-env-dispatcher",
            daemon=True,
        )
        self._env_dispatcher.start()


        if self.worker_pydebug:
            setup_worker_debugging_env()

    @property
    def work_dir(self) -> str:
        """Base working directory for the manager and its plugins."""
        return self._work_dir

    @property
    def log_dir(self) -> str:
        """Base directory for manager and plugin log files."""
        return self._log_dir

    @log_dir.setter
    def log_dir(self, path: str) -> None:
        log_dir = resolve_directory(path, self.work_dir)
        with self._plugins_lock:
            if log_dir == self._log_dir:
                self._log_dir_setting = path
                return
            if self._plugins or self._loading_plugins:
                raise RuntimeError("log_dir cannot be changed after plugin loading starts")

            self._update_log_dir(log_dir)
            self._log_dir_setting = path

    @work_dir.setter
    def work_dir(self, path: str) -> None:
        work_dir = resolve_directory(path)
        if work_dir == self._work_dir:
            return

        with self._plugins_lock:
            if self._plugins or self._loading_plugins:
                raise RuntimeError("work_dir cannot be changed after plugin loading starts")

            log_dir = resolve_directory(self._log_dir_setting, work_dir)
            os.makedirs(work_dir, exist_ok=True)
            self._update_log_dir(log_dir)
            self._work_dir = work_dir

    def _update_log_dir(self, log_dir: str) -> None:
        run_log_dir = os.path.join(log_dir, self.run_id)
        if self.write_logs:
            os.makedirs(run_log_dir, exist_ok=True)
        self._logging.set_log_file(os.path.join(run_log_dir, "manager.log"))
        self._log_dir = log_dir
        self.run_log_dir = run_log_dir

    def set_log_formats(
        self,
        file_format: Optional[str] = None,
        console_format: Optional[str] = None,
    ) -> None:
        """Change this manager's file and/or console log format."""
        self._logging.set_formats(file_format, console_format)

    def _plugin_runtime_settings(
        self,
        descriptor: PluginDescriptor,
    ) -> PluginRuntimeSettings:
        if descriptor.display_logs is not None and not isinstance(descriptor.display_logs, bool):
            raise TypeError("descriptor.display_logs must be a bool or None")
        if descriptor.log_level is not None and (
            isinstance(descriptor.log_level, bool)
            or not isinstance(descriptor.log_level, int)
        ):
            raise TypeError("descriptor.log_level must be an integer logging level or None")
        if descriptor.write_logs is not None and not isinstance(descriptor.write_logs, bool):
            raise TypeError("descriptor.write_logs must be a bool or None")

        work_dir = (
            self.work_dir
            if descriptor.work_dir is None
            else resolve_directory(descriptor.work_dir, self.work_dir)
        )
        log_dir = (
            self.log_dir
            if descriptor.log_dir is None
            else resolve_directory(descriptor.log_dir, work_dir)
        )
        run_log_dir = os.path.join(log_dir, self.run_id)
        effective_write_logs = (
            self.write_logs
            if descriptor.write_logs is None
            else descriptor.write_logs
        )
        os.makedirs(work_dir, exist_ok=True)
        if effective_write_logs:
            os.makedirs(run_log_dir, exist_ok=True)
        return PluginRuntimeSettings(
            work_dir=work_dir,
            log_dir=run_log_dir,
            log_level=(
                self.log_level if descriptor.log_level is None else descriptor.log_level
            ),
            display_logs=(
                self.display_logs
                if descriptor.display_logs is None
                else descriptor.display_logs
            ),
            write_logs=effective_write_logs,
            timeout_request=self.timeout_request,
        )


    def load_plugins(
        self,
        descriptors: List[PluginDescriptor],
        parallel: bool = True,
    ) -> Dict[str, bool]:

        """
        Load and initialize multiple plugins.
        
        Args:
            descriptors: List of PluginDescriptor objects
            parallel: Whether to load plugins in parallel

        Returns:
            Mapping of plugin IDs to whether each plugin loaded successfully.
        """

        if parallel:
            with ThreadPoolExecutor(
                max_workers=max(1, len(descriptors)),
                thread_name_prefix="plugin-load",
            ) as executor:
                futures = [
                    (descriptor.name, executor.submit(self.load_plugin, descriptor))
                    for descriptor in descriptors
                ]
                return {
                    plugin_id: future.result()
                    for plugin_id, future in futures
                }
        return {
            descriptor.name: self.load_plugin(descriptor)
            for descriptor in descriptors
        }

    def _wait_for_worker_ready(
        self,
        plugin_id: str,
        process: mp.Process,
        initialized: Any,
        startup_phase: Synchronized,
    ) -> bool:
        deadline = None
        last_phase = None
        log_interval = max(self.timeout_start, 0.1)
        next_status_log = time.monotonic() + log_interval

        while process.is_alive():
            if initialized.is_set():
                return True

            phase = PluginStartupPhase(startup_phase.value)
            now = time.monotonic()
            if phase is not last_phase:
                last_phase = phase
                if phase is PluginStartupPhase.WAITING_FOR_DEBUGGER:
                    deadline = (
                        None
                        if self.timeout_debugger_attach is None
                        else now + self.timeout_debugger_attach
                    )
                elif phase is PluginStartupPhase.READY:
                    deadline = None
                elif phase is PluginStartupPhase.FAILED:
                    self.logger.error(
                        "Plugin %s failed during startup",
                        plugin_id,
                    )
                    return False
                else:
                    deadline = now + self.timeout_initialize

            if deadline is not None and now >= deadline:
                if initialized.is_set():
                    return True
                current_phase = PluginStartupPhase(startup_phase.value)
                if current_phase is not phase:
                    continue
                timeout = self.timeout_initialize
                if (
                    phase is PluginStartupPhase.WAITING_FOR_DEBUGGER
                    and self.timeout_debugger_attach is not None
                ):
                    timeout = self.timeout_debugger_attach
                self.logger.error(
                    "Plugin %s timed out during startup phase %s after %.1f seconds",
                    plugin_id,
                    phase.name.lower(),
                    timeout,
                )
                startup_phase.value = int(PluginStartupPhase.FAILED)
                return False

            if now >= next_status_log:
                if phase is PluginStartupPhase.WAITING_FOR_DEBUGGER:
                    self.logger.info(
                        "Plugin %s is waiting for debugger attachment",
                        plugin_id,
                    )
                else:
                    self.logger.info(
                        "Plugin %s is still initializing (phase=%s)",
                        plugin_id,
                        phase.name.lower(),
                    )
                next_status_log = now + log_interval

            wait_timeout = 0.1
            if deadline is not None:
                wait_timeout = min(wait_timeout, max(0.0, deadline - now))
            initialized.wait(timeout=wait_timeout)

        return initialized.is_set()

    def load_plugin(
        self,
        descriptor: PluginDescriptor,
        options: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Load and initialize a plugin (spawn subprocess).
        
        Args:
            descriptor: Plugin descriptor
            options: Initial options for plugin (passed to plugin on start)
        
        Returns:
            True if loaded successfully
        """
        plugin_id = descriptor.name
        merged_options = dict(descriptor.options or {})
        merged_options.update(options or {})
        options = merged_options

        with self._plugins_lock:
            if plugin_id in self._plugins or plugin_id in self._loading_plugins:
                self.logger.error(f"Plugin {plugin_id} is already loaded or loading")
                return False
            self._loading_plugins.add(plugin_id)
        
        plugin_rpc = None
        process = None
        plugin_info = None
        init_error_r = None
        init_error_w = None
        loaded = False
        try:
            runtime_settings = self._plugin_runtime_settings(descriptor)
            plugin_rpc = PluginRPC(plugin_id, timeout=self.timeout_request)
            self._init_plugin_rpc(plugin_rpc)
            worker_channel = plugin_rpc.channel.remote_descriptor()  # For worker to manager communication
            initialized = mp.Event()
            initialized.clear()  # Ensure not set
            stopped_event = mp.Event()
            stopped_event.clear()
            startup_phase = mp.Value("i", PluginStartupPhase.NOT_STARTED)
            init_error_r, init_error_w = mp.Pipe(duplex=False)

            # Spawn worker process
            process = mp.Process(
                target=PluginWorker.run,
                args=(
                    plugin_id,
                    descriptor.to_dict(),
                    options,
                    worker_channel,
                    initialized,
                    runtime_settings,
                    startup_phase,
                    init_error_w,
                    stopped_event,
                ),
                name=f"plugin-{plugin_id}"
            )
            
            
            plugin_info = PluginInfo(
                plugin_id=plugin_id,
                process=process,
                rpc=plugin_rpc,
                descriptor=descriptor,
                options=options,
                startup_phase=startup_phase,
                stopped_event=stopped_event,
            )
            with self._plugins_lock:
                self._plugins[plugin_id] = plugin_info
            
            process.start()
            plugin_rpc.channel.close_remote()
            init_error_w.close()
            init_error_w = None
            is_ready = self._wait_for_worker_ready(
                plugin_id,
                process,
                initialized,
                startup_phase,
            )

            # If the loop exited and is_ready is False, the process died before setting the event
            if not is_ready:
                phase = PluginStartupPhase(startup_phase.value)
                init_error = None
                try:
                    if init_error_r.poll(timeout=0.1):
                        init_error = init_error_r.recv()
                except Exception:
                    init_error = None
                if init_error:
                    self.logger.error(
                        "Plugin %s did not become ready (last phase=%s):\n%s",
                        plugin_id,
                        phase.name.lower(),
                        init_error,
                    )
                else:
                    self.logger.error(
                        "Plugin %s did not become ready (last phase=%s)",
                        plugin_id,
                        phase.name.lower(),
                    )
                return False

            self.logger.info(f"Plugin {plugin_id} loaded")
            loaded = True
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading plugin {descriptor}: {e}")
            return False
        finally:
            for conn in (init_error_w, init_error_r):
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
            if not loaded:
                if process is not None and process.pid is not None:
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=self.timeout_start)
                    if process.is_alive():
                        process.kill()
                        process.join(timeout=self.timeout_start)
                if plugin_rpc is not None:
                    plugin_rpc.shutdown()
                with self._plugins_lock:
                    if self._plugins.get(plugin_id) is plugin_info:
                        del self._plugins[plugin_id]
                self._cleanup_plugin_registrations(plugin_id)
            with self._plugins_lock:
                self._loading_plugins.discard(plugin_id)
    
    def start_plugin(self, plugin_id: str) -> bool:
        """
        Start a loaded plugin and register subscriptions.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            True if started successfully
        """
        with self._plugins_lock:
            info = self._plugins.get(plugin_id)
        if info is None:
            self.logger.error(f"Plugin {plugin_id} not loaded")
            return False
        
        try:
            pending = info.rpc.plugin_start()
            res = pending.wait(timeout=self.timeout_start)
            if not res:
                self.logger.warning(f"Plugin {plugin_id} failed to start within {self.timeout_start} seconds")
                return False
            if res.error:
                self.logger.error(f"Plugin {plugin_id} start error: {res.error}")
                return False

            return res.result
        except Exception as e:
            self.logger.error(f"Error starting plugin {plugin_id}: {e}")
            return False

    def start_plugins(
        self,
        plugin_ids: List[str],
        parallel: bool = True,
    ) -> Dict[str, bool]:
        """
        Start multiple loaded plugins.

        Args:
            plugin_ids: Identifiers of the plugins to start
            parallel: Whether to start plugins in parallel

        Returns:
            Mapping of plugin IDs to whether each plugin started successfully.
        """
        if parallel:
            with ThreadPoolExecutor(
                max_workers=max(1, len(plugin_ids)),
                thread_name_prefix="plugin-start",
            ) as executor:
                futures = [
                    (plugin_id, executor.submit(self.start_plugin, plugin_id))
                    for plugin_id in plugin_ids
                ]
                return {
                    plugin_id: future.result()
                    for plugin_id, future in futures
                }
        return {
            plugin_id: self.start_plugin(plugin_id)
            for plugin_id in plugin_ids
        }
        
    
    def _wait_for_graceful_stop(self, info: PluginInfo, timeout: float) -> bool:
        stopped_event = info.stopped_event
        if stopped_event is None:
            return False
        if stopped_event.is_set():
            return True
        deadline = time.monotonic() + max(0.0, timeout)
        while info.process.is_alive():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if stopped_event.wait(timeout=min(0.05, remaining)):
                return True
        return stopped_event.is_set()

    def stop_plugin(self, plugin_id: str) -> bool:
        """
        Stop a running plugin.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            True if stopped successfully
        """
        with self._plugins_lock:
            info = self._plugins.get(plugin_id)
        if info is None:
            return False
        
        try:
            if info.stop_confirmed:
                return True

            if info.stopped_event is not None and info.stopped_event.is_set():
                info.stop_confirmed = True
                return True

            if not info.process.is_alive():
                info.stop_confirmed = True
                return True  # Already stopped

            deadline = time.monotonic() + self.timeout_start
            pending = info.rpc.plugin_stop()
            res = pending.wait(timeout=self.timeout_start)
            if not res:
                if info.stopped_event is not None and info.stopped_event.is_set():
                    info.stop_confirmed = True
                    return True
                self.logger.warning(f"Plugin {plugin_id} failed to stop within {self.timeout_start} seconds")
                return False
            if res.error:
                if info.stopped_event is not None:
                    if info.stopped_event.is_set():
                        info.stop_confirmed = True
                        return True
                    if "RPC endpoint is stopping" in str(res.error):
                        remaining = max(0.0, deadline - time.monotonic())
                        if self._wait_for_graceful_stop(info, remaining):
                            info.stop_confirmed = True
                            return True
                self.logger.error(f"Plugin {plugin_id} stop error: {res.error}")
                return False
            
            self.logger.info(f"Plugin {plugin_id} stop: {res.result}")
            if res.result:
                info.stop_confirmed = True
            return res.result
        except Exception as e:
            self.logger.error(f"Error stopping plugin {plugin_id}: {e}")
            return False
    
    def unload_plugin(self, plugin_id: str) -> bool:
        """
        Unload a plugin and terminate its process.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            True if unloaded successfully
        """
        try:
            with self._plugins_lock:
                info = self._plugins.get(plugin_id)

            if info is not None and info.process.is_alive() and not info.stop_confirmed:
                self.stop_plugin(plugin_id)

            with self._plugins_lock:
                info = self._plugins.pop(plugin_id, None)

            if info is not None:
                process = info.process
                info.rpc.shutdown()  # Clean up RPC resources

                process.join(timeout=self.timeout_start)

                if process.is_alive():
                    process.terminate()
                    process.join(timeout=self.timeout_start)

                if process.is_alive():
                    process.kill()
                    process.join(timeout=1.0)

                self._cleanup_plugin_registrations(plugin_id)
            
            self.logger.info(f"Plugin {plugin_id} unloaded")
            return True
            
        except Exception as e:
            self.logger.error(f"Error unloading plugin {plugin_id}: {e}", exc_info=True)
            return False

    def _cleanup_plugin_registrations(self, plugin_id: str) -> None:
        with self._plugins_lock:
            for env, plugins in list(self._env_subscribers.items()):
                if plugin_id in plugins:
                    plugins.remove(plugin_id)
                if not plugins:
                    del self._env_subscribers[env]

            for topic, plugins in list(self._topic_subscribers.items()):
                removed = plugins.pop(plugin_id, None)
                if removed is not None:
                    removed.close()
                if not plugins:
                    del self._topic_subscribers[topic]

            for topic, publisher in list(self._topic_publishers.items()):
                if publisher == plugin_id:
                    del self._topic_publishers[topic]
        
    def start_all_plugins(self, parallel=True) -> Dict[str, bool]:
        """Start all loaded plugins and return each plugin's result."""
        with self._plugins_lock:
            ids = list(self._plugins.keys())
        return self.start_plugins(ids, parallel=parallel)
    
    def stop_all_plugins(self) -> None:
        """Stop all running plugins."""
        with self._plugins_lock:
            plugin_ids = list(self._plugins.keys())
        for plugin_id in plugin_ids:
            self.stop_plugin(plugin_id)

    
    def unload_all_plugins(self) -> None:
        """Shut down the plugin manager and all plugins."""
        self.disable_monitoring()
        release_cuda_ipc()
        self.stop_all_plugins()
        with self._plugins_lock:
            plugin_ids = list(self._plugins.keys())
        for plugin_id in plugin_ids:
            self.unload_plugin(plugin_id)
        self.shutdown_env_dispatcher()
        release_cuda_ipc()
        if getattr(self, "_logging", None) is not None:
            self._logging.close()

    def close(self) -> None:
        """Shut down all plugins and release manager resources."""
        self.unload_all_plugins()

    def __enter__(self) -> "PluginManager":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ========================================================================
    # Utility Methods for Plugin Management
    # ========================================================================

    
     
    def plugin_status(self, plugin_id: str) -> Optional[ProcessStatus]:
        """
        Get status of a plugin.
        
        Args:
            plugin_id: Plugin identifier
        
        Returns:
            ProcessStatus object, or None if not found
        """
        with self._plugins_lock:
            info = self._plugins.get(plugin_id)
        if info is None:
            return None
        process = info.process
        alive = process.is_alive()
        pid = process.pid
        worker_status = None
        
        if alive:
            try:
                pending = info.rpc.plugin_status()
                status_response = pending.wait(timeout=1.0)
                if status_response is None:
                    self.logger.debug(f"[{plugin_id}] No status received")
                elif status_response.error:
                    self.logger.debug(f"[{plugin_id}] Error getting status: {status_response.error}")
                else:
                    worker_status = status_response.result
            except Exception as e:
                self.logger.debug(f"[{plugin_id}] Failed to get status: {e}")
        
        return ProcessStatus(
            plugin_id=plugin_id,
            process_alive=alive,
            process_pid=pid,
            worker_status=worker_status,
            startup_phase=(
                PluginStartupPhase(info.startup_phase.value).name.lower()
                if info.startup_phase is not None
                else None
            ),
        )
    
    def plugin_status_all(self) -> Dict[str, ProcessStatus]:
        """
        Get status of all plugins.
        
        Returns:
            Dictionary of plugin_id -> ProcessStatus
        """
        with self._plugins_lock:
            plugin_ids = tuple(self._plugins.keys())
        return {
            plugin_id: self.plugin_status(plugin_id)
            for plugin_id in plugin_ids
        }
    
    def plugin_list(self) -> List[str]:
        """List all loaded plugin IDs."""
        with self._plugins_lock:
            return list(self._plugins.keys())

    def plugin_call(
        self,
        plugin_id: str,
        method_name: str,
        *args,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> Any:
        """Invoke a method on a loaded plugin via RPC."""
        with self._plugins_lock:
            info = self._plugins.get(plugin_id)
        if info is None:
            raise ValueError(f"Plugin {plugin_id} not found")

        effective_timeout = self.timeout_request if timeout is None else timeout
        pending = info.rpc.plugin_call(plugin_id, method_name, *args, **kwargs)
        response = pending.wait(timeout=effective_timeout)
        if response is None:
            raise TimeoutError(
                f"Timed out calling '{method_name}' on plugin '{plugin_id}' after {effective_timeout}s"
            )
        if response.error:
            raise RuntimeError(
                f"Error calling '{method_name}' on plugin '{plugin_id}': {response.error}"
            )
        return response.result

    def plugin_errors(
        self,
        plugin_id: str,
        timeout: Optional[float] = None,
    ) -> List[str]:
        """Return recorded error messages for a loaded plugin."""
        with self._plugins_lock:
            info = self._plugins.get(plugin_id)
        if info is None or not info.process.is_alive():
            return []

        effective_timeout = self.timeout_request if timeout is None else timeout
        pending = info.rpc.plugin_errors(plugin_id)
        response = pending.wait(timeout=effective_timeout)
        if response is None or response.error:
            return []
        return list(response.result or [])

    def enable_monitoring(
        self,
        sample_interval: float = 1.0,
        report_file: Optional[Union[str, os.PathLike]] = None,
        report_hotkey: Optional[Union[bool, str]] = None,
    ):
        """Enable optional, read-only pipeline monitoring."""
        from parallelazy.plugin_monitor import MonitoringAdapter, PipelineMonitor

        if isinstance(sample_interval, bool) or not isinstance(sample_interval, (int, float)):
            raise TypeError("sample_interval must be a number")
        if sample_interval <= 0:
            raise ValueError("sample_interval must be greater than zero")
        if report_hotkey is not None and not isinstance(report_hotkey, (bool, str)):
            raise TypeError("report_hotkey must be a bool, a one-character string, or None")

        created = False
        with self._monitor_lock:
            if self._monitor is not None:
                monitor = self._monitor
            else:
                monitor = PipelineMonitor(
                    MonitoringAdapter(self),
                    sample_interval=sample_interval,
                    logger=self.logger,
                )
                self._monitor = monitor
                created = True

        if created:
            self._env.begin_monitoring()
        monitor.start()

        if report_file is not None:
            monitor.start_report_file(report_file)
        if report_hotkey is True:
            monitor.start_hotkey("m")
        elif isinstance(report_hotkey, str):
            monitor.start_hotkey(report_hotkey)
        elif report_hotkey is False:
            monitor.stop_hotkey()

        return monitor

    def disable_monitoring(self) -> None:
        """Synchronously stop pipeline monitoring."""
        with self._monitor_lock:
            monitor = self._monitor
        if monitor is not None:
            monitor.stop()
        self._env.end_monitoring()
        with self._monitor_lock:
            self._monitor = None

    def get_monitoring_snapshot(self):
        """Return a point-in-time monitoring snapshot."""
        from parallelazy.plugin_monitor import MonitoringAdapter, PipelineMonitor

        with self._monitor_lock:
            monitor = self._monitor
        if monitor is not None:
            return monitor.get_snapshot()

        monitor = PipelineMonitor(
            MonitoringAdapter(self),
            sample_interval=1.0,
            logger=self.logger,
        )
        return monitor.sample()

    def print_monitoring_report(self) -> None:
        """Print the latest monitoring snapshot without triggering a new sample."""
        from parallelazy.plugin_monitor import MonitoringAdapter, PipelineMonitor

        with self._monitor_lock:
            monitor = self._monitor
        if monitor is not None:
            monitor.print_report()
            return

        monitor = PipelineMonitor(
            MonitoringAdapter(self),
            sample_interval=1.0,
            logger=self.logger,
        )
        monitor.print_report(monitor.sample())

    def enable_report_hotkey(self, key: str = "m") -> bool:
        """Enable a hotkey that prints monitoring reports in an interactive TTY."""
        with self._monitor_lock:
            monitor = self._monitor
        if monitor is None:
            monitor = self.enable_monitoring()
        return monitor.start_hotkey(key)

    def disable_report_hotkey(self) -> None:
        """Disable the report hotkey and restore terminal input state."""
        with self._monitor_lock:
            monitor = self._monitor
        if monitor is not None:
            monitor.stop_hotkey()

    def enable_report_file(self, path: Union[str, os.PathLike]) -> str:
        """Enable writing monitoring reports to a file on each sample."""
        with self._monitor_lock:
            monitor = self._monitor
        if monitor is None:
            monitor = self.enable_monitoring()
        return monitor.start_report_file(path)

    def disable_report_file(self) -> None:
        """Stop updating the monitoring report file."""
        with self._monitor_lock:
            monitor = self._monitor
        if monitor is not None:
            monitor.stop_report_file()
    
    
    
    

    # ========================================================================
    # Environment Variable Management
    # ========================================================================

    def env_set(self, name:str, value:Any) -> bool:
        value = sanitize_for_ipc(value)
        self._env.set(name, value)
        self._env_update_queue.put((name, value))
        return True

    def _dispatch_environment_updates(self) -> None:
        while True:
            update = self._env_update_queue.get()
            if update is self._env_dispatcher_stop:
                return
            name, value = update
            self._notify_env_update(name, value)

    def shutdown_env_dispatcher(self) -> None:
        self._env_update_queue.put(self._env_dispatcher_stop)
        self._env_dispatcher.join(timeout=1.0)

    def _notify_env_update(self, name:str, value:Any=None) -> None:
        with self._plugins_lock:
            subscriber_ids = tuple(self._env_subscribers.get(name, ()))
            rpc_by_plugin = {
                plugin_id: self._plugins[plugin_id].rpc
                for plugin_id in subscriber_ids
                if plugin_id in self._plugins
            }
        for plugin_id, rpc in rpc_by_plugin.items():
            try:
                rpc.env_update(name, value)
            except Exception as e:
                self.logger.debug(
                    "Failed to notify plugin %s about environment variable %s: %s",
                    plugin_id,
                    name,
                    e,
                )

    def env_get(self, name:str=None, default:Any=None) -> Any:
        if name is None:
            return self._env.get_all()
        return self._env.get(name, default)
    

    def env_subscribe(self, plugin_id: str, var_name: str) -> bool:
        with self._plugins_lock:
            if plugin_id not in self._plugins:
                return False

            if var_name not in self._env_subscribers:
                self._env_subscribers[var_name] = []

            if plugin_id not in self._env_subscribers[var_name]:
                self._env_subscribers[var_name].append(plugin_id)

        return True

    def env_unsubscribe(self, plugin_id: str, var_name: str) -> bool:   
        with self._plugins_lock:
            if plugin_id not in self._plugins:
                return False

            if var_name in self._env_subscribers and plugin_id in self._env_subscribers[var_name]:
                self._env_subscribers[var_name].remove(plugin_id)
                if not self._env_subscribers[var_name]:
                    del self._env_subscribers[var_name]
        return True
    
    # ========================================================================
    # Service Management (Optional, for future extension)
    # ========================================================================

    def topic_publish(self, topic: str, data: Any) -> int:
        """
        Publish data to a topic.
        
        Args:
            topic: Topic name
            data: Message data
        
        Returns:
            Number of subscribers that received the message
        """
        data, has_tensor = sanitize_for_ipc_with_info(data)
        with self._plugins_lock:
            subscribers = list(self._topic_subscribers.get(topic, {}).items())

        if not subscribers:
            return 0

        from parallelazy.plugin_worker import _deliver_to_subscribers
        delivered, dead_ids = _deliver_to_subscribers(
            topic, data, subscribers, self.logger, has_tensor=has_tensor
        )

        if dead_ids:
            with self._plugins_lock:
                topic_subs = self._topic_subscribers.get(topic)
                if topic_subs is not None:
                    for sub_id in dead_ids:
                        removed = topic_subs.pop(sub_id, None)
                        if removed is not None:
                            removed.close()
                    if not topic_subs:
                        del self._topic_subscribers[topic]

        return delivered

    def topic_list(self, plugin_id: Optional[str] = None) -> List[str]:
        with self._plugins_lock:
            if plugin_id is None:
                return sorted(
                    set(self._topic_publishers.keys())
                    | set(self._topic_subscribers.keys())
                )
            return sorted(
                topic
                for topic, pub in self._topic_publishers.items()
                if pub == plugin_id
            )

    # ========================================================================
    # PLUGIN Lifecycle
    # ========================================================================



    # ========================================================================
    # RPC Responder: Handle requests from plugins
    # ========================================================================
    
    def _init_plugin_rpc(self, plugin_rpc: PluginRPC) -> None:
        """Initialize RPC callbacks for a master.

        #PLUGIN_START = "plugin_start"
        #PLUGIN_STOP = "plugin_stop"
        #PLUGIN_STATUS = "plugin_status"
        #PLUGIN_STATUS_ALL = "plugin_status_all"
        #PLUGIN_ERRORS = "plugin_errors"
        PLUGIN_CALL = "plugin_call"

        ENV_SET = "env_set"
        ENV_GET = "env_get"
        ENV_GET_ALL = "env_get_all"
        ENV_SUBSCRIBE = "env_subscribe"
        ENV_UNSUBSCRIBE = "env_unsubscribe"
        #ENV_UPDATE = "env_update"

        TOPIC_SUBSCRIBE = "topic_subscribe"
        TOPIC_UNSUBSCRIBE = "topic_unsubscribe"
        TOPIC_PUBLISH = "topic_publish"
        TOPIC_LIST = "topic_list"
        """

        #TODO: Handy but later
        plugin_rpc.on_request(CommandType.PLUGIN_LOAD, self._respond_plugin_load)
        plugin_rpc.on_request(CommandType.PLUGIN_START, self._respond_plugin_start)
        plugin_rpc.on_request(CommandType.PLUGIN_STOP, self._respond_plugin_stop)
        plugin_rpc.on_request(CommandType.PLUGIN_UNLOAD, self._respond_plugin_unload)
        plugin_rpc.on_request(CommandType.PLUGIN_CALL, self._respond_call)  # forward
        plugin_rpc.on_request(CommandType.PLUGIN_STATUS, self._respond_plugin_status)
        plugin_rpc.on_request(CommandType.PLUGIN_STATUS_ALL, self._respond_plugin_status_all)
        plugin_rpc.on_request(CommandType.PLUGIN_ERRORS, self._respond_plugin_errors)

        plugin_rpc.on_request(CommandType.ENV_SET, self._respond_env_set)
        plugin_rpc.on_request(CommandType.ENV_GET, self._respond_env_get)
        plugin_rpc.on_request(CommandType.ENV_GET_ALL, self._respond_env_get_all)
        plugin_rpc.on_request(CommandType.ENV_SUBSCRIBE, self._respond_env_subscribe) 
        plugin_rpc.on_request(CommandType.ENV_UNSUBSCRIBE, self._respond_env_unsubscribe)
        
        plugin_rpc.on_request(CommandType.TOPIC_SUBSCRIBE, self._respond_topic_subscribe)
        plugin_rpc.on_request(CommandType.TOPIC_UNSUBSCRIBE, self._respond_topic_unsubscribe)
        plugin_rpc.on_request(CommandType.TOPIC_ANNOUNCE, self._respond_topic_announce)
        plugin_rpc.on_request(CommandType.TOPIC_LIST, self._respond_topic_list)

    # ========================================================================
    # LIFECYCLE: 
    # ========================================================================

    def _respond_plugin_load(self, rpc: PluginRPC, req: RequestPluginLoad) -> bool:
        if not isinstance(req.descriptor, PluginDescriptor):
            raise TypeError("descriptor must be a PluginDescriptor")
        if not req.descriptor.name:
            raise ValueError("descriptor.name is required")

        options = dict(req.descriptor.options or {})
        options.update(req.options or {})
        return self.load_plugin(req.descriptor, options)

    def _respond_plugin_start(self, rpc: PluginRPC, req: RequestPluginStart) -> bool:
        if not req.plugin_id:
            raise ValueError("plugin_id is required")
        if req.plugin_id == rpc.plugin_id:
            raise ValueError("A plugin cannot start itself through lifecycle RPC")
        return self.start_plugin(req.plugin_id)

    def _respond_plugin_stop(self, rpc: PluginRPC, req: RequestPluginStop) -> bool:
        if not req.plugin_id:
            raise ValueError("plugin_id is required")
        if req.plugin_id == rpc.plugin_id:
            raise ValueError("A plugin cannot stop itself through lifecycle RPC")
        return self.stop_plugin(req.plugin_id)

    def _respond_plugin_unload(self, rpc: PluginRPC, req: RequestPluginUnload) -> bool:
        if not req.plugin_id:
            raise ValueError("plugin_id is required")
        if req.plugin_id == rpc.plugin_id:
            raise ValueError("A plugin cannot unload itself through lifecycle RPC")
        with self._plugins_lock:
            if req.plugin_id not in self._plugins:
                return False
        return self.unload_plugin(req.plugin_id)

    def _respond_plugin_status(self, rpc: PluginRPC, req: RequestPluginStatus) -> Optional[ProcessStatus]:
        """Return the requested plugin status, defaulting to the requesting plugin."""
        return self.plugin_status(req.plugin_id or req.sender)

    def _respond_plugin_status_all(self, rpc: PluginRPC, req: RequestPluginStatusAll) -> Dict[str, ProcessStatus]:
        """Return the status of every loaded plugin."""
        return self.plugin_status_all()

    def _respond_plugin_errors(self, rpc: PluginRPC, req: RequestPluginErrors) -> List[str]:
        """Return recorded error messages for the requested plugin."""
        return self.plugin_errors(req.plugin_id or req.sender)


    # ========================================================================

    def _respond_call(self, rpc: PluginRPC, req: RequestPluginCall) -> Any:
        with self._plugins_lock:
            info = self._plugins.get(req.plugin_id)
        if info is None:
            raise ValueError(f"Plugin {req.plugin_id} not found")
        
        remote_result = info.rpc.forward(req)
        return remote_result
    
    # ========================================================================
    # ENVIRONMENT: Variables and Subscriptions
    # ========================================================================
    
    def _respond_env_set(self, rpc: PluginRPC, req: RequestEnvSet) -> None:
        return self.env_set(req.name, req.value)
    
    def _respond_env_get(self, rpc: PluginRPC, req: RequestEnvGet) -> Any:
        return self._env.get(req.name, ENV_MISSING)
    
    def _respond_env_get_all(self, rpc: PluginRPC, req: RequestEnvGetAll) -> Dict[str, Any]:
        return self.env_get()
    
    def _respond_env_subscribe(self, rpc: PluginRPC, req: RequestEnvSubscribe) -> bool:
        return self.env_subscribe(req.sender, req.name)

    def _respond_env_unsubscribe(self, rpc: PluginRPC, req: RequestEnvUnsubscribe) -> bool:
        return self.env_unsubscribe(req.sender, req.name)

    # ========================================================================
    # TOPIC PUB/SUB
    # ========================================================================
    

    def _respond_topic_subscribe(self, rpc: PluginRPC, req: RequestTopicSubscribe) -> bool:
        """
        Subscribe a plugin to a topic.
        Allow subscription also for non (yet, hopefully) existing topics.
        """
        sub = req.subscriber
        topic = sub.topic
        plugin_sub = req.sender
        forward_rpc = None

        with self._plugins_lock:
            if plugin_sub not in self._plugins:
                sub.close()
                return False

            plugin_pub = self._topic_publishers.get(topic, None)
            if plugin_pub is not None:
                pub_info = self._plugins.get(plugin_pub)
                if pub_info is None:
                    sub.close()
                    return False
                forward_rpc = pub_info.rpc

            if topic not in self._topic_subscribers:
                self._topic_subscribers[topic] = {}

            previous = self._topic_subscribers[topic].get(plugin_sub)
            if previous is not None and previous is not sub:
                previous.close()
            self._topic_subscribers[topic][plugin_sub] = sub

        remote_result = True  # Default to success

        if forward_rpc is not None:
            try:
                remote_result = forward_rpc.forward(req)
                self.logger.debug(f"topic_subscribe: {remote_result}")
            except Exception as e:
                self.logger.error(
                    "Failed to forward topic_subscribe for '%s' to publisher '%s': %s",
                    topic,
                    plugin_pub,
                    e,
                )
                remote_result = False

        if not remote_result:
            with self._plugins_lock:
                subscribers = self._topic_subscribers.get(topic)
                if subscribers is not None:
                    removed = subscribers.pop(plugin_sub, None)
                    if removed is not None:
                        removed.close()
                    if not subscribers:
                        del self._topic_subscribers[topic]
            return False

        return True


    def _respond_topic_unsubscribe(self, rpc: PluginRPC, req: RequestTopicUnsubscribe) -> bool:
        """
        Unsubscribe a plugin from a topic.
        """
        sub = req.subscriber
        topic = sub.topic
        src_id = sub.plugin_id
        forward_rpc = None

        with self._plugins_lock:
            if src_id not in self._plugins:
                return False

            subscribers = self._topic_subscribers.get(topic)
            if subscribers is not None:
                removed = subscribers.pop(src_id, None)
                if removed is not None:
                    removed.close()
                if not subscribers:
                    del self._topic_subscribers[topic]

            dst_id = self._topic_publishers.get(topic)
            if dst_id is not None and dst_id in self._plugins:
                forward_rpc = self._plugins[dst_id].rpc

        if forward_rpc is not None:
            try:
                remote_result = forward_rpc.forward(req)
                return bool(remote_result)
            except Exception as e:
                self.logger.warning(
                    "Failed to forward topic_unsubscribe for '%s' to publisher: %s",
                    topic,
                    e,
                )

        return True

    def _respond_topic_announce(self, rpc: PluginRPC, req: RequestTopicAnnounce) -> Dict[str, Subscriber]:
        """
        Announce a new topic.
        """
        with self._plugins_lock:
            self._topic_publishers[req.topic] = req.sender

            if req.topic not in self._topic_subscribers:
                self._topic_subscribers[req.topic] = {}
            return dict(self._topic_subscribers[req.topic])

    def _respond_topic_list(self, rpc: PluginRPC, req: RequestTopicList) -> List[str]:
        return self.topic_list(plugin_id=req.plugin_id)