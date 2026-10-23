import os
import re
import socket
import logging
import torch.multiprocessing as mp
import warnings
import sys

logger = logging.getLogger(__name__)

def _notify_vscode_to_attach(port: int, process_name: str) -> None:
    ext_port_str = os.environ.get('_DEBUG_SPAWN_EXT_PORT')
    if not ext_port_str:
        return
        
    try:
        ext_port = int(ext_port_str)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1.0)
            s.connect(('127.0.0.1', ext_port))
            s.sendall(f"{port}:{process_name}".encode('utf-8'))
    except Exception as e:
        logger.warning(f"Failed to notify VS Code extension on port {ext_port_str}: {e}")

def get_debugpy():
    if "debugpy" in sys.modules:
        return sys.modules["debugpy"]
    else:
        try:
            import debugpy
            return debugpy
        except ImportError:
            return None

def is_debugpy_attached() -> bool:
    debugpy = get_debugpy()
    if debugpy:
        return debugpy.is_client_connected()
    return False

def get_debugpy_path():
    debugpy = get_debugpy()
    if debugpy and hasattr(debugpy, "__file__"):
        # Returns the 'libs' folder containing the IDE's debugpy
        return os.path.dirname(os.path.dirname(debugpy.__file__))
    return None

def setup_worker_debugging_env() -> None:
    """Call this in the main process before spawning workers to export the debugger path."""
    if is_debugpy_attached():
        debug_path = get_debugpy_path()
        if debug_path:
            os.environ["_VSCODE_DEBUGPY_PATH"] = debug_path

def is_debugging_enabled() -> bool:
    """Check if debugging is enabled for the current process."""
    return "_VSCODE_DEBUGPY_PATH" in os.environ

def init_debugger(process_name: str = 'worker', wait_for_client: bool = True) -> None:
    """Call this at the very beginning of your spawned worker's execution."""
    # 1. Inject the IDE's debugpy path into sys.path before importing
    debug_path = os.environ.get("_VSCODE_DEBUGPY_PATH")
    if debug_path and debug_path not in sys.path:
        sys.path.insert(0, debug_path)

    # 2. Proceed with standard debugger initialization
    try:
        debugpy = get_debugpy()
        if debugpy is None:
            logger.warning("debugpy is not installed. Debugging will not be available.")
            return
        
        _, assigned_port = debugpy.listen(("localhost", 0))
        
        logger.info(f"Debugger listening on port {assigned_port}")
        print(f"Debugger listening on port {assigned_port} for {process_name} process", flush=True) 
        
        _notify_vscode_to_attach(assigned_port, process_name)

        if wait_for_client:
            logger.info("Waiting for debugger to attach...")
            debugpy.wait_for_client()
            logger.info("Debugger attached")
            
    except Exception as e:
        logger.warning(f"Failed to initialize debugger: {e}")

def process_start_spawn(force=False):
    current = mp.get_start_method(allow_none=True)
    if current not in ('spawn', None):
        warnings.warn(f"Expected spawn start method, got '{current}'. CUDA may be unreliable.")
    elif current is None:
        mp.set_start_method('spawn', force=force)

def camel_to_kebab(text):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', text)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1)
    return s2.lower()

def camel_to_snake(text):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()