
import os
import re
import socket
import logging
import torch.multiprocessing as mp
import warnings

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

# Notice: 'port: int' is completely gone from the arguments
def init_debugger(process_name: str = 'worker', wait_for_client: bool = True) -> None:
    try:
        import debugpy
        
        # Passing 0 tells the OS to assign a random free port.
        # debugpy returns the actual port it bound to.
        _, assigned_port = debugpy.listen(("localhost", 0))
        
        logger.info(f"Debugger listening on port {assigned_port}")
        print(f"Debugger listening on port {assigned_port} for {process_name} process", flush=True) 
        
        # Ping the extension silently bypassing stdout
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
    # Find transitions from lowercase/number to uppercase
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', text)
    # Find transitions from lowercase to uppercase/number
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1)
    return s2.lower()

def camel_to_snake(text):
    # Find transitions from lowercase/number to uppercase
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    # Find transitions from lowercase to uppercase/number
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()
