"""Real subprocess HTTP service used by demo integration tests."""
import os
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager

import requests


@contextmanager
def mock_http(root):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    url = f'http://127.0.0.1:{port}'
    env = {**os.environ, 'PYTHONPATH': 'src', 'STORAGE_DEMO_ROOT': str(root), 'MOCK_PORT': str(port)}
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen([sys.executable, '-m', 'lakehouse_storage.mock_server'], env=env, stdout=log, stderr=log)
        try:
            for _ in range(200):
                if process.poll() is not None:
                    log.seek(0)
                    raise RuntimeError(log.read().decode())
                try:
                    if requests.get(url + '/health', timeout=0.2).status_code == 200:
                        break
                except requests.RequestException:
                    pass
                time.sleep(0.05)
            else:
                raise TimeoutError('mock did not become ready')
            yield url, process
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=15)
