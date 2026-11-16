"""Service-only environment defaults and demo namespace checks."""
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


def require_namespace(target, root):
    """Allow only descendants of an explicitly isolated demo/benchmark root."""
    target, root = str(target), str(root)
    if root.startswith('s3://'):
        parsed = urlsplit(root)
        if parsed.scheme != 's3' or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError('invalid demo S3 root')
        parts = [parsed.netloc, *parsed.path.split('/')]
        if not any(p in {'storage_demo', 'storage_benchmark', 'smart-dairy-storage-demo'} for p in parts):
            raise ValueError('root must identify a demo namespace')
        if any(p in {'.', '..'} for p in urlsplit(target).path.split('/')):
            raise ValueError('relative URI segments forbidden')
        if not target.startswith(root.rstrip('/') + '/'):
            raise ValueError('target outside demo namespace')
    else:
        allowed = Path(root).resolve()
        path = Path(target).resolve()
        if not any(p in {'storage_demo', 'storage_benchmark'} for p in allowed.parts):
            raise ValueError('root must identify a demo namespace')
        if path == allowed or not path.is_relative_to(allowed):
            raise ValueError('target outside demo namespace')
    return target


@dataclass(frozen=True)
class ServiceConfig:
    root: Path
    silver_root: str
    silver_allow_root: str
    api_url: str

    @classmethod
    def from_env(cls):
        root = Path(os.getenv('STORAGE_DEMO_ROOT', 'data/storage_demo')).resolve()
        silver = os.getenv('STORAGE_DEMO_SILVER_ROOT', str(root / 'silver'))
        allowed = os.getenv('STORAGE_DEMO_SILVER_ALLOW_ROOT', str(root))
        require_namespace(silver, allowed)
        return cls(root, silver, allowed, os.getenv('MOCK_URL', 'http://127.0.0.1:8010'))
