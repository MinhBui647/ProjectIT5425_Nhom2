"""Service-only environment defaults and demo namespace checks."""
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


def _s3_parts(uri):
    # Demo URIs use literal keys. Reject encoding rather than let the backend
    # decode/normalize a different path after the namespace check.
    if any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in uri) or '%' in uri or '\\' in uri:
        raise ValueError('ambiguous demo S3 URI')
    parsed = urlsplit(uri)
    if (parsed.scheme != 's3' or not parsed.netloc or parsed.query or parsed.fragment
            or '@' in parsed.netloc or ':' in parsed.netloc or '?' in uri or '#' in uri):
        raise ValueError('invalid demo S3 URI')
    parts = parsed.path.removeprefix('/').removesuffix('/').split('/') if parsed.path.strip('/') else []
    if any(p in {'', '.', '..'} for p in parts) or '//' in parsed.path:
        raise ValueError('relative or empty URI segments forbidden')
    return parsed.netloc, parts


def require_namespace(target, root):
    """Allow only descendants of an explicitly isolated demo/benchmark root."""
    target, root = str(target), str(root)
    if root.startswith('s3://'):
        bucket, root_parts = _s3_parts(root)
        target_bucket, target_parts = _s3_parts(target)
        if not any(p in {'storage_demo', 'storage_benchmark', 'smart-dairy-storage-demo'} for p in [bucket, *root_parts]):
            raise ValueError('root must identify a demo namespace')
        if (target_bucket != bucket or len(target_parts) <= len(root_parts)
                or target_parts[:len(root_parts)] != root_parts):
            raise ValueError('target outside demo namespace')
    else:
        allowed = Path(root).resolve()
        path = Path(target).resolve()
        if not any(p in {'storage_demo', 'storage_benchmark'} for p in allowed.parts):
            raise ValueError('root must identify a demo namespace')
        if path == allowed or not path.is_relative_to(allowed):
            raise ValueError('target outside demo namespace')
    return target


def require_local_tree(target, root):
    path = Path(require_namespace(target, root)).resolve()
    if path.exists() and any(entry.is_symlink() for entry in path.rglob('*')):
        raise ValueError('demo output tree must not contain descendant symlinks')
    return path


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
