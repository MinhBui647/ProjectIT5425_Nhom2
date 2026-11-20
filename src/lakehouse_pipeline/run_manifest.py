"""Versioned durable run evidence; config is an explicit non-secret allowlist."""
import hashlib
import importlib.metadata
import json
import re
import subprocess
from pathlib import Path
from lakehouse_storage.mock_server.store import canonical_json
from lakehouse_storage.service_config import require_local_tree, require_namespace
from lakehouse_storage.worker_state import atomic_json

CONFIG_KEYS = {'sources', 'optional_sources', 'window', 'mode', 'request', 'silver_root', 'root', 'expected_recalls', 'farms'}


def fingerprint(config):
    if set(config) - CONFIG_KEYS:
        raise ValueError('config contains unsupported or secret fields')
    def check(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if re.search(r'secret|password|token|credential|access.key', key, re.I):
                    raise ValueError('secret fields forbidden in run config')
                check(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                check(item)
    check(config)
    return hashlib.sha256(canonical_json(config)).hexdigest()


def environment():
    try:
        commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        import os
        commit = os.getenv('LAKEHOUSE_CODE_REVISION', 'unknown')
    versions = {}
    for package in ('polars', 'pyarrow', 'deltalake', 'great-expectations'):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = 'unavailable'
    import os
    return {'git_commit': commit, 'image': os.getenv('LAKEHOUSE_IMAGE_ID', 'unknown'), 'dependencies': versions}


class ManifestStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.directory = require_local_tree(self.root / 'runs', self.root)

    def path(self, identity):
        return self.directory / (hashlib.sha256(identity.encode()).hexdigest() + '.json')

    def prepare(self, identity, config):
        digest = fingerprint(config)
        path = self.path(identity)
        if path.exists():
            existing = self.load(path)
            if existing['config_digest'] != digest or existing['identity'] != identity:
                raise ValueError('run config conflict')
            return existing
        value = {'schema_version': 1, 'identity': identity, 'config': config, 'config_digest': digest,
                 'environment': environment(), 'status': 'PREPARING', 'stages': {}, 'artifacts': [], 'silver_snapshot': {}}
        self.save(path, value)
        return value

    def load(self, path):
        path = Path(require_namespace(path, self.directory))
        try:
            value = json.loads(path.read_text())
            if (type(value['schema_version']) is not int or value['schema_version'] != 1 or value['config_digest'] != fingerprint(value['config'])
                    or self.path(value['identity']) != path or value['status'] not in {'PREPARING', 'RUNNING', 'SUCCESS', 'FAILED', 'NEEDS_REVIEW'}
                    or not isinstance(value['stages'], dict) or not isinstance(value['artifacts'], list)):
                raise ValueError('invalid run manifest')
            return value
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError('malformed run manifest') from exc

    def save(self, path, value):
        require_local_tree(self.directory, self.root)
        require_namespace(path, self.directory)
        atomic_json(path, value)
