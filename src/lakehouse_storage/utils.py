"""Shared utilities for the lakehouse storage engine."""

import hashlib
from pathlib import Path

_CHUNK_SIZE = 1 << 20


def sha256_file(path: str | Path) -> str:
    """Return the hex SHA-256 digest of a file."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()
