"""Integrity must cover every source, including incomplete lineage."""
from datetime import datetime, timezone

import polars as pl
import pytest
from deltalake import write_deltalake

from lakehouse_storage.config import get_storage_options
from lakehouse_storage.time_travel import trace_batch
from lakehouse_storage.utils import sha256_file


def table_with_sources(tmp_path, sources):
    uri = str(tmp_path / 'silver')
    data = pl.DataFrame({
        'batch_id': ['B1'] * len(sources),
        '_bronze_file': [p for p, h in sources],
        '_bronze_sha256': [h for p, h in sources],
        '_ingested_at': [datetime.now(timezone.utc)] * len(sources),
    })
    write_deltalake(uri, data.to_arrow(), storage_options=get_storage_options())
    return uri


def test_every_source_hashed_once(tmp_path, monkeypatch):
    from lakehouse_storage import time_travel
    a, b = tmp_path / 'a.parquet', tmp_path / 'b.parquet'
    a.write_bytes(b'a'); b.write_bytes(b'b')
    sources = [(str(a), sha256_file(a)), (str(b), sha256_file(b))]
    uri = table_with_sources(tmp_path, sources * 2)
    calls = []
    def checksum(path):
        calls.append(str(path))
        return sha256_file(path)
    monkeypatch.setattr(time_travel, 'sha256_file', checksum)
    result = trace_batch(uri, 'B1')
    assert result['integrity_verified'] is True
    assert len(result['lineage_sources']) == 2
    assert calls == [str(a), str(b)]
    assert result['lineage']['bronze_file'] == str(a)


@pytest.mark.parametrize('fault', ['corrupt', 'missing', 'null_hash', 'conflict', 'null_path', 'unreadable'])
def test_bad_second_source_preserves_first(tmp_path, monkeypatch, fault):
    from lakehouse_storage import time_travel
    a, b = tmp_path / 'a.parquet', tmp_path / 'b.parquet'
    a.write_bytes(b'a'); b.write_bytes(b'b')
    sources = [(str(a), sha256_file(a)), (str(b), sha256_file(b))]
    if fault == 'corrupt': b.write_bytes(b'tampered')
    if fault == 'missing': b.unlink()
    if fault == 'null_hash': sources[1] = (str(b), None)
    if fault == 'null_path': sources[1] = (None, sources[1][1])
    if fault == 'conflict': sources.append((str(b), 'f' * 64))
    if fault == 'unreadable':
        def checksum(path):
            if str(path) == str(b): raise PermissionError('denied')
            return sha256_file(path)
        monkeypatch.setattr(time_travel, 'sha256_file', checksum)
    uri = table_with_sources(tmp_path, sources)
    result = trace_batch(uri, 'B1')
    assert result['integrity_verified'] is False
    assert result['lineage_sources'][0]['integrity_verified'] is True
    assert all(s['reason'] for s in result['lineage_sources'][1:])
