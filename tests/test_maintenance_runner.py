"""A real Delta layout is optimized only during an explicit stopped-ingest phase."""
from datetime import datetime, timezone

import polars as pl
import pytest
from deltalake import DeltaTable, write_deltalake
from lakehouse_storage.config import get_storage_options
from lakehouse_storage.maintenance_policy import MaintenancePolicy
from lakehouse_storage.maintenance_runner import MaintenanceRunner


def fixture_policy(tmp_path, **changes):
    root = tmp_path / 'storage_demo'
    uri = str(root / 'silver' / 'test')
    for i in range(4):
        write_deltalake(uri, pl.DataFrame({'id': [i], 'line_id': ['L1']}).to_arrow(), mode='append', partition_by=['line_id'], storage_options=get_storage_options())
    return MaintenancePolicy(uri, str(root), file_count_threshold=2, **changes)


def test_explicit_phase_and_real_compaction(tmp_path):
    policy = fixture_policy(tmp_path)
    runner = MaintenanceRunner(tmp_path / 'storage_demo' / 'history.jsonl')
    assert runner.run(policy)['decision'] == 'SKIP'
    assert runner.run(policy, enabled=True)['decision'] == 'SKIP'
    before = pl.read_delta(policy.table_uri).sort('id')
    result = runner.run(policy, enabled=True, ingestion_stopped=True)
    assert result['decision'] == 'RUN'
    assert result['before']['file_count'] == 4
    assert result['after']['file_count'] < 4
    assert before.equals(pl.read_delta(policy.table_uri).sort('id'))
    assert runner.run(policy, enabled=True, ingestion_stopped=True)['reason'] == 'cooldown'


def test_vacuum_preview_never_deletes(tmp_path):
    policy = fixture_policy(tmp_path)
    runner = MaintenanceRunner(tmp_path / 'storage_demo' / 'history.jsonl')
    before = set((tmp_path / 'storage_demo').rglob('*.parquet'))
    result = runner.run(policy, action='vacuum_preview', enabled=True, ingestion_stopped=True)
    assert result['decision'] == 'RUN'
    assert result['result']['dry_run'] is True
    assert before == set((tmp_path / 'storage_demo').rglob('*.parquet'))


def test_zorder_rejects_partition_or_unknown_columns(tmp_path):
    for columns in [('line_id',), ('no_such_column',)]:
        policy = fixture_policy(tmp_path / columns[0], zorder_columns=columns)
        runner = MaintenanceRunner(tmp_path / 'storage_demo' / 'history.jsonl')
        assert runner.run(policy, action='zorder', enabled=True, ingestion_stopped=True)['decision'] == 'FAIL'


def test_compact_forwards_target_size(monkeypatch):
    from lakehouse_storage import maintenance
    class Optimize:
        def compact(self, **kwargs):
            assert kwargs['target_size'] == 7 * 1024 * 1024
    class Table:
        optimize = Optimize()
        def file_uris(self): return []
    monkeypatch.setattr(maintenance, '_get_delta_table', lambda _: Table())
    monkeypatch.setattr(maintenance, '_table_total_bytes', lambda _: 0)
    maintenance.compact_table('unused', target_file_size_mb=7)
