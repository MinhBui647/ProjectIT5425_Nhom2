"""Maintenance preserves queries and pre-maintenance time travel."""
import polars as pl
from lakehouse_storage.maintenance_runner import MaintenanceRunner
from test_maintenance_runner import fixture_policy


def test_compaction_zorder_history_invariants(tmp_path):
    policy = fixture_policy(tmp_path, cooldown_seconds=0, zorder_columns=('id',))
    runner = MaintenanceRunner(tmp_path / 'history.jsonl')
    baseline = pl.read_delta(policy.table_uri).sort('id')
    for action in ('compact', 'zorder', 'vacuum_preview'):
        record = runner.run(policy, action, enabled=True, ingestion_stopped=True)
        assert record['decision'] == 'RUN', record
        assert baseline.equals(pl.read_delta(policy.table_uri).sort('id'))
        assert baseline.equals(pl.read_delta(policy.table_uri, version=3).sort('id'))
