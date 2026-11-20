"""Restart restores cooldown and never calls incomplete work successful."""
from datetime import datetime, timedelta, timezone

from lakehouse_storage.maintenance_history import MaintenanceHistory
from lakehouse_storage.maintenance_runner import MaintenanceRunner
from test_maintenance_runner import fixture_policy


def test_unfinished_operation_unknown_and_cooldown(tmp_path):
    policy = fixture_policy(tmp_path)
    path = tmp_path / 'history.jsonl'
    now = datetime.now(timezone.utc)
    MaintenanceHistory(path).append({'run_id':'unfinished', 'table':policy.table_uri, 'action':'compact', 'decision':'STARTED', 'started_epoch':now.timestamp()})
    runner = MaintenanceRunner(path)
    assert runner.history.records()[0]['decision'] == 'UNKNOWN'
    assert runner.run(policy, enabled=True, ingestion_stopped=True, now=now)['reason'] == 'cooldown'


def test_window_and_persisted_metrics(tmp_path):
    policy = fixture_policy(tmp_path, window_start_hour=2, window_end_hour=3)
    path = tmp_path / 'history.jsonl'
    runner = MaintenanceRunner(path)
    assert runner.run(policy, enabled=True, ingestion_stopped=True, now=datetime(2026,1,1,1,tzinfo=timezone.utc))['decision'] == 'SKIP'
    result = runner.run(policy, enabled=True, ingestion_stopped=True, now=datetime(2026,1,1,2,tzinfo=timezone.utc))
    assert result['decision'] == 'RUN'
    restored = MaintenanceHistory(path).records()[-1]
    assert restored['after']['active_bytes'] <= restored['after']['retained_physical_bytes']
    assert restored['duration_seconds'] > 0
    assert restored['ended_at'] and restored['before']['version'] < restored['after']['version']
