"""All sources to real MinIO, pinned historical reads and replay evidence."""
import os
import uuid
import polars as pl
import pytest
from lakehouse_storage import config as core_config
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.maintenance_policy import MaintenancePolicy
from lakehouse_storage.maintenance_runner import MaintenanceRunner
from lakehouse_pipeline.pipeline import prepare_run, run_pipeline
from lakehouse_pipeline.silver_snapshot import read_snapshot
from lakehouse_pipeline.run_manifest import ManifestStore
from test_lakehouse_pipeline_e2e import HttpClient
from test_mock_manifest import FULL

pytestmark = [pytest.mark.minio, pytest.mark.skipif(os.getenv('STORAGE_DEMO_MINIO_TEST') != '1', reason='isolated real MinIO required')]


def test_full_minio_versions_replay_compaction_and_cooldown(tmp_path, monkeypatch):
    monkeypatch.setattr(core_config, 'MINIO_ENDPOINT', os.getenv('STORAGE_DEMO_TEST_ENDPOINT', '127.0.0.1:29000'))
    monkeypatch.setattr(core_config, 'MINIO_ACCESS_KEY', 'storagedemo')
    monkeypatch.setattr(core_config, 'MINIO_SECRET_KEY', os.getenv('STORAGE_DEMO_MINIO_PASSWORD', 'storage-demo-local-only'))
    monkeypatch.setattr(core_config, 'MINIO_SECURE', False)
    root = tmp_path / 'storage_demo'
    remote = 's3://smart-dairy-storage-demo/storage_demo/workflow-tests/' + uuid.uuid4().hex
    config = ServiceConfig(root, remote + '/silver', remote, 'http://test')
    client = HttpClient(root / 'mock')
    path = prepare_run(config, 'minio/full', {'sources': FULL, 'samples_per_batch': 10, 'batches': 3, 'fao_variant': 'both'}, client=client)
    result = run_pipeline(config, path, client=client)
    snapshot = result['silver_snapshot']
    assert len(snapshot) == 6
    before = read_snapshot(snapshot, 'sensor_telemetry').sort('timestamp')
    assert before.height == 30
    assert run_pipeline(config, path, client=client)['silver_snapshot'] == snapshot
    policy = MaintenancePolicy(snapshot['sensor_telemetry']['uri'], remote, file_count_threshold=2)
    history = root / 'maintenance.jsonl'
    runner = MaintenanceRunner(history)
    compact = runner.run(policy, enabled=True, ingestion_stopped=True)
    assert compact['decision'] == 'RUN', compact
    assert read_snapshot(snapshot, 'sensor_telemetry').sort('timestamp').equals(before)
    assert pl.read_delta(policy.table_uri, storage_options=core_config.get_storage_options()).sort('timestamp').equals(before)
    assert MaintenanceRunner(history).run(policy, enabled=True, ingestion_stopped=True)['reason'] == 'cooldown'
    preview = runner.run(policy, 'vacuum_preview', enabled=True, ingestion_stopped=True)
    assert preview['result']['dry_run'] is True
    evidence = {'remote': remote, 'snapshot': snapshot, 'compaction': compact, 'vacuum_preview': preview}
    from lakehouse_storage.worker_state import atomic_json
    atomic_json(root / 'minio_evidence.json', evidence)
