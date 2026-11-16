"""Opt-in S3 integration; never uses the shared factory bucket."""
import os
import uuid

import polars as pl
import pytest
import requests

from lakehouse_storage import config as core_config
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.maintenance_policy import MaintenancePolicy
from lakehouse_storage.maintenance_runner import MaintenanceRunner
from lakehouse_storage.mock_client import MockClient
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.time_travel import get_version_at_date, trace_batch
from storage_demo_helpers import mock_http

pytestmark = [pytest.mark.minio, pytest.mark.skipif(os.getenv('STORAGE_DEMO_MINIO_TEST') != '1', reason='set STORAGE_DEMO_MINIO_TEST=1 and start isolated demo MinIO')]


@pytest.fixture
def s3_demo(tmp_path, monkeypatch):
    endpoint = os.getenv('STORAGE_DEMO_TEST_ENDPOINT', '127.0.0.1:19000')
    monkeypatch.setattr(core_config, 'MINIO_ENDPOINT', endpoint)
    monkeypatch.setattr(core_config, 'MINIO_ACCESS_KEY', 'storagedemo')
    monkeypatch.setattr(core_config, 'MINIO_SECRET_KEY', os.getenv('STORAGE_DEMO_MINIO_PASSWORD', 'storage-demo-local-only'))
    monkeypatch.setattr(core_config, 'MINIO_SECURE', False)
    root = tmp_path / 'storage_demo'
    remote = 's3://smart-dairy-storage-demo/storage_demo/tests/' + uuid.uuid4().hex
    return root, remote


def test_s3_normal_replay_resume_and_maintenance(s3_demo):
    from deltalake import DeltaTable
    from datetime import datetime, timezone
    root, remote = s3_demo
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'samples_per_batch': 10, 'batches': 3}, timeout=5).json()
        worker = IngestionWorker(ServiceConfig(root, remote, 's3://smart-dairy-storage-demo/storage_demo', url), MockClient(url))
        records = worker.run(run['run_id'])
        assert all(r['status'] == 'DONE' for r in records), records
        uri = remote + '/sensor_telemetry'
        before = pl.read_delta(uri, storage_options=core_config.get_storage_options()).sort('timestamp')
        assert before.height == 30
        assert pl.read_delta(remote + '/mes_lims', storage_options=core_config.get_storage_options()).height == 1
        replay = worker.process(run['run_id'], 1, replay=True)
        assert replay['p3_result']['status'] == 'SKIPPED_DUPLICATE'
        state = worker.state.load(run['run_id'], 2)
        state['status'] = 'PROCESSING'; worker.state.save(state)
        resumed = worker.process(run['run_id'], 2)
        assert resumed['bronze_path'] == state['bronze_path']
        assert resumed['p3_result']['status'] == 'SKIPPED_DUPLICATE'
        trace = trace_batch(uri, before['batch_id'][0])
        assert trace['integrity_verified'] and len(trace['lineage_sources']) == 3
        policy = MaintenancePolicy(uri, 's3://smart-dairy-storage-demo/storage_demo', file_count_threshold=2)
        runner = MaintenanceRunner(root / 'maintenance.jsonl')
        compact = runner.run(policy, enabled=True, ingestion_stopped=True)
        assert compact['decision'] == 'RUN', compact
        assert before.equals(pl.read_delta(uri, storage_options=core_config.get_storage_options()).sort('timestamp'))
        dt = DeltaTable(uri, storage_options=core_config.get_storage_options())
        last = max(e['timestamp'] for e in dt.history())
        target = datetime.fromtimestamp(last/1000, tz=timezone.utc).isoformat()
        assert get_version_at_date(uri, target) == dt.version()
        assert runner.run(policy, 'vacuum_preview', enabled=True, ingestion_stopped=True)['result']['dry_run']


@pytest.mark.parametrize('scenario', ['invalid-value', 'invalid-time', 'missing-field', 'transient-500', 'delay'])
def test_s3_faults(s3_demo, scenario):
    root, remote = s3_demo
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'scenario':scenario, 'samples_per_batch':10, 'batches':1, 'failure_attempts':2, 'delay_seconds':0.1}, timeout=5).json()
        client = MockClient(url, timeout=0.02 if scenario=='delay' else 5, max_attempts=3, backoff=0.01)
        worker = IngestionWorker(ServiceConfig(root, remote, 's3://smart-dairy-storage-demo/storage_demo', url), client)
        if scenario == 'delay':
            with pytest.raises(requests.Timeout): worker.process(run['run_id'], 1)
        else:
            record = worker.process(run['run_id'], 1)
            assert record['status'] == ('DONE' if scenario == 'transient-500' else 'REJECTED'), record
