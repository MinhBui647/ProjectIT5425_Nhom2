"""HTTP payload validation and publication preserve the raw contract."""
import json
import polars as pl
import pytest
from fastapi.testclient import TestClient
from lakehouse_storage.mock_server.app import create_app
from lakehouse_storage.mock_client import decode_batch, PayloadError
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.worker_state import NeedsReview


class LocalClient:
    def __init__(self, root, **request):
        self.http = TestClient(create_app(root / 'mock'))
        self.run = self.http.post('/runs', json={'samples_per_batch': 10, 'batches': 1, **request}).json()
    def batch(self, run_id, sequence):
        return decode_batch(self.http.get(f'/runs/{run_id}/batches/{sequence}').content, run_id, sequence)


def config(root):
    return ServiceConfig(root, str(root / 'silver'), str(root), 'http://unused')


def test_publish_then_reuse_known_file(tmp_path):
    root = tmp_path / 'storage_demo'
    client = LocalClient(root)
    worker = IngestionWorker(config(root), client)
    record = worker.publish(client.run['run_id'], 1)
    assert record['status'] == 'BRONZE_READY'
    data = pl.read_parquet(record['bronze_path'])
    assert data.schema['uht_temp'] == pl.Float64
    assert data.schema['timestamp'] == pl.String
    assert '_generated_at' in data.columns
    assert worker.publish(client.run['run_id'], 1)['bronze_path'] == record['bronze_path']
    assert len(list((root / 'bronze').rglob('*.parquet'))) == 1


def test_digest_rejected_before_publish(tmp_path):
    client = LocalClient(tmp_path)
    content = client.http.get(f"/runs/{client.run['run_id']}/batches/1").json()
    content['data'][0]['uht_temp'] += 1
    with pytest.raises(PayloadError, match='digest'):
        decode_batch(json.dumps(content).encode(), client.run['run_id'], 1)


def test_publishing_crash_requires_review(tmp_path):
    root = tmp_path / 'storage_demo'
    client = LocalClient(root)
    worker = IngestionWorker(config(root), client)
    worker.state.save({'run_id': client.run['run_id'], 'sequence': 1, 'table_name': 'sensor_telemetry', 'status': 'PUBLISHING', 'attempts': 1})
    with pytest.raises(NeedsReview): worker.publish(client.run['run_id'], 1)
    assert not list(root.rglob('*.parquet'))
