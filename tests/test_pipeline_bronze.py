"""Full source identity survives publication, replay and corruption."""
import json
import pytest
import polars as pl
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.mock_client import decode_batch
from lakehouse_storage.mock_server.models import RunRequest
from lakehouse_storage.mock_server.store import RunStore
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.worker_state import NeedsReview
from test_mock_manifest import FULL


class StoredClient:
    def __init__(self, root, **kwargs):
        self.store = RunStore(root)
        self.metadata = self.store.create(RunRequest(samples_per_batch=10, batches=1, sources=FULL, fao_variant='both', **kwargs))
    def run(self, run_id):
        return self.store.run(run_id)
    def batch(self, run_id, sequence):
        return decode_batch(self.store.batch(run_id, sequence), run_id, sequence, self.metadata['batches'][sequence])


def worker_at(root, client):
    return IngestionWorker(ServiceConfig(root, str(root / 'silver'), str(root), 'http://unused'), client)


def test_publish_every_source_reuses_original_bytes(tmp_path):
    root = tmp_path / 'storage_demo'
    client = StoredClient(root / 'mock')
    worker = worker_at(root, client)
    run = client.metadata
    for entry in run['batches']:
        record = worker.publish(run['run_id'], entry['sequence'])
        assert record['table_name'] == entry['table_name']
        assert record['schema_variant'] == entry['schema_variant']
        assert record['status'] == 'BRONZE_READY'
        if entry['bronze_only']:
            assert '/2026-01-15/' in record['bronze_path']
        assert worker.publish(run['run_id'], entry['sequence']) == record
    assert len(list((root / 'bronze').rglob('*.parquet'))) == run['batch_count']
    record = worker.publish(run['run_id'], 3)
    from pathlib import Path
    Path(record['bronze_path']).write_bytes(b'corrupt')
    with pytest.raises(NeedsReview):
        worker.publish(run['run_id'], 3)


def test_state_rejects_table_or_manifest_digest_mismatch(tmp_path):
    root = tmp_path / 'storage_demo'
    client = StoredClient(root / 'mock')
    worker = worker_at(root, client)
    record = worker.publish(client.metadata['run_id'], 2)
    record['payload_sha256'] = 'a' * 64
    worker.state.save(record)
    with pytest.raises(NeedsReview):
        worker.publish(client.metadata['run_id'], 2)


def test_raw_json_restores_nullable_int32_metadata_width():
    from lakehouse_storage.ingestion_worker import raw_frame
    frame = raw_frame({'table_name': 'food_recalls', 'data': [{'recall_number': 'R1', 'report_date': '20260115',
        'classification': 'Class I', '_created_at': '2026-01-15T00:00:00Z', 'meta_results_total': 12}]})
    assert frame.schema['meta_results_total'] == pl.Int32
