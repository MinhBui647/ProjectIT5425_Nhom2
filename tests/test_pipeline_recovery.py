"""Boundary recovery outside Airflow before permitting task retries."""
import pytest
import polars as pl
from lakehouse_storage.worker_state import NeedsReview
from lakehouse_storage.config import get_storage_options
from test_pipeline_bronze import StoredClient, worker_at


def test_weather_resume_after_commit_before_state_save(tmp_path, monkeypatch):
    root = tmp_path / 'storage_demo'
    client = StoredClient(root / 'mock')
    run_id = client.metadata['run_id']
    sequence = next(e['sequence'] for e in client.metadata['batches'] if e['table_name'] == 'weather')
    worker = worker_at(root, client)
    save = worker.state.save
    def crash(record):
        if record['status'] == 'DONE':
            raise SystemExit('simulated process termination after Delta commit')
        save(record)
    monkeypatch.setattr(worker.state, 'save', crash)
    with pytest.raises(SystemExit):
        worker.process(run_id, sequence)
    resumed = worker_at(root, client).process(run_id, sequence)
    assert resumed['status'] == 'DONE'
    assert resumed['previous_commit_outcome'] == 'unknown'
    assert resumed['p3_result']['status'] == 'SKIPPED_DUPLICATE'
    assert pl.read_delta(resumed['silver_uri'], storage_options=get_storage_options()).height == 5
    assert resumed['p3_result']['committed_evidence']['accepted_rows'] == 5


def test_unknown_bronze_publication_never_rewrites(tmp_path, monkeypatch):
    from lakehouse_storage import ingestion_worker
    root = tmp_path / 'storage_demo'
    client = StoredClient(root / 'mock')
    worker = worker_at(root, client)
    original = ingestion_worker.write_bronze_batch
    def crash(*args, **kwargs):
        original(*args, **kwargs)
        raise SystemExit('publish before state save')
    monkeypatch.setattr(ingestion_worker, 'write_bronze_batch', crash)
    with pytest.raises(SystemExit):
        worker.publish(client.metadata['run_id'], 2)
    with pytest.raises(NeedsReview):
        worker_at(root, client).publish(client.metadata['run_id'], 2)
    assert len(list((root / 'bronze').rglob('*.parquet'))) == 1
