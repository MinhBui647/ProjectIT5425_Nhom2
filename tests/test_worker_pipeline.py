"""HTTP -> immutable Bronze -> unchanged P3 -> real local Delta."""
from pathlib import Path
import polars as pl
import pytest
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.mock_client import MockClient
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.time_travel import trace_batch
from storage_demo_helpers import mock_http


def make_worker(root, url):
    return IngestionWorker(ServiceConfig(root, str(root / 'silver'), str(root), url), MockClient(url))


def test_normal_and_replay_use_p3_gate(tmp_path):
    import requests
    root = tmp_path / 'storage_demo'
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'samples_per_batch': 20, 'batches': 2}, timeout=5).json()
        worker = make_worker(root, url)
        records = [worker.process(run['run_id'], i) for i in range(3)]
        assert [r['status'] for r in records] == ['DONE'] * 3
        uri = str(root / 'silver' / 'sensor_telemetry')
        before = pl.read_delta(uri)
        assert before.height == 40
        assert set(before['_quality_status']) <= {'PASS', 'PASS_WITH_REJECTIONS'}
        assert before['_quality_run_id'].null_count() == 0
        replay = worker.process(run['run_id'], 1, replay=True)
        assert replay['p3_result']['status'] == 'SKIPPED_DUPLICATE'
        assert pl.read_delta(uri).height == 40
        trace = trace_batch(uri, before['batch_id'][0])
        assert trace['integrity_verified']
        assert len(trace['lineage_sources']) == 2
        assert len(list((root / 'bronze').rglob('*.parquet'))) == 3


@pytest.mark.parametrize(('scenario', 'stage'), [('invalid-value','p3'), ('invalid-time','p3'), ('missing-field','bronze')])
def test_rejection_at_correct_stage(tmp_path, scenario, stage):
    import requests
    root = tmp_path / 'storage_demo'
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'scenario': scenario, 'samples_per_batch': 10}, timeout=5).json()
        record = make_worker(root, url).process(run['run_id'], 1)
        assert record['status'] == 'REJECTED'
        assert record['stage'] == stage
        assert not (root / 'silver' / 'sensor_telemetry').exists()


def test_write_exception_with_artifact_requires_review(tmp_path, monkeypatch):
    import requests
    from lakehouse_storage import ingestion_worker
    from lakehouse_storage.worker_state import NeedsReview
    write = ingestion_worker.write_bronze_batch
    def fail_after_write(*args, **kwargs):
        path = write(*args, **kwargs)
        if args[1] == 'sensor_telemetry': raise OSError('after artifact write')
        return path
    monkeypatch.setattr(ingestion_worker, 'write_bronze_batch', fail_after_write)
    root = tmp_path/'storage_demo'
    with mock_http(root) as (url, _):
        run = requests.post(url+'/runs',json={'samples_per_batch':5,'batches':1},timeout=5).json()
        worker = make_worker(root,url)
        with pytest.raises(NeedsReview): worker.run(run['run_id'])
        assert worker.state.load(run['run_id'],1)['status'] == 'NEEDS_REVIEW'
        assert len(list((root/'bronze').rglob('*.parquet'))) == 2


def test_structural_rejection_is_not_refetched(tmp_path, monkeypatch):
    import requests
    root = tmp_path/'storage_demo'
    with mock_http(root) as (url, _):
        run = requests.post(url+'/runs',json={'scenario':'missing-field','samples_per_batch':5,'batches':1},timeout=5).json()
        worker = make_worker(root,url)
        record = worker.process(run['run_id'],1)
        monkeypatch.setattr(worker.client, 'batch', lambda *_: pytest.fail('rejected payload refetched'))
        assert worker.process(run['run_id'],1) == record


def test_silver_table_symlink_cannot_escape_demo(tmp_path):
    import requests
    root = tmp_path/'storage_demo'
    outside = tmp_path/'shared_silver'; outside.mkdir()
    (root/'silver').mkdir(parents=True)
    (root/'silver'/'sensor_telemetry').symlink_to(outside, target_is_directory=True)
    with mock_http(root) as (url, _):
        run = requests.post(url+'/runs',json={'samples_per_batch':5,'batches':1},timeout=5).json()
        record = make_worker(root,url).process(run['run_id'],1)
        assert record['status'] == 'FAILED'
        assert not list(outside.iterdir())


def test_once_skip_policy_advances_past_terminal_rejection(tmp_path):
    import requests
    root = tmp_path/'storage_demo'
    with mock_http(root) as (url, _):
        run = requests.post(url+'/runs',json={'scenario':'missing-field','samples_per_batch':5,'batches':3},timeout=5).json()
        worker = make_worker(root,url)
        worker.run(run['run_id'],limit=2,skip_rejected=True)
        worker.run(run['run_id'],limit=1,skip_rejected=True)
        assert worker.state.load(run['run_id'],2)['status'] == 'REJECTED'
