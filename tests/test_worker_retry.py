"""Transport retry is finite; shutdown and crash tests use real processes."""
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests
from lakehouse_storage.mock_client import MockClient
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.worker_state import NeedsReview
from test_worker_pipeline import make_worker
from storage_demo_helpers import mock_http


@pytest.mark.parametrize('status', [429, 500, 503])
def test_transport_retry_budget(monkeypatch, status):
    calls, sleeps = [], []
    def get(*args, **kwargs):
        calls.append(1)
        response = requests.Response(); response.status_code = status
        raise requests.HTTPError(response=response)
    monkeypatch.setattr(requests, 'get', get)
    client = MockClient('http://unused', max_attempts=3, backoff=0.1, sleep=sleeps.append)
    with pytest.raises(requests.HTTPError): client.get('/health')
    assert len(calls) == 3
    assert sleeps == [0.1, 0.2]


def test_404_not_retried(monkeypatch):
    calls = []
    def get(*args, **kwargs):
        calls.append(1)
        response = requests.Response(); response.status_code = 404
        raise requests.HTTPError(response=response)
    monkeypatch.setattr(requests, 'get', get)
    with pytest.raises(requests.HTTPError): MockClient('http://unused', sleep=lambda _: None).get('/missing')
    assert len(calls) == 1


@pytest.mark.parametrize('boundary', ['BRONZE_READY', 'DONE'])
def test_kill_between_side_effect_and_checkpoint(tmp_path, boundary):
    root = tmp_path / 'storage_demo'
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'samples_per_batch': 8, 'batches': 1}, timeout=5).json()
        code = '''
import sys, time
from lakehouse_storage.worker_state import StateStore, single_instance
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.mock_client import MockClient
from lakehouse_storage.service_config import ServiceConfig
save = StateStore.save
def paused(self, record):
    if record['status'] == sys.argv[2]:
        print('boundary', flush=True)
        time.sleep(60)
    return save(self, record)
StateStore.save = paused
config = ServiceConfig.from_env()
with single_instance(config.root / 'demo-phase.lock'):
    IngestionWorker(config, MockClient(config.api_url)).process(sys.argv[1], 1)
'''
        env = {**os.environ, 'PYTHONPATH': 'src', 'STORAGE_DEMO_ROOT': str(root), 'MOCK_URL': url}
        child = subprocess.Popen([sys.executable, '-c', code, run['run_id'], boundary], env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        try:
            import select
            ready, _, _ = select.select([child.stdout], [], [], 30)
            assert ready and child.stdout.readline().strip() == 'boundary'
        finally:
            child.kill(); child.wait(timeout=5)
        worker = make_worker(root, url)
        if boundary == 'BRONZE_READY':
            with pytest.raises(NeedsReview): worker.process(run['run_id'], 1)
            assert len(list((root / 'bronze').rglob('*.parquet'))) == 1
        else:
            record = worker.process(run['run_id'], 1)
            assert record['status'] == 'DONE'
            assert record['previous_commit_outcome'] == 'unknown'
            assert record['p3_result']['status'] == 'SKIPPED_DUPLICATE'
            import polars as pl
            assert pl.read_delta(str(root / 'silver' / 'sensor_telemetry')).height == 8


def test_cli_sigterm_completes_inflight_batch(tmp_path):
    root = tmp_path / 'storage_demo'
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'scenario': 'delay', 'delay_seconds': 1, 'samples_per_batch': 10}, timeout=5).json()
        env = {**os.environ, 'PYTHONPATH': 'src', 'STORAGE_DEMO_ROOT': str(root), 'MOCK_URL': url}
        child = subprocess.Popen([sys.executable, '-m', 'lakehouse_storage.ingestion_worker', '--run-id', run['run_id']], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            checkpoint = root / 'worker_state' / run['run_id'] / '1.json'
            for _ in range(400):
                if checkpoint.exists(): break
                assert child.poll() is None
                time.sleep(0.025)
            assert checkpoint.exists()
            child.terminate()
            assert child.wait(timeout=30) == 0, child.stderr.read().decode()
            import json
            assert json.loads(checkpoint.read_text())['status'] == 'DONE'
            assert not (root / 'worker_state' / run['run_id'] / '2.json').exists()
        finally:
            if child.poll() is None: child.kill(); child.wait(timeout=5)


def test_cli_changed_silver_target_exits_nonzero(tmp_path):
    root = tmp_path / 'storage_demo'
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'samples_per_batch': 5, 'batches': 1}, timeout=5).json()
        make_worker(root, url).run(run['run_id'])
        env = {**os.environ, 'PYTHONPATH': 'src', 'STORAGE_DEMO_ROOT': str(root),
               'STORAGE_DEMO_SILVER_ROOT': str(root / 'silver_b'),
               'STORAGE_DEMO_SILVER_ALLOW_ROOT': str(root), 'MOCK_URL': url}
        child = subprocess.run([sys.executable, '-B', '-m', 'lakehouse_storage.ingestion_worker',
                                '--run-id', run['run_id']], env=env, capture_output=True, text=True, timeout=30)
        assert child.returncode != 0
        assert 'target' in child.stderr.lower()
        assert not (root / 'silver_b').exists()
