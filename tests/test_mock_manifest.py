"""Manifest identity, atomic visibility and durable create semantics."""
import concurrent.futures
import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from lakehouse_storage.mock_server.app import create_app
from lakehouse_storage.mock_server.models import RunRequest
from lakehouse_storage.mock_server.store import RunStore, canonical_json

FULL = ['mes_lims', 'sensor_telemetry', 'market_prices', 'market_indices', 'weather', 'food_recalls', 'market_reports']


def test_full_manifest_restart_and_raw_variants(tmp_path):
    store = RunStore(tmp_path)
    run = store.create(RunRequest(sources=FULL, fao_variant='both', samples_per_batch=10, batches=1))
    assert run['manifest_version'] == 2
    assert run['batch_count'] == 9
    assert {b['table_name'] for b in run['batches']} == set(FULL)
    for entry in run['batches']:
        payload = store.batch(run['run_id'], entry['sequence'])
        assert payload == RunStore(tmp_path).batch(run['run_id'], entry['sequence'])
        envelope = json.loads(payload)
        assert entry['payload_sha256'] == hashlib.sha256(canonical_json(envelope['data'])).hexdigest()
        assert envelope['source'] == entry['source']
        assert entry['bronze_only'] == (entry['table_name'] == 'market_reports')
    assert not any(p.name.endswith('.tmp') for p in tmp_path.iterdir())


def test_idempotent_http_concurrent_restart_and_conflict(tmp_path):
    body = {'samples_per_batch': 5, 'batches': 1}
    def create(_):
        with TestClient(create_app(tmp_path)) as client:
            response = client.post('/runs', json=body, headers={'Idempotency-Key': 'pipeline-123'})
            assert response.status_code == 201
            return response.json()
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        first, second = list(pool.map(create, range(2)))
    assert first == second == create(None)
    assert len(list(tmp_path.glob('*/run.json'))) == 1
    client = TestClient(create_app(tmp_path))
    assert client.post('/runs', json={'seed': 2}, headers={'Idempotency-Key': 'pipeline-123'}).status_code == 409


@pytest.mark.parametrize('body', [{'sources': ['unknown']}, {'sources': []}, {'sources': ['weather', 'weather']}, {'fao_variant': 'unknown'}])
def test_invalid_controls_rejected(tmp_path, body):
    assert TestClient(create_app(tmp_path)).post('/runs', json=body).status_code == 422


def test_staging_is_invisible(tmp_path, monkeypatch):
    store = RunStore(tmp_path)
    def fail(*args):
        raise RuntimeError('power loss before publication')
    monkeypatch.setattr('lakehouse_storage.mock_server.store.os.replace', fail)
    with pytest.raises(RuntimeError):
        store.create(RunRequest(samples_per_batch=5, batches=1))
    assert not list(tmp_path.glob('*/run.json')) or all(p.parent.name.startswith('.') for p in tmp_path.glob('*/run.json'))
    for path in tmp_path.glob('.*.tmp'):
        with pytest.raises((ValueError, FileNotFoundError)):
            store.run(path.name)


def test_response_lost_after_publish_retry_keeps_identity(tmp_path, monkeypatch):
    original = RunStore.create
    seen = []
    def lose_response(self, *args, **kwargs):
        value = original(self, *args, **kwargs)
        seen.append(value['run_id'])
        if len(seen) == 1:
            raise RuntimeError('response connection lost after publish')
        return value
    monkeypatch.setattr(RunStore, 'create', lose_response)
    client = TestClient(create_app(tmp_path), raise_server_exceptions=False)
    headers = {'Idempotency-Key': 'lost-response'}
    assert client.post('/runs', json={'samples_per_batch': 2}, headers=headers).status_code == 503
    response = client.post('/runs', json={'samples_per_batch': 2}, headers=headers)
    assert response.status_code == 201
    assert seen[0] == seen[1] == response.json()['run_id']
    assert len(list(tmp_path.glob('*/run.json'))) == 1
