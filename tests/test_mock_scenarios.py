"""Faults are bounded and deterministic per run and sequence."""
from fastapi.testclient import TestClient
from lakehouse_storage.mock_server.app import create_app
import pytest


@pytest.mark.parametrize(('scenario', 'field', 'expected'), [
    ('invalid-value', 'uht_temp', -10.0),
    ('invalid-time', 'timestamp', 'invalid-time'),
    ('missing-field', 'line_id', None),
])
def test_payload_faults(tmp_path, scenario, field, expected):
    client = TestClient(create_app(tmp_path))
    run = client.post('/runs', json={'scenario': scenario, 'samples_per_batch': 3}).json()
    route = f"/runs/{run['run_id']}/batches/1"
    response = client.get(route)
    assert response.json()['data'][0].get(field) == expected
    assert response.content == client.get(route).content


@pytest.mark.parametrize('first_digit', ['1', '2'])
def test_transient_attempts_persist(tmp_path, monkeypatch, first_digit):
    import uuid
    monkeypatch.setattr('lakehouse_storage.mock_server.store.uuid.uuid4',
                        lambda: uuid.UUID(first_digit + 'a' * 31))
    client = TestClient(create_app(tmp_path))
    run = client.post('/runs', json={'scenario': 'transient-500', 'failure_attempts': 2}).json()
    route = f"/runs/{run['run_id']}/batches/1"
    assert client.get(route).status_code == 500
    client = TestClient(create_app(tmp_path))
    assert client.get(route).status_code == 500
    assert client.get(route).status_code == 200
    assert client.get(f"/runs/{run['run_id']}/batches/2").status_code == 500


def test_delay_is_bounded_and_payload_unchanged(tmp_path):
    import time
    client = TestClient(create_app(tmp_path))
    run = client.post('/runs', json={'scenario': 'delay', 'delay_seconds': 0.05}).json()
    route = f"/runs/{run['run_id']}/batches/1"
    start = time.monotonic()
    first = client.get(route)
    assert time.monotonic() - start >= 0.05
    assert first.content == client.get(route).content
