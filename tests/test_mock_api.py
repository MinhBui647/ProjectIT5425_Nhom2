"""The HTTP contract returns stored bytes and bounded control errors."""
import json

from fastapi.testclient import TestClient
from lakehouse_storage.mock_server.app import create_app


def test_api_contract_and_restart(tmp_path):
    client = TestClient(create_app(tmp_path))
    assert client.get('/health').json()['status'] == 'ok'
    assert client.post('/runs', json={'samples_per_batch': 0}).status_code == 422
    assert client.get('/runs/' + 'f' * 32).status_code == 404
    run = client.post('/runs', json={'samples_per_batch': 10, 'batches': 1})
    assert run.status_code == 201
    run_id = run.json()['run_id']
    first = client.get(f'/runs/{run_id}/batches/1')
    assert first.status_code == 200
    assert json.loads(first.content)['sequence'] == 1
    restarted = TestClient(create_app(tmp_path))
    assert restarted.get(f'/runs/{run_id}/batches/1').content == first.content
    assert restarted.get(f'/runs/{run_id}/batches/99').status_code == 404
    assert restarted.get(f'/runs/{run_id}/batches/-1').status_code == 404


def test_real_http_entrypoint_restart(tmp_path):
    import requests
    from storage_demo_helpers import mock_http
    root = tmp_path / 'storage_demo'
    with mock_http(root) as (url, process):
        run = requests.post(url + '/runs', json={'samples_per_batch': 3}, timeout=5).json()
        route = f"/runs/{run['run_id']}/batches/1"
        original = requests.get(url + route, timeout=5).content
    with mock_http(root) as (url, process):
        assert requests.get(url + route, timeout=5).content == original
