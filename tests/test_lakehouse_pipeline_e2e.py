from fastapi.testclient import TestClient
import pytest
from lakehouse_pipeline.pipeline import prepare_run, execute_stage, run_pipeline, BusinessFailure
from lakehouse_storage.mock_server.app import create_app
from lakehouse_storage.mock_client import MockClient
from lakehouse_storage.service_config import ServiceConfig
from test_mock_manifest import FULL


class HttpClient(MockClient):
    def __init__(self, root):
        super().__init__('http://test')
        self.http = TestClient(create_app(root))
    def get(self, route):
        res = self.http.get(route)
        res.raise_for_status()
        return res.content
    def create(self, request, key):
        res = self.http.post('/runs', json=request, headers={'Idempotency-Key': key})
        res.raise_for_status()
        return res.json()


def test_full_run_and_clear_reuse_manifest(tmp_path):
    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    path = prepare_run(config, 'demo/full', {'sources': FULL, 'fao_variant': 'both', 'samples_per_batch': 10, 'batches': 1}, client=client)
    result = run_pipeline(config, path, client=client)
    assert result['status'] == 'SUCCESS' and len(result['silver_snapshot']) == 6
    replay = run_pipeline(config, path, client=client)
    assert result['silver_snapshot'] == replay['silver_snapshot']
    assert path == prepare_run(config, 'demo/full', {'sources': FULL, 'fao_variant': 'both', 'samples_per_batch': 10, 'batches': 1}, client=client)
    assert len(list((root / 'mock').glob('*/run.json'))) == 1


def test_failed_source_preserves_partial_commits_and_blocks_finalize(tmp_path):
    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    path = prepare_run(config, 'demo/bad', {'sources': FULL, 'samples_per_batch': 5, 'batches': 1, 'scenario': 'invalid-value'}, client=client)
    with pytest.raises(BusinessFailure):
        run_pipeline(config, path, client=client)
    from lakehouse_pipeline.run_manifest import ManifestStore
    result = ManifestStore(root).load(path)
    assert result['status'] == 'FAILED'
    assert any(r.get('p3_result', {}).get('committed_evidence') for r in result['artifacts'])
    with pytest.raises(BusinessFailure):
        execute_stage(config, path, 'finalize', client=client)


@pytest.mark.parametrize('optional', [False, True])
def test_empty_source_has_explicit_no_data_policy(tmp_path, monkeypatch, optional):
    from lakehouse_pipeline.source_config import SourceConfig
    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    monkeypatch.setattr('lakehouse_storage.mock_server.store.generate_batches', lambda *_: iter([('weather', [])]))
    source = SourceConfig(sources=['weather'], optional_sources=['weather'] if optional else [],
                          start='2026-01-15T00:00:00Z', end='2026-01-16T00:00:00Z')
    path = prepare_run(config, 'no-data', {'sources': ['weather'], 'end_time': '2026-01-16T00:00:00Z'}, source_config=source, client=client)
    if optional:
        result = run_pipeline(config, path, client=client)
        assert result['status'] == 'SUCCESS' and result['silver_snapshot'] == {}
        assert result['artifacts'][0]['status'] == 'NO_DATA'
    else:
        with pytest.raises(BusinessFailure, match='NO_DATA'):
            run_pipeline(config, path, client=client)
