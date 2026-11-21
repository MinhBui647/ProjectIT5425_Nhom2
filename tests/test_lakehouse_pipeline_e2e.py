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


@pytest.mark.parametrize('table,missing', [('sensor_telemetry', 1), ('sensor_telemetry', 2), ('market_reports', 1)])
def test_missing_checkpoint_blocks_snapshot_and_finalize_then_resumes(tmp_path, table, missing):
    from lakehouse_pipeline.run_manifest import ManifestStore
    from lakehouse_storage.ingestion_worker import IngestionWorker
    from lakehouse_storage.config import get_storage_options
    import polars as pl

    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    path = prepare_run(config, 'partial', {'sources': [table], 'samples_per_batch': 10, 'batches': 3}, client=client)
    store = ManifestStore(root)
    run_id = store.load(path)['mock_run_id']
    metadata = client.run(run_id)
    worker = IngestionWorker(config, client)
    for entry in metadata['batches']:
        if entry['sequence'] != missing:
            assert worker.process(run_id, entry['sequence'])['status'] == 'DONE'
    paths = sorted((root / 'bronze').rglob('*.parquet'))
    hashes = {p: p.read_bytes() for p in paths}
    for stage in ('snapshot', 'finalize'):
        with pytest.raises(BusinessFailure, match=f'missing checkpoint.*{missing}.*{table}'):
            execute_stage(config, path, stage, client=client)
        assert store.load(path)['status'] == 'FAILED'
    result = run_pipeline(config, path, client=client)
    assert result['status'] == 'SUCCESS'
    assert len(result['artifacts']) == metadata['batch_count']
    assert all(p.read_bytes() == content for p, content in hashes.items())
    assert len(list((root / 'bronze').rglob('*.parquet'))) == metadata['batch_count']
    if table == 'sensor_telemetry':
        assert pl.read_delta(str(root / 'silver' / table), storage_options=get_storage_options()).height == 30
    else:
        assert result['silver_snapshot'] == {}


def test_finalize_rechecks_checkpoint_completeness_after_capture(tmp_path):
    from lakehouse_pipeline.run_manifest import ManifestStore
    from lakehouse_storage.ingestion_worker import IngestionWorker

    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    path = prepare_run(config, 'lost-checkpoint', {'sources': ['sensor_telemetry'], 'samples_per_batch': 10, 'batches': 2}, client=client)
    store = ManifestStore(root)
    run_id = store.load(path)['mock_run_id']
    worker = IngestionWorker(config, client)
    worker.run(run_id)
    execute_stage(config, path, 'snapshot', client=client)
    pinned = store.load(path)['silver_snapshot']
    worker.state.path(run_id, 1).unlink()
    with pytest.raises(BusinessFailure, match='missing checkpoint.*1.*sensor_telemetry'):
        execute_stage(config, path, 'finalize', client=client)
    assert store.load(path)['silver_snapshot'] == pinned


def test_optional_source_still_requires_every_no_data_checkpoint(tmp_path, monkeypatch):
    from lakehouse_pipeline.source_config import SourceConfig
    from lakehouse_pipeline.run_manifest import ManifestStore
    from lakehouse_storage.ingestion_worker import IngestionWorker

    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    monkeypatch.setattr('lakehouse_storage.mock_server.store.generate_batches', lambda *_: iter([('weather', []), ('weather', [])]))
    source = SourceConfig(sources=['weather'], optional_sources=['weather'],
                          start='2026-01-15T00:00:00Z', end='2026-01-16T00:00:00Z')
    path = prepare_run(config, 'partial-empty', {'sources': ['weather'], 'end_time': '2026-01-16T00:00:00Z'}, source_config=source, client=client)
    run_id = ManifestStore(root).load(path)['mock_run_id']
    assert IngestionWorker(config, client).process(run_id, 0)['status'] == 'NO_DATA'
    with pytest.raises(BusinessFailure, match='missing checkpoint.*1.*weather'):
        execute_stage(config, path, 'snapshot', client=client)
    assert run_pipeline(config, path, client=client)['silver_snapshot'] == {}


@pytest.mark.parametrize('completed', [False, True])
@pytest.mark.parametrize('change', ['append', 'compact'])
def test_captured_snapshot_survives_latest_changes_and_stage_retries(tmp_path, completed, change):
    from lakehouse_pipeline.run_manifest import ManifestStore
    from lakehouse_pipeline.silver_snapshot import read_snapshot
    from lakehouse_storage.maintenance import compact_table
    from deltalake import DeltaTable
    from lakehouse_storage.config import get_storage_options

    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    request = {'sources': ['sensor_telemetry'], 'samples_per_batch': 10, 'batches': 3}
    path = prepare_run(config, 'original', request, client=client)
    if completed:
        run_pipeline(config, path, client=client)
    else:
        execute_stage(config, path, 'govern', 'sensor_telemetry', client=client)
        execute_stage(config, path, 'snapshot', client=client)
    store = ManifestStore(root)
    first = store.load(path)
    pinned = first['silver_snapshot']
    rows = read_snapshot(pinned, 'sensor_telemetry').sort('timestamp')
    if change == 'append':
        other = prepare_run(config, 'other', {**request, 'seed': 43}, client=client)
        run_pipeline(config, other, client=client)
    else:
        compact_table(pinned['sensor_telemetry']['uri'], target_file_size_mb=128)
    assert DeltaTable(pinned['sensor_telemetry']['uri'], storage_options=get_storage_options()).version() > pinned['sensor_telemetry']['version']
    for stage in ('finalize', 'snapshot', 'finalize'):
        execute_stage(config, path, stage, client=client)
        assert store.load(path)['silver_snapshot'] == pinned
    result = run_pipeline(config, path, client=client)
    assert result['silver_snapshot'] == pinned
    assert result['mock_run_id'] == first['mock_run_id']
    assert [r['bronze_path'] for r in result['artifacts']] == [r['bronze_path'] for r in first['artifacts']]
    assert read_snapshot(result['silver_snapshot'], 'sensor_telemetry').sort('timestamp').equals(rows)


@pytest.mark.parametrize('damage', ['missing_map', 'missing_table', 'uri', 'version_bool', 'version_missing', 'total', 'batches', 'extra_table'])
def test_invalid_pinned_snapshot_is_not_recaptured(tmp_path, damage):
    from lakehouse_pipeline.run_manifest import ManifestStore
    from lakehouse_storage.worker_state import NeedsReview
    import copy
    from deltalake.exceptions import DeltaError

    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    path = prepare_run(config, 'damaged', {'sources': ['weather']}, client=client)
    run_pipeline(config, path, client=client)
    store = ManifestStore(root)
    value = store.load(path)
    if damage == 'missing_map':
        del value['silver_snapshot']
    elif damage == 'missing_table':
        value['silver_snapshot'] = {}
    elif damage == 'extra_table':
        value['silver_snapshot']['sensor_telemetry'] = copy.deepcopy(value['silver_snapshot']['weather'])
    else:
        key, bad = {'uri': ('uri', str(root / 'other')),
                    'version_bool': ('version', True), 'version_missing': ('version', 9999),
                    'total': ('total_rows', 9999), 'batches': ('batches', [])}[damage]
        value['silver_snapshot']['weather'][key] = bad
    damaged = copy.deepcopy(value.get('silver_snapshot'))
    store.save(path, value)
    for stage in ('snapshot', 'finalize', 'snapshot'):
        with pytest.raises((ValueError, NeedsReview, DeltaError)):
            execute_stage(config, path, stage, client=client)
        assert store.load(path).get('silver_snapshot') == damaged


@pytest.mark.parametrize('table', ['weather', 'market_reports'])
def test_empty_pinned_map_is_reused_without_capture(tmp_path, monkeypatch, table):
    from lakehouse_pipeline.source_config import SourceConfig

    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    if table == 'weather':
        monkeypatch.setattr('lakehouse_storage.mock_server.store.generate_batches', lambda *_: iter([('weather', [])]))
    source = SourceConfig(sources=[table], optional_sources=[table],
                          start='2026-01-15T00:00:00Z', end='2026-01-16T00:00:00Z')
    path = prepare_run(config, 'empty-pinned', {'sources': [table], 'end_time': '2026-01-16T00:00:00Z'}, source_config=source, client=client)
    assert run_pipeline(config, path, client=client)['silver_snapshot'] == {}
    def unexpected_capture(*args, **kwargs):
        pytest.fail('an already captured empty map must be validated, not captured again')
    monkeypatch.setattr('lakehouse_pipeline.pipeline.capture_snapshot', unexpected_capture)
    assert run_pipeline(config, path, client=client)['silver_snapshot'] == {}


def test_legacy_empty_capture_survives_failure_before_validation(tmp_path, monkeypatch):
    from lakehouse_pipeline import pipeline
    from lakehouse_pipeline.source_config import SourceConfig
    from lakehouse_pipeline.run_manifest import ManifestStore

    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://test')
    client = HttpClient(root / 'mock')
    monkeypatch.setattr('lakehouse_storage.mock_server.store.generate_batches', lambda *_: iter([('weather', [])]))
    source = SourceConfig(sources=['weather'], optional_sources=['weather'],
                          start='2026-01-15T00:00:00Z', end='2026-01-16T00:00:00Z')
    path = prepare_run(config, 'legacy-empty', {'sources': ['weather'], 'end_time': '2026-01-16T00:00:00Z'}, source_config=source, client=client)
    run_pipeline(config, path, client=client)
    store = ManifestStore(root)
    value = store.load(path)
    value['stages']['snapshot'].pop('captured', None)
    store.save(path, value)
    original = pipeline._records
    def unavailable(*args, **kwargs):
        if kwargs.get('require_complete'):
            raise BusinessFailure('checkpoint temporarily unavailable')
        return original(*args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(pipeline, '_records', unavailable)
        with pytest.raises(BusinessFailure, match='temporarily unavailable'):
            execute_stage(config, path, 'snapshot', client=client)
    def unexpected_capture(*args, **kwargs):
        pytest.fail('legacy captured map must survive a failed retry')
    monkeypatch.setattr(pipeline, 'capture_snapshot', unexpected_capture)
    assert run_pipeline(config, path, client=client)['silver_snapshot'] == {}
