"""Opt-in S3 integration; never uses the shared factory bucket."""
import os
import uuid

import polars as pl
import pytest
import requests

from lakehouse_storage import config as core_config
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.maintenance_policy import MaintenancePolicy
from lakehouse_storage.maintenance_runner import MaintenanceRunner
from lakehouse_storage.mock_client import MockClient
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.time_travel import get_version_at_date, trace_batch
from storage_demo_helpers import mock_http

pytestmark = [pytest.mark.minio, pytest.mark.skipif(os.getenv('STORAGE_DEMO_MINIO_TEST') != '1', reason='set STORAGE_DEMO_MINIO_TEST=1 and start isolated demo MinIO')]


@pytest.fixture
def s3_demo(tmp_path, monkeypatch):
    endpoint = os.getenv('STORAGE_DEMO_TEST_ENDPOINT', '127.0.0.1:19000')
    monkeypatch.setattr(core_config, 'MINIO_ENDPOINT', endpoint)
    monkeypatch.setattr(core_config, 'MINIO_ACCESS_KEY', 'storagedemo')
    monkeypatch.setattr(core_config, 'MINIO_SECRET_KEY', os.getenv('STORAGE_DEMO_MINIO_PASSWORD', 'storage-demo-local-only'))
    monkeypatch.setattr(core_config, 'MINIO_SECURE', False)
    root = tmp_path / 'storage_demo'
    remote = 's3://smart-dairy-storage-demo/storage_demo/tests/' + uuid.uuid4().hex
    return root, remote


def test_s3_normal_replay_resume_and_maintenance(s3_demo):
    from deltalake import DeltaTable
    from datetime import datetime, timezone
    root, remote = s3_demo
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'samples_per_batch': 10, 'batches': 3}, timeout=5).json()
        worker = IngestionWorker(ServiceConfig(root, remote, 's3://smart-dairy-storage-demo/storage_demo', url), MockClient(url))
        records = worker.run(run['run_id'])
        assert all(r['status'] == 'DONE' for r in records), records
        uri = remote + '/sensor_telemetry'
        before = pl.read_delta(uri, storage_options=core_config.get_storage_options()).sort('timestamp')
        assert before.height == 30
        assert pl.read_delta(remote + '/mes_lims', storage_options=core_config.get_storage_options()).height == 1
        replay = worker.process(run['run_id'], 1, replay=True)
        assert replay['p3_result']['status'] == 'SKIPPED_DUPLICATE'
        state = worker.state.load(run['run_id'], 2)
        state['status'] = 'PROCESSING'; worker.state.save(state)
        resumed = worker.process(run['run_id'], 2)
        assert resumed['bronze_path'] == state['bronze_path']
        assert resumed['p3_result']['status'] == 'SKIPPED_DUPLICATE'
        trace = trace_batch(uri, before['batch_id'][0])
        assert trace['integrity_verified'] and len(trace['lineage_sources']) == 3
        policy = MaintenancePolicy(uri, 's3://smart-dairy-storage-demo/storage_demo', file_count_threshold=2)
        runner = MaintenanceRunner(root / 'maintenance.jsonl')
        compact = runner.run(policy, enabled=True, ingestion_stopped=True)
        assert compact['decision'] == 'RUN', compact
        assert before.equals(pl.read_delta(uri, storage_options=core_config.get_storage_options()).sort('timestamp'))
        dt = DeltaTable(uri, storage_options=core_config.get_storage_options())
        last = max(e['timestamp'] for e in dt.history())
        target = datetime.fromtimestamp(last/1000, tz=timezone.utc).isoformat()
        assert get_version_at_date(uri, target) == dt.version()
        assert runner.run(policy, 'vacuum_preview', enabled=True, ingestion_stopped=True)['result']['dry_run']


@pytest.mark.parametrize('scenario', ['invalid-value', 'invalid-time', 'missing-field', 'transient-500', 'delay'])
def test_s3_faults(s3_demo, scenario):
    root, remote = s3_demo
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'scenario':scenario, 'samples_per_batch':10, 'batches':1, 'failure_attempts':2, 'delay_seconds':0.1}, timeout=5).json()
        client = MockClient(url, timeout=0.02 if scenario=='delay' else 5, max_attempts=3, backoff=0.01)
        worker = IngestionWorker(ServiceConfig(root, remote, 's3://smart-dairy-storage-demo/storage_demo', url), client)
        if scenario == 'delay':
            with pytest.raises(requests.Timeout): worker.process(run['run_id'], 1)
        else:
            record = worker.process(run['run_id'], 1)
            assert record['status'] == ('DONE' if scenario == 'transient-500' else 'REJECTED'), record


def test_s3_strict_diff_trace_and_history(s3_demo):
    from deltalake import DeltaTable
    from lakehouse_storage.bronze_writer import write_bronze_batch, verify_checksum
    from lakehouse_storage.silver_manager import append_to_delta, ingest_bronze_file
    from lakehouse_storage.time_travel import diff_versions, load_version
    from lakehouse_storage.mock_sensor_stream import generate_sensor_batch
    root, remote = s3_demo
    uri = remote + '/sensor_telemetry'
    raw = generate_sensor_batch(5, seed=42, batch_id='S3_REVIEW')
    first = write_bronze_batch(raw, 'sensor_telemetry', root / 'bronze')
    ingest_bronze_file(first, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
    for invalid in (raw.drop('line_id'), raw.with_columns(pl.lit(None).alias('line_id'))):
        with pytest.raises(ValueError):
            append_to_delta(invalid, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
        assert DeltaTable(uri, storage_options=core_config.get_storage_options()).version() == 0
    second = write_bronze_batch(raw.with_columns(pl.lit('LINE_UHT_2').alias('line_id')),
                                'sensor_telemetry', root / 'bronze')
    ingest_bronze_file(second, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
    assert diff_versions(uri, 0, 1)['counts']['added_count'] == 5
    before = pl.read_delta(uri, storage_options=core_config.get_storage_options()).sort(['line_id', 'timestamp'])
    assert before.height == 10 and load_version(uri, 0).height == 5
    trace = trace_batch(uri, 'S3_REVIEW')
    assert trace['integrity_verified'] and len(trace['lineage_sources']) == 2
    runner = MaintenanceRunner(root / 'review_maintenance.jsonl')
    policy = MaintenancePolicy(uri, 's3://smart-dairy-storage-demo/storage_demo', file_count_threshold=2)
    result = runner.run(policy, enabled=True, ingestion_stopped=True)
    assert result['decision'] == 'RUN', result
    after = pl.read_delta(uri, storage_options=core_config.get_storage_options()).sort(['line_id', 'timestamp'])
    assert before.equals(after) and load_version(uri, 0).height == 5
    assert runner.run(policy, 'vacuum_preview', enabled=True, ingestion_stopped=True)['result']['dry_run']
    assert all(verify_checksum(path) for path in (first, second))
    print({'endpoint': core_config.MINIO_ENDPOINT, 'isolated_uri': uri,
           'rows': after.height, 'version': DeltaTable(uri, storage_options=core_config.get_storage_options()).version()})


def test_s3_encoded_escape_cannot_compact_outside_allow_root(s3_demo):
    from deltalake import DeltaTable, write_deltalake
    from lakehouse_storage.service_config import require_namespace
    from lakehouse_storage.time_travel import load_version
    root, remote = s3_demo
    allowed = remote + '/allowed'
    escaped = remote + '/escaped/table'
    options = core_config.get_storage_options()
    for value in (1, 2):
        write_deltalake(escaped, pl.DataFrame({'id': [value]}).to_arrow(),
                        mode='append', storage_options=options)
    before = DeltaTable(escaped, storage_options=options)
    version, files = before.version(), sorted(before.file_uris())
    rows = pl.read_delta(escaped, storage_options=options).sort('id')
    for segment in ('%2E%2E', '%2e%2e', '.%2e', '%252e%252e'):
        attack = allowed + '/' + segment + '/escaped/table'
        with pytest.raises(ValueError):
            require_namespace(attack, allowed)
        with pytest.raises(ValueError):
            MaintenancePolicy(attack, allowed, file_count_threshold=2)
    after = DeltaTable(escaped, storage_options=options)
    assert after.version() == version and sorted(after.file_uris()) == files
    assert rows.equals(pl.read_delta(escaped, storage_options=options).sort('id'))
    result = MaintenanceRunner(root / 'namespace_maintenance.jsonl').run(
        MaintenancePolicy(escaped, remote, file_count_threshold=2),
        enabled=True, ingestion_stopped=True)
    assert result['decision'] == 'RUN', result
    assert rows.equals(pl.read_delta(escaped, storage_options=options).sort('id'))
    assert load_version(escaped, 0).height == 1
    print({'case': 'namespace', 'uri': escaped, 'blocked_version': version,
           'compacted_version': DeltaTable(escaped, storage_options=options).version(),
           'rows': rows.height})


def test_s3_checkpoint_target_change_fails_closed(s3_demo):
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError
    from lakehouse_storage.worker_state import NeedsReview
    root, remote = s3_demo
    options = core_config.get_storage_options()
    with mock_http(root) as (url, _):
        run = requests.post(url + '/runs', json={'samples_per_batch': 10, 'batches': 1}, timeout=5).json()
        worker = IngestionWorker(ServiceConfig(root, remote + '/a', remote, url), MockClient(url))
        assert all(r['status'] == 'DONE' for r in worker.run(run['run_id']))
        uri = remote + '/a/sensor_telemetry'
        before = pl.read_delta(uri, storage_options=options).sort('timestamp')
        checkpoint = worker.state.path(run['run_id'], 1).read_bytes()
        changed = IngestionWorker(ServiceConfig(root, remote + '/b', remote, url), MockClient(url))
        with pytest.raises(NeedsReview, match='target'):
            changed.run(run['run_id'])
        with pytest.raises(TableNotFoundError):
            DeltaTable(remote + '/b/sensor_telemetry', storage_options=options)
        assert worker.state.path(run['run_id'], 1).read_bytes() == checkpoint
        assert before.equals(pl.read_delta(uri, storage_options=options).sort('timestamp'))
        assert DeltaTable(uri, storage_options=options).version() == 0
        assert worker.run(run['run_id']) == []
        assert worker.process(run['run_id'], 1, replay=True)['p3_result']['status'] == 'SKIPPED_DUPLICATE'
        print({'case': 'checkpoint_target', 'uri': uri, 'rows': before.height, 'blocked_target': remote + '/b'})


def test_s3_timestamp_input_types_have_same_partition(s3_demo):
    from datetime import date, datetime
    from lakehouse_storage.silver_manager import append_to_delta, read_silver_table
    _, remote = s3_demo
    string = pl.DataFrame({'timestamp': ['2026-01-16T01:00:00+07:00'],
                           'line_id': ['LINE_UHT_1'], 'uht_temp': [140.0]})
    typed = string.with_columns(pl.col('timestamp').str.to_datetime(time_zone='Asia/Ho_Chi_Minh'))
    results = []
    for name, frame in [('string', string), ('typed', typed)]:
        uri = remote + '/' + name + '/sensor_telemetry'
        append_to_delta(frame, uri, ['line_id', 'ingestion_date'], 'sensor_telemetry')
        result = read_silver_table(uri, date_from='2026-01-15', date_to='2026-01-15')
        assert result.height == 1
        assert result['timestamp'][0] == datetime.fromisoformat('2026-01-15T18:00:00+00:00')
        assert result['ingestion_date'][0] == date(2026, 1, 15)
        results.append(result.select('timestamp', 'ingestion_date'))
    assert results[0].equals(results[1])
    print({'case': 'timezone', 'prefix': remote, 'ingestion_date': '2026-01-15'})
