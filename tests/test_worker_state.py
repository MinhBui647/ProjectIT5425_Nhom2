"""Checkpoint corruption and process ownership must fail closed."""
import json
import subprocess
import sys

import pytest
from lakehouse_storage.worker_state import StateStore, NeedsReview, single_instance


def test_atomic_checkpoint_roundtrip_and_corruption(tmp_path):
    state = StateStore(tmp_path)
    record = {'run_id': 'a'*32, 'sequence': 1, 'table_name': 'sensor_telemetry', 'status': 'FETCHING', 'attempts': 0}
    state.save(record)
    assert state.load('a'*32, 1) == record
    state.path('a'*32, 1).write_text('{broken')
    with pytest.raises(NeedsReview): state.load('a'*32, 1)
    assert state.path('a'*32, 1).with_suffix('.review.json').exists()
    assert state.path('a'*32, 1).read_text() == '{broken'


def test_only_one_instance_and_release_after_kill(tmp_path):
    lock = tmp_path / 'phase.lock'
    code = "from lakehouse_storage.worker_state import single_instance; from pathlib import Path; import sys;\nwith single_instance(Path(sys.argv[1])): print('locked',flush=True); sys.stdin.read()"
    p = subprocess.Popen([sys.executable, '-c', code, str(lock)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == 'locked'
        with pytest.raises(RuntimeError, match='already running'):
            with single_instance(lock): pass
    finally:
        p.kill(); p.wait(timeout=5)
    with single_instance(lock): pass


def test_namespace_rejects_shared_escape_and_symlink(tmp_path):
    from lakehouse_storage.service_config import require_namespace
    root = tmp_path / 'storage_demo'
    root.mkdir()
    (root / 'escape').symlink_to(tmp_path, target_is_directory=True)
    for target in [root, tmp_path / 'shared', root / 'escape' / 'silver']:
        with pytest.raises(ValueError): require_namespace(target, root)
    with pytest.raises(ValueError): require_namespace('s3://smart-dairy-lakehouse/02_silver_curated/x', 's3://smart-dairy-lakehouse/02_silver_curated')


@pytest.mark.parametrize('changes', [{'attempts':True}, {'durations':None}, {'durations':{'fetch_seconds':-1}}, {'status':'FAILED','bronze_path':'/some/path'}])
def test_malformed_checkpoint_fields_need_review(tmp_path, changes):
    state = StateStore(tmp_path)
    record = {'run_id':'a'*32,'sequence':1,'table_name':'sensor_telemetry','status':'FETCHING','attempts':0, **changes}
    state.save(record)
    with pytest.raises(NeedsReview): state.load('a'*32,1)


def test_done_without_p3_evidence_is_corrupt(tmp_path):
    state = StateStore(tmp_path)
    record = {'run_id':'a'*32,'sequence':1,'table_name':'sensor_telemetry','status':'DONE','attempts':1,
              'durations':{},'bronze_path':'/some/path.parquet','bronze_sha256':'b'*64,'payload_sha256':'c'*64}
    state.save(record)
    with pytest.raises(NeedsReview): state.load('a'*32,1)


@pytest.mark.parametrize('suffix', [
    '%2E%2E/escaped/table', '%2e%2e/escaped/table', '.%2e/escaped/table',
    '%252e%252e/escaped/table', '%2f../escaped/table', '%5c../escaped/table',
    'safe/../table', 'safe/./table', 'safe//table', 'table?x=1', 'table#fragment',
    'bad%/table', 'bad%GG/table', 'table\\other', 'table\nother',
])
def test_s3_namespace_rejects_ambiguous_uri(suffix):
    from lakehouse_storage.service_config import require_namespace
    root = 's3://smart-dairy-storage-demo/storage_demo/allowed'
    with pytest.raises(ValueError):
        require_namespace(root + '/' + suffix, root)


@pytest.mark.parametrize('root', [
    's3://smart-dairy-storage-demo/storage_demo/%2e%2e/allowed',
    's3://smart-dairy-storage-demo/storage_demo/../allowed',
    's3://smart-dairy-storage-demo/storage_demo//allowed',
])
def test_s3_namespace_validates_root_too(root):
    from lakehouse_storage.service_config import require_namespace
    with pytest.raises(ValueError):
        require_namespace(root + '/table', root)


def test_s3_namespace_compares_bucket_and_segments():
    from lakehouse_storage.service_config import require_namespace
    root = 's3://smart-dairy-storage-demo/storage_demo/allowed'
    target = root + '/sensor_telemetry'
    assert require_namespace(target, root + '/') == target
    for invalid in [root, root + '_sibling/table', target.replace('s3:', 'https:'),
                    target.replace('smart-dairy-storage-demo', 'other-bucket')]:
        with pytest.raises(ValueError):
            require_namespace(invalid, root)


def test_run_target_binding_preserves_original_evidence(tmp_path):
    state = StateStore(tmp_path)
    run_id = 'd' * 32
    first = 's3://smart-dairy-storage-demo/storage_demo/a'
    state.bind_target(run_id, first)
    target = state.path(run_id, 0).parent / 'target.json'
    evidence = target.read_bytes()
    state.bind_target(run_id, first)
    with pytest.raises(NeedsReview, match='target'):
        state.bind_target(run_id, first + '_other')
    assert target.read_bytes() == evidence
    assert target.with_suffix('.review.json').exists()


def test_run_preflight_checks_later_checkpoint_before_starting(tmp_path):
    state = StateStore(tmp_path)
    run_id = 'e' * 32
    root = str(tmp_path / 'silver')
    state.bind_target(run_id, root)
    record = {'run_id': run_id, 'sequence': 9, 'table_name': 'sensor_telemetry',
              'status': 'FETCHING', 'attempts': 0, 'silver_uri': root + '_other/sensor_telemetry'}
    state.save(record)
    path = state.path(run_id, 9)
    evidence = path.read_bytes()
    with pytest.raises(NeedsReview, match='target'):
        state.validate_targets(run_id, root)
    assert path.read_bytes() == evidence


def test_legacy_sensor_checkpoint_can_bind_matching_manifest_without_rewriting(tmp_path):
    import hashlib
    import json
    from lakehouse_storage.worker_state import StateStore
    store = StateStore(tmp_path)
    run = 'a' * 32
    digest = 'b' * 64
    record = {'run_id': run, 'sequence': 1, 'table_name': 'sensor_telemetry',
              'status': 'BRONZE_READY', 'attempts': 1,
              'silver_uri': '/tmp/storage_demo/silver/sensor_telemetry',
              'payload_sha256': digest, 'bronze_sha256': 'c' * 64,
              'bronze_path': '/tmp/storage_demo/bronze/sensor.parquet'}
    store.save(record)
    original = store.path(run, 1).read_bytes()
    store.bind_manifest({'manifest_version': 2, 'run_id': run, 'batches': [
        {'sequence': 0, 'table_name': 'mes_lims'},
        {'sequence': 1, 'table_name': 'sensor_telemetry', 'source': 'S1', 'schema_variant': 'raw',
         'bronze_only': False, 'payload_sha256': digest}]})
    assert store.load(run, 1, silver_root='/tmp/storage_demo/silver')['table_name'] == 'sensor_telemetry'
    assert store.path(run, 1).read_bytes() == original
