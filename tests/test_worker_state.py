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
