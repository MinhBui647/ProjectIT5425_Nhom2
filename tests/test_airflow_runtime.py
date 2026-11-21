"""Scheduler/API integration on an explicitly isolated Compose QA project.

Run from the host (Docker CLI + requests), not in a task container. No Docker
socket is mounted into the application. All test runs use fresh UUID identities.
"""
import hashlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.getenv('LAKEHOUSE_AIRFLOW_TEST') != '1' or os.getenv('STORAGE_DEMO_MINIO_TEST') != '1',
    reason='real scheduler and isolated MinIO flags required')]
URL = os.getenv('LAKEHOUSE_TEST_API', 'http://127.0.0.1:28080')
COMPOSE = ['docker', 'compose', '-p', os.getenv('LAKEHOUSE_TEST_PROJECT', 'lakehouse-workflow-qa'), '-f', 'infra/docker-compose.lakehouse.yml']


def session():
    s = requests.Session()
    deadline = time.monotonic() + 120
    while True:
        try:
            response = s.get(URL + '/auth/token', timeout=10)
            if response.ok:
                break
        except requests.RequestException:
            pass
        if time.monotonic() > deadline:
            pytest.fail('Airflow API not ready')
        time.sleep(1)
    response.raise_for_status()
    s.headers['Authorization'] = 'Bearer ' + response.json()['access_token']
    return s


def api(s, method, path, **kwargs):
    response = s.request(method, URL + '/api/v2' + path, timeout=15, **kwargs)
    response.raise_for_status()
    return response.json()


def trigger(s, scenario='normal', *, dag='lakehouse_pipeline', extra=None):
    run = 'qa-' + scenario + '-' + uuid.uuid4().hex[:12]
    api(s, 'PATCH', f'/dags/{dag}', json={'is_paused': False})
    conf = {'window_start': '2026-01-15T00:00:00Z', 'window_end': '2026-01-16T00:00:00Z',
            'request': {'scenario': scenario, 'fao_variant': 'both', 'samples_per_batch': 20, 'batches': 2}}
    if extra:
        conf.update(extra)
    api(s, 'POST', f'/dags/{dag}/dagRuns', json={'dag_run_id': run, 'logical_date': None, 'conf': conf})
    return run


def wait_run(s, run, dag='lakehouse_pipeline', timeout=600):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = api(s, 'GET', f'/dags/{dag}/dagRuns/{run}')
        if result['state'] in {'success', 'failed'}:
            return result
        time.sleep(1)
    pytest.fail(f'DAG did not terminate: {dag}/{run}')


def tasks(s, run):
    return {t['task_id']: t for t in api(s, 'GET', f'/dags/lakehouse_pipeline/dagRuns/{run}/taskInstances')['task_instances']}


def container_python(code, *args):
    result = subprocess.run(COMPOSE + ['exec', '-T', 'airflow-scheduler', '/opt/processing/bin/python', '-c', code, *args],
                            capture_output=True, text=True, check=True, timeout=60)
    return json.loads(result.stdout.strip().splitlines()[-1])


def manifest_path(run):
    digest = hashlib.sha256(('lakehouse_pipeline/' + run).encode()).hexdigest()
    return '/opt/lakehouse/storage_demo/runs/' + digest + '.json'


def manifest(run):
    return container_python('import json,sys; print(open(sys.argv[1]).read())', manifest_path(run))


def record(name, value):
    directory = Path(os.getenv('LAKEHOUSE_RUNTIME_EVIDENCE', '/tmp/lakehouse-runtime-evidence'))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / (name + '.json')).write_text(json.dumps(value, indent=2))


def test_full_scheduler_run_restart_and_clear_no_new_data():
    s = session()
    run = trigger(s)
    assert wait_run(s, run)['state'] == 'success', tasks(s, run)
    first = manifest(run)
    assert first['status'] == 'SUCCESS' and len(first['silver_snapshot']) == 6
    assert all(t['state'] == 'success' for t in tasks(s, run).values())
    other = trigger(s, extra={'request': {'sources': ['sensor_telemetry'], 'seed': 43,
                                         'samples_per_batch': 20, 'batches': 2}})
    assert wait_run(s, other)['state'] == 'success', tasks(s, other)
    assert manifest(other)['silver_snapshot']['sensor_telemetry']['version'] > first['silver_snapshot']['sensor_telemetry']['version']
    subprocess.run(COMPOSE + ['restart', 'airflow-scheduler', 'mock'], check=True, capture_output=True, timeout=90)
    # Clearing reuses the same durable prepare identity and original Bronze paths.
    api(s, 'POST', f'/dags/lakehouse_pipeline/dagRuns/{run}/clear', json={'dry_run': False})
    assert wait_run(s, run)['state'] == 'success', tasks(s, run)
    second = manifest(run)
    assert second['mock_run_id'] == first['mock_run_id']
    assert second['silver_snapshot'] == first['silver_snapshot']
    assert [r['bronze_path'] for r in second['artifacts']] == [r['bronze_path'] for r in first['artifacts']]
    pinned_rows = container_python("""
import json,sys
from lakehouse_pipeline.silver_snapshot import read_snapshot
value=json.load(open(sys.argv[1]))
print(json.dumps({name:read_snapshot(value['silver_snapshot'],name).height for name in value['silver_snapshot']}))
""", manifest_path(run))
    assert pinned_rows == {name: entry['total_rows'] for name, entry in first['silver_snapshot'].items()}
    record('normal-restart-clear', {'run_id': run, 'other_run_id': other, 'pinned_rows': pinned_rows,
                                   'manifest': second, 'tasks': tasks(s, run)})


@pytest.mark.parametrize('scenario', ['transient-500', 'invalid-value', 'missing-field'])
def test_scheduler_transient_and_business_failures(scenario):
    s = session()
    run = trigger(s, scenario)
    expected = 'success' if scenario == 'transient-500' else 'failed'
    assert wait_run(s, run)['state'] == expected, tasks(s, run)
    evidence = manifest(run)
    state = tasks(s, run)
    if expected == 'failed':
        assert state['report_run']['state'] == 'success'
        assert state['finalize_run']['state'] == 'upstream_failed'
        assert evidence['status'] == 'FAILED'
        assert any(r.get('p3_result', {}).get('committed_evidence') for r in evidence['artifacts'])
        failing = state['govern_sensor_telemetry' if scenario == 'invalid-value' else 'bronze_sensor_telemetry']
        assert failing['try_number'] == 1
    record(scenario, {'run_id': run, 'manifest': evidence, 'tasks': state})


def test_scheduler_checksum_failure_blocks_success():
    s = session()
    run = trigger(s)
    assert wait_run(s, run)['state'] == 'success'
    first = manifest(run)
    artifact = next(r for r in first['artifacts'] if r['table_name'] == 'sensor_telemetry')
    sidecar = str(Path(artifact['bronze_path']).with_suffix('.sha256'))
    original = container_python('import json,sys; from pathlib import Path; print(json.dumps(Path(sys.argv[1]).read_text())); Path(sys.argv[1]).write_text("0"*64)', sidecar)
    try:
        api(s, 'POST', '/dags/lakehouse_pipeline/clearTaskInstances', json={'dry_run': False, 'only_failed': False,
            'dag_run_id': run, 'task_ids': ['verify_sensor_telemetry'], 'include_downstream': True})
        assert wait_run(s, run)['state'] == 'failed'
        state = tasks(s, run)
        assert state['verify_sensor_telemetry']['state'] == 'failed'
        assert state['report_run']['state'] == 'success'
        assert manifest(run)['status'] == 'NEEDS_REVIEW'
        record('checksum-failure', {'run_id': run, 'tasks': state, 'manifest': manifest(run)})
    finally:
        container_python('import json,sys; from pathlib import Path; Path(sys.argv[1]).write_text(sys.argv[2]); print("{}")', sidecar, original)


def test_maintenance_scheduler_uses_phase_lock_and_preserves_rows():
    s = session()
    probe = """
import json
import polars as pl
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.config import get_storage_options
from deltalake import DeltaTable
uri=ServiceConfig.from_env().silver_root+'/sensor_telemetry'
table=DeltaTable(uri,storage_options=get_storage_options())
print(json.dumps({'rows':pl.read_delta(uri,storage_options=get_storage_options()).height,'version':table.version()}))
"""
    before = container_python(probe)
    run = trigger(s, dag='lakehouse_maintenance', extra={'action': 'compact'})
    assert wait_run(s, run, dag='lakehouse_maintenance')['state'] == 'success'
    after = container_python(probe)
    assert after['rows'] == before['rows']
    history = container_python("import json; from pathlib import Path; print(json.dumps([json.loads(l) for l in Path('/opt/lakehouse/storage_demo/maintenance.jsonl').read_text().splitlines()]))")
    assert any(r['decision'] == 'RUN' for r in history)
    record('maintenance-dag', {'run_id': run, 'state': 'success', 'before': before, 'after': after, 'history': history})


def test_two_dags_serialize_the_same_mutation_pool():
    from datetime import datetime
    s = session()
    ingest = trigger(s)
    maintain = trigger(s, dag='lakehouse_maintenance', extra={'action': 'vacuum_preview'})
    assert wait_run(s, ingest)['state'] == 'success'
    assert wait_run(s, maintain, dag='lakehouse_maintenance')['state'] == 'success'
    maintenance_tasks = api(s, 'GET', f'/dags/lakehouse_maintenance/dagRuns/{maintain}/taskInstances')['task_instances']
    intervals = []
    for task in [*tasks(s, ingest).values(), *maintenance_tasks]:
        assert task['pool'] == 'lakehouse_mutation'
        if task.get('start_date') and task.get('end_date'):
            intervals.append((datetime.fromisoformat(task['start_date']), datetime.fromisoformat(task['end_date']), task['task_id']))
    intervals.sort()
    assert all(previous[1] <= current[0] for previous, current in zip(intervals, intervals[1:]))
    record('two-dag-pool', {'ingest_run': ingest, 'maintenance_run': maintain,
                           'intervals': [[a.isoformat(), b.isoformat(), name] for a, b, name in intervals]})
