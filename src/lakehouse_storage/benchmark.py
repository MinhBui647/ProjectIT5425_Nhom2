"""Storage-layout benchmark through the real HTTP/P3 ingestion path."""
import csv
import importlib.metadata
import os
import platform
import shutil
import time
from pathlib import Path

import requests
from deltalake import DeltaTable
from lakehouse_storage.config import get_storage_options

from lakehouse_storage.benchmark_workloads import QUERY_TEXT, business_frame, fingerprint, measure, query, verify_equal
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.maintenance_policy import MaintenancePolicy
from lakehouse_storage.maintenance_runner import MaintenanceRunner, snapshot
from lakehouse_storage.mock_client import MockClient
from lakehouse_storage.service_config import ServiceConfig, require_namespace
from lakehouse_storage.worker_state import atomic_json


def environment(report_dir):
    packages = {name: importlib.metadata.version(name) for name in
                ['polars', 'pyarrow', 'deltalake', 'numpy', 'great_expectations', 'fastapi']}
    limits = {name: Path('/sys/fs/cgroup', name).read_text().strip()
              for name in ['memory.max', 'cpu.max'] if Path('/sys/fs/cgroup', name).exists()}
    cpuinfo = Path('/proc/cpuinfo')
    cpu = next((line.split(':',1)[1].strip() for line in cpuinfo.read_text().splitlines() if line.startswith('model name')), platform.processor()) if cpuinfo.exists() else platform.processor()
    return {'python': platform.python_version(), 'platform': platform.platform(), 'cpu': cpu,
            'implementation_revision': os.getenv('STORAGE_DEMO_REVISION', 'unknown'),
            'cpu_count': os.cpu_count(), 'ram_bytes': os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES'),
            'disk_free_bytes': shutil.disk_usage(report_dir).free, 'container_limits': limits, 'packages': packages}


def run_benchmark(*, mock_url, work_root, silver_root, allow_root, report_dir,
                  rows=10000, batch_sizes=None, repeats=5, warmups=1, seed=42):
    batch_sizes = [1000, 10000] if batch_sizes is None else batch_sizes
    if (not 1 <= rows <= 1_000_000 or repeats < 5 or warmups < 1 or not batch_sizes
            or len(set(batch_sizes)) != len(batch_sizes)
            or any(size < 1 or size > 50000 or rows % size or rows // size > 200 for size in batch_sizes)):
        raise ValueError('invalid scale/batch sizes/repetitions; sizes must divide rows, max 200 batches')
    work_root, report_dir = Path(work_root).resolve(), Path(report_dir).resolve()
    require_namespace(work_root / 'batch_0', work_root)
    require_namespace(silver_root, allow_root)
    if work_root.exists() or report_dir.exists():
        raise ValueError('benchmark requires fresh work and report directories')
    report_dir.mkdir(parents=True)
    result = {'environment': environment(report_dir), 'seed': seed, 'rows': rows, 'warmups': warmups,
              'repeats': repeats, 'mock_url': mock_url, 'silver_root': silver_root, 'query_text': QUERY_TEXT,
              'cache_policy': 'uncontrolled OS/object cache; warmup before each workload; baseline then compacted in listed order',
              'configurations': []}
    reference = None
    runner = MaintenanceRunner(report_dir / 'operations.jsonl')
    for size in batch_sizes:
        remote = silver_root.rstrip('/') + f'/batch_{size}'
        if any(DeltaTable.is_deltatable(remote + '/' + table, storage_options=get_storage_options())
               for table in ('sensor_telemetry', 'mes_lims')):
            raise ValueError('benchmark refuses existing Delta targets')
        response = requests.post(mock_url.rstrip('/') + '/runs', json={
            'samples_per_batch': size, 'batches': rows // size, 'seed': seed}, timeout=120)
        response.raise_for_status()
        run = response.json()
        local = work_root / f'batch_{size}'
        remote = silver_root.rstrip('/') + f'/batch_{size}'
        config = ServiceConfig(local, remote, allow_root, mock_url)
        worker = IngestionWorker(config, MockClient(mock_url))
        start = time.perf_counter()
        records = worker.run(run['run_id'])
        duration = time.perf_counter() - start
        if len(records) != run['batch_count'] or any(r['status'] != 'DONE' for r in records):
            raise RuntimeError(f'benchmark ingestion failed: {records}')
        uri = remote + '/sensor_telemetry'
        accepted = business_frame(uri)
        written = sum(r['rows_written'] for r in records if r['table_name'] == 'sensor_telemetry')
        if accepted.height != written:
            raise ValueError('accepted rows differ from P3 write evidence')
        if reference is not None:
            verify_equal(reference, accepted)
        reference = accepted
        before = snapshot(uri)
        # Business batch id comes from the persisted mock, not the quality run id.
        batch_id = worker.client.batch(run['run_id'], 1)['data'][0]['batch_id']
        expected = {name: query(uri, before['version'], name, batch_id) for name in QUERY_TEXT}
        baseline = {name: measure(lambda n=name: query(uri, before['version'], n, batch_id), repeats=repeats, warmups=warmups) for name in QUERY_TEXT}
        operation = runner.run(MaintenancePolicy(uri, allow_root, file_count_threshold=2, cooldown_seconds=0), enabled=True, ingestion_stopped=True)
        if operation['decision'] == 'FAIL':
            raise RuntimeError(operation['error'])
        after = snapshot(uri)
        for name in QUERY_TEXT:
            verify_equal(expected[name], query(uri, after['version'], name, batch_id))
        verify_equal(accepted, business_frame(uri))
        optimized = {name: measure(lambda n=name: query(uri, after['version'], n, batch_id), repeats=repeats, warmups=warmups) for name in QUERY_TEXT}
        sensor_records = [r for r in records if r['table_name'] == 'sensor_telemetry']
        entry = {'batch_size': size, 'mock_run_id': run['run_id'], 'table_uri': uri,
                 'input_sensor_rows': rows, 'accepted_sensor_rows': accepted.height,
                 'rejected_sensor_rows': sum(r['p3_result'].get('rejected_rows', 0) for r in sensor_records),
                 'accepted_business_sha256': fingerprint(accepted), 'correctness_verified': True,
                 'pipeline_seconds': duration, 'input_rows_per_second': rows / duration,
                 'throughput_scope': 'HTTP + Bronze + P3 validation/reporting + Delta (including MES)',
                 'worker_outcomes': {status: sum(r['status'] == status for r in records) for status in ['DONE','REJECTED','FAILED']},
                 'stage_seconds': {stage: sum(r['durations'].get(stage,0) for r in records) for stage in ['fetch_seconds','publish_seconds','p3_seconds']},
                 'before': before, 'after': after, 'compaction_seconds': operation['duration_seconds'],
                 'queries': {'baseline': baseline, 'compacted': optimized}}
        result['configurations'].append(entry)
        atomic_json(report_dir / 'metrics.json', result)
    with (report_dir / 'queries.csv').open('w', newline='') as handle:
        writer = csv.writer(handle); writer.writerow(['batch_size', 'layout', 'query', 'repeat', 'seconds'])
        for entry in result['configurations']:
            for layout, queries in entry['queries'].items():
                for name, metric in queries.items():
                    writer.writerows((entry['batch_size'], layout, name, i+1, seconds) for i, seconds in enumerate(metric['raw_seconds']))
    return result


def main():
    import argparse
    import json
    import uuid
    from datetime import datetime, timezone
    parser = argparse.ArgumentParser(description='Fresh isolated benchmark, HTTP/P3 included; no vacuum.')
    parser.add_argument('--mock-url', default=os.getenv('MOCK_URL', 'http://127.0.0.1:18010'))
    parser.add_argument('--rows', type=int, default=100000)
    parser.add_argument('--batch-sizes', default='1000,10000,50000')
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid.uuid4().hex[:8])
    parser.add_argument('--silver-root', help='Fresh child of the allowed demo/benchmark prefix')
    parser.add_argument('--allow-root', default='s3://smart-dairy-storage-demo/storage_benchmark')
    args = parser.parse_args()
    import re
    if not re.fullmatch('[A-Za-z0-9_-]{1,80}', args.run_id):
        parser.error('invalid run id')
    work_root = Path('data/storage_benchmark') / args.run_id
    report_dir = Path('reports/storage') / args.run_id
    result = run_benchmark(mock_url=args.mock_url, work_root=work_root,
                           silver_root=args.silver_root or args.allow_root.rstrip('/') + '/' + args.run_id,
                           allow_root=args.allow_root, report_dir=report_dir, rows=args.rows,
                           batch_sizes=[int(s) for s in args.batch_sizes.split(',')],
                           repeats=args.repeats, warmups=args.warmups, seed=args.seed)
    atomic_json(report_dir / 'command.json', {'argv': __import__('sys').argv})
    print(json.dumps({'report_dir': str(report_dir), 'configurations': len(result['configurations'])}))


if __name__ == '__main__':
    main()
