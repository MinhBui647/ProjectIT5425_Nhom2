"""Airflow-only wrappers; processing libraries load in a separate interpreter."""
import json
import os
import subprocess
from datetime import datetime, timezone

FULL_SOURCES = ['mes_lims', 'sensor_telemetry', 'market_prices', 'market_indices', 'weather', 'food_recalls', 'market_reports']


def resolve_window(conf, run_type, interval_start, interval_end):
    if str(run_type) in {'manual', 'DagRunType.MANUAL'}:
        start, end = conf.get('window_start'), conf.get('window_end')
    else:
        start, end = interval_start, interval_end
    def utc(value):
        value = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if value is None or value.tzinfo is None:
            raise ValueError('explicit timezone-aware window required')
        return value.astimezone(timezone.utc)
    start, end = utc(start), utc(end)
    if start >= end:
        raise ValueError('non-empty data interval required')
    return start.isoformat(), end.isoformat()


def invoke_processing(arguments, *, module='lakehouse_pipeline'):
    from airflow.exceptions import AirflowException, AirflowFailException
    try:
        result = subprocess.run([os.getenv('PROCESSING_PYTHON', '/opt/processing/bin/python'), '-m', module, '--stdin'],
                                input=json.dumps(arguments), text=True, capture_output=True, timeout=540)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AirflowException('processing process unavailable or timed out') from exc
    try:
        payload = json.loads(result.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError) as exc:
        raise AirflowException('processing response missing; inspect durable manifest/checkpoint') from exc
    if result.returncode == 2 or payload.get('category') == 'business':
        raise AirflowFailException(payload.get('error', 'business stage failed'))
    if result.returncode or payload.get('status') == 'FAILED':
        raise AirflowException(payload.get('error', 'processing infrastructure failed'))
    return payload


def prepare_task(**context):
    from airflow.exceptions import AirflowFailException
    conf = context['dag_run'].conf or {}
    try:
        start, end = resolve_window(conf, context['dag_run'].run_type,
                                    context.get('data_interval_start'), context.get('data_interval_end'))
        request = {'sources': FULL_SOURCES, 'samples_per_batch': 80, 'batches': 2, **conf.get('request', {})}
        request.update(start_time=start, end_time=end)
    except (TypeError, ValueError) as exc:
        raise AirflowFailException(str(exc)) from exc
    result = invoke_processing({'stage': 'prepare', 'identity': context['dag'].dag_id + '/' + context['run_id'], 'request': request})
    return {**result, 'sources': request['sources']}


def stage_task(stage, table=None, **context):
    prepared = context['ti'].xcom_pull(task_ids='prepare_run')
    if table and table not in prepared['sources']:
        return {'status': 'NOT_SELECTED'}
    return invoke_processing({'stage': stage, 'manifest': prepared['manifest_path'], 'table': table})


def report_task(**context):
    """An all_done observer always has an all_success finalizer downstream."""
    prepared = context['ti'].xcom_pull(task_ids='prepare_run')
    if not prepared:
        return {'status': 'NO_PREPARE_EVIDENCE'}
    from pathlib import Path
    manifest = json.loads(Path(prepared['manifest_path']).read_text())
    return {'manifest_path': prepared['manifest_path'], 'status': manifest['status'],
            'completed_stages': len(manifest['stages'])}


def maintenance_task(**context):
    conf = context['dag_run'].conf or {}
    return invoke_processing({'action': conf.get('action', 'compact')}, module='lakehouse_storage.maintenance_cli')
