"""Finite, restartable business stages shared by the CLI and Airflow."""
import time
from pathlib import Path
from lakehouse_storage.ingestion_worker import IngestionWorker
from lakehouse_storage.mock_client import MockClient
from lakehouse_storage.mock_server.models import RunRequest
from lakehouse_storage.mock_server.store import RunStore
from lakehouse_storage.service_config import require_namespace, require_local_tree
from lakehouse_storage.worker_state import NeedsReview, single_instance
from lakehouse_pipeline.run_manifest import ManifestStore
from lakehouse_pipeline.source_config import SourceConfig
from lakehouse_pipeline.silver_snapshot import capture_snapshot, validate_snapshot


class BusinessFailure(ValueError):
    """Rejected data/config or incomplete required source; do not retry."""


class TransientFailure(RuntimeError):
    """Infrastructure failure with a finite outer retry budget."""


class CollectedClient:
    def __init__(self, root):
        self.store = RunStore(root)
    def run(self, run_id):
        return self.store.run(run_id)
    def batch(self, run_id, sequence):
        from lakehouse_storage.mock_client import decode_batch
        metadata = self.run(run_id)
        return decode_batch(self.store.batch(run_id, sequence), run_id, sequence, metadata['batches'][sequence])


def _client(config, value, client=None):
    return client or (MockClient(config.api_url) if value['config']['mode'] == 'mock'
                      else CollectedClient(config.root / 'collected'))


def prepare_run(config, identity, request, *, source_config=None, client=None):
    require_namespace(config.silver_root, config.silver_allow_root)
    model = RunRequest(**request)
    source = source_config or SourceConfig(sources=model.sources, start=model.start_time, end=model.end_time)
    if tuple(model.sources) != source.sources or (model.start_time, model.end_time) != source.utc_window():
        raise BusinessFailure('source/request window or source selection mismatch')
    settings = {'sources': list(source.sources), 'optional_sources': list(source.optional_sources),
                'window': [t.isoformat() for t in source.utc_window()], 'mode': source.mode,
                'request': model.model_dump(mode='json'), 'root': str(config.root.resolve()),
                'silver_root': config.silver_root.rstrip('/'), 'expected_recalls': source.expected_recalls,
                'farms': [list(f) for f in source.farms]}
    store = ManifestStore(config.root)
    with single_instance(config.root / 'demo-phase.lock'):
        value = store.prepare(identity, settings)
        path = store.path(identity)
        if value.get('mock_run_id'):
            return path
        try:
            if source.mode == 'mock':
                metadata = (client or MockClient(config.api_url)).create(settings['request'], 'pipeline-' + path.stem)
            else:
                collected = RunStore(require_local_tree(config.root / 'collected', config.root))
                if source.mode == 'live':
                    from lakehouse_pipeline.sources import collect_live
                    metadata = collected.create(model, 'pipeline-' + path.stem, lambda _: collect_live(source))
                else:
                    metadata = collected.create(model, 'pipeline-' + path.stem)
            value.update(mock_run_id=metadata['run_id'], status='RUNNING')
            store.save(path, value)
            return path
        except Exception as exc:
            value.update(status='FAILED', error=str(exc))
            store.save(path, value)
            raise


def execute_stage(config, path, stage, table=None, *, client=None):
    """Lock the whole stage, persist partial evidence even on failure."""
    store = ManifestStore(config.root)
    with single_instance(config.root / 'demo-phase.lock'):
        value = store.load(path)
        if value['config']['silver_root'] != config.silver_root.rstrip('/') or value['config']['root'] != str(config.root.resolve()):
            raise BusinessFailure('run target/root changed')
        worker = IngestionWorker(config, _client(config, value, client))
        key = stage + (':' + table if table else '')
        started = time.perf_counter()
        metadata = None
        try:
            if stage in {'snapshot', 'finalize'}:
                snapshot_stage = value['stages'].get('snapshot', {})
                pinned = (snapshot_stage.get('captured') is True or snapshot_stage.get('status') == 'SUCCESS'
                          or value['status'] == 'SUCCESS' or bool(value.get('silver_snapshot')))
                if pinned:
                    # Preserve legacy capture evidence before any retry can fail.
                    value['stages'].setdefault('snapshot', {})['captured'] = True
            metadata = worker.bind_manifest(value['mock_run_id'])
            if set(value['config']['sources']) - {b['table_name'] for b in metadata['batches']}:
                raise BusinessFailure('required source absent from manifest')
            if stage in {'bronze', 'verify', 'govern'}:
                if table not in value['config']['sources']:
                    raise BusinessFailure('table not selected')
                for entry in metadata['batches']:
                    if entry['table_name'] != table:
                        continue
                    if stage == 'verify':
                        known = worker.state.load(metadata['run_id'], entry['sequence'], silver_root=worker.silver_root)
                        if not known or (not known.get('bronze_path') and known['status'] != 'NO_DATA'):
                            raise BusinessFailure('verify requires published Bronze evidence')
                    record = (worker.process(metadata['run_id'], entry['sequence']) if stage == 'govern'
                              else worker.publish(metadata['run_id'], entry['sequence']))
                    if record['status'] in {'REJECTED', 'NEEDS_REVIEW'}:
                        raise BusinessFailure(record.get('reason', 'batch rejected'))
                    if record['status'] == 'FAILED':
                        raise TransientFailure(record.get('reason') or record.get('p3_result', {}).get('reason', 'infrastructure failure'))
                    if record['status'] == 'NO_DATA' and table not in value['config']['optional_sources']:
                        raise BusinessFailure(f'required source NO_DATA: {table}')
            elif stage in {'snapshot', 'finalize'}:
                records = _records(worker, metadata, require_complete=True)
                for record in records:
                    if record.get('bronze_path'):
                        worker.publish(metadata['run_id'], record['sequence'])
                if pinned:
                    try:
                        validate_snapshot(value.get('silver_snapshot'), records, value['config']['sources'], value['config']['optional_sources'])
                    except (ValueError, KeyError, TypeError) as exc:
                        raise NeedsReview(f'pinned snapshot needs review: {exc}') from exc
                elif stage == 'snapshot':
                    value['silver_snapshot'] = capture_snapshot(records, value['config']['sources'], value['config']['optional_sources'])
                    value['stages'].setdefault('snapshot', {})['captured'] = True
                else:
                    raise BusinessFailure('snapshot stage not complete')
                if stage == 'finalize':
                    value['status'] = 'SUCCESS'
            else:
                raise BusinessFailure('unknown pipeline stage')
            value['stages'].setdefault(key, {}).update(status='SUCCESS', duration_seconds=time.perf_counter() - started)
            value['stages'][key].pop('error', None)
        except Exception as exc:
            value['status'] = 'NEEDS_REVIEW' if isinstance(exc, NeedsReview) else 'FAILED'
            value['stages'].setdefault(key, {}).update(status=value['status'], error=str(exc), duration_seconds=time.perf_counter() - started)
            raise
        finally:
            if metadata:
                value['artifacts'] = _records(worker, metadata, tolerate_corrupt=True)
            store.save(path, value)
        return {'manifest_path': str(path), 'stage': key, 'status': value['stages'][key]['status']}


def _records(worker, metadata, tolerate_corrupt=False, require_complete=False):
    records = []
    for entry in metadata['batches']:
        try:
            record = worker.state.load(metadata['run_id'], entry['sequence'], silver_root=worker.silver_root)
        except NeedsReview:
            if not tolerate_corrupt:
                raise
            record = {**entry, 'status': 'NEEDS_REVIEW'}
        if record is None and require_complete:
            raise BusinessFailure(f"missing checkpoint: sequence {entry['sequence']} table {entry['table_name']}")
        if record:
            records.append(record)
    return records


def run_pipeline(config, path, *, client=None):
    value = ManifestStore(config.root).load(path)
    for table in value['config']['sources']:
        for stage in ('bronze', 'verify', 'govern'):
            execute_stage(config, path, stage, table, client=client)
    for stage in ('snapshot', 'finalize'):
        execute_stage(config, path, stage, client=client)
    return ManifestStore(config.root).load(path)
