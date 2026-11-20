"""Sequential demo ingestion; known artifacts are never serialized again."""
import time
from pathlib import Path

import polars as pl
import pyarrow as pa

from lakehouse_storage.bronze_writer import write_bronze_batch, verify_checksum
from lakehouse_storage.schemas import get_bronze_schema, get_bronze_schema_variants
from lakehouse_storage.service_config import require_namespace, require_local_tree
from lakehouse_storage.utils import sha256_file
from lakehouse_storage.worker_state import NeedsReview, StateStore


def raw_frame(envelope):
    frame = pl.from_dicts(envelope['data'], infer_schema_length=None)
    # Restore JSON's lost numeric widths, without coercing invalid values.
    table = envelope['table_name']
    if table == 'market_reports':
        return frame
    variants = get_bronze_schema_variants(table)
    schema = next((v for v in variants if all(f.name in frame.columns for f in v if not f.nullable)), variants[0])
    for field in schema:
        if field.name not in frame.columns:
            continue
        dtype = frame.schema[field.name]
        if pa.types.is_floating(field.type) and dtype.is_numeric():
            frame = frame.with_columns(pl.col(field.name).cast(pl.Float64))
        elif pa.types.is_integer(field.type) and dtype.is_integer():
            frame = frame.with_columns(pl.col(field.name).cast(pl.Int32 if pa.types.is_int32(field.type) else pl.Int64))
    return frame


def _valid_date(value, fmt):
    from datetime import datetime
    try:
        datetime.strptime(value, fmt)
        return True
    except (TypeError, ValueError):
        return False


class IngestionWorker:
    def __init__(self, config, client):
        self.config, self.client = config, client
        self.bronze_root = Path(require_namespace(config.root / 'bronze', config.root))
        require_namespace(config.silver_root, config.silver_allow_root)
        self.silver_root = (config.silver_root.rstrip('/') if config.silver_root.startswith('s3://')
                            else str(Path(config.silver_root).resolve()))
        self.state = StateStore(require_local_tree(config.root / 'worker_state', config.root))

    def bind_manifest(self, run_id):
        metadata = self.client.run(run_id) if callable(getattr(self.client, 'run', None)) else self.client.run
        self.state.bind_manifest(metadata)
        return metadata

    def review(self, record, reason):
        record.update(status='NEEDS_REVIEW', reason=reason,
                      artifacts=[str(p) for p in self.bronze_root.rglob('*.parquet')])
        self.state.save(record)
        raise NeedsReview(reason)

    def publish(self, run_id, sequence):
        self.state.bind_target(run_id, self.silver_root)
        self.bind_manifest(run_id)
        entry = self.state.entry(run_id, sequence)
        table = entry['table_name'] if entry else ('mes_lims' if sequence == 0 else 'sensor_telemetry')
        require_local_tree(self.bronze_root, self.config.root)
        record = self.state.load(run_id, sequence, silver_root=self.silver_root) or {
            'run_id': run_id, 'sequence': sequence,
            'table_name': table,
            **({k: entry[k] for k in ('source', 'schema_variant', 'bronze_only')} if entry else {}),
            'status': 'FETCHING', 'attempts': 0, 'durations': {},
            'silver_uri': f"{self.silver_root}/{table}",
        }
        if record['status'] in {'PUBLISHING', 'NEEDS_REVIEW'}:
            self.review(record, 'publication outcome unknown; inspect artifacts before resuming')
        if record.get('bronze_path'):
            path = Path(require_namespace(record['bronze_path'], self.bronze_root))
            if not path.is_file() or not verify_checksum(path) or sha256_file(path) != record['bronze_sha256']:
                self.review(record, 'known Bronze checksum mismatch or missing artifact')
            return record
        if record['status'] in {'REJECTED', 'NO_DATA'}:
            return record
        record.setdefault('durations', {})
        record['status'] = 'FETCHING'
        record['attempts'] += 1
        self.state.save(record)
        started = time.perf_counter()
        envelope = self.client.batch(run_id, sequence)
        record['durations']['fetch_seconds'] = time.perf_counter() - started
        if record.get('payload_sha256') and record['payload_sha256'] != envelope['payload_sha256']:
            self.review(record, 'payload identity changed across retry')
        record['payload_sha256'] = envelope['payload_sha256']
        if not envelope['data']:
            record.update(status='NO_DATA', stage='collect', rows_written=0)
            self.state.save(record)
            return record
        try:
            frame = raw_frame(envelope)
        except (ValueError, pl.exceptions.PolarsError) as exc:
            record.update(status='REJECTED', reason=str(exc), stage='bronze')
            self.state.save(record)
            return record
        if table == 'market_reports':
            try:
                for row in envelope['data']:
                    value = row['published_date']
                    if not any(_valid_date(value, fmt) for fmt in ('%Y-%m-%d', '%m/%d/%Y %H:%M:%S')):
                        raise ValueError('USDA published_date invalid')
            except (KeyError, TypeError, ValueError) as exc:
                record.update(status='REJECTED', reason=str(exc), stage='bronze')
                self.state.save(record)
                return record
        record['status'] = 'PUBLISHING'
        self.state.save(record)
        started = time.perf_counter()
        try:
            path = write_bronze_batch(frame, envelope['table_name'], bronze_root=self.bronze_root,
                                      validate_schema=table != 'market_reports')
        except ValueError as exc:
            record.update(status='REJECTED', reason=str(exc), stage='bronze')
            self.state.save(record)
            return record
        record.update(bronze_path=str(Path(path).resolve()), bronze_sha256=sha256_file(path), status='BRONZE_READY')
        record['durations']['publish_seconds'] = time.perf_counter() - started
        if not verify_checksum(path):
            self.review(record, 'published checksum mismatch')
        self.state.save(record)
        return record

    def process(self, run_id, sequence, replay=False):
        from lakehouse_storage.governance_adapter import process_file, classify_result, committed_evidence
        record = self.publish(run_id, sequence)
        if record['status'] in {'REJECTED', 'NO_DATA'} or (record['status'] == 'DONE' and not replay):
            return record
        if record['table_name'] == 'market_reports':
            record.update(status='DONE', stage='bronze_only', rows_written=0)
            self.state.save(record)
            return record
        if record['status'] == 'PROCESSING':
            record['previous_commit_outcome'] = 'unknown'
        record.update(status='PROCESSING', stage='p3')
        self.state.save(record)
        started = time.perf_counter()
        try:
            result = process_file(record['bronze_path'], record['table_name'], self.config)
            outcome = classify_result(result)
            if outcome == 'DONE':
                result['committed_evidence'] = committed_evidence(result, record['bronze_sha256'])
            record.update(status=outcome, p3_result=result,
                          rows_written=result['rows_written'])
        except Exception as exc:
            record.update(status='FAILED', reason=str(exc), rows_written=None)
        record['durations']['p3_seconds'] = time.perf_counter() - started
        self.state.save(record)
        return record

    def run(self, run_id, limit=None, skip_rejected=False, stop=None):
        import threading
        stop = stop or threading.Event()
        self.state.bind_target(run_id, self.silver_root)
        self.state.validate_targets(run_id, self.silver_root)
        metadata = self.bind_manifest(run_id)
        records, processed = [], 0
        for sequence in range(metadata['batch_count']):
            if stop.is_set():
                break
            existing = self.state.load(run_id, sequence, silver_root=self.silver_root)
            if existing and (existing['status'] == 'DONE' or (skip_rejected and existing['status'] == 'REJECTED')):
                continue
            if limit is not None and processed >= limit:
                break
            try:
                record = self.process(run_id, sequence)
            except NeedsReview:
                raise
            except Exception as exc:
                record = self.state.load(run_id, sequence)
                if record is None:
                    raise
                if record['status'] == 'PUBLISHING':
                    self.review(record, f'publication outcome unknown: {exc}')
                record.update(status='FAILED', reason=str(exc), stage='fetch', rows_written=None)
                self.state.save(record)
            records.append(record)
            processed += 1
            if record['status'] != 'DONE' and not (skip_rejected and record['status'] == 'REJECTED'):
                break
        return records


def main():
    import argparse
    import json
    import os
    import signal
    import threading
    import requests
    from lakehouse_storage.mock_client import MockClient
    from lakehouse_storage.service_config import ServiceConfig
    from lakehouse_storage.worker_state import atomic_json, single_instance

    parser = argparse.ArgumentParser(description='Single demo worker; stop before maintenance.')
    parser.add_argument('--run-id', default=os.getenv('STORAGE_DEMO_RUN_ID'))
    parser.add_argument('--create-run', action='store_true', help='Create once and reuse saved active run on restart')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--continuous', action='store_true', help='Wait for shutdown after the finite run completes')
    parser.add_argument('--skip-rejected', action='store_true')
    parser.add_argument('--replay-sequence', type=int)
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error('--limit must be positive')
    config = ServiceConfig.from_env()
    client = MockClient(config.api_url, timeout=float(os.getenv('MOCK_TIMEOUT_SECONDS', '15')),
                        max_attempts=int(os.getenv('MOCK_MAX_ATTEMPTS', '3')))
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    with single_instance(config.root / 'demo-phase.lock'):
        active = config.root / 'active_run.json'
        run_id = args.run_id
        if not run_id and args.create_run:
            if active.exists():
                run_id = json.loads(active.read_text())['run_id']
            else:
                # POST is deliberately not retried: creation is not idempotent.
                response = requests.post(config.api_url + '/runs', json=json.loads(os.getenv('MOCK_RUN_REQUEST', '{}')), timeout=120)
                response.raise_for_status()
                run_id = response.json()['run_id']
                atomic_json(active, {'run_id': run_id})
        if not run_id:
            parser.error('provide --run-id or --create-run')
        worker = IngestionWorker(config, client)
        records = ([worker.process(run_id, args.replay_sequence, replay=True)] if args.replay_sequence is not None
                   else worker.run(run_id, limit=1 if args.once else args.limit, skip_rejected=args.skip_rejected, stop=stop))
        summary = {'run_id': run_id, 'records': records,
                   'rows_written_reported': sum(r.get('rows_written') or 0 for r in records),
                   'http_attempts': client.attempts}
        atomic_json(config.root / 'worker_summary.json', summary)
        print(json.dumps(summary), flush=True)
        failed = any(r['status'] in {'FAILED', 'NEEDS_REVIEW'} or (r['status'] == 'REJECTED' and not args.skip_rejected) for r in records)
        if args.continuous and not failed:
            while not stop.wait(0.5):
                pass
        return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
