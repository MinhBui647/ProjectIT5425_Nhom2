"""Sequential demo ingestion; known artifacts are never serialized again."""
import time
from pathlib import Path

import polars as pl
import pyarrow as pa

from lakehouse_storage.bronze_writer import write_bronze_batch, verify_checksum
from lakehouse_storage.schemas import get_bronze_schema
from lakehouse_storage.service_config import require_namespace
from lakehouse_storage.utils import sha256_file
from lakehouse_storage.worker_state import NeedsReview, StateStore


def raw_frame(envelope):
    frame = pl.from_dicts(envelope['data'], infer_schema_length=None)
    # Restore JSON's lost numeric widths, without coercing invalid values.
    for field in get_bronze_schema(envelope['table_name']):
        if field.name not in frame.columns:
            continue
        dtype = frame.schema[field.name]
        if pa.types.is_floating(field.type) and dtype.is_numeric():
            frame = frame.with_columns(pl.col(field.name).cast(pl.Float64))
        elif pa.types.is_integer(field.type) and dtype.is_integer():
            frame = frame.with_columns(pl.col(field.name).cast(pl.Int64))
    return frame


class IngestionWorker:
    def __init__(self, config, client):
        self.config, self.client = config, client
        self.bronze_root = Path(require_namespace(config.root / 'bronze', config.root))
        require_namespace(config.silver_root, config.silver_allow_root)
        self.state = StateStore(config.root / 'worker_state')

    def review(self, record, reason):
        record.update(status='NEEDS_REVIEW', reason=reason,
                      artifacts=[str(p) for p in self.bronze_root.rglob('*.parquet')])
        self.state.save(record)
        raise NeedsReview(reason)

    def publish(self, run_id, sequence):
        record = self.state.load(run_id, sequence) or {
            'run_id': run_id, 'sequence': sequence,
            'table_name': 'mes_lims' if sequence == 0 else 'sensor_telemetry',
            'status': 'FETCHING', 'attempts': 0, 'durations': {},
        }
        if record['status'] in {'PUBLISHING', 'NEEDS_REVIEW'}:
            self.review(record, 'publication outcome unknown; inspect artifacts before resuming')
        if record.get('bronze_path'):
            path = Path(require_namespace(record['bronze_path'], self.bronze_root))
            if not path.is_file() or not verify_checksum(path) or sha256_file(path) != record['bronze_sha256']:
                self.review(record, 'known Bronze checksum mismatch or missing artifact')
            return record
        record['status'] = 'FETCHING'
        record['attempts'] += 1
        self.state.save(record)
        started = time.perf_counter()
        envelope = self.client.batch(run_id, sequence)
        record['durations']['fetch_seconds'] = time.perf_counter() - started
        record['payload_sha256'] = envelope['payload_sha256']
        frame = raw_frame(envelope)
        record['status'] = 'PUBLISHING'
        self.state.save(record)
        started = time.perf_counter()
        try:
            path = write_bronze_batch(frame, envelope['table_name'], bronze_root=self.bronze_root)
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
