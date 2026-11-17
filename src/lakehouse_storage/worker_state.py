"""Local bookkeeping and POSIX single-process ownership, not a Delta ledger."""
import json
import math
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path


class NeedsReview(RuntimeError):
    pass


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.' + path.name)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(value, handle, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def single_instance(path):
    import fcntl
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('demo process already running') from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class StateStore:
    STATUSES = {'FETCHING', 'PUBLISHING', 'BRONZE_READY', 'PROCESSING', 'DONE', 'REJECTED', 'FAILED', 'NEEDS_REVIEW'}

    def __init__(self, root):
        self.root = Path(root)

    def path(self, run_id, sequence):
        if not re.fullmatch('[a-f0-9]{32}', run_id) or type(sequence) is not int or sequence < 0:
            raise ValueError('invalid checkpoint identity')
        return self.root / run_id / f'{sequence}.json'

    def save(self, record):
        if record['status'] not in self.STATUSES:
            raise ValueError('invalid checkpoint status')
        atomic_json(self.path(record['run_id'], record['sequence']), record)

    def bind_target(self, run_id, silver_root):
        path = self.path(run_id, 0).parent / 'target.json'
        expected = {'run_id': run_id, 'silver_root': silver_root}
        if path.exists():
            try:
                if json.loads(path.read_text()) != expected:
                    raise ValueError('Silver target differs from the saved run target')
            except (ValueError, TypeError) as exc:
                atomic_json(path.with_suffix('.review.json'), {'status': 'NEEDS_REVIEW', 'reason': str(exc)})
                raise NeedsReview(f'checkpoint target needs review: {path}') from exc
        else:
            # Legacy evidence must identify the destination before a run can bind.
            self.validate_targets(run_id, silver_root)
            atomic_json(path, expected)

    def validate_targets(self, run_id, silver_root):
        for path in self.path(run_id, 0).parent.glob('*.json'):
            if path.stem.isdecimal():
                self.load(run_id, int(path.stem), silver_root=silver_root)

    def load(self, run_id, sequence, *, silver_root=None):
        path = self.path(run_id, sequence)
        if not path.exists():
            return None
        try:
            record = json.loads(path.read_text())
            table = 'mes_lims' if sequence == 0 else 'sensor_telemetry'
            if (record['run_id'], record['sequence'], record['table_name']) != (run_id, sequence, table):
                raise ValueError('checkpoint identity mismatch')
            if silver_root is not None and record.get('silver_uri') != f'{silver_root}/{table}':
                raise ValueError('checkpoint Silver target missing or mismatched')
            if record['status'] not in self.STATUSES:
                raise ValueError('unknown checkpoint status')
            if type(record['attempts']) is not int or record['attempts'] < 0:
                raise ValueError('invalid attempt count')
            durations = record.get('durations', {})
            if not isinstance(durations, dict) or any(type(v) not in {int, float} or not math.isfinite(v) or v < 0 for v in durations.values()):
                raise ValueError('invalid checkpoint durations')
            if record['status'] in {'BRONZE_READY', 'PROCESSING', 'DONE'} or record.get('bronze_path'):
                if not all(record.get(k) for k in ('bronze_path', 'bronze_sha256', 'payload_sha256')):
                    raise ValueError('checkpoint artifact evidence missing')
                if not isinstance(record['bronze_path'], str) or any(not isinstance(record[k], str) or not re.fullmatch('[a-f0-9]{64}', record[k]) for k in ('bronze_sha256', 'payload_sha256')):
                    raise ValueError('invalid artifact digest/path')
            if record['status'] == 'DONE':
                from lakehouse_storage.governance_adapter import classify_result
                result = record.get('p3_result')
                if not isinstance(result, dict) or classify_result(result) != 'DONE':
                    raise ValueError('DONE requires successful P3 evidence')
                if (result.get('table_name'), result.get('bronze_file')) != (table, record['bronze_path']):
                    raise ValueError('DONE P3 identity mismatch')
                if 'silver_uri' in record and result.get('silver_uri') != record['silver_uri']:
                    raise ValueError('DONE P3 target mismatch')
                if type(record.get('rows_written')) is not int or record['rows_written'] != result['rows_written']:
                    raise ValueError('DONE rows_written evidence mismatch')
            return record
        except (ValueError, KeyError, TypeError) as exc:
            atomic_json(path.with_suffix('.review.json'), {'status': 'NEEDS_REVIEW', 'reason': str(exc)})
            raise NeedsReview(f'corrupt checkpoint: {path}: {exc}') from exc
