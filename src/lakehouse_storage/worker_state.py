"""Local bookkeeping and POSIX single-process ownership, not a Delta ledger."""
import json
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

    def load(self, run_id, sequence):
        path = self.path(run_id, sequence)
        if not path.exists():
            return None
        try:
            record = json.loads(path.read_text())
            table = 'mes_lims' if sequence == 0 else 'sensor_telemetry'
            if (record['run_id'], record['sequence'], record['table_name']) != (run_id, sequence, table):
                raise ValueError('checkpoint identity mismatch')
            if record['status'] not in self.STATUSES:
                raise ValueError('unknown checkpoint status')
            if not isinstance(record['attempts'], int) or record['attempts'] < 0:
                raise ValueError('invalid attempt count')
            if record['status'] in {'BRONZE_READY', 'PROCESSING', 'DONE'}:
                if not all(record.get(k) for k in ('bronze_path', 'bronze_sha256', 'payload_sha256')):
                    raise ValueError('checkpoint artifact evidence missing')
            return record
        except (ValueError, KeyError, TypeError) as exc:
            atomic_json(path.with_suffix('.review.json'), {'status': 'NEEDS_REVIEW', 'reason': str(exc)})
            raise NeedsReview(f'corrupt checkpoint: {path}') from exc
