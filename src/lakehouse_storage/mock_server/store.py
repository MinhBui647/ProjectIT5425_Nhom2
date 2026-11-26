"""Immutable payload bytes with an atomic run publication boundary."""
import hashlib
import json
import os
import re
import uuid
from pathlib import Path

from lakehouse_storage.mock_server.generator import generate_batches
from lakehouse_storage.mock_server.models import TABLE_SOURCES
import fcntl
from lakehouse_storage.mock_server.scenarios import apply_payload_fault

MAX_RESPONSE_BYTES = 32 * 1024 * 1024


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


class RunStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, run_id):
        if not re.fullmatch(r'[a-f0-9]{32}', run_id):
            raise ValueError('invalid run id')
        return self.root / run_id

    def create(self, request, idempotency_key=None, batch_factory=None):
        with (self.root / 'create.lock').open('a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            return self._create(request, idempotency_key, batch_factory)

    def _create(self, request, idempotency_key, batch_factory):
        config = request.model_dump(mode='json')
        run_id = (hashlib.sha256(idempotency_key.encode()).hexdigest()[:32]
                  if idempotency_key is not None else uuid.uuid4().hex)
        if self._path(run_id).exists():
            existing = self.run(run_id)
            if existing['request'] != config:
                raise ValueError('idempotency key config conflict')
            return existing
        temporary = self.root / f'.{run_id}.tmp'
        if temporary.exists():
            raise ValueError('staging outcome unknown; needs review')
        temporary.mkdir()
        metadata = {'run_id': run_id, 'manifest_version': 2, 'batches': [], 'request': config}
        for sequence, (table, rows) in enumerate(batch_factory(run_id) if batch_factory else generate_batches(request, run_id)):
            if table == request.fault_table:
                rows = apply_payload_fault(rows, request.scenario, table)
            variant = ('wide' if rows and 'Month' in rows[0] else 'long') if table == 'market_indices' else 'raw'
            entry = {'sequence': sequence, 'table_name': table, 'source': TABLE_SOURCES[table],
                     'schema_variant': variant, 'bronze_only': table == 'market_reports',
                     'window': {'start': request.start_time.isoformat(), 'end': request.end_time.isoformat()},
                     'payload_sha256': hashlib.sha256(canonical_json(rows)).hexdigest()}
            metadata['batches'].append(entry)
            envelope = {**entry, 'run_id': run_id, 'sequence': sequence, 'table_name': table,
                        'payload_sha256': hashlib.sha256(canonical_json(rows)).hexdigest(), 'data': rows}
            content = canonical_json(envelope)
            if len(content) > MAX_RESPONSE_BYTES:
                raise ValueError('response exceeds limit')
            (temporary / f'{sequence}.json').write_bytes(content)
        metadata['batch_count'] = len(metadata['batches'])
        (temporary / 'run.json').write_bytes(canonical_json(metadata))
        os.replace(temporary, self._path(run_id))
        return metadata

    def run(self, run_id):
        return json.loads((self._path(run_id) / 'run.json').read_bytes())

    def batch(self, run_id, sequence):
        metadata = self.run(run_id)
        if not 0 <= sequence < metadata['batch_count']:
            raise FileNotFoundError('unknown sequence')
        return (self._path(run_id) / f'{sequence}.json').read_bytes()
