"""Immutable payload bytes with an atomic run publication boundary."""
import hashlib
import json
import os
import re
import uuid
from pathlib import Path

from lakehouse_storage.mock_server.generator import generate_batches

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

    def create(self, request):
        run_id = uuid.uuid4().hex
        temporary = self.root / f'.{run_id}.tmp'
        temporary.mkdir()
        metadata = {'run_id': run_id, 'batch_count': request.batches + 1,
                    'request': request.model_dump(mode='json')}
        for sequence, (table, rows) in enumerate(generate_batches(request, run_id)):
            envelope = {'run_id': run_id, 'sequence': sequence, 'table_name': table,
                        'payload_sha256': hashlib.sha256(canonical_json(rows)).hexdigest(), 'data': rows}
            content = canonical_json(envelope)
            if len(content) > MAX_RESPONSE_BYTES:
                raise ValueError('response exceeds limit')
            (temporary / f'{sequence}.json').write_bytes(content)
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
