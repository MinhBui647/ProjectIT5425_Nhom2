"""Bounded HTTP client and digest validation before Bronze publication."""
import hashlib
import json
import re
import time

import requests

MAX_RESPONSE_BYTES = 32 * 1024 * 1024


class PayloadError(ValueError):
    pass


def decode_batch(content, run_id, sequence, entry=None):
    from lakehouse_storage.mock_server.store import canonical_json
    try:
        if len(content) > MAX_RESPONSE_BYTES:
            raise ValueError('response limit exceeded')
        envelope = json.loads(content)
        expected_table = entry['table_name'] if entry else ('mes_lims' if sequence == 0 else 'sensor_telemetry')
        if (envelope['run_id'], envelope['sequence'], envelope['table_name']) != (run_id, sequence, expected_table):
            raise ValueError('envelope identity mismatch')
        if entry and any(envelope.get(k) != entry.get(k) for k in ('source', 'schema_variant', 'bronze_only', 'payload_sha256', 'window')):
            raise ValueError('envelope manifest mismatch')
        rows = envelope['data']
        if not isinstance(rows, list) or not 0 <= len(rows) <= 50000 or not all(isinstance(r, dict) for r in rows):
            raise ValueError('invalid rows')
        digest = hashlib.sha256(canonical_json(rows)).hexdigest()
        if envelope['payload_sha256'] != digest:
            raise ValueError('payload digest mismatch')
        return envelope
    except (ValueError, KeyError, TypeError) as exc:
        raise PayloadError(str(exc)) from exc


class MockClient:
    def __init__(self, base_url, timeout=15, max_attempts=3, backoff=0.2, sleep=time.sleep):
        self.base_url = base_url.rstrip('/')
        if not 1 <= max_attempts <= 10 or not 0 <= backoff <= 10 or not 0 < timeout <= 120:
            raise ValueError("invalid transport retry bounds")
        self.max_attempts, self.backoff, self.sleep = max_attempts, backoff, sleep
        self.timeout = timeout
        self.attempts = 0
        self.manifests = {}

    def get(self, route):
        for attempt in range(self.max_attempts):
            self.attempts += 1
            try:
                return self._get_once(route)
            except requests.HTTPError as exc:
                retryable = exc.response is not None and (exc.response.status_code == 429 or 500 <= exc.response.status_code < 600)
                if not retryable or attempt + 1 == self.max_attempts:
                    raise
            except (requests.Timeout, requests.ConnectionError):
                if attempt + 1 == self.max_attempts:
                    raise
            self.sleep(self.backoff * 2**attempt)

    def _get_once(self, route):
        with requests.get(self.base_url + route, timeout=self.timeout, stream=True, allow_redirects=False) as response:
            response.raise_for_status()
            if response.status_code != 200:
                raise PayloadError('unexpected HTTP status')
            content = bytearray()
            for chunk in response.iter_content(65536):
                content.extend(chunk)
                if len(content) > MAX_RESPONSE_BYTES:
                    raise PayloadError('response limit exceeded')
            return bytes(content)

    def create(self, request, key):
        """Only idempotent POSTs may be retried after a lost response."""
        for attempt in range(self.max_attempts):
            try:
                response = requests.post(self.base_url + '/runs', json=request,
                                         headers={'Idempotency-Key': key}, timeout=120, allow_redirects=False)
                response.raise_for_status()
                if response.status_code != 201 or len(response.content) > MAX_RESPONSE_BYTES:
                    raise PayloadError('invalid create response')
                return response.json()
            except requests.HTTPError as exc:
                if exc.response is None or (exc.response.status_code != 429 and not 500 <= exc.response.status_code < 600) or attempt + 1 == self.max_attempts:
                    raise
            except (requests.Timeout, requests.ConnectionError):
                if attempt + 1 == self.max_attempts:
                    raise
            self.sleep(self.backoff * 2**attempt)

    def run(self, run_id):
        if not re.fullmatch('[a-f0-9]{32}', run_id):
            raise PayloadError('invalid run id')
        try:
            metadata = json.loads(self.get(f'/runs/{run_id}'))
            if not isinstance(metadata, dict):
                raise PayloadError('invalid run metadata')
            if metadata.get('run_id') != run_id or type(metadata.get('batch_count')) is not int or not 1 <= metadata['batch_count'] <= 210:
                raise PayloadError('invalid run metadata')
            if 'manifest_version' in metadata:
                from lakehouse_storage.mock_server.models import TABLE_SOURCES
                if metadata['manifest_version'] != 2 or not isinstance(metadata.get('batches'), list) or len(metadata.get('batches', [])) != metadata['batch_count']:
                    raise PayloadError('unsupported or incomplete manifest')
                for i, entry in enumerate(metadata['batches']):
                    if not isinstance(entry, dict):
                        raise PayloadError('invalid batch manifest')
                    table = entry.get('table_name')
                    if (type(entry.get('sequence')) is not int or entry.get('sequence') != i or table not in TABLE_SOURCES or entry.get('source') != TABLE_SOURCES[table]
                            or type(entry.get('bronze_only')) is not bool or entry.get('bronze_only') != (table == 'market_reports')
                            or entry.get('schema_variant') not in ({'long', 'wide'} if table == 'market_indices' else {'raw'})
                            or not re.fullmatch('[a-f0-9]{64}', str(entry.get('payload_sha256', '')))):
                        raise PayloadError('invalid batch manifest')
        except (ValueError, TypeError, KeyError) as exc:
            raise PayloadError(str(exc)) from exc
        self.manifests[run_id] = metadata
        return metadata

    def batch(self, run_id, sequence):
        if not re.fullmatch('[a-f0-9]{32}', run_id) or type(sequence) is not int or sequence < 0:
            raise PayloadError('invalid batch identity')
        metadata = self.manifests.get(run_id) or self.run(run_id)
        if sequence >= metadata['batch_count']:
            raise PayloadError('sequence outside manifest')
        entry = metadata.get('batches', [None] * metadata['batch_count'])[sequence]
        return decode_batch(self.get(f'/runs/{run_id}/batches/{sequence}'), run_id, sequence, entry)
