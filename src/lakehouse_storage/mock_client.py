"""Bounded HTTP client and digest validation before Bronze publication."""
import hashlib
import json
import re
import time

import requests

MAX_RESPONSE_BYTES = 32 * 1024 * 1024


class PayloadError(ValueError):
    pass


def decode_batch(content, run_id, sequence):
    from lakehouse_storage.mock_server.store import canonical_json
    try:
        if len(content) > MAX_RESPONSE_BYTES:
            raise ValueError('response limit exceeded')
        envelope = json.loads(content)
        expected_table = 'mes_lims' if sequence == 0 else 'sensor_telemetry'
        if (envelope['run_id'], envelope['sequence'], envelope['table_name']) != (run_id, sequence, expected_table):
            raise ValueError('envelope identity mismatch')
        rows = envelope['data']
        if not isinstance(rows, list) or not 1 <= len(rows) <= 50000 or not all(isinstance(r, dict) for r in rows):
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

    def run(self, run_id):
        if not re.fullmatch('[a-f0-9]{32}', run_id):
            raise PayloadError('invalid run id')
        metadata = json.loads(self.get(f'/runs/{run_id}'))
        if metadata.get('run_id') != run_id or type(metadata.get('batch_count')) is not int or not 1 <= metadata['batch_count'] <= 201:
            raise PayloadError('invalid run metadata')
        return metadata

    def batch(self, run_id, sequence):
        if not re.fullmatch('[a-f0-9]{32}', run_id) or type(sequence) is not int or sequence < 0:
            raise PayloadError('invalid batch identity')
        return decode_batch(self.get(f'/runs/{run_id}/batches/{sequence}'), run_id, sequence)
