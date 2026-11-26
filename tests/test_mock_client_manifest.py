import json
import pytest
from lakehouse_storage.mock_client import MockClient, PayloadError, decode_batch
from lakehouse_storage.mock_server.models import RunRequest
from lakehouse_storage.mock_server.store import RunStore, canonical_json


def test_decode_checks_manifest_for_every_source(tmp_path):
    store = RunStore(tmp_path)
    run = store.create(RunRequest(sources=['weather', 'market_reports']))
    for entry in run['batches']:
        content = store.batch(run['run_id'], entry['sequence'])
        assert decode_batch(content, run['run_id'], entry['sequence'], entry)['table_name'] == entry['table_name']
        altered = json.loads(content)
        altered['source'] = 'GDT'
        with pytest.raises(PayloadError):
            decode_batch(canonical_json(altered), run['run_id'], entry['sequence'], entry)


def test_manifest_digest_is_not_replaced_by_self_signed_payload(tmp_path):
    store = RunStore(tmp_path)
    run = store.create(RunRequest(sources=['weather']))
    entry = run['batches'][0]
    content = json.loads(store.batch(run['run_id'], 0))
    content['payload_sha256'] = 'a' * 64
    with pytest.raises(PayloadError):
        decode_batch(canonical_json(content), run['run_id'], 0, entry)


@pytest.mark.parametrize('metadata', [[], {'run_id': 'a'*32, 'batch_count': 1, 'manifest_version': 2, 'batches': ['bad']},
    {'run_id': 'a'*32, 'batch_count': 1, 'manifest_version': 2, 'batches': [{'sequence': False, 'table_name': 'weather', 'source': 'S4', 'schema_variant': 'raw', 'bronze_only': False, 'payload_sha256': 'b'*64}]}])
def test_malformed_manifest_is_nonretryable_payload_error(metadata, monkeypatch):
    client = MockClient('http://unused')
    monkeypatch.setattr(client, 'get', lambda *_: canonical_json(metadata))
    with pytest.raises(PayloadError):
        client.run('a'*32)
