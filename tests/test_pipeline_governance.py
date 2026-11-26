import polars as pl
from lakehouse_storage.config import get_storage_options
from test_pipeline_bronze import StoredClient, worker_at


def test_all_silver_tables_have_committed_quality_evidence_and_replay(tmp_path):
    root = tmp_path / 'storage_demo'
    client = StoredClient(root / 'mock')
    worker = worker_at(root, client)
    run_id = client.metadata['run_id']
    records = worker.run(run_id)
    assert len(records) == client.metadata['batch_count'], records
    assert all(r['status'] == 'DONE' for r in records), records
    before = {}
    for r in records:
        if r['bronze_only']:
            assert r['rows_written'] == 0 and 'p3_result' not in r
            assert not (root / 'silver' / 'market_reports').exists()
            continue
        evidence = r['p3_result']['committed_evidence']
        assert evidence['bronze_sha256'] == r['bronze_sha256']
        assert evidence['quality_run_ids']
        assert evidence['accepted_rows'] > 0
        uri = r['silver_uri']
        before[uri] = pl.read_delta(uri, storage_options=get_storage_options()).height
    for r in records:
        replay = worker.process(run_id, r['sequence'], replay=True)
        if not r['bronze_only']:
            assert replay['p3_result']['status'] == 'SKIPPED_DUPLICATE'
            assert replay['p3_result']['committed_evidence'] == r['p3_result']['committed_evidence']
    assert all(pl.read_delta(uri, storage_options=get_storage_options()).height == n for uri, n in before.items())


def test_identical_content_at_new_bronze_path_uses_original_commit_provenance(tmp_path):
    from lakehouse_storage.mock_server.models import RunRequest
    root = tmp_path / 'storage_demo'
    client = StoredClient(root / 'mock')
    worker = worker_at(root, client)
    first_id = client.metadata['run_id']
    sequence = next(e['sequence'] for e in client.metadata['batches'] if e['table_name'] == 'weather')
    first = worker.process(first_id, sequence)
    client.metadata = client.store.create(RunRequest(**client.metadata['request']))
    second = worker.process(client.metadata['run_id'], sequence)
    assert second['status'] == 'DONE', second
    assert second['bronze_path'] != first['bronze_path']
    assert second['bronze_sha256'] == first['bronze_sha256']
    assert second['p3_result']['status'] == 'SKIPPED_DUPLICATE'
    assert second['p3_result']['committed_evidence']['quality_run_ids'] == first['p3_result']['committed_evidence']['quality_run_ids']
    assert second['p3_result']['committed_evidence']['bronze_paths'] == [first['bronze_path']]
