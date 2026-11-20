import polars as pl
from lakehouse_pipeline.silver_snapshot import capture_snapshot, read_snapshot
from lakehouse_storage.config import get_storage_options
from test_pipeline_bronze import StoredClient, worker_at


def test_capture_reads_pinned_version_after_later_append(tmp_path):
    root = tmp_path / 'storage_demo'
    client = StoredClient(root / 'mock')
    worker = worker_at(root, client)
    records = worker.run(client.metadata['run_id'])
    snapshot = capture_snapshot(records, list(client.metadata['request']['sources']))
    assert len(snapshot) == 6
    old = read_snapshot(snapshot, 'sensor_telemetry')
    from lakehouse_storage.silver_manager import append_to_delta
    append_to_delta(old, str(root / 'silver' / 'sensor_telemetry'), ['line_id', 'ingestion_date'], 'sensor_telemetry')
    assert pl.read_delta(str(root / 'silver' / 'sensor_telemetry'), storage_options=get_storage_options()).height == old.height * 2
    assert read_snapshot(snapshot, 'sensor_telemetry').equals(old)
