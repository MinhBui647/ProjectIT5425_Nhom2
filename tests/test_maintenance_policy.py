"""Maintenance only targets isolated namespaces with bounded policies."""
import pytest
from lakehouse_storage.maintenance_policy import MaintenancePolicy


@pytest.mark.parametrize('changes', [{'file_count_threshold': 0}, {'small_file_ratio': 1.1}, {'target_file_size_mb': 0}, {'cooldown_seconds': -1}, {'retention_hours': 167}, {'window_start_hour': 24}])
def test_invalid_policy(tmp_path, changes):
    root = tmp_path / 'storage_demo'
    with pytest.raises(ValueError): MaintenancePolicy(str(root / 'silver' / 'sensor'), str(root), **changes)


def test_shared_table_rejected():
    with pytest.raises(ValueError):
        MaintenancePolicy('s3://smart-dairy-lakehouse/02_silver_curated/sensor_telemetry', 's3://smart-dairy-lakehouse/02_silver_curated')


def test_encoded_escape_rejected_by_policy_and_worker(tmp_path):
    from lakehouse_storage.ingestion_worker import IngestionWorker
    from lakehouse_storage.service_config import ServiceConfig
    root = 's3://smart-dairy-storage-demo/storage_demo/allowed'
    escaped = root + '/%2E%2E/escaped/table'
    with pytest.raises(ValueError):
        MaintenancePolicy(escaped, root)
    config = ServiceConfig(tmp_path / 'storage_demo', escaped, root, 'http://unused')
    with pytest.raises(ValueError):
        IngestionWorker(config, object())
    assert not list(tmp_path.rglob('*.json'))
