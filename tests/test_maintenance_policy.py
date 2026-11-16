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
