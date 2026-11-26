"""Static deployment contracts plus Docker config validation in runtime QA."""
from pathlib import Path
import yaml


def test_compose_services_share_pool_volume_and_executor():
    compose = yaml.safe_load(Path('infra/docker-compose.lakehouse.yml').read_text())
    services = compose['services']
    assert {'postgres', 'minio', 'mock', 'airflow-init', 'airflow-api', 'airflow-scheduler', 'airflow-dag-processor'} <= services.keys()
    for name in ('airflow-api', 'airflow-scheduler', 'airflow-dag-processor'):
        service = services[name]
        assert service['environment']['AIRFLOW__CORE__EXECUTOR'] == 'LocalExecutor'
        assert service['environment']['AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS'] == 'true'
        assert 'runtime:/opt/lakehouse/storage_demo' in service['volumes']
        assert not any('docker.sock' in mount for mount in service['volumes'])
    assert services['minio']['ports'][0].startswith('127.0.0.1:')
    assert services['airflow-api']['ports'][0].startswith('127.0.0.1:')


def test_init_preserves_state_and_sets_single_slot_pool():
    text = Path('infra/init_airflow.py').read_text()
    assert "'lakehouse_mutation', '1'" in text
    assert "'migrate'" in text and 'reset' not in text


def test_legacy_compose_only_has_maintenance_one_shot():
    services = yaml.safe_load(Path('infra/docker-compose.storage.yml').read_text())['services']
    assert 'scheduler' not in services
    assert services['maintenance']['command'] == ['python', '-m', 'lakehouse_storage.maintenance_cli', '--once']
    assert '--once' in services['maintenance']['command']
