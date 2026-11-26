import os
import pytest
pytestmark = pytest.mark.skipif(os.getenv('LAKEHOUSE_AIRFLOW_TEST') != '1', reason='Airflow image required')


def test_maintenance_shares_mutation_pool_and_has_no_catchup():
    from airflow.models import DagBag
    bag = DagBag('/opt/airflow/dags', include_examples=False)
    assert not bag.import_errors
    dag = bag.dags['lakehouse_maintenance']
    assert dag.max_active_runs == 1 and not dag.catchup
    task = dag.get_task('maintain_silver')
    assert task.pool == 'lakehouse_mutation'
    assert task.retries == 0


def test_maintenance_job_identity_and_utc_are_stable_after_parse_restart():
    from airflow.models import DagBag
    first = DagBag('/opt/airflow/dags', include_examples=False).dags['lakehouse_maintenance']
    second = DagBag('/opt/airflow/dags', include_examples=False).dags['lakehouse_maintenance']
    assert first.task_ids == second.task_ids == ['maintain_silver']
    assert str(first.timezone) == 'UTC'
    assert not first.catchup and first.max_active_runs == 1
