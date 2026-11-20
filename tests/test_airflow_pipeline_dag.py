import os
from pathlib import Path
import pytest
pytestmark = pytest.mark.skipif(os.getenv('LAKEHOUSE_AIRFLOW_TEST') != '1', reason='Airflow image required')


def test_pipeline_graph_and_limits():
    from airflow.models import DagBag
    bag = DagBag(os.environ.get('AIRFLOW__CORE__DAGS_FOLDER', '/opt/airflow/dags'), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags['lakehouse_pipeline']
    assert dag.max_active_runs == 1 and not dag.catchup
    assert set(t.task_id for t in dag.leaves) == {'finalize_run'}
    assert dag.get_task('finalize_run').trigger_rule.value == 'all_success'
    assert dag.get_task('report_run').trigger_rule == 'all_done'
    assert len(dag.tasks) == 25
    for task in dag.tasks:
        assert task.pool == 'lakehouse_mutation'
        assert task.execution_timeout.total_seconds() <= 600
        assert task.retries <= 1


def test_explicit_data_interval_timetable():
    from airflow.timetables.interval import CronDataIntervalTimetable
    from airflow.timetables.base import TimeRestriction
    import pendulum
    timetable = CronDataIntervalTimetable('0 0 * * *', timezone='UTC')
    info = timetable.next_dagrun_info(last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=pendulum.datetime(2026, 1, 1, tz='UTC'), latest=None, catchup=True))
    assert (info.data_interval.end - info.data_interval.start).total_seconds() == 86400
