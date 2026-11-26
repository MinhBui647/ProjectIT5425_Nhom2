"""Bounded source tasks; no HTTP/Delta operations while parsing this DAG."""
import os
from datetime import timedelta
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from lakehouse_pipeline.airflow_tasks import FULL_SOURCES, prepare_task, stage_task, report_task

cron = os.getenv('LAKEHOUSE_CRON_UTC')
with DAG('lakehouse_pipeline', start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
         schedule=CronDataIntervalTimetable(cron, timezone='UTC') if cron else None,
         catchup=False, max_active_runs=1, is_paused_upon_creation=True,
         default_args={'pool': 'lakehouse_mutation', 'retries': 1, 'retry_delay': timedelta(seconds=5),
                       'execution_timeout': timedelta(minutes=10)}, tags=['lakehouse', 'demo']) as dag:
    prepare = PythonOperator(task_id='prepare_run', python_callable=prepare_task)
    previous = prepare
    for table in FULL_SOURCES:
        for stage in ('bronze', 'verify', 'govern'):
            task = PythonOperator(task_id=f'{stage}_{table}', python_callable=stage_task,
                                  op_kwargs={'stage': stage, 'table': table})
            previous >> task
            previous = task
    snapshot = PythonOperator(task_id='snapshot_silver', python_callable=stage_task, op_kwargs={'stage': 'snapshot'})
    report = PythonOperator(task_id='report_run', python_callable=report_task, trigger_rule='all_done', retries=0)
    finalize = PythonOperator(task_id='finalize_run', python_callable=stage_task, op_kwargs={'stage': 'finalize'})
    previous >> snapshot
    snapshot >> report
    [snapshot, report] >> finalize
