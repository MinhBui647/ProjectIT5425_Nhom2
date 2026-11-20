"""Same one-slot pool and local phase lock as ingestion, vacuum preview only."""
import os
from datetime import timedelta
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from lakehouse_pipeline.airflow_tasks import maintenance_task

with DAG('lakehouse_maintenance', start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
         schedule=os.getenv('MAINTENANCE_CRON_UTC') or None, catchup=False,
         max_active_runs=1, is_paused_upon_creation=True,
         default_args={'pool': 'lakehouse_mutation', 'retries': 0,
                       'execution_timeout': timedelta(minutes=10)}, tags=['lakehouse', 'maintenance']) as dag:
    PythonOperator(task_id='maintain_silver', python_callable=maintenance_task)
