"""Idempotent Airflow metadata initialization; preserves existing volumes."""
import subprocess

subprocess.run(['airflow', 'db', 'migrate'], check=True)
subprocess.run(['airflow', 'pools', 'set', 'lakehouse_mutation', '1', 'Single-host Silver mutations and capture'], check=True)
