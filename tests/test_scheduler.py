"""Stable UTC jobs share one executor and stop cleanly on SIGTERM."""
import os
import subprocess
import sys
import time

from lakehouse_storage.maintenance_policy import MaintenancePolicy
from lakehouse_storage.maintenance_runner import MaintenanceRunner
from lakehouse_storage.maintenance_scheduler import build_scheduler


def test_stable_jobs_and_single_executor(tmp_path):
    policy = MaintenancePolicy(str(tmp_path / 'storage_demo' / 'table'), str(tmp_path / 'storage_demo'))
    runner = MaintenanceRunner(tmp_path / 'history.jsonl')
    scheduler = build_scheduler([policy], runner)
    scheduler.start(paused=True)
    try:
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].coalesce and jobs[0].max_instances == 1
        assert jobs[0].misfire_grace_time == 30
        assert str(scheduler.timezone) == 'UTC'
        assert build_scheduler([policy], runner).get_jobs()[0].id == jobs[0].id
    finally:
        scheduler.shutdown()


def test_scheduler_subprocess_lock_and_sigterm(tmp_path):
    root = tmp_path / 'storage_demo'
    env = {**os.environ, 'PYTHONPATH': 'src', 'STORAGE_DEMO_ROOT': str(root), 'MAINTENANCE_INTERVAL_SECONDS': '0.1'}
    child = subprocess.Popen([sys.executable, '-m', 'lakehouse_storage.maintenance_scheduler'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        import select
        ready, _, _ = select.select([child.stdout], [], [], 10)
        assert ready and child.stdout.readline().strip() == 'scheduler ready'
        other = subprocess.run([sys.executable, '-m', 'lakehouse_storage.maintenance_scheduler'], env=env, capture_output=True, timeout=10)
        assert other.returncode != 0
        assert b'already running' in other.stderr
        time.sleep(0.25)
        child.terminate()
        assert child.wait(timeout=10) == 0
        from lakehouse_storage.maintenance_history import MaintenanceHistory
        assert all(r['decision'] == 'SKIP' for r in MaintenanceHistory(root / 'maintenance.jsonl').records())
    finally:
        if child.poll() is None: child.kill(); child.wait(timeout=5)
