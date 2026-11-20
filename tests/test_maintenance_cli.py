"""One-shot entry point retains runner safety and cross-process phase ownership."""
import os
import subprocess
import sys
import pytest
from lakehouse_storage.maintenance_cli import run_once
from lakehouse_storage.service_config import ServiceConfig
from lakehouse_storage.worker_state import single_instance


def test_disabled_one_shot_skips_without_table(tmp_path, monkeypatch):
    root = tmp_path / 'storage_demo'
    monkeypatch.setenv('MAINTENANCE_ENABLED', 'false')
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://unused')
    results = run_once(config)
    assert all(r['decision'] == 'SKIP' and 'phase' in r['reason'] for r in results)


def test_cli_import_does_not_require_apscheduler():
    code = '''
import sys
from importlib.abc import MetaPathFinder
class Block(MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.startswith('apscheduler'):
            raise ImportError('forbidden dependency')
sys.meta_path.insert(0, Block())
import lakehouse_storage.maintenance_cli
'''
    subprocess.run([sys.executable, '-c', code], check=True, timeout=30)


def test_cli_cannot_enter_mutation_while_pipeline_holds_lock(tmp_path):
    root = tmp_path / 'storage_demo'
    with single_instance(root / 'demo-phase.lock'):
        result = subprocess.run([sys.executable, '-m', 'lakehouse_storage.maintenance_cli', '--once'],
            env={**os.environ, 'STORAGE_DEMO_ROOT': str(root)}, capture_output=True, text=True, timeout=30)
    assert result.returncode == 3 and 'already running' in result.stdout


def test_one_shot_forwards_preview_and_writes_skip_history(tmp_path, monkeypatch):
    from lakehouse_storage.maintenance_runner import MaintenanceRunner
    from lakehouse_storage.maintenance_history import MaintenanceHistory
    root = tmp_path / 'storage_demo'
    config = ServiceConfig(root, str(root / 'silver'), str(root), 'http://unused')
    forwarded = []
    original = MaintenanceRunner.run
    def run(self, policy, action='compact', **kwargs):
        forwarded.append(action)
        return original(self, policy, action, **kwargs)
    monkeypatch.setattr(MaintenanceRunner, 'run', run)
    monkeypatch.setenv('MAINTENANCE_ENABLED', 'false')
    assert all(r['decision'] == 'SKIP' for r in run_once(config, 'vacuum_preview'))
    assert forwarded == ['vacuum_preview', 'vacuum_preview']
    assert all(r['decision'] == 'SKIP' for r in MaintenanceHistory(root / 'maintenance.jsonl').records())
