"""Run in the Airflow interpreter; no processing dependencies imported at parse."""
import json
import os
import subprocess
from datetime import datetime, timezone
import pytest

pytestmark = pytest.mark.skipif(os.getenv('LAKEHOUSE_AIRFLOW_TEST') != '1', reason='Airflow image required')


def test_manual_and_scheduled_windows_and_invalid_window():
    from lakehouse_pipeline.airflow_tasks import resolve_window
    assert resolve_window({'window_start': '2026-01-01T07:00:00+07:00', 'window_end': '2026-01-02T00:00:00Z'}, 'manual', None, None)[0] == '2026-01-01T00:00:00+00:00'
    a, b = datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert resolve_window({}, 'scheduled', a, b) == (a.isoformat(), b.isoformat())
    with pytest.raises(ValueError):
        resolve_window({}, 'manual', a, b)
    with pytest.raises(ValueError):
        resolve_window({}, 'scheduled', a, a)


def test_business_failure_does_not_retry(monkeypatch):
    from airflow.exceptions import AirflowFailException, AirflowException
    from lakehouse_pipeline.airflow_tasks import invoke_processing
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: subprocess.CompletedProcess(a, 2, json.dumps({'status': 'FAILED', 'error': 'quality failed'}), ''))
    with pytest.raises(AirflowFailException):
        invoke_processing({'stage': 'govern'})
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: subprocess.CompletedProcess(a, 3, json.dumps({'status': 'FAILED', 'error': 'timeout'}), ''))
    with pytest.raises(AirflowException) as error:
        invoke_processing({'stage': 'govern'})
    assert not isinstance(error.value, AirflowFailException)
