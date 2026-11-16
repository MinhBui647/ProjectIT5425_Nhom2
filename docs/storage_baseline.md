# Storage baseline (2026-11-17)

Branch: `feature/lakehouse-reliability-automation`. No downstream source/tests,
Bronze/Silver writer, schema or shared configuration changes.

- Original `.venv`: Python 3.12, Delta 1.6.6, Polars 1.44.2, NumPy 1.26.4,
  PyArrow 25.0.1 (outside requirements). Storage baseline: **104 passed**.
- Original full suite: **6 collection errors**, missing Great Expectations,
  Pillow/OpenLineage and pandas. These are environment failures, not regressions.
- Clean `/tmp/storage-services-venv`: install `requirements.txt` plus
  `infra/requirements-storage.txt`, freeze in `infra/constraints-storage.txt`.
  PyArrow 18.1.0, NumPy 1.26.4; `pip check`: no broken requirements.
- Full suite in service environment after trace fixes: **311 passed, 1 skipped**,
  32.88 s. Skip is the existing optional integration test.
- `tests/test_storage_compatibility.py`: frozen public signatures, Silver schemas,
  exports and partitions; subprocess blocks optional packages during core import.
- T02 RED: **5 failures, 15 passes**; GREEN time-travel + compatibility: **22 passed**.
- T03 RED: **7 failures**; GREEN traceability/time-travel/integration/compatibility:
  **30 passed**. Local Delta and temporary Bronze files only.

Reproduce:

```bash
python3 -m venv /tmp/storage-services-venv
/tmp/storage-services-venv/bin/pip install -r requirements.txt -r infra/requirements-storage.txt -c infra/constraints-storage.txt
/tmp/storage-services-venv/bin/pip check
PYTHONPATH=src /tmp/storage-services-venv/bin/python -m pytest tests/ -q
```

Full constraints include transitive packages. They do not change the shared venv.
