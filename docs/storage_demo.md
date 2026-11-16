# Storage demo walkthrough

Implementation status and acceptance evidence live in `tasks/todo.md`.
`tasks/plan.md` is the scope; earlier plans are historical.

## 1. Start the isolated stack

Follow environment setup in [storage_baseline.md](storage_baseline.md) and Docker
commands in [storage_operations.md](storage_operations.md). MinIO is on 19000,
mock API on 18010. No shared tables are used.

## 2. Normal ingestion and replay

Use a fresh session name for each demo. In a terminal using the service venv:

```bash
export PYTHONPATH=src
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo/classroom_01"
export STORAGE_DEMO_SILVER_ROOT=s3://smart-dairy-storage-demo/storage_demo/classroom_01/silver
export STORAGE_DEMO_SILVER_ALLOW_ROOT=s3://smart-dairy-storage-demo/storage_demo
export MINIO_ENDPOINT=127.0.0.1:19000
export MINIO_ACCESS_KEY=storagedemo
export MINIO_SECRET_KEY=storage-demo-local-only
export MOCK_URL=http://127.0.0.1:18010
python -m lakehouse_storage.ingestion_worker --create-run
```

The first sequence is MES; remaining sequences are sensor chunks in that MES
production window. The worker stores `active_run.json`. Read its run id, then:

```bash
python -m lakehouse_storage.ingestion_worker --run-id RUN_ID --replay-sequence 1
```

Expect P3 `SKIPPED_DUPLICATE`, reported rows=0 and unchanged Silver row count.
All actual writes run through P3. `_contract_version`, `_quality_status`,
`_quality_run_id` and lineage columns come from P3, with existing types/partitions.

## 3. Demonstrate a rejection

Use a fresh `STORAGE_DEMO_ROOT` and Silver child, then set:

```bash
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo/classroom_rejection_01"
export STORAGE_DEMO_SILVER_ROOT=s3://smart-dairy-storage-demo/storage_demo/classroom_rejection_01/silver
MOCK_RUN_REQUEST='{"samples_per_batch":20,"batches":2,"scenario":"invalid-value"}' \
  python -m lakehouse_storage.ingestion_worker --create-run
```

Expect MES accepted and sensor REJECTED at P3, with no sensor Silver write.
`missing-field` is rejected by Bronze validation instead. Default behavior stops;
`--skip-rejected` records the outcome and advances. Use `transient-500` to observe
finite retry and `delay` with `MOCK_TIMEOUT_SECONDS` to demonstrate timeouts.

## 4. Maintenance after ingestion has stopped

Restore the normal session before maintenance and tracing:

```bash
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo/classroom_01"
export STORAGE_DEMO_SILVER_ROOT=s3://smart-dairy-storage-demo/storage_demo/classroom_01/silver
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true MAINTENANCE_FILE_THRESHOLD=2 \
  python -m lakehouse_storage.maintenance_scheduler --once
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true \
  python -m lakehouse_storage.maintenance_scheduler --once --action vacuum_preview
```

Inspect `maintenance.jsonl`: version, active file count/bytes, timings and decisions.
Preview does not delete files. Cooldown survives restart. The local guard covers
only these demo entry points; external writers must remain stopped.

## 5. Trace all sources

```python
import os
from lakehouse_storage import read_silver_table, trace_batch, get_version_at_date
uri = os.environ['STORAGE_DEMO_SILVER_ROOT'] + '/sensor_telemetry'
frame = read_silver_table(uri)
trace = trace_batch(uri, frame['batch_id'][0])
print(trace['integrity_verified'], len(trace['lineage_sources']))
print(get_version_at_date(uri, '2026-12-01T00:00:00+07:00'))
```

`lineage` preserves the representative source for old callers. `lineage_sources`
contains all unique path/hash pairs. Tests corrupt a temporary second file and
prove the overall integrity becomes false while preserving valid sources.
For container Bronze paths, run trace inside the same worker image/mounts.

## 6. Benchmark and figures

Run [storage_benchmark.md](storage_benchmark.md). Actual results and limitations
are in [storage_results.md](storage_results.md). Regenerate figures:

```bash
python -m lakehouse_storage.benchmark_report reports/storage/RUN_ID/metrics.json
```

Four charts cover active file count, file sizes, query latency with dispersion and
end-to-end throughput. Raw JSON/CSV includes package versions, scale, seed,
workload text and cache policy. The results do not claim compaction always helps.

## Verification and limits

Single local POSIX worker/phase lock; no distributed exactly-once guarantee.
PUBLISHING uncertainty and corrupt state require manual review. PROCESSING resume
reuses Bronze and records the previous commit outcome as unknown. Local service
output trees reject symlink escapes. Shared writers, destructive vacuum,
production rollout and 1M-scale performance claims are outside this demo run.

```bash
STORAGE_DEMO_MINIO_TEST=1 PYTHONPATH=src /tmp/storage-services-venv/bin/python -m pytest tests/ -q -rs
/tmp/storage-services-venv/bin/python -m pip check
docker compose -f infra/docker-compose.storage.yml config --quiet
git diff --check
```
