# Storage demo operations

This demo uses one sequential worker. It does not provide distributed exactly-once
processing or coordinate writers outside this stack. Keep shared factory tables
out of the demo namespace. New code rejects targets outside the configured demo
allow-root; local roots must contain `storage_demo` or `storage_benchmark`.

## Local service environment

Use the clean environment and constraints in [storage_baseline.md](storage_baseline.md).
All commands below need `PYTHONPATH=src` and that environment's Python.

```bash
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo"
python -m lakehouse_storage.mock_server
# Another terminal; --create-run stores identity for restart.
python -m lakehouse_storage.ingestion_worker --create-run
python -m lakehouse_storage.ingestion_worker --run-id RUN_ID --replay-sequence 1
# Only after the worker has exited:
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true MAINTENANCE_FILE_THRESHOLD=2 \
  python -m lakehouse_storage.maintenance_scheduler --once
python -m lakehouse_storage.maintenance_scheduler --once --action vacuum_preview
```

The default scheduler records SKIP until both maintenance flags are true. Hours
and cron use UTC (`MAINTENANCE_WINDOW_START_UTC`, `MAINTENANCE_WINDOW_END_UTC`,
`MAINTENANCE_CRON_UTC`). One local `demo-phase.lock` prevents overlap between demo
entry points; it does not lock P3, other hosts or existing shared writers.
Vacuum is always `dry_run=True`, retention >=168 hours and retention enforcement
on. Z-order is opt-in using existing non-partition columns.

## Scenarios and failure handling

| Scenario | HTTP outcome (sensor sequences) | Expected rejection stage |
|---|---|---|
| normal | 200, stable persisted bytes | P3 accepts |
| invalid-value | 200, uht_temp=-10 | P3 quality gate |
| invalid-time | 200, invalid timestamp | P3 transform/quality |
| missing-field | 200, missing line_id | Bronze required-field validation |
| transient-500 | first N requests per sequence fail, then 200 | finite HTTP retry |
| delay | bounded 0-5 seconds | timeout retry if configured shorter |

POST `/runs` creates a new identity and is **not idempotent**, so clients do not
retry it automatically. Control errors return 422; unknown run/sequence returns
404. GET replay returns exactly the stored envelope bytes, including metadata.
The API is a local demo endpoint; bind to loopback or a private Compose network.

Checkpoint JSON lives under `worker_state/<run>/<sequence>.json`. Known artifacts
are checksum-verified and reused. Corrupt checkpoint files are preserved with a
`.review.json` diagnostic. A crash in PUBLISHING stops at NEEDS_REVIEW and lists
candidate artifacts. Inspect them manually before resetting or replacing state;
no automatic orphan deletion or publication retry occurs. PROCESSING can resume
using the same Bronze file and P3 duplicate gate, with the previous commit outcome
marked unknown. Reported zero rows on replay does not imply that the previous
process committed zero rows.

Default policy stops on rejection; `--skip-rejected` records it and moves on.
Transport timeout/network/429/5xx retry has finite attempts and exponential backoff;
404, schema and quality failures are not retried automatically. SIGTERM finishes
the current batch, then exits. `--once` processes one pending batch, `--limit N`
bounds batches, `--continuous` waits for termination after the finite run is done.

History `maintenance.jsonl` stores decisions, errors, timings and layout metrics.
Unfinished STARTED records read as UNKNOWN and contribute to cooldown. Active file
bytes and retained physical Parquet bytes differ; S3 physical bytes are unknown
unless measured independently. Compaction does not reclaim retained history.
