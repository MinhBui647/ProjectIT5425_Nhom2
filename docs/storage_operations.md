# Storage demo operations

This demo uses one sequential worker. It does not provide distributed exactly-once
processing or coordinate writers outside this stack. Keep shared factory tables
out of the demo namespace. New code rejects targets outside the configured demo
allow-root; local roots must contain `storage_demo` or `storage_benchmark`.

## Local service environment

Run these commands from the repository root to create a separate service
environment with the pinned dependencies, then activate it:

```bash
python3 -m venv /tmp/storage-services-venv
/tmp/storage-services-venv/bin/python -m pip install \
  -r requirements.txt -r infra/requirements-storage.txt -c infra/constraints-storage.txt
/tmp/storage-services-venv/bin/python -m pip check
source /tmp/storage-services-venv/bin/activate
export PYTHONPATH=src
```

The full constraints include transitive packages. This leaves the shared project
venv unchanged. All commands below need `PYTHONPATH=src` and this environment's
Python. For a container-only demo, use the Docker section instead.

```bash
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo"
python -m lakehouse_storage.mock_server
# Another terminal; --create-run stores identity for restart.
python -m lakehouse_storage.ingestion_worker --create-run
python -m lakehouse_storage.ingestion_worker --run-id RUN_ID --replay-sequence 1
# Only after the worker has exited:
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true MAINTENANCE_FILE_THRESHOLD=2 \
  python -m lakehouse_storage.maintenance_scheduler --once
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true \
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

## Docker demo (two phases)

```bash
mkdir -p reports/storage/demo
# Dependency download uses host network at build time to avoid this host's bridge DNS issue.
# Services themselves use the isolated Compose network.
docker compose -f infra/docker-compose.storage.yml config --quiet
docker compose -f infra/docker-compose.storage.yml build
docker compose -f infra/docker-compose.storage.yml up -d minio mock
docker compose -f infra/docker-compose.storage.yml --profile ingest up -d worker
docker compose -f infra/docker-compose.storage.yml wait worker
# Confirm exit code zero before enabling maintenance:
docker compose -f infra/docker-compose.storage.yml ps -a
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true \
  docker compose -f infra/docker-compose.storage.yml --profile maintain run --rm scheduler \
  python -m lakehouse_storage.maintenance_scheduler --once
# No destructive vacuum; an explicit preview can be run the same way.
```

Default host ports are 18010 (mock) and 19000 (MinIO), bound to loopback. Existing
9000/9001 services are unaffected. Worker/scheduler share the same absolute
`/demo/storage_demo/bronze` mount and phase lock. State, Bronze, mock payloads and
reports persist separately. The mock receives no MinIO credentials. Build and
service versions are pinned in the Dockerfile, Compose and constraints. Check the
shareable variables in `infra/storage.env.example`; actual credentials are local.

Opt-in S3 tests use unique prefixes in the demo bucket and retain them for
inspection (no automatic cleanup of shared resources):

```bash
STORAGE_DEMO_MINIO_TEST=1 PYTHONPATH=src /tmp/storage-services-venv/bin/python \
  -m pytest tests/test_storage_minio_demo.py -m minio -q
```

Verified 2026-11-17: MinIO/mock healthy, worker exit 0 (4001 rows reported),
maintenance compaction 4 -> 1 sensor files, image `pip check` clean; all six S3 E2E
tests passed. The Docker bridge DNS failed on this machine; host networking is
used **only for build dependency download**, with isolated runtime networking.
