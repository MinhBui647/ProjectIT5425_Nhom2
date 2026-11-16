# Storage validation evidence - 2026-11-17

## Environment and baseline

See [storage_baseline.md](storage_baseline.md). Original core tests: 104 pass.
Original shared venv has six collection errors from missing optional dependencies.
A clean service venv and Docker build use the checked-in constraints; both pip
checks pass. The shared venv was not modified.

## Implementation checks

- Time lookup RED 5 failures -> GREEN 22 tests; includes real local Delta,
  fixed history timestamps, timezone offsets, equal timestamps and before-first.
- Multi-source trace RED 7 failures -> GREEN 30 tests; corruption/missing/null/
  conflict/unreadable cases and one hash per source.
- Mock persisted-byte replay across subprocess/restart and HTTP; controls/scenarios
  tested, sampling fixed at 1Hz regardless of delivery. Batch-size equivalence
  regression was added before fixing the generator.
- Real worker subprocess killed between Bronze publication/checkpoint and between
  P3 commit/DONE; NEEDS_REVIEW and duplicate-gated resume verified. SIGTERM exits
  after the current batch. Finite retries use a fake clock; HTTP/P3 tests use real
  services and Delta.
- Maintenance invariants cover logical rows, historic versions, dry-run vacuum,
  target-size forwarding, partition/unknown Z-order columns, windows, persisted
  cooldown, UNKNOWN incomplete runs and process ownership.
- S3 E2E: six pass using unique owned prefixes in the demo MinIO bucket. Local
  tests and S3 metrics are kept separate.
- Independent model review reproduced two additional defects (Bronze/Silver
  descendant symlink escape and continuous scheduler dropping the requested
  action). RED tests preceded fixes. Reviewer recheck found both resolved, with
  no remaining critical issue in the stated single-worker scope.
- Parent review added seven RED cases for uncertain publication, structural
  rejection retry, checkpoint corruption and existing benchmark targets; also
  proved --once/skip-rejected advancement. Focused GREEN: **49 pass**.
- Mutation check replaced actual query results with expected results inside the
  benchmark equality guard. The correctness-guard test failed, then the source
  was restored. No skipped test/assertion suppression was introduced.
- Report mapping/render smoke + core compatibility: **3 pass**. All four actual
  PNG figures were opened and checked for axes, units, labels, overlap and
  dispersion; SVG variants are emitted by the same script.

## Final regression command

```bash
STORAGE_DEMO_MINIO_TEST=1 PYTHONPATH=src MPLCONFIGDIR=/tmp/storage-mpl \
  /tmp/storage-services-venv/bin/python -m pytest tests/ -q -rs
```

Final result after review and DONE evidence hardening: **394 passed, 1 skipped**, 63.24 seconds. The existing opt-in
live chDB acceptance was skipped because CHDB_SILVER_ACCEPTANCE was unset.
The six new MinIO tests were enabled and passed. Downstream source and existing
downstream tests remained unchanged. No schema/Bronze/Silver writer changes.

The previously skipped chDB file was then run unchanged with its explicit opt-in
flag against a fresh P3 table in the dedicated demo bucket: **2 passed**, 2.17 s.
This covers the live test that was skipped in the full run, without modifying
its expectations or pointing it at shared factory tables.

```bash
CHDB_SILVER_ACCEPTANCE=1 CHDB_MINIO_ENDPOINT=http://127.0.0.1:19000 \
MINIO_BUCKET=smart-dairy-storage-demo MINIO_ACCESS_KEY=storagedemo \
MINIO_SECRET_KEY=storage-demo-local-only PYTHONPATH=src \
/tmp/storage-services-venv/bin/python -m pytest tests/test_data_governance_chdb_acceptance.py -q
```

The fresh container worker uses `/demo/storage_demo/acceptance_20261117` and writes
through P3 to `s3://smart-dairy-storage-demo/02_silver_curated`, wholly inside the
dedicated demo bucket. Worker exit 0; 60 sensor rows + 1 MES row. Maintenance runs
after the worker exits and compacts sensor 3 -> 1 files; source trace remains valid.


## Runtime scope

The demo runs one POSIX worker per local namespace, then maintenance after ingest
stops. It does not establish concurrency safety with external P3 writers or
multi-host exactly-once delivery. Uncertain publication needs manual review;
previous PROCESSING outcome can be unknown. Vacuum preview does not reclaim disk.
The 100k benchmark is synthetic, with one line and two ingestion dates; 1M and
Z-order performance trials were not run. They are optional extensions, not claimed
results. Only compaction speed and counts actually measured are reported.

Authoritative dependency behavior consulted: [APScheduler 3 user guide](https://apscheduler.readthedocs.io/en/3.x/userguide.html).
The installed delta-rs `TableOptimizer.compact` signature was inspected and
its `target_size` argument verified by both call adapter and real Delta tests.

Final container replay returned SKIPPED_DUPLICATE with 0 new rows. After compaction
and vacuum preview, the table still has 60 sensor rows, 3 distinct valid Bronze
sources and overall integrity=true. Mock and MinIO remained healthy. Preview logs
confirm dry_run=true. Report Markdown table formatting was checked after render;
its focused render test passed again after the formatting correction.

A final corruption regression proved DONE without P3 evidence was accepted incorrectly
(RED: 1 failure). DONE now requires matching successful P3 identity and reported
row count; the full final suite above confirms the fix.

The final image was rebuilt after DONE validation changed; replay succeeded against
the existing real checkpoint (DONE / SKIPPED_DUPLICATE, 0 new rows). Native chDB
also ran inside the slim image and read the 60 governed sensor rows successfully.
