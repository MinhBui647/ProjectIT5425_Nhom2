# Reproducing storage benchmarks

Use the isolated service environment from `storage_baseline.md`. Start Docker
MinIO/mock from `storage_operations.md`. Stop demo ingestion before maintenance.
The harness owns a separate namespace for each invocation and refuses existing
work/report directories. It never vacuums and never uses shared factory tables.

```bash
MINIO_ENDPOINT=127.0.0.1:19000 MINIO_ACCESS_KEY=storagedemo \
MINIO_SECRET_KEY=storage-demo-local-only PYTHONPATH=src \
/tmp/storage-services-venv/bin/python -m lakehouse_storage.benchmark \
  --rows 100000 --batch-sizes 1000,10000,50000 --seed 42 --warmups 1 --repeats 5 \
  --run-id experiment_100k_20261117
```

`--run-id` must be new for each run. For local Delta use
`--allow-root "$PWD/data/storage_benchmark" --silver-root "$PWD/data/storage_benchmark/TARGET/silver"`
and a distinct CLI run id. Never combine local and S3 results into a single speedup.
Batch sizes must divide the scale, with <=50k samples/batch and <=200 batches/run.
For 1M rows choose e.g. `5000,10000,50000`.

Artifacts in `reports/storage/<run_id>`:

- `metrics.json`: controls, environment, package/container limits, query semantics,
  counts, fingerprints, before/after versions/file sizes, raw latencies, median,
  IQR and standard deviation, stage durations and worker outcomes.
- `queries.csv`: every timed repetition in seconds.
- `operations.jsonl`: compaction decision, error and before/after evidence.
- `command.json`: exact invocation; plotting adds PNG/SVG figures and summary.

The mock generates one seeded stream and slices it, so batch size does not change
sensor values. Each run has a unique business batch id, excluded from the
cross-configuration sensor fingerprint. All accepted business columns are compared
between configurations. Baseline and compacted queries are checked for equal
results before optimized latencies are recorded. Timing never gates pass/fail.

End-to-end throughput includes HTTP fetch/JSON validation, Bronze publication,
P3 validation, report/figure creation and Delta writes, including one MES row.
Mock run generation (POST) is outside that timer. `p3_seconds` includes reporting;
it is not a storage-only timer. Report input, accepted and rejected sensor counts.
All query runs use at least one warmup and five timed repetitions. The OS/object
cache is uncontrolled; call these warm workloads, not cold-cache tests. Layout
order is baseline then compacted, workload order appears in `query_text`.

The workload is synthetic: one production line and one ingestion-date partition.
The batch lookup reads timestamp/uht_temp with a batch predicate; hourly and
line/date aggregates provide separate workloads. There is no evidence here for
multi-partition pruning speedup, real factory causality or a cross-format
compression advantage. Z-order is opt-in in the maintenance runner and tested for
correctness; this initial benchmark reports compaction only. S3 retained physical
bytes are unknown. Active bytes are not reclaimed disk space.
