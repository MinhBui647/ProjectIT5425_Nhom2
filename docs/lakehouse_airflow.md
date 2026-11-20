# Airflow source → Bronze → Silver

## Môi trường và chạy stack

Airflow 3.1.7, standard provider 1.11.0, Python 3.12; image có hai interpreter.
`python` chạy Airflow, `/opt/processing/bin/python` chạy core/P3. PostgreSQL 18.1
lưu metadata. LocalExecutor chạy task trong scheduler container, không cần Redis,
Celery worker hoặc Docker socket. API server và DAG processor là service riêng.
Căn cứ: [Airflow 3.1.7 Compose](https://airflow.apache.org/docs/apache-airflow/3.1.7/howto/docker-compose/index.html)
và [architecture](https://airflow.apache.org/docs/apache-airflow/3.1.7/core-concepts/overview.html).

```bash
cp infra/airflow.env.example /tmp/lakehouse-airflow.env
# Set non-default secrets in this untracked file if required by your environment.
docker compose --env-file /tmp/lakehouse-airflow.env \
  -f infra/docker-compose.lakehouse.yml build
docker compose --env-file /tmp/lakehouse-airflow.env \
  -f infra/docker-compose.lakehouse.yml up -d
docker compose -f infra/docker-compose.lakehouse.yml ps
```

Service `volume-init` sets runtime/log/payload ownership to AIRFLOW_UID before mock and Airflow init.
Named volumes: runtime `/opt/lakehouse/storage_demo`, payloads `.../mock`, logs
`/opt/airflow/logs`, PostgreSQL `/var/lib/postgresql`, MinIO data. Init migrates DB
and sets `lakehouse_mutation` pool=1. All task processes use same UID/GID and
absolute paths. Never restart with down -v. Use a unique Compose project `-p`
for a clean experiment; existing project volumes remain untouched.

Local ports: API/UI 28080, mock 28010, MinIO 29000; change LAKEHOUSE_*_PORT env if
needed. This stack uses its own demo bucket/prefix and never shared factory URI.
SimpleAuth uses `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`. The all-admin demo is bound to localhost; do not expose this demo stack
as a public service. Credentials are env-only and absent from data manifests.

## DAGs, trigger và window

Ingestion graph: prepare → [bronze → verify → govern] per selected source →
snapshot_silver → report_run → finalize_run. Finalize also directly depends on
snapshot. report_run all_done is an observer, not a success leaf. REJECTED,
NEEDS_REVIEW, missing required source and required NO_DATA fail the run.
USDA has no Silver stage mutation; its govern callable confirms Bronze-only.

```bash
docker compose -f infra/docker-compose.lakehouse.yml exec airflow-scheduler \
  airflow dags list-import-errors
docker compose -f infra/docker-compose.lakehouse.yml exec airflow-scheduler \
  airflow dags unpause lakehouse_pipeline
docker compose -f infra/docker-compose.lakehouse.yml exec airflow-scheduler \
  airflow dags trigger lakehouse_pipeline --run-id manual-full-001 \
  --conf '{"window_start":"2026-01-15T00:00:00Z","window_end":"2026-01-16T00:00:00Z","request":{"fao_variant":"both","samples_per_batch":80,"batches":2}}'
```

Manual runs require explicit timezone-aware start/end. Default source list is all
seven tables. A request may select S1/S2 for quick runs. Default schedule=None;
set LAKEHOUSE_CRON_UTC to enable an explicit UTC CronDataIntervalTimetable, then
restart parser/scheduler. Scheduled window uses data_interval_start/end, not
now(). max_active_runs=1, catchup=False. Do not use bare cron semantics to infer
a day window. Maintenance has its own optional MAINTENANCE_CRON_UTC.

## Retry, clear, restart và khóa

HTTP has max 3 attempts (timeout/network/429/5xx). Ingestion task has 1 retry,
5-second retry delay, execution timeout 10 minutes; processing subprocess timeout
540 seconds. Maximum HTTP attempts for one task operation = 3 × 2 = 6. Idempotent
POST uses the same key across task retries; business/schema/quality/review errors
raise AirflowFailException and bypass retry. Maintenance has zero automatic retries
because a mutation may have an unknown outcome.

Use task logs and the manifest to diagnose failures. Clear only affected task(s)
and required downstream after diagnosis; preserve prepare identity and volumes.
The durable mapping is keyed by dag_id/run_id; XCom is only a reference. Retry
reads the same manifest and Bronze path. A PUBLISHING orphan is NEEDS_REVIEW and
requires operator inspection, not a new automatic publish. Clearing a failed
quality task without changing source evidence must fail again.

All stages use pool lakehouse_mutation plus `demo-phase.lock` on the runtime
volume. Manual CLI must use the same runtime volume. Stop writers outside this
mechanism or use a separate namespace. These guards cover one host, not arbitrary
external writers or multi-host exactly-once.

## Maintenance, cutover và rollback

Leave maintenance disabled until ingestion and external writers for the target
are stopped. Set MAINTENANCE_ENABLED=true and MAINTENANCE_INGESTION_STOPPED=true,
restart Airflow services with that env, then trigger lakehouse_maintenance with
`{"action":"compact"}` or `{"action":"vacuum_preview"}`. The task acquires the
same local phase lock. Cooldown persists in maintenance.jsonl across restart.
Z-order requires explicit nonpartition columns. Vacuum is preview-only >=168h.

During cutover, stop the legacy scheduler service first; never start both schedulers
against the same target. Retain its old image and all volumes. To rollback, pause
both new DAGs, wait for running tasks to stop, stop Airflow scheduler, then run the
old image only with exclusive access. Do not reset DB or remove runtime/history.
New metadata/manifest paths do not authorize automatic migration of legacy state.
