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
Python. For a container-only demo, use option A below instead.

## Setup mock server: chọn Docker hoặc Python local

Mock server sinh dữ liệu giả S1 (sensor) và S2 (MES), lưu payload của từng run,
rồi cung cấp qua HTTP. Server không tự đẩy dữ liệu và không ghi Bronze/Silver.
Worker gọi API để lấy dữ liệu và đưa qua P3. Chạy riêng mock không cần MinIO.
Các lệnh dưới đây chạy từ thư mục gốc repository, trên Bash.

### A. Chạy bằng Docker Compose

Cần Docker Engine, Docker Compose v2, `curl` và Python 3 trên host để chạy các
lệnh kiểm tra API bên dưới. Không cần tạo venv nếu chỉ chạy mock bằng Docker.

```bash
docker compose -f infra/docker-compose.storage.yml build mock
docker compose -f infra/docker-compose.storage.yml up -d mock
docker compose -f infra/docker-compose.storage.yml ps mock
export MOCK_URL=http://127.0.0.1:18010
curl --fail --silent --show-error --retry 10 --retry-connrefused --retry-delay 1 "$MOCK_URL/health"
```

Kết quả health phải là `{"status":"ok"}`. Mở Swagger UI tại
[http://127.0.0.1:18010/docs](http://127.0.0.1:18010/docs) để thử API.
Host dùng cổng **18010**; các container trong cùng Compose dùng
`http://mock:8010`. Payload nằm trên named volume `payloads`, mount vào
`/demo/storage_demo/mock`; restart container vẫn đọc lại được run cũ.

Xem log hoặc dừng riêng mock, không xóa volume:

```bash
docker compose -f infra/docker-compose.storage.yml logs --tail=100 mock
docker compose -f infra/docker-compose.storage.yml stop mock
```

Chỉ chạy lệnh `stop` khi đã thử API/worker xong. Chạy lại bằng `up -d mock`.

### B. Chạy bằng Python local

Tạo venv theo mục "Local service environment" ở trên. Tại **terminal 1**:

```bash
source /tmp/storage-services-venv/bin/activate
export PYTHONPATH=src
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo/mock_local"
export MOCK_STORE="$STORAGE_DEMO_ROOT/mock"
export MOCK_HOST=127.0.0.1
export MOCK_PORT=8010
python -m lakehouse_storage.mock_server
```

Giữ terminal này chạy; Uvicorn phải báo đang nghe tại `http://127.0.0.1:8010`.
Payload được lưu tại `data/storage_demo/mock_local/mock/<run_id>/`.
`MOCK_STORE` phải nằm bên trong `STORAGE_DEMO_ROOT`, root có thành phần
`storage_demo` hoặc `storage_benchmark`. Nhấn **Ctrl+C** để dừng; chạy lại cùng
root sẽ dùng lại payload đã lưu.

Tại **terminal 2**, vẫn ở thư mục gốc repository:

```bash
export MOCK_URL=http://127.0.0.1:8010
curl --fail --silent --show-error "$MOCK_URL/health"
```

Mở Swagger UI tại [http://127.0.0.1:8010/docs](http://127.0.0.1:8010/docs).
Nếu đổi `MOCK_PORT`, đổi cả port trong `MOCK_URL` ở terminal gọi API/worker.

### Tạo run và lấy dữ liệu: dùng được cho cả A và B

Giữ `MOCK_URL` tương ứng với cách vừa chọn. Các lệnh sau dùng `curl` và
`python3`, không cần `jq`. Tạo một run gồm **1 MES row + 40 sensor rows**:

```bash
RUN_ID=$(curl --fail --silent --show-error -X POST "$MOCK_URL/runs" \
  -H 'Content-Type: application/json' \
  -d '{"seed":42,"start_time":"2026-01-15T00:00:00Z","samples_per_batch":20,"batches":2,"scenario":"normal"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')
echo "$RUN_ID"
curl --fail --silent --show-error "$MOCK_URL/runs/$RUN_ID" | python3 -m json.tool
curl --fail --silent --show-error "$MOCK_URL/runs/$RUN_ID/batches/0" | python3 -m json.tool
curl --fail --silent --show-error "$MOCK_URL/runs/$RUN_ID/batches/1" \
  | python3 -c 'import json,sys; b=json.load(sys.stdin); print(b["table_name"], b["sequence"], len(b["data"]))'
```

Metadata có `batch_count=3`: sequence **0** là `mes_lims` (1 row), sequence
**1, 2** là `sensor_telemetry` (20 rows/batch). Lệnh cuối in
`sensor_telemetry 1 20`. Batch response có `run_id`, `sequence`, `table_name`,
`payload_sha256`, `data`. GET lại cùng run/sequence trả đúng payload bytes đã lưu;
POST lại tạo run mới. Sensor timestamps cách nhau 1 giây trong cửa sổ sản xuất
MES; tốc độ HTTP không phải tốc độ phát sensor theo thời gian thực.

### Nối worker với mock

Nếu worker chạy trên host, activate venv ở terminal worker và giữ `MOCK_URL`
đúng cổng **18010 (Docker)** hoặc **8010 (local)**. Để ghi Silver lên MinIO,
khởi động thêm dịch vụ:

```bash
docker compose -f infra/docker-compose.storage.yml up -d minio
```

Làm tiếp [walkthrough, mục 2](storage_demo.md#2-normal-ingestion-and-replay) để
cấu hình namespace và credentials. Lệnh `--create-run` trong walkthrough tự tạo
run mới (hoặc dùng active run đã lưu); để nạp đúng run vừa tạo qua curl, thay
lệnh đó bằng `python -m lakehouse_storage.ingestion_worker --run-id "$RUN_ID"`
trong cùng terminal chứa biến `RUN_ID`. Khi worker chạy trong Compose, URL đã
được cấu hình là `http://mock:8010`.

| Hiện tượng | Kiểm tra |
|---|---|
| Connection refused | Server còn chạy, `MOCK_URL` đúng cổng, health đã trả 200 |
| Address already in use | Chọn `MOCK_PORT` khác khi chạy local; Docker dùng `STORAGE_DEMO_MOCK_PORT` và cập nhật URL host |
| HTTP 404 | `RUN_ID` đúng, sequence từ 0 đến `batch_count - 1`, dùng đúng server/store đã tạo run |
| HTTP 422 khi POST | `samples_per_batch`: 1-50.000; `batches`: 1-200; tổng sensor rows không quá 1.000.000; scenario hợp lệ |
| Worker không kết nối MinIO | Mock health không kiểm tra MinIO; kiểm tra riêng endpoint, credentials và dịch vụ MinIO |

## Local worker and maintenance

Sau khi mock đã chạy, tại terminal khác có venv và `PYTHONPATH=src`. Ví dụ này
dùng mock local mặc định ở cổng 8010 và Silver local; nếu dùng mock Docker,
đổi `MOCK_URL` sang `http://127.0.0.1:18010`.

```bash
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo"
export MOCK_URL=http://127.0.0.1:8010
# --create-run stores identity for restart.
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

## Post-review governed writes and handoff (2026-11-17)

For generator output consumed by Gold, use the CLI examples in
[`lakehouse_storage/README.md`](../src/lakehouse_storage/README.md). Both CLIs now
run P3; explicit `--silver-root` (or `STORAGE_GENERATOR_SILVER_ROOT`) is required
for Delta output. JSON includes actual gate status/counts; FAIL exits 1. Existing
low-level generator helpers remain available for storage-only callers and do not
establish Gold eligibility. Existing tables missing any of `_contract_version`,
`_quality_status`, `_quality_run_id` fail clearly; migration remains an explicit
separate operation.

Keep ingestion sequential. P3 checksum idempotency currently ignores date filters
and has a check/append concurrency race. Reproducers and evidence are in
`reports/storage/post_review_20261117/`. FAO `Month` datetime parsing is also a P3
handoff: do not alter Bronze or lower the gate to load it. P2's relative crawler
root differs from storage when launched at repo root; verify resolved roots before
collecting new input. No downstream module was changed in this follow-up.

Bronze publishing now uses exclusive reservations, same-filesystem staging and
no-replace hard links (Linux verification). A `.parquet` appears only after its
checksum is ready. Hidden staging, `.reserve`, or orphan `.sha256` may survive a
crash; they are excluded from Parquet inputs. Preserve them for inspection rather
than deleting or republishing blindly. Do not use a filesystem lacking exclusive
creation/hard-link semantics for this protocol. Existing valid legacy pairs work.

Silver named-table writes validate required/null/cast constraints before commit;
existing nullable Delta metadata is not automatically migrated. Version diff now
uses entity keys and raises on null/duplicate keys. For unknown table shapes,
pass `id_column` explicitly (string/list/tuple). Updates sharing a key are outside
added/removed semantics.

Re-run isolated S3 verification:

```bash
docker compose -f infra/docker-compose.storage.yml up -d minio
STORAGE_DEMO_MINIO_TEST=1 PYTHONPATH=src TMPDIR=/tmp \
  PYTHONDONTWRITEBYTECODE=1 MPLCONFIGDIR=/tmp/storage-mpl \
  .venv/bin/python -B -m pytest tests/test_storage_minio_demo.py -q -s
```

The demo bucket/prefix is separate from shared Silver. Maintenance remains a
second phase after ingestion stops; vacuum remains preview-only. Local Delta
verification uses `/tmp` paths without spaces due to the pinned environment's
percent-encoded-path read limitation.
