# Demo tái lập đến Silver

## 1. Prerequisites và clean environment

Cần Docker Engine/Compose, đủ RAM cho LocalExecutor/P3 và dung lượng image.
Build context chỉ copy requirements/source/tests/DAG/init; không dùng dữ liệu hoặc
notebook đang có. Thực hiện từ root repository. Một project name mới tạo volumes
QA sạch mà không xóa project/volumes đang dùng.

```bash
cp infra/airflow.env.example /tmp/lakehouse-demo.env
docker compose --env-file /tmp/lakehouse-demo.env -p lakehouse-demo \
  -f infra/docker-compose.lakehouse.yml build
docker compose --env-file /tmp/lakehouse-demo.env -p lakehouse-demo \
  -f infra/docker-compose.lakehouse.yml up -d
docker compose -p lakehouse-demo -f infra/docker-compose.lakehouse.yml ps
```

Expected: volume-init và airflow-init exit 0; PostgreSQL/MinIO/mock/API/scheduler/
DAG processor healthy. Đợi health checks, không trigger ngay sau khi container
vừa tạo. volume-init chown runtime/log/payload về AIRFLOW_UID, không xóa artifact.
UI/API ở localhost:28080, mock:28010, MinIO:29000. Đổi ports trong env nếu trùng.

## 2. Quick S1/S2 bằng CLI

```bash
docker compose -p lakehouse-demo -f infra/docker-compose.lakehouse.yml exec -T \
  airflow-scheduler /opt/processing/bin/python -m lakehouse_pipeline \
  --identity quick-001 --request '{"samples_per_batch":20,"batches":2}'
```

Expected SUCCESS, mes_lims 1 row và sensor_telemetry 40 rows trong run đầu trên
target rỗng; version map có hai bảng. Gọi lại đúng identity/request không tăng rows.
Giữ metadata khi retry; đổi request cùng identity bị conflict.

## 3. Full S1–S5 bằng scheduler thật

```bash
docker compose -p lakehouse-demo -f infra/docker-compose.lakehouse.yml exec -T \
  airflow-scheduler airflow dags list-import-errors
docker compose -p lakehouse-demo -f infra/docker-compose.lakehouse.yml exec -T \
  airflow-scheduler airflow dags unpause lakehouse_pipeline
docker compose -p lakehouse-demo -f infra/docker-compose.lakehouse.yml exec -T \
  airflow-scheduler airflow dags trigger lakehouse_pipeline --run-id manual-full-001 \
  --conf '{"window_start":"2026-01-15T00:00:00Z","window_end":"2026-01-16T00:00:00Z","request":{"samples_per_batch":20,"batches":2,"fao_variant":"both"}}'
```

Expected 25 tasks success, six Silver tables and ten Bronze files: MES 1, sensor
2, GDT 1, FAO 2, weather 1, FDA 1, USDA 2. Full run target rỗng có sensor40,
MES1, GDT3, FAO7 (long1 + wide6), weather5, FDA1; USDA không có Silver.
Nếu quick/demo trước đó cùng target, total_rows còn gồm commit trước; manifest
lưu accepted rows riêng theo Bronze checksum. Dữ liệu giống hệt có thể được P3
nhận là duplicate nên không suy total_rows bằng số lần trigger nhân số mẫu.

Không có import errors. Xem trạng thái/logs qua UI hoặc `airflow dags list-runs
--dag-id lakehouse_pipeline`. Named volumes giữ payload/state/manifest/logs.
Manifest ở `/opt/lakehouse/storage_demo/runs/sha256(identity).json`, identity DAG
là `lakehouse_pipeline/manual-full-001`.

## 4. Query, checksum và snapshot lịch sử

```bash
docker compose -p lakehouse-demo -f infra/docker-compose.lakehouse.yml exec -T \
  airflow-scheduler /opt/processing/bin/python - <<'PY'
import hashlib, json
from pathlib import Path
from lakehouse_pipeline.silver_snapshot import read_snapshot
from lakehouse_storage.bronze_writer import verify_checksum
identity = 'lakehouse_pipeline/manual-full-001'
path = Path('/opt/lakehouse/storage_demo/runs') / (hashlib.sha256(identity.encode()).hexdigest()+'.json')
m = json.loads(path.read_text())
assert m['status'] == 'SUCCESS'
for r in m['artifacts']:
    assert verify_checksum(r['bronze_path'])
for table in m['silver_snapshot']:
    frame = read_snapshot(m['silver_snapshot'], table)
    print(table, m['silver_snapshot'][table]['version'], frame.height)
PY
```

Với sensor có thể gọi `time_travel.trace_batch(uri, batch_id)` để xem tất cả
lineage_sources và integrity tổng. Snapshot version vẫn đọc được sau append hoặc
compaction mới nếu chưa bị vacuum. Đây không phải backup hay multi-table ACID.

## 5. Replay, transient, lỗi và restart

Trong UI clear cả DAG run sau thành công; prepare dùng lại identity/key và
Bronze paths, rows/version map không tăng do clear. Restart `mock` và
`airflow-scheduler` bằng Compose `restart`, rồi clear/retry vẫn dùng volume cũ.

Trigger run ID mới và request scenario transient-500: tối đa hai HTTP lỗi mỗi
sequence rồi thành công trong budget ba HTTP attempts. invalid-value ở sensor
bị P3 reject; missing-field bị Bronze reject. Expected DAG failed,
report_run success nhưng finalize_run upstream_failed; manifest FAILED giữ
MES Silver commit đã có. Không hứa rollback toàn run. Xem quality reports và
quarantine trên runtime volume. Không đổi threshold/schema để làm run xanh.

Checksum experiment chỉ trên bản demo riêng: sửa sidecar của một artifact, clear
verify task và downstream. Expected NEEDS_REVIEW và DAG failed. Khôi phục sidecar
chưa đồng nghĩa checkpoint review đã được giải quyết; cần operator kiểm evidence.
Không thử phá checksum trên dữ liệu chung.

## 6. Maintenance và stop

Sau ingestion và external writers dừng, bật MAINTENANCE_ENABLED=true và
MAINTENANCE_INGESTION_STOPPED=true trong env, up -d để cập nhật services.
Trigger lakehouse_maintenance với conf action compact. Expected RUN khi đủ file
nhỏ; SKIP khi threshold/window/cooldown không đạt; FAIL trả task lỗi.
Kiểm logical rows và pinned read trước/sau; vacuum_preview chỉ liệt kê candidate,
không giải phóng dung lượng. Cooldown lưu trong maintenance.jsonl qua restart.

```bash
docker compose -p lakehouse-demo -f infra/docker-compose.lakehouse.yml stop
# Resume without deleting data:
docker compose --env-file /tmp/lakehouse-demo.env -p lakehouse-demo \
  -f infra/docker-compose.lakehouse.yml up -d
```

[Code walkthrough](lakehouse_code_walkthrough.md), [operations](storage_operations.md),
[Airflow](lakehouse_airflow.md), [mock](mock_server.md), [versioning](data_versioning.md).
