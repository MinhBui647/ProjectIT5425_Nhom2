# Data Governance Acceptance Evidence

Ngày nghiệm thu: 2026-10-16 (Asia/Bangkok).

## Môi trường

- MinIO: `bitnamilegacy/minio:latest`, bucket `smart-dairy-lakehouse`.
- Marquez API/UI: `marquezproject/marquez:0.50.0` và
  `marquezproject/marquez-web:0.50.0`.
- PostgreSQL: `postgres:14`.
- Input: sáu fixture Bronze đã khóa trong `tests/fixtures/bronze`, có SHA-256
  sidecar và manifest.

Bucket/credentials đã được kiểm tra bằng S3 API trước khi chạy. Prefix Silver
ban đầu không có object.

## Kết quả MinIO end-to-end

| Bảng | Lần đầu | Dòng Silver | Delta version | Partition |
|---|---:|---:|---:|---|
| sensor_telemetry | PASS | 60 | 0 | line_id, ingestion_date |
| mes_lims | PASS | 1 | 0 | line_id, ingestion_date |
| weather | PASS | 24 | 0 | farm_id, ingestion_date |
| market_prices | PASS | 3 | 0 | ingestion_date |
| market_indices | PASS | 1 | 0 | ingestion_date |
| food_recalls | PASS | 5 | 0 | ingestion_date |

`read_silver_table()` đã đọc lại đủ sáu bảng. Mỗi schema chứa toàn bộ contract
columns và năm lineage columns bắt buộc. Mọi `_quality_status` đều là `PASS`.

Lượt chạy thứ hai trả `SKIPPED_DUPLICATE` cho 6/6 file, `rows_written = 0` và
mọi Delta table giữ version `0`.

## Artifact và lineage

- Data Docs được sinh thành công trong thư mục runtime dưới `tmp/`.
- Mỗi run có JSON quality report và figures; Sensor có đủ missingness heatmap,
  physical-band/outlier chart và lineage DAG.
- Marquez API xác nhận 6/6 job `bronze_to_silver.<table>` ở trạng thái
  `COMPLETED`, mỗi job có một input Bronze và một output Silver.
- Không còn event trong spool của lượt nghiệm thu vì tất cả emission thành công.

## Local acceptance và tests

- Clean, đúng 0,5%, trên 0,5%, checksum sai và duplicate đều đã được kiểm thử
  bằng local Delta không cần network.
- Governance coverage cuối: `93.87%` (ngưỡng yêu cầu `>=85%`), 83 test pass.
- Toàn repo: 253 test pass; `pip check`, compile và `git diff --check` đều pass.

Kết luận: các điều kiện acceptance và Definition of Done của pipeline governance đều đạt.

## Bổ sung sau review ngày 2026-10-19

### Idempotency có contract version

- Duplicate key hiện là `target_table + bronze_sha256 + contract_version`.
- Cùng bảng, SHA và contract version trả `SKIPPED_DUPLICATE` như trước.
- Cùng bảng/SHA với contract version mới, hoặc khác bảng với cùng SHA, được phép
  xử lý.
- Delta table đã tồn tại nhưng thiếu `_contract_version` trả lỗi rõ ràng; không
  tự mặc định dữ liệu legacy là `v1`.

### Downstream Silver acceptance qua chDB

Acceptance được chạy bằng `chDB 4.4.0` (`chdb-core 26.9.0`) trong Linux
container, đọc trực tiếp Delta table Sensor trên MinIO qua `deltaLake(...)`.

- URI: `02_silver_curated/sensor_telemetry`.
- Schema đọc được 20 cột; có đủ `line_id`, `uht_temp`, `_bronze_sha256`,
  `_contract_version` và `_quality_status`.
- Query `count(*)` và `avg(uht_temp) GROUP BY line_id` chạy thành công.
- Kết quả `LINE_UHT_1`: 60 dòng, `avg(uht_temp) = 139.85112838745118`.

Test live được skip mặc định trên Windows và đã chạy PASS riêng bằng Docker;
lệnh tái hiện được ghi trong `README.md`.

### Bằng chứng Marquez/OpenLineage

`reports/lineage/sensor_telemetry_marquez.json` lưu response thật từ Marquez API
cho job `bronze_to_silver.sensor_telemetry`. Bundle xác nhận:

- latest run `COMPLETED`;
- input dataset
  `file://bronze/sensor_telemetry/sensor_telemetry_20261010_051357_098374.parquet`;
- graph edge từ Bronze dataset đến job;
- graph edge từ job đến
  `s3://smart-dairy-lakehouse/02_silver_curated/sensor_telemetry`.

### Regression sau bổ sung

- Toàn repo: 260 test pass, 1 live integration test skip mặc định.
- Governance: 90 test pass, 1 live integration test skip; coverage `92.13%`
  trên package `data_governance`, vẫn vượt ngưỡng `85%`.
