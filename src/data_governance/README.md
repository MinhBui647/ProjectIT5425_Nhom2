# Data Governance: Bronze to Silver

Module này triển khai pipeline quản trị dữ liệu cho sáu bảng MVP:
`sensor_telemetry`, `mes_lims`, `weather`, `market_prices`,
`market_indices` và `food_recalls`.

Luồng xử lý cho mỗi file là:

`checksum Bronze -> transform canonical -> validate/outlier -> quality gate -> reports/Data Docs -> Delta Silver -> OpenLineage`

Bronze luôn chỉ đọc. Delta chỉ được commit sau khi các kiểm tra và artifact bắt
buộc đã hoàn tất.

## Cài đặt

Yêu cầu Python 3.11 và Docker Desktop.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install -e .
```

Khởi động dịch vụ demo:

```powershell
docker compose -p dairy-minio -f infra/docker-compose.minio.yml up -d
docker compose -p dairy-marquez -f infra/docker-compose.marquez.yml up -d
```

- MinIO API/Console: `http://localhost:9000` / `http://localhost:9001`
- Marquez API/UI: `http://localhost:5000` / `http://localhost:3000`
- Credentials MinIO chỉ dùng demo: `dairy` / `hustit5425`
- Bucket: `smart-dairy-lakehouse`

## Chạy pipeline

Chạy cả sáu bảng trên MinIO, sinh Data Docs và gửi lineage đến Marquez:

```powershell
.\.venv\Scripts\python.exe -m data_governance.run_governance `
  --tables sensor_telemetry,mes_lims,weather,market_prices,market_indices,food_recalls `
  --bronze-root tests/fixtures/bronze `
  --fail-threshold 0.005 `
  --build-data-docs `
  --lineage-endpoint http://localhost:5000
```

Chạy local không cần dịch vụ mạng:

```powershell
.\.venv\Scripts\python.exe -m data_governance.run_governance `
  --table sensor_telemetry `
  --bronze-root tests/fixtures/bronze `
  --silver-root tmp/local_silver `
  --no-lineage
```

Các tùy chọn chính:

- `--table` lặp lại hoặc `--tables` nhận danh sách phân cách bằng dấu phẩy.
- `--date-from/--date-to` lọc theo ngày nghiệp vụ, định dạng `YYYY-MM-DD`.
- `--dry-run` chạy toàn bộ kiểm tra nhưng không ghi Silver.
- `--build-data-docs` chạy checkpoint GX và dựng HTML.
- `--no-lineage` tắt emission; `--replay-lineage` phát lại spool trước khi chạy.
- `--lineage-spool` đổi thư mục lưu event khi Marquez unavailable.

Exit code:

- `0`: không có file FAIL; gồm `PASS`, `PASS_WITH_REJECTIONS` và duplicate.
- `2`: có quality/checksum FAIL.
- `3`: lỗi hạ tầng, cấu hình hoặc lỗi ngoài dự kiến.

CLI luôn tiếp tục các file còn lại và in một JSON summary ở cuối.

## Contract và quality gate

Contract version hiện tại là `v1`; registry vận hành nằm tại
`src/data_governance/contracts.py`, còn schema Silver và partition nằm tại
`src/lakehouse_storage/schemas.py`.

- Hard failure (checksum, thiếu cột, sai kiểu, bảng rỗng) không được ghi Silver.
- Tỷ lệ dòng lỗi `> 0.5%` trả `FAIL`, Delta không đổi.
- Tỷ lệ dòng lỗi `<= 0.5%` trả `PASS_WITH_REJECTIONS`; dòng hợp lệ vào Silver,
  dòng lỗi vào quarantine.
- Sensor được bổ sung Hampel/Rolling Z-score `is_anomaly_flag`.
- Rerun cùng bảng, `_bronze_sha256` và `_contract_version` trả
  `SKIPPED_DUPLICATE`; cùng SHA với contract version mới được phép xử lý lại.
- Delta table đã tồn tại nhưng thiếu `_contract_version` bị từ chối rõ ràng;
  pipeline không tự suy đoán legacy table là `v1`.

## Artifact

- Delta Silver: `s3://smart-dairy-lakehouse/02_silver_curated/<table>`.
- JSON quality summary: `reports/data_quality/<run_id>/summary.json`.
- Quarantine: `data/quality_quarantine/<table>/<date>/<run_id>/`.
- Data Docs: `reports/data_docs/index.html`.
- Figures: `reports/figures/<run_id>/`.
- OpenLineage spool: `reports/lineage_spool/<run_id>/`.
- Marquez API evidence: `reports/lineage/sensor_telemetry_marquez.json`.

Mỗi Silver row giữ `_bronze_file`, `_bronze_sha256`, `_quality_status`,
`_quality_run_id` và `_contract_version` để truy vết.

Xuất lại bằng chứng graph Marquez cho bảng Sensor:

```powershell
.\.venv\Scripts\python.exe -m data_governance.marquez_evidence `
  --endpoint http://localhost:5000 `
  --output reports/lineage/sensor_telemetry_marquez.json
```

File chỉ được ghi khi API trả cả input Bronze và output Silver; bundle giữ response
job, runs và lineage graph để review mà không phụ thuộc screenshot UI.

## Downstream Silver acceptance bằng chDB

Downstream analytics dùng `chDB (ClickHouse in-process)`. chDB chạy trên Linux; ở máy Windows,
dùng container cùng network MinIO để query trực tiếp Delta snapshot thay vì đọc
glob các file Parquet:

```powershell
$workspace = (Resolve-Path '.').Path
docker run --rm --network dairy-minio_default `
  -v "${workspace}:/workspace:ro" -w /workspace `
  -e PYTHONPATH=/workspace/src `
  -e CHDB_MINIO_ENDPOINT=http://minio:9000 `
  -e MINIO_BUCKET=smart-dairy-lakehouse `
  -e MINIO_ACCESS_KEY=dairy `
  -e MINIO_SECRET_KEY=hustit5425 `
  python:3.11-slim sh -lc `
  "pip install --no-cache-dir chdb==4.4.0 && python -m data_governance.chdb_silver_acceptance"
```

Acceptance kiểm tra schema bắt buộc rồi chạy `count(*)` và `avg(uht_temp)` theo
`line_id`. Pytest live tương ứng nằm tại
`tests/test_data_governance_chdb_acceptance.py` và
chỉ bật khi đặt `CHDB_SILVER_ACCEPTANCE=1` trong môi trường có chDB + MinIO.

## Kiểm tra và xử lý sự cố

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m pip check
docker compose -p dairy-minio -f infra/docker-compose.minio.yml ps
docker compose -p dairy-marquez -f infra/docker-compose.marquez.yml ps
```

- Không kết nối MinIO: kiểm tra port `9000`, credentials và bucket; lỗi ghi trả
  exit `3`.
- Marquez tạm ngừng: quality pipeline vẫn giữ kết quả, event được spool; bật lại
  dịch vụ rồi chạy với `--replay-lineage`.
- Quality exit `2`: mở `summary.json`; không sửa Bronze, hãy sửa nguồn hoặc tạo
  Bronze batch mới.
- Port `3000/5000/9000/9001` bận: dừng process đang dùng port hoặc đổi mapping
  compose và endpoint CLI tương ứng.
- Trên Windows path dài: dùng tên `--gx-project-root` ngắn; identifier GX trong
  module đã được rút gọn để tương thích workspace OneDrive sâu.

Dừng dịch vụ nhưng giữ volume demo:

```powershell
docker compose -p dairy-marquez -f infra/docker-compose.marquez.yml down
docker compose -p dairy-minio -f infra/docker-compose.minio.yml down
```

Kết quả nghiệm thu gần nhất xem tại `ACCEPTANCE_REPORT.md` cùng thư mục.
