# Hướng dẫn demo storage

Luồng demo: mock S1/S2 → worker tuần tự → Bronze → P3 → Silver; sau đó chạy lại
batch, kiểm tra từ chối, bảo trì và truy vết. Benchmark và kết quả đo ở mục 6.

## 1. Khởi động môi trường riêng

Chạy tại thư mục gốc repository. Dùng Docker cho mock và MinIO:

```bash
docker compose -f infra/docker-compose.storage.yml build mock
docker compose -f infra/docker-compose.storage.yml up -d minio mock
export MOCK_URL=http://127.0.0.1:18010
curl --fail --silent --show-error --retry 10 --retry-connrefused --retry-delay 1 "$MOCK_URL/health"
```

Health trả `{"status":"ok"}`. Swagger UI:
[http://127.0.0.1:18010/docs](http://127.0.0.1:18010/docs); MinIO dùng cổng 19000.
Worker trên host cần venv theo
[Môi trường dịch vụ](storage_operations.md#local-service-environment).

Nếu chạy mock bằng Python local, làm theo
[hướng dẫn](storage_operations.md#b-chạy-bằng-python-local), giữ terminal server
mở và đặt `MOCK_URL=http://127.0.0.1:8010` trong terminal worker. Mock chỉ cung cấp
HTTP; worker ghi Bronze, gọi P3 và ghi Silver trong namespace demo riêng.

<a id="2-normal-ingestion-and-replay"></a>
## 2. Nạp dữ liệu và chạy lại batch

Dùng tên phiên mới cho mỗi demo. Tại terminal đã kích hoạt venv:

```bash
export PYTHONPATH=src
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo/classroom_01"
export STORAGE_DEMO_SILVER_ROOT=s3://smart-dairy-storage-demo/storage_demo/classroom_01/silver
export STORAGE_DEMO_SILVER_ALLOW_ROOT=s3://smart-dairy-storage-demo/storage_demo
export MINIO_ENDPOINT=127.0.0.1:19000
export MINIO_ACCESS_KEY=storagedemo
export MINIO_SECRET_KEY=storage-demo-local-only
export MOCK_URL="${MOCK_URL:-http://127.0.0.1:18010}"
python -m lakehouse_storage.ingestion_worker --create-run
```

Sequence đầu là MES; các sequence sau là sensor trong cửa sổ sản xuất MES đó.
Worker lưu `active_run.json`. Lấy run ID trong tệp rồi chạy:

```bash
python -m lakehouse_storage.ingestion_worker --run-id RUN_ID --replay-sequence 1
```

Kỳ vọng: P3 trả `SKIPPED_DUPLICATE`, số dòng ghi mới bằng 0, tổng dòng Silver
không đổi. P3 tạo `_contract_version`, `_quality_status`, `_quality_run_id` và
các cột lineage; giữ kiểu dữ liệu và partition hiện có.

## 3. Kiểm tra batch bị từ chối

Chọn `STORAGE_DEMO_ROOT` và nhánh Silver mới:

```bash
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo/classroom_rejection_01"
export STORAGE_DEMO_SILVER_ROOT=s3://smart-dairy-storage-demo/storage_demo/classroom_rejection_01/silver
MOCK_RUN_REQUEST='{"samples_per_batch":20,"batches":2,"scenario":"invalid-value"}' \
  python -m lakehouse_storage.ingestion_worker --create-run
```

MES được chấp nhận; sensor bị P3 từ chối (`REJECTED`), không ghi Silver sensor.
Kịch bản `missing-field` bị chặn tại kiểm tra Bronze. Mặc định worker dừng;
`--skip-rejected` ghi nhận kết quả rồi chuyển tiếp. `transient-500` minh họa retry
hữu hạn; `delay` kết hợp `MOCK_TIMEOUT_SECONDS` minh họa timeout.

## 4. Bảo trì sau khi dừng ingestion

Khôi phục cấu hình phiên nạp bình thường trước khi bảo trì và truy vết:

```bash
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo/classroom_01"
export STORAGE_DEMO_SILVER_ROOT=s3://smart-dairy-storage-demo/storage_demo/classroom_01/silver
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true MAINTENANCE_FILE_THRESHOLD=2 \
  python -m lakehouse_storage.maintenance_scheduler --once
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true \
  python -m lakehouse_storage.maintenance_scheduler --once --action vacuum_preview
```

Kiểm tra `maintenance.jsonl`: version, số file/dung lượng đang hoạt động, thời gian
và quyết định. Vacuum preview không xóa file. Cooldown được giữ qua restart.
Khóa local chỉ bảo vệ các entry point demo; phải dừng các writer bên ngoài.

## 5. Truy vết mọi nguồn

```python
import os
from lakehouse_storage import read_silver_table, trace_batch, get_version_at_date
uri = os.environ['STORAGE_DEMO_SILVER_ROOT'] + '/sensor_telemetry'
frame = read_silver_table(uri)
trace = trace_batch(uri, frame['batch_id'][0])
print(trace['integrity_verified'], len(trace['lineage_sources']))
print(get_version_at_date(uri, '2026-12-01T00:00:00+07:00'))
```

`lineage` giữ nguồn đại diện cho caller cũ; `lineage_sources` chứa đủ cặp path/hash
khác nhau. Test làm hỏng file nguồn thứ hai trong thư mục tạm để xác nhận integrity
tổng chuyển thành false, đồng thời giữ kết quả nguồn hợp lệ. Với đường dẫn Bronze
trong container, chạy trace trong cùng image và mount của worker.

## 6. Benchmark và biểu đồ

Chạy theo [Hướng dẫn benchmark](storage_benchmark.md); xem số đo và giới hạn tại
[Kết quả thực nghiệm](storage_results.md). Tạo lại biểu đồ:

```bash
python -m lakehouse_storage.benchmark_report reports/storage/RUN_ID/metrics.json
```

Bốn biểu đồ thể hiện số file hoạt động, phân bố kích thước, latency kèm độ phân tán
và thông lượng toàn trình. JSON/CSV lưu phiên bản package, quy mô, seed, truy vấn
và chính sách cache. Không kết luận compaction luôn làm truy vấn nhanh hơn.

## Kiểm chứng và giới hạn

Chỉ một worker POSIX với khóa pha local; chưa bảo đảm exactly-once phân tán.
`PUBLISHING` chưa rõ kết quả hoặc checkpoint hỏng cần kiểm tra thủ công.
Resume `PROCESSING` dùng lại Bronze; kết quả commit trước đó có thể là unknown.
Cây thư mục đầu ra chặn symlink vượt root. Writer dùng chung, vacuum xóa thật,
triển khai production và kết luận hiệu năng quy mô 1M nằm ngoài demo này.

```bash
STORAGE_DEMO_MINIO_TEST=1 PYTHONPATH=src /tmp/storage-services-venv/bin/python -m pytest tests/ -q -rs
/tmp/storage-services-venv/bin/python -m pip check
docker compose -f infra/docker-compose.storage.yml config --quiet
git diff --check
```
