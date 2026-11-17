# Hướng dẫn vận hành demo storage

Demo dùng một worker tuần tự trong namespace riêng. Khóa local không điều phối
writer bên ngoài hoặc bảo đảm exactly-once phân tán. Root local phải có thành phần
`storage_demo` hoặc `storage_benchmark`; target ngoài allow-root bị từ chối.

<a id="local-service-environment"></a>
## Môi trường dịch vụ local

Tại thư mục gốc repository, tạo venv riêng với dependency đã pin rồi kích hoạt:

```bash
python3 -m venv /tmp/storage-services-venv
/tmp/storage-services-venv/bin/python -m pip install \
  -r requirements.txt -r infra/requirements-storage.txt -c infra/constraints-storage.txt
/tmp/storage-services-venv/bin/python -m pip check
source /tmp/storage-services-venv/bin/activate
export PYTHONPATH=src
```

Constraints gồm dependency gián tiếp. Các lệnh dưới dùng Python của venv này và
`PYTHONPATH=src`. Nếu chỉ chạy container, chọn phương án A.

## Khởi động mock server

Mock sinh S1 (sensor) và S2 (MES), lưu payload từng run rồi cung cấp qua HTTP.
Worker lấy batch và gọi P3. Mock không tự ghi Bronze/Silver hoặc cần MinIO.
Các lệnh dưới dùng Bash tại thư mục gốc repository.

### A. Chạy bằng Docker Compose

Host cần Docker Engine, Compose v2, `curl` và Python 3. Chỉ chạy mock không cần venv:

```bash
docker compose -f infra/docker-compose.storage.yml build mock
docker compose -f infra/docker-compose.storage.yml up -d mock
docker compose -f infra/docker-compose.storage.yml ps mock
export MOCK_URL=http://127.0.0.1:18010
curl --fail --silent --show-error --retry 10 --retry-connrefused --retry-delay 1 "$MOCK_URL/health"
```

Health trả `{"status":"ok"}`. Swagger UI:
[http://127.0.0.1:18010/docs](http://127.0.0.1:18010/docs).
Host dùng cổng **18010**; container gọi `http://mock:8010`. Volume `payloads` mount
vào `/demo/storage_demo/mock`, giữ payload khi restart.

Xem log hoặc dừng mock sau khi thử API/worker; `stop` không xóa volume. Khởi động
lại bằng `up -d mock`:

```bash
docker compose -f infra/docker-compose.storage.yml logs --tail=100 mock
docker compose -f infra/docker-compose.storage.yml stop mock
```

### B. Chạy bằng Python local

Tại **terminal 1**, kích hoạt môi trường dịch vụ và chạy server:

```bash
source /tmp/storage-services-venv/bin/activate
export PYTHONPATH=src
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo/mock_local"
export MOCK_STORE="$STORAGE_DEMO_ROOT/mock"
export MOCK_HOST=127.0.0.1
export MOCK_PORT=8010
python -m lakehouse_storage.mock_server
```

Giữ terminal mở; Uvicorn nghe tại `http://127.0.0.1:8010`. Payload nằm trong
`data/storage_demo/mock_local/mock/<run_id>/`. `MOCK_STORE` phải nằm dưới
`STORAGE_DEMO_ROOT`; root phải chứa `storage_demo` hoặc `storage_benchmark`.
Ctrl+C dừng server; chạy lại cùng root sẽ đọc payload cũ.

Tại **terminal 2**, ở thư mục gốc repository:

```bash
export MOCK_URL=http://127.0.0.1:8010
curl --fail --silent --show-error "$MOCK_URL/health"
```

Swagger UI: [http://127.0.0.1:8010/docs](http://127.0.0.1:8010/docs).
Nếu đổi `MOCK_PORT`, cập nhật `MOCK_URL` trong terminal gọi API/worker.

### Tạo run và đọc batch

Giữ `MOCK_URL` theo phương án đã chọn. Dùng `curl` và `python3`, không cần `jq`.
Tạo run gồm **1 dòng MES + 40 dòng sensor**:

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

Metadata trả `batch_count=3`: sequence **0** là `mes_lims` (1 dòng); sequence
**1, 2** là `sensor_telemetry` (20 dòng/batch). Lệnh cuối in
`sensor_telemetry 1 20`. Envelope gồm `run_id`, `sequence`, `table_name`,
`payload_sha256`, `data`. GET cùng run/sequence trả nguyên byte đã lưu; POST tạo
run mới. Sensor lấy mẫu mỗi giây trong cửa sổ MES, độc lập tốc độ truyền HTTP.

### Kết nối worker với mock

Kích hoạt venv trong terminal worker. Host gọi cổng **18010** nếu mock chạy Docker,
hoặc **8010** nếu chạy local. Muốn ghi Silver trên MinIO, khởi động dịch vụ:

```bash
docker compose -f infra/docker-compose.storage.yml up -d minio
```

Cấu hình namespace và thông tin kết nối theo
[Demo, mục 2](storage_demo.md#2-normal-ingestion-and-replay). `--create-run` tạo run
mới hoặc dùng active run đã lưu. Để nạp run vừa tạo bằng curl, dùng
`python -m lakehouse_storage.ingestion_worker --run-id "$RUN_ID"` tại terminal có
biến `RUN_ID`. Worker trong Compose dùng `http://mock:8010`.

| Hiện tượng | Cách kiểm tra |
|---|---|
| Connection refused | Server đang chạy, `MOCK_URL` đúng cổng, health trả 200 |
| Address already in use | Đổi `MOCK_PORT` khi chạy local; với Docker, đổi `STORAGE_DEMO_MOCK_PORT` và URL host |
| HTTP 404 | Đúng `RUN_ID`, sequence thuộc 0 đến `batch_count - 1`, đúng server/store |
| HTTP 422 khi POST | `samples_per_batch`: 1-50.000; `batches`: 1-200; tổng sensor không quá 1.000.000 dòng; scenario hợp lệ |
| Không kết nối MinIO | Kiểm tra riêng endpoint, thông tin xác thực và dịch vụ MinIO; mock health không kiểm tra MinIO |

## Worker và bảo trì local

Chạy mock trước; mở terminal khác với venv và `PYTHONPATH=src`. Ví dụ dưới dùng
mock local cổng 8010 và Silver local. Với mock Docker, đổi `MOCK_URL` sang
`http://127.0.0.1:18010`.

```bash
export STORAGE_DEMO_ROOT="$PWD/data/storage_demo"
export MOCK_URL=http://127.0.0.1:8010
# --create-run lưu identity để dùng lại khi restart.
python -m lakehouse_storage.ingestion_worker --create-run
python -m lakehouse_storage.ingestion_worker --run-id RUN_ID --replay-sequence 1
# Chỉ chạy sau khi worker đã thoát:
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true MAINTENANCE_FILE_THRESHOLD=2 \
  python -m lakehouse_storage.maintenance_scheduler --once
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true \
  python -m lakehouse_storage.maintenance_scheduler --once --action vacuum_preview
```

Scheduler ghi `SKIP` đến khi cả hai cờ bảo trì bằng true. Lịch và khung giờ dùng
UTC: `MAINTENANCE_WINDOW_START_UTC`, `MAINTENANCE_WINDOW_END_UTC`,
`MAINTENANCE_CRON_UTC`. `demo-phase.lock` ngăn các entry point demo chạy chồng;
không khóa P3, host khác hoặc writer dùng chung. Vacuum luôn `dry_run=True`,
retention tối thiểu 168 giờ và bật enforcement. Z-order chỉ bật khi cấu hình,
dùng cột có sẵn không thuộc partition.

## Kịch bản và xử lý lỗi

| Scenario | Phản hồi HTTP cho sensor | Kết quả dự kiến |
|---|---|---|
| normal | 200, byte đã lưu không đổi | P3 chấp nhận |
| invalid-value | 200, uht_temp=-10 | P3 chặn tại quality gate |
| invalid-time | 200, timestamp sai | Lỗi transform/kiểm định P3 |
| missing-field | 200, thiếu line_id | Bronze chặn do thiếu cột bắt buộc |
| transient-500 | N lần đầu mỗi sequence lỗi, sau đó 200 | HTTP retry hữu hạn |
| delay | Trễ 0-5 giây | Retry timeout nếu timeout cấu hình ngắn hơn |

POST `/runs` tạo identity mới, **không idempotent**, nên client không tự retry.
Request điều khiển sai trả 422; run/sequence không tồn tại trả 404. GET chạy lại
trả nguyên byte envelope đã lưu, gồm metadata. API chỉ phục vụ demo local; bind
vào loopback hoặc mạng Compose riêng.

Checkpoint lưu ở `worker_state/<run>/<sequence>.json`. Worker xác minh checksum
và dùng lại artifact đã biết. Checkpoint hỏng được giữ lại kèm `.review.json`.
Crash tại `PUBLISHING` chuyển `NEEDS_REVIEW` và liệt kê artifact liên quan: kiểm
tra thủ công trước khi đổi state; không tự xóa orphan hoặc retry publish.
Resume `PROCESSING` dùng lại Bronze và duplicate gate P3; commit trước đó có thể
là unknown. Replay báo 0 dòng mới không chứng minh process trước chưa ghi dòng nào.

Mặc định dừng khi batch bị từ chối; `--skip-rejected` ghi nhận rồi chuyển tiếp.
Timeout/network/429/5xx có retry hữu hạn và backoff lũy thừa; 404, lỗi schema hoặc
quality không tự retry. SIGTERM đợi batch hiện tại xong rồi thoát. `--once` xử lý
một batch chờ; `--limit N` giới hạn số batch; `--continuous` đợi tín hiệu dừng sau
khi run hữu hạn kết thúc.

`maintenance.jsonl` lưu quyết định, lỗi, thời gian và số đo layout. Record
`STARTED` chưa kết thúc được đọc là `UNKNOWN` và vẫn tính cooldown. Active bytes
khác dung lượng Parquet vật lý còn giữ lại; dung lượng vật lý S3 chưa biết nếu
không đo riêng. Compaction không giải phóng file lịch sử.

## Demo Docker hai pha

```bash
mkdir -p reports/storage/demo
# Chỉ tải dependency lúc build qua host network để tránh lỗi DNS của Docker bridge.
# Dịch vụ lúc chạy dùng mạng Compose riêng.
docker compose -f infra/docker-compose.storage.yml config --quiet
docker compose -f infra/docker-compose.storage.yml build
docker compose -f infra/docker-compose.storage.yml up -d minio mock
docker compose -f infra/docker-compose.storage.yml --profile ingest up -d worker
docker compose -f infra/docker-compose.storage.yml wait worker
# Xác nhận exit code 0 trước khi bật bảo trì:
docker compose -f infra/docker-compose.storage.yml ps -a
MAINTENANCE_ENABLED=true MAINTENANCE_INGESTION_STOPPED=true \
  docker compose -f infra/docker-compose.storage.yml --profile maintain run --rm scheduler \
  python -m lakehouse_storage.maintenance_scheduler --once
# Không vacuum xóa thật; chạy preview riêng theo cùng cách.
```

Host mặc định bind loopback: mock **18010**, MinIO **19000**; không tranh cổng
9000/9001. Worker/scheduler dùng cùng mount tuyệt đối `/demo/storage_demo/bronze`
và khóa pha. State, Bronze, payload mock và báo cáo lưu riêng. Mock không nhận
thông tin xác thực MinIO. Phiên bản build/dịch vụ được pin tại Dockerfile,
Compose và constraints. Biến cấu hình mẫu: `infra/storage.env.example`.

Test S3 dùng prefix mới trong bucket demo và giữ lại để kiểm tra:

```bash
STORAGE_DEMO_MINIO_TEST=1 PYTHONPATH=src /tmp/storage-services-venv/bin/python \
  -m pytest tests/test_storage_minio_demo.py -m minio -q
```

Mốc xác minh 2026-11-17 của gói demo ban đầu: MinIO/mock healthy; worker exit 0,
báo 4001 dòng; compaction sensor từ 4 xuống 1 file; `pip check` trong image đạt;
6 test S3 E2E đạt. Máy này gặp lỗi DNS trên Docker bridge, nên chỉ tải dependency
khi build qua host network; dịch vụ lúc chạy vẫn dùng mạng Compose riêng.

## Ghi qua P3 và bàn giao sau review (2026-11-17)

Generator dùng với Gold chạy theo
[`lakehouse_storage/README.md`](../src/lakehouse_storage/README.md). Hai CLI gọi P3;
bắt buộc `--silver-root` hoặc `STORAGE_GENERATOR_SILVER_ROOT` khi ghi Delta.
JSON trả trạng thái gate và số dòng thực; FAIL trả exit code 1. Helper low-level
chỉ cung cấp thao tác storage, chưa bảo đảm điều kiện Gold. Bảng cũ thiếu một trong
`_contract_version`, `_quality_status`, `_quality_run_id` bị báo lỗi; migration
phải thực hiện riêng.

Giữ ingestion tuần tự. P3 còn lỗi chống trùng khi lọc ngày và race giữa kiểm tra
trùng với append; FAO còn lỗi parse datetime của `Month`. Xem reproducer tại
`reports/storage/post_review_20261117/`. Không sửa Bronze hoặc hạ gate để nạp FAO.
P2 có root crawler tương đối khác storage khi chạy từ repo root; kiểm tra đường
dẫn tuyệt đối trước khi thu thập. Các module phụ thuộc chưa được sửa trong đợt này.

Bronze giữ tên độc quyền, ghi staging cùng filesystem rồi publish bằng hard link
không ghi đè, đã kiểm chứng trên Linux. `.parquet` chỉ xuất hiện sau checksum.
Crash có thể để lại staging ẩn, `.reserve` hoặc `.sha256` mồ côi; chúng không là
đầu vào Parquet. Giữ lại để kiểm tra, không tự xóa/publish lại. Filesystem phải
hỗ trợ tạo độc quyền và hard link; cặp file legacy hợp lệ vẫn đọc được.

Ghi Silver có tên bảng kiểm tra cột bắt buộc, null và ép kiểu trước commit;
không tự migrate nullable metadata cũ. Diff dùng khóa thực thể, báo lỗi khi khóa
null/trùng. Với bảng chưa nhận diện, truyền `id_column` rõ ràng (string/list/tuple).
Update cùng khóa không được tính là thêm/xóa.

Chạy lại kiểm thử S3 riêng:

```bash
docker compose -f infra/docker-compose.storage.yml up -d minio
STORAGE_DEMO_MINIO_TEST=1 PYTHONPATH=src TMPDIR=/tmp \
  PYTHONDONTWRITEBYTECODE=1 MPLCONFIGDIR=/tmp/storage-mpl \
  .venv/bin/python -B -m pytest tests/test_storage_minio_demo.py -q -s
```

Bảo trì chỉ chạy sau khi dừng ingestion; vacuum chỉ preview. Kiểm chứng Delta
local dùng đường dẫn `/tmp` không có dấu cách do giới hạn đọc URI percent-encoded
trong môi trường đã pin.
