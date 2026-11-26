# Benchmark workflow: phạm vi số liệu

Số đo code mới dùng duration từng stage trong run manifest và elapsed DAG từ
Airflow API. Tổng elapsed trừ tổng stage processing là phần chờ/orchestration
quan sát được, không phải riêng CPU overhead của scheduler. HTTP/P3/reporting
nằm trong processing stage; không gọi toàn bộ thời gian đó là ghi Delta.

MinIO tests dùng prefix UUID, không vacuum destructive. Snapshot read được kiểm
sau append/compaction. Số liệu lịch sử bên dưới mô tả benchmark storage cũ và
không chứng minh Airflow nhanh/chậm hơn CLI.

# Hướng dẫn chạy lại benchmark storage

Chuẩn bị môi trường dịch vụ và MinIO/mock theo
[Hướng dẫn vận hành](storage_operations.md). Dừng ingestion trước khi bảo trì.
Mỗi lần chạy dùng namespace riêng; harness từ chối thư mục dữ liệu/báo cáo đã có,
không chạy vacuum và không dùng bảng nhà máy dùng chung.

```bash
STORAGE_DEMO_REVISION=$(git rev-parse HEAD) MINIO_ENDPOINT=127.0.0.1:19000 MINIO_ACCESS_KEY=storagedemo \
MINIO_SECRET_KEY=storage-demo-local-only PYTHONPATH=src \
/tmp/storage-services-venv/bin/python -m lakehouse_storage.benchmark \
  --rows 100000 --batch-sizes 1000,10000,50000 --seed 42 --warmups 1 --repeats 5
```

## Cấu hình

CLI mặc định tạo mã lần chạy mới; `--run-id` chỉ nhận mã chưa tồn tại. Với Delta
local, dùng
`--allow-root "$PWD/data/storage_benchmark" --silver-root "$PWD/data/storage_benchmark/TARGET/silver"`
và mã lần chạy riêng.
Không gộp kết quả local và S3 để tính mức tăng tốc.

Kích thước batch phải chia hết tổng số dòng; giới hạn 50k mẫu/batch và 200
batch/run. Với 1M dòng, có thể chọn `5000,10000,50000`.

## Tệp kết quả

Thư mục `reports/storage/<run_id>` gồm:

| Tệp | Nội dung |
|---|---|
| `metrics.json` | Cấu hình, môi trường, giới hạn package/container, ngữ nghĩa truy vấn, số dòng, fingerprint, version/kích thước file trước-sau, latency thô, median, IQR, độ lệch chuẩn, thời gian từng bước và kết quả worker |
| `queries.csv` | Từng lần đo truy vấn, đơn vị giây |
| `operations.jsonl` | Quyết định compaction, lỗi và bằng chứng trước-sau |
| `command.json` | Lệnh thực thi; bước vẽ bổ sung PNG/SVG và báo cáo tóm tắt |

## Kiểm soát tính đúng

Mock sinh một chuỗi dữ liệu theo seed rồi chia batch; thay batch size không làm
đổi giá trị sensor. Mỗi run có batch ID nghiệp vụ riêng, được loại khỏi fingerprint
so sánh giữa các cấu hình. So sánh toàn bộ cột nghiệp vụ được chấp nhận; chỉ ghi
latency sau compaction khi kết quả truy vấn khớp với trước compaction. Tốc độ không
phải điều kiện đạt/trượt.

## Phạm vi đo

Thông lượng toàn trình gồm HTTP, xác minh JSON, publish Bronze, kiểm định P3,
tạo báo cáo/biểu đồ và ghi Delta, cộng một dòng MES. POST tạo run nằm ngoài khoảng
đo. `p3_seconds` gồm thời gian báo cáo, không đại diện riêng thời gian ghi storage.
Báo đủ số dòng sensor đầu vào, được chấp nhận và bị loại.

Mỗi truy vấn có ít nhất 1 lần làm nóng và 5 lần đo. Không kiểm soát cache của OS
hoặc object store, do đó không gọi đây là phép đo cold-cache. Đo trước rồi sau
compaction; thứ tự workload nằm trong `query_text`.

Dữ liệu giả lập có một dây chuyền; 100k dòng trải qua hai partition ngày theo
ngữ nghĩa P3 hiện có. Tra cứu batch đọc timestamp/uht_temp theo điều kiện batch;
tổng hợp theo giờ và theo dây chuyền/ngày là các workload riêng.

Chưa có bằng chứng về tăng tốc nhờ pruning nhiều partition, quan hệ nhân quả
nghiệp vụ hoặc lợi thế nén giữa định dạng. Runner có Z-order tùy chọn và đã kiểm
tra tính đúng; benchmark này chỉ đo compaction. Dung lượng vật lý S3 còn giữ lại
chưa xác định; giảm active bytes không đồng nghĩa giải phóng dung lượng đĩa.
