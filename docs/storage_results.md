# Kết quả benchmark storage (2026-11-17)

Đã đo **100.000 dòng sensor** trên MinIO demo, seed 42, batch size 1k/10k/50k.
Mỗi cấu hình chấp nhận 100.000 dòng, loại 0 dòng; fingerprint nghiệp vụ giống nhau.
Trước/sau compaction giữ nguyên kết quả truy vấn và schema; không chạy vacuum.

Revision: `a6638ffb0d37e5e8cd3e0c506368c2c66da1e78c`. Máy Intel(R) Core(TM)
i7-8565U CPU @ 1.80GHz, 8 CPU logic, 15.30 GiB RAM; Python host 3.12.3.
Phiên bản package và giới hạn quan sát được nằm trong metrics. Compose không
áp thêm giới hạn CPU/RAM.

Nguồn số liệu: [metrics](../reports/storage/measured_100k_20261117/metrics.json),
[CSV thô](../reports/storage/measured_100k_20261117/queries.csv),
[nhật ký bảo trì](../reports/storage/measured_100k_20261117/operations.jsonl),
[lệnh chạy](../reports/storage/measured_100k_20261117/command.json),
[tóm tắt và 4 biểu đồ](../reports/storage/measured_100k_20261117/summary.md).

## Thông lượng nạp

| Batch size | Số file trước → sau | Toàn trình (s) | Dòng đầu vào/s | Compaction (s) |
|---:|---:|---:|---:|---:|
| 1,000 | 101 → 2 | 68.13 | 1468 | 0.345 |
| 10,000 | 11 → 2 | 13.24 | 7553 | 0.247 |
| 50,000 | 3 → 2 | 10.88 | 9193 | 0.331 |

Mỗi batch size chỉ đo một đợt nạp: HTTP, xác minh JSON, Bronze, P3 kiểm định/báo
cáo/biểu đồ và Delta, cộng một dòng MES. POST tạo mock run nằm ngoài thời gian đo.
Thời gian P3 không phải thời gian riêng storage. Chưa lặp nhiều đợt ingestion,
nên biểu đồ thông lượng không có thanh sai số.

## Độ trễ truy vấn

| Batch size | Workload | Median / IQR trước compaction (ms) | Median / IQR sau compaction (ms) |
|---:|---|---:|---:|
| 1,000 | line_date_aggregate | 51.95 / 5.97 | 27.99 / 0.43 |
| 1,000 | batch_lookup | 56.24 / 0.60 | 32.60 / 2.19 |
| 1,000 | hourly_aggregate | 52.37 / 1.98 | 26.02 / 6.48 |
| 10,000 | line_date_aggregate | 19.21 / 1.94 | 18.98 / 0.74 |
| 10,000 | batch_lookup | 27.05 / 2.20 | 22.97 / 0.40 |
| 10,000 | hourly_aggregate | 23.14 / 3.09 | 20.05 / 0.98 |
| 50,000 | line_date_aggregate | 12.24 / 0.62 | 13.06 / 0.27 |
| 50,000 | batch_lookup | 16.78 / 1.22 | 16.53 / 0.06 |
| 50,000 | hourly_aggregate | 12.30 / 0.57 | 15.01 / 0.16 |

Mỗi truy vấn/layout có **1 lần làm nóng + 5 lần đo**; lưu mẫu thô và độ lệch chuẩn
(SD). Biểu đồ dùng median và SD; bảng dùng median và khoảng tứ phân vị (IQR).
Đo trước rồi sau compaction; không kiểm soát cache OS/object store nên không gọi
là cold-cache. Đợt đo cuối chạy sau khi toàn bộ test kết thúc để tránh tranh tài nguyên.

Compaction cải thiện rõ cấu hình nhiều file nhỏ. Với batch 50k, tổng hợp theo
dây chuyền/ngày và theo giờ chậm hơn; tra cứu batch gần tương đương. Tăng tốc
không phải tiêu chí đạt: ít file hơn chưa bảo đảm truy vấn nhanh hơn ở quy mô nhỏ.

## Giới hạn kết quả

Dữ liệu có một dây chuyền, một batch nghiệp vụ và hai ngày ingestion theo ngữ
nghĩa P3 hiện có. `batch_lookup` đọc cả batch, chưa chứng minh lợi ích lọc chọn
hoặc pruning nhiều batch. Không suy rộng sang nghiệp vụ, nhiều dây chuyền hoặc
hiệu quả nén giữa định dạng.

Active bytes lưu riêng trước/sau; dung lượng vật lý S3 còn giữ lại chưa đo,
ghi null/unknown. Delta giữ file lịch sử nên chưa thể kết luận đã giải phóng đĩa.
Z-order có test tính đúng ở runner nhưng chưa đo hiệu năng. Quy mô 1M chưa chạy;
100k là quy mô thực nghiệm được chọn cho demo tối thiểu.

Chỉ run `measured_100k_20261117` cung cấp số liệu cuối. Không gộp các lần smoke
hoặc khảo sát. Một lần đo sau review đã dừng vì chạy trùng thời gian với tests;
namespace được giữ để kiểm tra và không dùng trong thống kê.

## Chạy lại

```bash
STORAGE_DEMO_REVISION=$(git rev-parse HEAD) MINIO_ENDPOINT=127.0.0.1:19000 \
MINIO_ACCESS_KEY=storagedemo MINIO_SECRET_KEY=storage-demo-local-only PYTHONPATH=src \
/tmp/storage-services-venv/bin/python -m lakehouse_storage.benchmark \
  --rows 100000 --batch-sizes 1000,10000,50000 --seed 42 --warmups 1 --repeats 5 \
  --run-id NEW_UNIQUE_RUN_ID
PYTHONPATH=src MPLCONFIGDIR=/tmp/storage-mpl /tmp/storage-services-venv/bin/python \
  -m lakehouse_storage.benchmark_report reports/storage/NEW_UNIQUE_RUN_ID/metrics.json
```
