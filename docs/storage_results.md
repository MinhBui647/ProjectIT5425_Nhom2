# Kết quả benchmark storage - 2026-11-17

Đã đo 100.000 sensor rows trên MinIO demo, seed 42, batch size 1k/10k/50k. Mỗi cấu hình đều có 100.000 accepted, 0 rejected; fingerprint business giống nhau. Baseline và compacted giữ cùng kết quả truy vấn và schema. Không vacuum.

Source revision: `a6638ffb0d37e5e8cd3e0c506368c2c66da1e78c`. Máy: Intel(R) Core(TM) i7-8565U CPU @ 1.80GHz, 8 logical CPUs, 15.30 GiB RAM. Host Python 3.12.3; package versions và giới hạn quan sát được trong metrics. Compose không áp thêm CPU/RAM limit.

Nguồn số liệu cuối: [metrics.json](../reports/storage/measured_100k_20261117/metrics.json), [raw CSV](../reports/storage/measured_100k_20261117/queries.csv), [operation log](../reports/storage/measured_100k_20261117/operations.jsonl), [command](../reports/storage/measured_100k_20261117/command.json), [summary + 4 figures](../reports/storage/measured_100k_20261117/summary.md).

| Batch size | Files trước → sau | Pipeline (s) | Input rows/s | Compaction (s) |
|---:|---:|---:|---:|---:|
| 1,000 | 101 → 2 | 68.13 | 1468 | 0.345 |
| 10,000 | 11 → 2 | 13.24 | 7553 | 0.247 |
| 50,000 | 3 → 2 | 10.88 | 9193 | 0.331 |

Throughput là một lần nạp cho mỗi batch size: gồm HTTP fetch, xác minh JSON, Bronze, P3 validation/reporting/figures và Delta, cộng một MES row. POST tạo mock run nằm ngoài timer. Không coi thời gian P3 là storage-only; không có error bar cho throughput vì chưa lặp nhiều đợt ingestion.

| Batch size | Workload | Baseline median / IQR (ms) | Compacted median / IQR (ms) |
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

Mỗi query/layout có 1 warmup + 5 timed repetitions; raw samples và standard deviation được giữ lại. Hình latency dùng median và SD, bảng trên dùng median và IQR. Chạy baseline rồi compacted; OS/object cache không kiểm soát, không gọi cold-cache. Đợt đo cuối chạy sau khi full test suite kết thúc để tránh tranh tài nguyên với tests.

Compaction giúp rõ ở cấu hình nhiều file nhỏ. Với batch 50k, line/date và hourly aggregate chậm hơn sau compaction; batch lookup gần tương đương. Không đặt speedup làm điều kiện pass. Ít file hơn không bảo đảm query nhanh hơn ở quy mô nhỏ.

Dữ liệu có một line, một business batch và hai ingestion dates theo semantics P3 hiện có. Batch lookup trong workload này đọc cả batch, chưa chứng minh lợi ích selective lookup/pruning trên nhiều batch. Không suy ra tác động nghiệp vụ, hiệu quả nhiều line hoặc compression giữa các định dạng.

Active bytes trước/sau được lưu riêng. S3 retained/physical bytes chưa đo, ghi null/unknown; không tuyên bố giải phóng disk vì Delta vẫn giữ file lịch sử. Z-order có test correctness ở runner nhưng chưa đo performance; 1M scale chưa chạy và không có kết luận về nó. 100k là scale thực nghiệm được chọn cho gói demo tối thiểu.

Các lần smoke/khảo sát đầu không gộp vào thống kê cuối. Một đợt đo sau review đã được dừng khi phát hiện trùng thời gian chạy tests; namespace đó được giữ để kiểm tra và không dùng trong báo cáo. Chỉ run `measured_100k_20261117` là nguồn các bảng/hình trên.

```bash
STORAGE_DEMO_REVISION=$(git rev-parse HEAD) MINIO_ENDPOINT=127.0.0.1:19000 \
MINIO_ACCESS_KEY=storagedemo MINIO_SECRET_KEY=storage-demo-local-only PYTHONPATH=src \
/tmp/storage-services-venv/bin/python -m lakehouse_storage.benchmark \
  --rows 100000 --batch-sizes 1000,10000,50000 --seed 42 --warmups 1 --repeats 5 \
  --run-id NEW_UNIQUE_RUN_ID
PYTHONPATH=src MPLCONFIGDIR=/tmp/storage-mpl /tmp/storage-services-venv/bin/python \
  -m lakehouse_storage.benchmark_report reports/storage/NEW_UNIQUE_RUN_ID/metrics.json
```
