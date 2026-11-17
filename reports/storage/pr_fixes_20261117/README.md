# Nghiệm thu sửa 5 lỗi P1 trước PR (2026-11-17)

R01–R07 hoàn thành. Full suite bật MinIO: **493 đạt, 1 bỏ qua**, **100,88 s**.
Test bỏ qua là chDB tùy chọn (`CHDB_SILVER_ACCEPTANCE=1` cần môi trường riêng).
MinIO riêng: **10 đạt, không bỏ qua**, **18,69 s**. Kết quả áp dụng cho luồng
một worker tuần tự trong namespace demo, không chứng minh production-ready.

Nguồn finding cục bộ: `reports/storage/review_20261117/README.md` tại `b0d9245`.
Giữ nguyên báo cáo cũ; bảng dưới là kết quả sau sửa. Logs sinh trong thư mục
này được giữ cục bộ, không đưa vào git (`*.log`).

## Sửa lỗi và regression

| Task | Thay đổi | RED → GREEN | Commit |
|---|---|---|---|
| R01 | Kiểm URI S3 theo bucket/segment; từ chối encoding và đường dẫn mơ hồ trước backend | 16 lỗi → 43 đạt | `202f098` |
| R02 | Bind run với Silver root, checkpoint với URI bảng; chặn đổi đích và legacy thiếu evidence trước skip/publish/replay | 5 lỗi → 51 đạt; 29 test state đạt sau bổ sung 2 ca | `d75ad3e` |
| R03 | Chuyển timestamp có timezone về TIMEZONE trước suy ra ngày partition | 5 lỗi → 66 đạt | `fe9b85f` |
| R04 | Resolve root Bronze trước containment/source check | 3 lỗi → 30 đạt | `64c442f` |
| R05 | Thử từng format ngày độc lập, giữ ưu tiên DMY trước MDY | 3 lỗi → 69 đạt | `122af47` |
| R06 | Thêm 3 test S3 cho namespace, checkpoint và timezone | 10/10 MinIO đạt | `3911f97` |

Logs: `r01_red.log`/`r01_green.log` đến `r05_red.log`/`r05_green.log`,
`r02_state_green.log`, `r06_minio.log`, `final_full_suite.log`.
Mỗi regression đỏ trước sửa đúng nguyên nhân, không hạ assertion downstream.

R01 chủ động từ chối mọi percent-encoding trong URI demo, kể cả encoding không
traversal. Đây là giới hạn đầu vào để guard và backend không diễn giải khác nhau.
R02 giữ JSON/checksum gốc khi báo lỗi; `.review.json` lưu lý do. Đích mới cần
state root mới. Checkpoint legacy chưa có công cụ migrate tự động.
R03 chỉ sửa lần ghi mới; không repartition lịch sử hoặc ghi đè ngày caller truyền.
R04/R05 giữ nguyên payload raw và giao thức publication Bronze.

## Kiểm chứng tích hợp

- CP-R2 trước bổ sung 3 test S3 và ca mock ID: **482 đạt, 8 bỏ qua**, **96,52 s**;
  7 skip là MinIO opt-in, 1 chDB. Đây không phải lần nghiệm thu MinIO.
- MinIO healthy tại `127.0.0.1:19000`; chỉ dùng bucket
  `smart-dairy-storage-demo`, prefix `storage_demo/tests/<uuid>`.
- Namespace: bảng `escaped/table` do test tạo ngoài allow-root hẹp; request
  traversal bị chặn, version 1/files/2 dòng giữ nguyên. Compaction hợp lệ sau đó
  tăng version 2, giữ 2 dòng và snapshot version 0. Không vacuum xóa thật.
- Checkpoint: P3 ghi 10 dòng sensor vào A; đổi sang B bị chặn, B chưa có Delta
  table, A giữ version 0 và số dòng. Resume/replay A tiếp tục qua duplicate gate.
- Timezone: String và Datetime `2026-01-16 01:00+07` đều lưu
  `2026-01-15 18:00 UTC`, date filter ngày 15 trả 1 dòng cho mỗi kiểu đầu vào.
- `environment.log`: Python 3.12.3, Polars 2.1.0, Arrow 18.1.0, Delta 1.6.6;
  core import, compile source trong bộ nhớ và pip check đạt; không đổi dependency.
- `original_bronze_integrity.log`: 168 file / 1.780.328 dòng trong inventory cũ
  giữ nguyên hash Parquet và sidecar. Không tái tuyên bố đã quality-gate toàn bộ.
- [Review độc lập](independent_review.md): 90 test tập trung đạt; bỏ guard `%`
  bằng mutation trong bộ nhớ làm 8 ca lỗi như dự kiến. Không còn finding
  Required/High/Medium trong R01–R05 qua review; R06 được đọc thêm và chạy bởi root.

### Lỗi test phát sinh trong full suite

Lượt bật MinIO đầu: **491 đạt, 1 lỗi, 1 bỏ qua** (`r06_suite_initial_failure.log`).
`test_transient_attempts_persist` dùng `.replace('/1', '/2')`, đổi cả run ID khi
ID bắt đầu bằng 1. Request đi nhầm run nên trả 404 thay vì 500. Đây là lỗi test
storage có sẵn, không phải lỗi runtime mới của năm sửa production.

Đã cố định ID prefix 1/2 để tái hiện: **1 lỗi, 1 đạt**
(`r06_mock_route_red.log`); dựng URL batch 2 rõ ràng, giữ toàn bộ assertion:
**6 đạt** (`r06_mock_route_green.log`). Commit `5303e6c`. Sau thay đổi này mới
chạy lại full suite, cho kết quả 493/1 ở đầu báo cáo.

`r05_test_clock_diagnosis.log` giữ một lỗi fixture ban đầu: thay lớp datetime
để đóng băng clock làm ca Datetime bị nhận diện sai. Fixture đã sửa trước RED
chính thức; không thay implementation để chiều theo lỗi test đó.

## Chạy lại

Từ repo root, dùng venv hiện có và MinIO demo:

```bash
docker compose -f infra/docker-compose.storage.yml up -d minio
PYTHONPATH=src TMPDIR=/tmp PYTHONDONTWRITEBYTECODE=1 MPLCONFIGDIR=/tmp/storage-mpl \
  STORAGE_DEMO_MINIO_TEST=1 STORAGE_DEMO_TEST_ENDPOINT=127.0.0.1:19000 \
  .venv/bin/python -B -m pytest tests/test_storage_minio_demo.py -q -s
PYTHONPATH=src TMPDIR=/tmp PYTHONDONTWRITEBYTECODE=1 MPLCONFIGDIR=/tmp/storage-mpl \
  STORAGE_DEMO_MINIO_TEST=1 STORAGE_DEMO_TEST_ENDPOINT=127.0.0.1:19000 \
  .venv/bin/python -B -m pytest tests/ -q -rs
.venv/bin/python -m pip check
```

Local Delta tests dùng `/tmp` không có dấu cách do giới hạn URI của môi trường
được pin. HTTP/TestClient/MinIO trong phiên này chạy ngoài sandbox hạn chế.

## Nội dung bàn giao PR

Tiêu đề đề xuất: `fix: address five storage review findings`.

Storage trước đây có thể vượt allow-root S3 qua URI mã hóa, dùng DONE của đích
Silver cũ khi đổi cấu hình, và suy ra ngày partition khác nhau cho cùng instant.
Bản sửa kiểm tra URI rõ ràng, ràng buộc checkpoint với đích và chuẩn hóa timezone;
đồng thời sửa xác minh root Bronze tương đối và vòng thử format ngày.

Validation: 493 pass/1 optional skip; MinIO 10/10, RED/GREEN cho cả 5 finding,
mutation guard, review độc lập, core import/compile/pip check và hash Bronze đạt.
Không đổi schema nghiệp vụ, API storage công khai, partition keys, dependency
hoặc mã/tests downstream. Checkpoint legacy thiếu đích cần xác minh trước resume.

Giới hạn giữ nguyên: một writer tuần tự; dừng ingestion trước maintenance;
vacuum preview. P3 vẫn còn FAO parse, date-filter idempotency và concurrent write;
P2 vẫn có root crawler khác storage. Không tự migrate dữ liệu hoặc giả PASS.
Chưa push hoặc mở PR trong đợt triển khai này.
