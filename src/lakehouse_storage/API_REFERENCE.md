# 📚 Lakehouse Storage API Reference

Tài liệu đặc tả kỹ thuật chi tiết toàn bộ các public symbol được xuất bản từ package `lakehouse_storage`. Phục vụ việc tích hợp vào hệ thống máy chủ, Data Engineering Pipelines và Data Analytics.

---

## 📑 Mục lục điều hướng

1. [Module: `schemas` (Source of Truth & Registry)](#1-module-schemas)
2. [Module: `bronze_writer` (Raw Immutable Landing)](#2-module-bronze_writer)
3. [Module: `silver_manager` (Delta Lake ACID Operations)](#3-module-silver_manager)
4. [Module: `time_travel` (ISO 22000 Audit & Historical Queries)](#4-module-time_travel)
5. [Module: `maintenance` (Table Lifecycle)](#5-module-maintenance)

---

## 1. Module: `schemas`

Trung tâm quản trị metadata dữ liệu (Source of Truth), định nghĩa cấu trúc PyArrow Schemas cho toàn bộ dự án.

### `get_bronze_schema` / `get_silver_schema`
```python
get_bronze_schema(table_name: str) -> pa.Schema
get_silver_schema(table_name: str) -> pa.Schema
```
- **Chức năng**: Lấy PyArrow Schema cho Bronze (trước khi transform) hoặc Silver (đã transform).
- **Tham số**: `table_name` có thể là alias (`S1`, `s2`) hoặc tên bảng gốc.

> [!NOTE] 
> **Lưu ý ép kiểu Silver**: Schema Silver đã tự động chuyển đổi các cột số thực về `float32` (giảm dung lượng) và chuyển đổi thời gian sang `timestamp[us, tz=UTC]`.

### `get_silver_table_uri`
```python
get_silver_table_uri(table_name: str, bucket: str | None = None) -> str
```
- **Chức năng**: Trả về định dạng S3 path chuẩn: `s3://<bucket>/02_silver_curated/<table_name>`.

---

## 2. Module: `bronze_writer`

Đảm nhận việc hạ cánh (landing) dữ liệu thô vào hệ thống file cục bộ một cách an toàn.

### `write_bronze_batch`
```python
write_bronze_batch(
    df: pl.DataFrame,
    table_name: str,
    bronze_root: str | Path | None = None,
    validate_schema: bool = True,
) -> str
```
- **Chức năng**: Ghi `df` thành file Parquet và tạo kèm file checksum `.sha256`. Tự động phân thư mục theo ngày.
- **Kết quả**: Trả về đường dẫn tuyệt đối của file Parquet đã ghi.

> [!IMPORTANT] 
> **Ownership / Lifetime Contract**
> Hàm không thay đổi/mutate `df` đầu vào. Các file được ghi ra ở tầng Bronze mang tính **Immutable (bất biến)**, chỉ sinh thêm file mới (Append-only).

> [!WARNING] 
> **Error Contract**
> - Bắn `ValueError` nếu `df` bị rỗng.
> - Bắn `ValueError` nếu `validate_schema=True` và dữ liệu thiếu các cột không-thể-null (non-nullable columns).
> - Bắn `FileExistsError` nếu phát hiện đụng độ đường dẫn file.

### `verify_checksum`
```python
verify_checksum(bronze_path: str | Path) -> bool
```
- **Chức năng**: Đọc mã băm thực tế của file Parquet đối chiếu với nội dung file `.sha256`. 
- **Error Contract**: Ném `FileNotFoundError` nếu file Parquet không tồn tại.

---

## 3. Module: `silver_manager`

Xử lý luồng đưa dữ liệu vào Delta Lake, đảm bảo tính toàn vẹn (ACID).

### `ingest_bronze_file`
```python
ingest_bronze_file(
    bronze_path: str | Path,
    silver_table_uri: str,
    partition_by: list[str],
    table_name: str | None = None,
    bronze_root: Path | str | None = None,
    enforce_checksum: bool = True,
) -> dict
```
- **Chức năng**: Đóng gói quy trình Medallion (đọc Parquet ➔ verify checksum ➔ đính kèm lineage metadata ➔ ghi Delta Lake).
- **Kết quả**: Trả về dictionary: `{"rows_ingested": int, "bronze_path": str, "sha256": str}`.

> [!WARNING]
> **Error Contract**
> Ném `ValueError` ngay lập tức và hủy tiến trình nạp (abort) nếu mã hash không khớp (`enforce_checksum=True`), ngăn chặn dữ liệu bẩn lọt lên tầng Silver.

### `append_to_delta`
```python
append_to_delta(
    df: pl.DataFrame,
    table_uri: str,
    partition_by: list[str],
    table_name: str | None = None,
) -> None
```
- **Chức năng**: Cốt lõi của việc ghi dữ liệu. Ép kiểu chuẩn (`_cast_to_schema`) và thực hiện **Commit Atomic** vào Delta table.

> [!IMPORTANT]
> **ACID Contract**
> Đảm bảo tính nhất quán (Consistency). Nếu bị gián đoạn giữa chừng hoặc gặp xung đột khi ghi đồng thời (Concurrent Writes), thao tác sẽ tự động rollback. Ném `DeltaProtocolError` nếu không thể dung hòa conflict.

### `read_silver_table`
```python
read_silver_table(
    table_uri: str,
    line_id: str | None = None,
    date_from: str | date | None = None,
    date_to: str | date | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> pl.DataFrame
```
- **Chức năng**: Truy vấn dữ liệu từ Delta Lake.
- **Tối ưu hóa (Edge Case)**: Hàm hỗ trợ **Partition Pruning**. Nếu truyền `date_from`/`date_to`, Delta engine sẽ tự động skip các file Parquet không liên quan mà không cần load toàn bộ dữ liệu vào RAM.

---

## 4. Module: `time_travel`

Trái tim của hệ thống kiểm toán (Audit) và truy vết lô hàng chuẩn ISO 22000.

### `trace_batch`
```python
trace_batch(table_uri: str, batch_id: str, id_column: str = "batch_id") -> dict
```
- **Chức năng**: Cung cấp bằng chứng minh bạch (Traceability). Tìm kiếm xem `batch_id` này bắt nguồn từ file Bronze thô nào, và checksum hiện tại của file thô đó có còn khớp với lúc ban đầu ghi vào Delta không.

> [!TIP]
> Đây là tính năng then chốt đáp ứng kiểm định An Toàn Thực Phẩm. Output trả về chứa field `"integrity_verified": True` chứng minh hệ thống dữ liệu chưa từng bị giả mạo.

### `diff_versions`
```python
diff_versions(table_uri: str, v1: int, v2: int,
              id_column: str | list[str] | tuple[str, ...] | None = None) -> dict
```
- **Chức năng**: So sánh 2 mốc version bất kỳ và tìm ra chính xác những dòng đã thêm vào (`added`) hoặc đã xóa đi (`removed`).

---

## 5. Module: `maintenance`

Chăm sóc sức khỏe hệ thống lưu trữ (Storage Lifecycle).

### `compact_table`
```python
compact_table(table_uri: str, target_file_size_mb: int = 128) -> dict
```
- **Chức năng**: Trị bệnh "phân mảnh" (Small File Problem). Gom các file Parquet vụn vặt (dưới 128MB) sinh ra do quá trình ghi stream vào một file vật lý to hơn để tăng tốc quét ổ đĩa (Sequential Scan).

### `zorder_table`
```python
zorder_table(table_uri: str, columns: list[str]) -> dict
```
- **Chức năng**: Thay đổi cách sắp xếp vật lý bên trong file theo đường cong **Z-Order Curve**.

> [!NOTE] 
> **Edge Case**: Tránh truyền các cột đã dùng làm `partition_by` vào tham số `columns` vì Delta Engine mặc định đã hỗ trợ lọc cấp độ thư mục. Z-Order chỉ hiệu quả với cột định danh thường xuyên đem vào mệnh đề `WHERE` (ví dụ: `line_id`).

### `vacuum_table`
```python
vacuum_table(
    table_uri: str,
    retention_hours: int = 168,
    dry_run: bool = True,
    enforce_retention_duration: bool = False,
) -> dict
```
- **Chức năng**: Quét và dọn dẹp các file rác (Orphan files) mồ côi đã quá thời hạn lưu trữ để tiết kiệm dung lượng MinIO S3.

> [!WARNING]
> **An toàn Dữ liệu**
> Tham số `dry_run` được bật mặc định (`True`). Nếu chạy nguyên bản, nó chỉ đóng vai trò cảnh báo, in ra danh sách các file chuẩn bị xóa. Hãy truyền rõ ràng `dry_run=False` nếu muốn ra lệnh xóa thật trên MinIO S3.

### Traceability corrections (storage demo, 2026-11-17)

`get_version_at_date(table_uri, target_date)` selects the latest commit timestamp
at or before the target, breaking equal timestamps by version. Naive targets
mean UTC; explicit offsets retain their meaning. Before the first commit it
returns `None`.

`trace_batch()` preserves the existing keys. `lineage` is the first representative
source; use the additive `lineage_sources` list for all distinct path/hash pairs.
Each entry includes `integrity_verified` and `reason`. Overall integrity requires
all sources to pass. Missing paths/files/hashes, unreadable files, checksum
mismatches and conflicting expected hashes are reported without discarding other
sources. Each path is hashed at most once. A damaged second source can therefore
change overall integrity from true to false without changing any Silver rows.

### Optional demo service interfaces

These modules are not imported by the core package. Install the service
requirements/constraints documented in
[the demo setup guide](../../docs/storage_operations.md).

| Interface | Inputs / outputs |
|---|---|
| `mock_server.app.create_app(store_root)` | FastAPI application; `/docs` documents run controls |
| `POST /runs` | Bounded seed/start_time/samples_per_batch/batches/scenario; 201 run_id/batch_count/request; unsafe to retry |
| `GET /health` | 200 status=ok when the payload store exists |
| `GET /runs/{id}` | Persisted run metadata; unknown id -> 404 |
| `GET /runs/{id}/batches/{sequence}` | Stored JSON bytes: run_id, sequence, table_name, payload_sha256, data; unknown ->404 |
| `IngestionWorker(config, client).process(run_id, sequence, replay=False)` | Checkpoint dict with status, artifact evidence, P3 result, reported rows and stage durations |
| `IngestionWorker.run(run_id, limit=None, skip_rejected=False, stop=None)` | Ordered records for pending sequences; default stops at a rejection/failure |
| `MaintenanceRunner(history_path).run(policy, action='compact', enabled=False, ingestion_stopped=False, now=None)` | RUN/SKIP/FAIL record with reason, metrics, timings and error |
| `build_scheduler(policies, runner, ..., action='compact')` | Same-process UTC scheduler; actions compact/zorder/vacuum_preview |
| `benchmark.run_benchmark(...)` | Fresh isolated HTTP/P3/layout experiment and JSON/CSV operation evidence |
| `benchmark_report.render_report(metrics_path)` | Four charts in PNG/SVG plus Markdown summary |

Core API signatures and schemas are frozen by `test_storage_compatibility.py`.
Workers use POSIX filesystem locking in these service entry points, not a lock on
external P3 writers. Known artifact checksums are checked before replay. Structural
REJECTED outcomes do not trigger another fetch. Retry is finite for network,
timeout, 429 and 5xx; quality failures stop or skip explicitly. PUBLISHING ambiguity
is NEEDS_REVIEW, and interrupted PROCESSING records previous outcome as unknown.
All mutable demo targets require allowlisted descendants and local output-tree
checks. Maintenance flags/window are explicit; vacuum always previews, with a
168-hour minimum and retention enforcement. Existing shared write policy and
`ingestion_date` semantics remain unchanged.

### Writer correctness (review follow-up, 2026-11-17)

`append_to_delta()` and `ingest_bronze_file()` remain low-level storage APIs;
callers must supply governed rows when targeting shared Silver. With a
`table_name`, they reject missing/null required business fields and failed casts
(including optional fields) before committing. Timestamp parse errors are rejected.
New tables preserve business-field nullability; existing nullable Delta metadata
is not migrated. Missing optional fields become typed nulls; extra operational
fields retain their Arrow types. `_cast_to_schema(strict=False)` retains the
legacy permissive cast behavior.

`write_bronze_batch()` reserves names exclusively across local processes, stages
bytes on the same filesystem, then links the checksum before the final Parquet.
The final `.parquet` is the completion marker. Publication uses no-replace hard
links and requires a filesystem supporting exclusive creation and hard links
(tested on Linux). Timestamp and numeric collision suffixes remain readable by P3.
A crash can leave hidden staging, a `.reserve` file or a checksum without Parquet;
these are not inputs and are not automatically deleted. Existing valid legacy
pairs remain readable. The pair is not a two-file atomic rename; this does not
provide multi-writer Delta transactions or general orphan recovery.

### Composite version differences

`diff_versions` keeps `added`, `removed`, `counts`, and `id_column`. A string
key returns a string `id_column`; a composite key returns the supplied list or
tuple. Defaults use Silver entity keys: sensor `(line_id,timestamp)`, weather
`(farm_id,observed_at)`, GDT `(source,product,contract,observed_at,currency,unit)`,
FAO `(source,index_name,period_start)`, recalls `(source,recall_id)`, MES `batch_id`,
metadata `file`. Unknown shapes retain the legacy fallback and log a suggestion
to supply a key. Empty snapshots work; missing, null or duplicate keys raise
`ValueError` instead of producing an ambiguous audit. Row order follows the
source snapshot. Updates sharing an unchanged key are outside added/removed
semantics. Anti joins compare keys without Python sets of all entity values.

### `ingest_governed_bronze_file`

```python
ingest_governed_bronze_file(bronze_path, table_name, *, bronze_root,
                          silver_root, reports_root) -> dict
```

Explicit roots are required. P3 is imported at call time and runs its unchanged
transformers, threshold and duplicate gate. The full P3 result is returned,
including FAIL outcomes without exceptions. Unexpected infrastructure failures
return `status='FAIL'`, `error_category='infrastructure'`, `rows_written=0`, and
`error`; a failure can still require checking commit state after an interrupted
write. Existing tables must have `_contract_version`, `_quality_status`, and
`_quality_run_id`; missing legacy metadata is reported without migration.
Reports, figures and quarantine live below `reports_root`. Invalid paths/table
arguments raise errors. No concurrent claim or transaction recovery is added.
The demo worker's `process_file` continues to use the shared demo namespace guard.

### Sửa 5 lỗi P1 trước PR (R01–R07, 2026-11-17)

- **Phạm vi S3:** `require_namespace()` kiểm tra cả root/target theo bucket và
  segment. URI demo dùng key literal; percent-encoding, dot segment, segment rỗng,
  backslash, khoảng trắng, query/fragment bị từ chối trước thao tác backend.
  Root trailing slash hợp lệ; target bằng root hoặc sibling prefix bị chặn.
- **Checkpoint worker:** run lưu `target.json` với đích Silver chuẩn hóa;
  checkpoint lưu `silver_uri` theo bảng. Đổi đích hoặc checkpoint legacy thiếu
  đích gây `NeedsReview` trước skip/publish/replay; CLI khác 0. JSON và Bronze gốc
  được giữ để xác minh. `run()` kiểm tra checkpoint hiện có trước side effect.
  Resume cùng đích giữ luồng P3; đích mới dùng state root mới.
- **Ngày Silver:** timestamp có timezone được chuyển về `TIMEZONE` (env
  `FACTORY_TIMEZONE`, mặc định UTC) trước khi suy ra `ingestion_date`. Naive
  Datetime được hiểu theo timezone cấu hình; ngày caller cung cấp được giữ.
  Không tự repartition dữ liệu lịch sử.
- **Xác minh Bronze:** `ingest_multiple_files(table_name, bronze_paths,
  bronze_root=None)` chuẩn hóa root tương đối/tuyệt đối, giữ containment,
  checksum và kết quả riêng từng file (`verified=False` kèm lỗi nếu cần).
- **Ngày Bronze:** thử ISO, YMD, DMY rồi MDY; DMY có ưu tiên khi mơ hồ. Fallback
  ngày UTC và dữ liệu raw giữ nguyên. Ví dụ `15/01/2026` và `01/15/2026` cùng
  partition `2026-01-15`, còn `01/02/2026` vào `2026-02-01`.

Không đổi chữ ký API storage công khai, schema hoặc partition keys. Các quy tắc
trên áp dụng cho lần ghi/chạy mới; không tự migrate checkpoint hoặc bảng cũ.
