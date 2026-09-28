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
diff_versions(table_uri: str, v1: int, v2: int, id_column: str | None = None) -> dict
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
