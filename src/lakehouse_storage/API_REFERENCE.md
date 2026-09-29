# Lakehouse Storage API Reference

Tài liệu kỹ thuật cho toàn bộ public API của package `lakehouse_storage` (v2.0.0).

Mọi symbol được liệt kê dưới đây đều được export từ `lakehouse_storage.__init__` và có thể import trực tiếp:

```python
from lakehouse_storage import write_bronze_batch, read_silver_table, ...
```

---

## Mục lục

1. [Module: `config`](#1-module-config)
2. [Module: `schemas`](#2-module-schemas)
3. [Module: `bronze_writer`](#3-module-bronze_writer)
4. [Module: `silver_manager`](#4-module-silver_manager)
5. [Module: `time_travel`](#5-module-time_travel)
6. [Module: `maintenance`](#6-module-maintenance)
7. [Data Generators](#7-data-generators)

---

## 1. Module: `config`

Cấu hình toàn cục, đọc từ environment variables. Module `config` được export trực tiếp:

```python
from lakehouse_storage import config
```

### Constants

| Constant | Type | Default | Mô tả |
|----------|------|---------|-------|
| `MINIO_ENDPOINT` | `str` | `"localhost:9000"` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `str` | `"dairy"` | MinIO access key |
| `MINIO_SECRET_KEY` | `str` | `"hustit5425"` | MinIO secret key |
| `MINIO_SECURE` | `bool` | `False` | Sử dụng HTTPS |
| `MINIO_BUCKET` | `str` | `"smart-dairy-lakehouse"` | Tên S3 bucket |
| `STORAGE_OPTIONS` | `dict` | *(xem bên dưới)* | Dict cho mọi Delta Lake S3 operation |
| `BRONZE_LOCAL_PATH` | `str` | `"data/01_bronze_vault"` | Thư mục Bronze trên local filesystem |
| `SILVER_LOCAL_SNAPSHOT_PATH` | `str` | `"data/02_silver_curated"` | Thư mục export Silver snapshot |
| `GOLD_LOCAL_PATH` | `str` | `"data/03_gold_marts"` | Thư mục Gold marts |
| `SILVER_TABLE_URIS` | `dict[str, str]` | | Mapping table name -> S3 URI. Hỗ trợ cả tên gốc, alias `S1`-`S5`, và tên `fact_*` |
| `VACUUM_RETENTION_HOURS_DEV` | `int` | `0` | Retention cho testing |
| `VACUUM_RETENTION_HOURS_SAFE` | `int` | `168` | Retention an toàn (7 ngày) |
| `COMPACTION_MIN_FILE_SIZE_MB` | `int` | `128` | Target file size cho compaction |
| `DEFAULT_LINE_IDS` | `list[str]` | `["LINE_UHT_1", "LINE_UHT_2"]` | Mã dây chuyền mặc định |
| `DEFAULT_SKU_CODES` | `list[str]` | `["UHT_PURE", "UHT_SWEETENED", "UHT_LOWFAT"]` | Mã SKU mặc định |

Tất cả giá trị được đọc từ environment variables tương ứng (ví dụ: `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `BRONZE_LOCAL_PATH`, ...).

### `get_storage_options`

```python
config.get_storage_options(
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> dict
```

Trả về dict `storage_options` cho Delta Lake S3 operations. Khi không truyền tham số, sử dụng giá trị từ config mặc định.

### `get_table_uri`

```python
config.get_table_uri(table_name: str) -> str
```

Trả về S3 URI cho bảng Silver. Tra trong `SILVER_TABLE_URIS` trước; nếu không tìm thấy, trả về `s3://<MINIO_BUCKET>/02_silver_curated/<table_name>`.

---

## 2. Module: `schemas`

Source of Truth cho toàn bộ PyArrow schemas. Định nghĩa cấu trúc Bronze (raw) và Silver (curated) cho 7 bảng dữ liệu.

### Schema Constants

Mỗi bảng có hai schema: Bronze (raw landing) và Silver (curated Delta Lake).

| Bronze Schema | Silver Schema | Bảng |
|---------------|---------------|------|
| `BRONZE_SENSOR_TELEMETRY_SCHEMA` | `SENSOR_TELEMETRY_SCHEMA` | IoT sensor telemetry |
| `BRONZE_MES_LIMS_SCHEMA` | `MES_LIMS_SCHEMA` | MES/LIMS production batch |
| `BRONZE_MARKET_PRICES_SCHEMA` | `MARKET_PRICES_SCHEMA` | Giá hàng hóa quốc tế |
| `BRONZE_MARKET_INDICES_SCHEMA` | `MARKET_INDICES_SCHEMA` | Chỉ số giá FAO |
| `BRONZE_WEATHER_SCHEMA` | `WEATHER_SCHEMA` | Thời tiết trang trại |
| `BRONZE_FOOD_RECALLS_SCHEMA` | `FOOD_RECALLS_SCHEMA` | Thu hồi an toàn thực phẩm |
| `BRONZE_METADATA_RAW_SCHEMA` | `BRONZE_METADATA_SCHEMA` | Bronze metadata |

**Sự khác biệt Bronze vs Silver:**

- Bronze: timestamp dạng `string`, số thực `float64`, có thêm `_source_system` và `_generated_at`.
- Silver: timestamp dạng `timestamp[us, tz]`, số thực `float32`, bỏ `_source_system`/`_generated_at` của Bronze, thêm lineage columns.

**Alias theo mã nguồn (S1-S5):**

| Alias | Bronze | Silver |
|-------|--------|--------|
| `BRONZE_S1_SCHEMA` | = `BRONZE_SENSOR_TELEMETRY_SCHEMA` | |
| `SILVER_S1_SCHEMA` | | = `SENSOR_TELEMETRY_SCHEMA` |
| `BRONZE_S2_SCHEMA` | = `BRONZE_MES_LIMS_SCHEMA` | |
| `SILVER_S2_SCHEMA` | | = `MES_LIMS_SCHEMA` |
| `BRONZE_S3_SCHEMA` | = `BRONZE_MARKET_PRICES_SCHEMA` | |
| `SILVER_S3_SCHEMA` | | = `MARKET_PRICES_SCHEMA` |
| `BRONZE_S4_SCHEMA` | = `BRONZE_WEATHER_SCHEMA` | |
| `SILVER_S4_SCHEMA` | | = `WEATHER_SCHEMA` |
| `BRONZE_S5_SCHEMA` | = `BRONZE_FOOD_RECALLS_SCHEMA` | |
| `SILVER_S5_SCHEMA` | | = `FOOD_RECALLS_SCHEMA` |

### Domain Constants

| Constant | Type | Mô tả |
|----------|------|-------|
| `TIMEZONE` | `str` | Timezone cho factory (mặc định `"UTC"`, đọc từ env `FACTORY_TIMEZONE`) |
| `LINEAGE_COLUMNS` | `list[str]` | `["_source_system", "_bronze_file", "_bronze_sha256", "_ingested_at", "ingestion_date"]` |
| `PARTITION_COLUMNS` | `dict[str, list[str]]` | Mapping table -> partition columns |
| `FARM_LOCATIONS` | `dict[str, dict]` | 5 trang trại: `MOC_CHAU`, `BA_VI`, `NGHE_AN`, `LAM_DONG`, `CU_CHI` (lat/lon/name) |
| `THI_THRESHOLD_STRESS` | `float` | `72.0` - Ngưỡng THI stress |
| `VALID_SKU_CODES` | `list[str]` | `["UHT_PURE", "UHT_SWEETENED", "UHT_LOWFAT"]` |
| `VALID_SHIFT_CODES` | `list[str]` | `["MORNING", "AFTERNOON", "NIGHT"]` |
| `VALID_SEVERITY_LEVELS` | `list[str]` | `["Class I", "Class II", "Class III"]` |
| `VALID_HAZARD_TYPES` | `list[str]` | `["Microbiological", "Foreign Matter", "Chemical", "Allergen", "Physical"]` |
| `VALID_MICROBIOLOGY_STATUS` | `list[str]` | `["PASSED", "FAILED", "PENDING"]` |
| `VALID_GDT_PRODUCTS` | `list[str]` | `["WMP", "SMP", "AMF", "BUTTER", "CHEDDAR"]` |
| `VALID_CME_PRODUCTS` | `list[str]` | `["CLASSIII", "CLASSIV", "BOT", "SMP_FUT", "WMP_FUT"]` |

### `SCHEMA_REGISTRY`

```python
SCHEMA_REGISTRY: dict[str, dict]
```

Dict mapping mỗi table name đến metadata: `source_name`, `table_name`, `bronze` schema, `silver` schema, `partition` columns, `id_column`, `description`.

### `SCHEMA_ALIASES`

```python
SCHEMA_ALIASES: dict[str, str]
```

Mapping alias `S1`-`S5` về tên bảng gốc (ví dụ: `"S1" -> "sensor_telemetry"`).

### Registry Functions

#### `get_bronze_schema`

```python
get_bronze_schema(table_name: str) -> pa.Schema
```

Trả về Bronze PyArrow schema cho bảng. `table_name` hỗ trợ alias (ví dụ: `"S1"`, `"s2"`) và tên gốc.

- **Raises:** `ValueError` nếu bảng không tồn tại trong registry.

#### `get_silver_schema`

```python
get_silver_schema(table_name: str) -> pa.Schema
```

Trả về Silver PyArrow schema cho bảng.

- **Raises:** `ValueError` nếu bảng không tồn tại trong registry.

#### `get_partition_columns`

```python
get_partition_columns(table_name: str) -> list[str]
```

Trả về danh sách partition columns cho bảng (dùng khi ghi Delta Lake).

- **Raises:** `ValueError` nếu bảng không tồn tại.

#### `get_source_name`

```python
get_source_name(table_name: str) -> str
```

Trả về tên thư mục Bronze source tương ứng với bảng.

- **Raises:** `ValueError` nếu bảng không tồn tại.

#### `get_table_name`

```python
get_table_name(table_name: str) -> str
```

Trả về tên bảng Delta Lake chuẩn hóa.

- **Raises:** `ValueError` nếu bảng không tồn tại.

#### `get_silver_table_uri`

```python
get_silver_table_uri(table_name: str, bucket: str | None = None) -> str
```

Trả về S3 URI cho Silver Delta table: `s3://<bucket>/02_silver_curated/<table_name>`. Khi `bucket=None`, sử dụng `MINIO_BUCKET` từ config.

- **Raises:** `ValueError` nếu bảng không tồn tại.

#### `get_id_column`

```python
get_id_column(table_name: str) -> str
```

Trả về tên cột primary key (ID) của bảng. Ví dụ: `"batch_id"` cho `mes_lims`, `"timestamp"` cho `sensor_telemetry`.

- **Raises:** `ValueError` nếu bảng không tồn tại.

#### `list_tables`

```python
list_tables() -> list[str]
```

Trả về danh sách tên tất cả bảng đã đăng ký: `["sensor_telemetry", "mes_lims", "market_prices", "market_indices", "weather", "food_recalls", "bronze_metadata"]`.

#### `get_table_description`

```python
get_table_description(table_name: str) -> str
```

Trả về mô tả ngắn về bảng.

- **Raises:** `ValueError` nếu bảng không tồn tại.

---

## 3. Module: `bronze_writer`

Ghi dữ liệu thô vào Bronze layer trên local filesystem. Đảm bảo tính bất biến (immutable, append-only).

### `write_bronze_batch`

```python
write_bronze_batch(
    df: pl.DataFrame,
    table_name: str,
    bronze_root: str | Path | None = None,
    validate_schema: bool = True,
) -> str
```

Ghi DataFrame thành file Parquet kèm SHA-256 sidecar checksum vào Bronze layer.

- **Parameters:**
  - `df`: Polars DataFrame chứa dữ liệu raw.
  - `table_name`: Tên bảng hoặc alias (`"sensor_telemetry"`, `"S1"`, ...).
  - `bronze_root`: Thư mục gốc Bronze (mặc định: `data/01_bronze_vault`).
  - `validate_schema`: Kiểm tra các cột non-nullable bắt buộc theo Bronze schema.
- **Returns:** Đường dẫn (string) tới file Parquet đã ghi.
- **Path convention:** `<bronze_root>/<source_name>/<YYYY-MM-DD>/<source_name>_<timestamp>.parquet`
- **Raises:**
  - `ValueError` nếu DataFrame rỗng.
  - `ValueError` nếu `validate_schema=True` và thiếu cột non-nullable.
  - `FileExistsError` nếu file đã tồn tại (append-only, không ghi đè).

> [!IMPORTANT]
> Hàm không thay đổi `df` đầu vào. Files Bronze bất biến - chỉ tạo mới, không ghi đè.

### `verify_checksum`

```python
verify_checksum(bronze_path: str | Path) -> bool
```

Kiểm tra SHA-256 checksum của file Parquet có khớp với nội dung file `.sha256` sidecar.

- **Parameters:**
  - `bronze_path`: Đường dẫn tới file Parquet (tự thêm `.parquet` nếu thiếu extension).
- **Returns:** `True` nếu checksum khớp, `False` nếu không khớp hoặc không có sidecar.
- **Raises:** `FileNotFoundError` nếu file Parquet không tồn tại.

### `list_bronze_files`

```python
list_bronze_files(
    table_name: str,
    bronze_root: str | Path | None = None,
    date_from: date | str | None = None,
    date_to: date | str | None = None,
) -> list[Path]
```

Liệt kê các file Bronze Parquet cho một bảng, lọc theo khoảng ngày tùy chọn.

- **Parameters:**
  - `table_name`: Tên bảng.
  - `bronze_root`: Thư mục gốc Bronze.
  - `date_from`, `date_to`: Lọc theo ngày partition folder (ISO format string hoặc `date` object).
- **Returns:** Danh sách `Path` đã sắp xếp. Trả về list rỗng nếu thư mục chưa tồn tại.

### `read_bronze_file`

```python
read_bronze_file(
    bronze_path: str | Path,
    bronze_root: str | Path | None = None,
) -> pl.DataFrame
```

Đọc một file Bronze Parquet trả về Polars DataFrame.

- **Parameters:**
  - `bronze_path`: Đường dẫn tới file Parquet.
  - `bronze_root`: Khi cung cấp, kiểm tra path traversal (path phải nằm trong root).
- **Returns:** `pl.DataFrame`.
- **Raises:** `ValueError` nếu path vượt khỏi `bronze_root` (path traversal).

### `ingest_multiple_files`

```python
ingest_multiple_files(
    table_name: str,
    bronze_paths: list[str | Path],
    bronze_root: str | Path | None = None,
) -> list[dict]
```

Verify checksum cho một batch file Bronze và trả về báo cáo.

- **Parameters:**
  - `table_name`: Tên bảng (dùng để validate thư mục source).
  - `bronze_paths`: Danh sách đường dẫn file Bronze.
  - `bronze_root`: Kiểm tra path traversal nếu cung cấp.
- **Returns:** List of dict, mỗi dict chứa:
  - Thành công: `{"path": str, "checksum": str, "verified": bool, "size_bytes": int}`
  - Lỗi: `{"path": str, "error": str, "verified": False}`

> [!NOTE]
> Hàm này chỉ verify checksum, không ghi dữ liệu lên Silver. Một file lỗi không ảnh hưởng các file khác trong batch.

---

## 4. Module: `silver_manager`

Quản lý đọc/ghi ACID trên Delta Lake (MinIO S3).

### `append_to_delta`

```python
append_to_delta(
    df: pl.DataFrame,
    table_uri: str,
    partition_by: list[str],
    table_name: str | None = None,
) -> None
```

Append DataFrame vào Delta table trên S3. Khi `table_name` được cung cấp, tự động thực hiện type casting (float64 -> float32, string timestamps -> `timestamp[us, tz]`), thêm `ingestion_date` và `_ingested_at`.

- **Parameters:**
  - `df`: Polars DataFrame.
  - `table_uri`: S3 URI của Delta table.
  - `partition_by`: Cột partition.
  - `table_name`: Tên bảng để áp dụng schema enforcement.
- **Returns:** `None`.
- **Behavior:** Nếu DataFrame rỗng sau transform, ghi warning và skip. Tự động tạo bảng mới nếu chưa tồn tại.

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

Quy trình Medallion hoàn chỉnh: đọc Bronze Parquet -> verify checksum -> thêm lineage columns (`_bronze_file`, `_bronze_sha256`) -> transform -> ghi Silver Delta.

- **Parameters:**
  - `bronze_path`: Đường dẫn file Bronze Parquet.
  - `silver_table_uri`: S3 URI của Delta table đích.
  - `partition_by`: Cột partition.
  - `table_name`: Tên bảng để schema enforcement.
  - `bronze_root`: Kiểm tra path traversal nếu cung cấp.
  - `enforce_checksum`: Verify `.sha256` sidecar trước khi ingest.
- **Returns:** `{"rows_ingested": int, "bronze_path": str, "sha256": str}` (khi thành công) hoặc `{"rows_ingested": 0, "bronze_path": str}` (khi file rỗng).
- **Raises:**
  - `FileNotFoundError` nếu Bronze file không tồn tại.
  - `ValueError` nếu `enforce_checksum=True` và checksum không khớp.
  - `ValueError` nếu path nằm ngoài `bronze_root`.

> [!WARNING]
> Khi `enforce_checksum=True` (mặc định), dữ liệu có checksum sai sẽ bị từ chối ngay, không cho lên Silver.

### `write_silver`

```python
write_silver(
    df: pl.DataFrame,
    table_name: str,
    source_bronze: str | Path,
    table_uri: str | None = None,
    bronze_root: Path | str | None = None,
    enforce_checksum: bool = True,
) -> int
```

Ghi một DataFrame đã được chuẩn bị/preprocessed vào Silver. Khác với `ingest_bronze_file` (tự đọc file Bronze), `write_silver` nhận DataFrame trực tiếp - phù hợp khi upstream component đã xử lý dữ liệu trước.

- **Parameters:**
  - `df`: Polars DataFrame (đã được preprocessed bởi upstream component).
  - `table_name`: Tên bảng hoặc alias.
  - `source_bronze`: Đường dẫn Bronze file gốc (dùng cho lineage).
  - `table_uri`: S3 URI đích (auto-resolve nếu `None`).
  - `bronze_root`: Kiểm tra path traversal.
  - `enforce_checksum`: Verify checksum Bronze sidecar.
- **Returns:** Phiên bản Delta table sau khi ghi, hoặc `-1` nếu DataFrame rỗng.
- **Raises:**
  - `ValueError` nếu table_name không hợp lệ, schema enforcement thất bại, path traversal, hoặc checksum sai.
  - `FileNotFoundError` nếu `source_bronze` không tồn tại.

> [!IMPORTANT]
> `write_silver` luôn thêm lineage columns (`_bronze_file`, `_bronze_sha256`) tự động từ `source_bronze`. Caller không cần tự thêm.

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

Đọc dữ liệu Silver từ Delta Lake với các bộ lọc tùy chọn.

- **Parameters:**
  - `table_uri`: S3 URI của Delta table.
  - `line_id`: Lọc theo `line_id` (bỏ qua nếu bảng không có cột này).
  - `date_from`, `date_to`: Lọc theo `ingestion_date` (ISO format string hoặc `date` object).
  - `columns`: Chọn cột cụ thể (projection pushdown).
  - `limit`: Giới hạn số dòng.
- **Returns:** `pl.DataFrame`.
- **Behavior:** Sử dụng lazy scan (`pl.scan_delta`) cho partition pruning trước khi collect.

### `export_silver_parquet`

```python
export_silver_parquet(
    table_uri: str,
    dest_path: str | Path,
    line_id: str | None = None,
    date_from: str | date | None = None,
    date_to: str | date | None = None,
) -> str
```

Export Silver table ra file Parquet tĩnh trên local filesystem.

- **Parameters:**
  - `table_uri`: S3 URI nguồn.
  - `dest_path`: Đường dẫn file Parquet đích (tự tạo parent directories).
  - `line_id`, `date_from`, `date_to`: Bộ lọc (giống `read_silver_table`).
- **Returns:** Đường dẫn (string) tới file đã ghi.

### `get_delta_table`

```python
get_delta_table(table_uri: str) -> DeltaTable
```

Trả về `DeltaTable` object (từ `deltalake` library) với `storage_options` đã cấu hình.

### `get_table_version`

```python
get_table_version(table_uri: str) -> int
```

Trả về số phiên bản hiện tại của Delta table.

### `get_table_stats`

```python
get_table_stats(table_uri: str) -> dict
```

Trả về thống kê bảng: `{"version": int, "row_count": int, "file_count": int}`.

### `delete_from_delta`

```python
delete_from_delta(table_uri: str, predicate: str) -> dict
```

Xóa các dòng khớp predicate (SQL expression).

- **Parameters:**
  - `table_uri`: S3 URI.
  - `predicate`: SQL predicate string (ví dụ: `"batch_id = 'BATCH_001'"`).
- **Returns:** `{"rows_deleted": int, "version": int}`.

### `create_delta_table`

```python
create_delta_table(
    table_uri: str,
    schema: pa.Schema,
    partition_by: list[str] | None = None,
) -> None
```

Tạo bảng Delta trống với schema cho trước. Tự động bổ sung partition columns (`ingestion_date`) và lineage columns (`_source_system`, `_bronze_file`, `_bronze_sha256`, `_ingested_at`) vào schema nếu chưa có.

- **Raises:** `DeltaError` nếu bảng đã tồn tại (mode `"error"`).

---

## 5. Module: `time_travel`

Truy vấn lịch sử và truy vết nguồn gốc (ISO 22000 traceability).

### `load_version`

```python
load_version(table_uri: str, version: int) -> pl.DataFrame
```

Tải toàn bộ dữ liệu bảng tại một phiên bản Delta cụ thể.

### `get_table_history`

```python
get_table_history(table_uri: str, limit: int = 10) -> pl.DataFrame
```

Trả về lịch sử commit của Delta table dưới dạng DataFrame (version, timestamp, operation, ...).

### `load_as_of`

```python
load_as_of(table_uri: str, timestamp_iso: str | int) -> pl.DataFrame
```

Tải dữ liệu bảng tại thời điểm cụ thể.

- **Parameters:**
  - `timestamp_iso`: ISO 8601 string (ví dụ: `"2026-01-15T10:00:00+07:00"`) hoặc epoch milliseconds (int/float).
- **Raises:** `ValueError` nếu timestamp không hợp lệ.

### `diff_versions`

```python
diff_versions(
    table_uri: str,
    v1: int,
    v2: int,
    id_column: str | None = None,
) -> dict
```

So sánh hai phiên bản Delta và trả về sự khác biệt.

- **Parameters:**
  - `v1`, `v2`: Số phiên bản để so sánh.
  - `id_column`: Cột primary key (tự động detect nếu không cung cấp, ưu tiên: `timestamp`, `observed_at`, `period_start`, `batch_id`, `recall_id`, `file`).
- **Returns:**
  ```python
  {
      "added": pl.DataFrame,     # Dòng có trong v2 nhưng không có trong v1
      "removed": pl.DataFrame,   # Dòng có trong v1 nhưng không có trong v2
      "counts": {
          "v1_rows": int,
          "v2_rows": int,
          "added_count": int,
          "removed_count": int,
      },
      "id_column": str,
  }
  ```
- **Raises:** `ValueError` nếu `id_column` không tồn tại trong bảng.

### `trace_batch`

```python
trace_batch(
    table_uri: str,
    batch_id: str,
    id_column: str = "batch_id",
) -> dict
```

Truy vết một batch về file Bronze gốc (ISO 22000 audit).

- **Parameters:**
  - `batch_id`: Giá trị ID cần truy vết.
  - `id_column`: Cột chứa ID (mặc định `"batch_id"`).
- **Returns:**
  ```python
  {
      "batch_id": str,
      "batch_info": dict,           # Dữ liệu batch dạng dict
      "lineage": {
          "bronze_file": str,       # Đường dẫn file Bronze gốc
          "bronze_sha256": str,     # SHA-256 khi ingest
          "actual_sha256": str | None,  # SHA-256 hiện tại (None nếu file không còn)
          "ingested_at": str | None,
      },
      "integrity_verified": bool,   # True nếu checksum vẫn khớp
  }
  ```
- **Raises:** `ValueError` nếu không tìm thấy batch hoặc thiếu cột `_bronze_file`.

> [!TIP]
> `integrity_verified = True` chứng minh file Bronze gốc chưa bị thay đổi kể từ khi ingest.

### `trace_event`

```python
trace_event(table_uri: str, event_id: str) -> dict
```

Truy vết một sensor event theo `timestamp`. Gọi `trace_batch` với `id_column="timestamp"`.

### `trace_batch_history`

```python
trace_batch_history(
    table_uri: str,
    batch_id: str,
    id_column: str = "batch_id",
) -> pl.DataFrame
```

Tìm tất cả phiên bản Delta mà batch xuất hiện. Trả về DataFrame với các cột dữ liệu gốc cộng thêm `_version` và `_commit_timestamp`. Trả về DataFrame rỗng nếu batch không tồn tại ở bất kỳ phiên bản nào.

### `generate_audit_report`

```python
generate_audit_report(
    table_uri: str,
    date_from: str,
    date_to: str,
    output_path: str | Path | None = None,
) -> pl.DataFrame
```

Tạo báo cáo kiểm toán ISO 22000 cho khoảng ngày.

- **Parameters:**
  - `date_from`, `date_to`: ISO date string (ví dụ: `"2026-01-01"`).
  - `output_path`: Nếu cung cấp, lưu kết quả thành Parquet (tự tạo parent directories).
- **Returns:** DataFrame chứa các cột audit: ID column (nếu có), `_bronze_file`, `_bronze_sha256`, `_ingested_at`, `ingestion_date`.

### `get_latest_version`

```python
get_latest_version(table_uri: str) -> int
```

Trả về số phiên bản mới nhất (hiện tại) của Delta table.

### `get_version_at_date`

```python
get_version_at_date(table_uri: str, target_date: str) -> int | None
```

Tìm phiên bản Delta tại một ngày cụ thể.

- **Parameters:**
  - `target_date`: ISO datetime string.
- **Returns:** Số phiên bản, hoặc `None` nếu không tìm thấy commit nào trước thời điểm đó.

---

## 6. Module: `maintenance`

Bảo trì và tối ưu Delta table: compaction, Z-order, vacuum.

### `compact_table`

```python
compact_table(
    table_uri: str,
    target_file_size_mb: int = 128,
) -> dict
```

Gom các file Parquet nhỏ thành file lớn hơn (bin-packing compaction).

- **Returns:**
  ```python
  {
      "files_before": int,
      "files_after": int,
      "files_removed": int,
      "bytes_before": int,
      "bytes_after": int,
      "bytes_removed": int,
  }
  ```

### `auto_compact_if_needed`

```python
auto_compact_if_needed(
    table_uri: str,
    file_count_threshold: int = 50,
    target_file_size_mb: int = 128,
) -> dict | None
```

Tự động compact nếu số file vượt ngưỡng.

- **Returns:** Dict kết quả compaction nếu chạy, `None` nếu bỏ qua (dưới ngưỡng).

### `zorder_table`

```python
zorder_table(table_uri: str, columns: list[str]) -> dict
```

Tối ưu data layout theo Z-order cho các cột lọc thường xuyên. Tự động loại bỏ partition columns (không thể Z-order).

- **Returns:**
  ```python
  {
      "columns": list[str],
      "files_before": int,
      "files_after": int,
      "files_added": int,
  }
  ```
- **Raises:** `ValueError` nếu `columns` rỗng.

> [!NOTE]
> Partition columns đã được Delta tự động skip khi filter, nên Z-order chỉ hiệu quả với các cột non-partition thường xuyên dùng trong `WHERE`.

### `vacuum_table`

```python
vacuum_table(
    table_uri: str,
    retention_hours: int = 168,
    dry_run: bool = True,
    enforce_retention_duration: bool = False,
) -> dict
```

Xóa file không còn được tham chiếu bởi Delta table.

- **Parameters:**
  - `retention_hours`: Giữ lại file mới hơn N giờ (mặc định 168 = 7 ngày).
  - `dry_run`: `True` (mặc định) chỉ liệt kê, không xóa.
  - `enforce_retention_duration`: Enforce minimum retention period.
- **Returns:**
  ```python
  {
      "dry_run": bool,
      "retention_hours": int,
      "files_removed": list[str],
      "files_count": int,
      "bytes_removed": int,
      "total_bytes_before": int,
  }
  ```

> [!WARNING]
> `dry_run=True` là mặc định. Truyền `dry_run=False` để thực sự xóa file trên S3.

### `vacuum_all_tables`

```python
vacuum_all_tables(
    table_uris: list[str],
    retention_hours: int = 168,
    dry_run: bool = True,
) -> list[dict]
```

Vacuum nhiều bảng. Mỗi bảng được xử lý độc lập; lỗi ở một bảng không ảnh hưởng các bảng khác.

- **Returns:** List of dict, mỗi dict chứa `table_uri` cùng kết quả vacuum hoặc `error`.

### `get_maintenance_stats`

```python
get_maintenance_stats(table_uri: str) -> dict
```

Thống kê chi tiết bảng cho maintenance (alias của `maintenance.get_table_stats`, khác với `silver_manager.get_table_stats`).

- **Returns:**
  ```python
  {
      "table_uri": str,
      "version": int,
      "file_count": int,
      "total_bytes": int,
      "total_bytes_human": str,    # Ví dụ: "12.50 MB"
      "partition_columns": list[str],
      "last_updated": datetime | None,
  }
  ```

### `get_file_sizes`

```python
get_file_sizes(table_uri: str) -> list[dict]
```

Chi tiết kích thước từng file data trong Delta table, sắp xếp giảm dần.

- **Returns:** List of `{"path": str, "size_bytes": int, "size_human": str}`.

### `get_optimization_recommendations`

```python
get_optimization_recommendations(table_uri: str) -> list[str]
```

Phân tích bảng và đưa ra khuyến nghị tối ưu (compaction, Z-order).

- **Returns:** List of recommendation strings.

### `run_full_maintenance`

```python
run_full_maintenance(
    table_uri: str,
    compact: bool = True,
    zorder_cols: list[str] | None = None,
    vacuum: bool = True,
    vacuum_retention_hours: int = 168,
    vacuum_dry_run: bool = True,
) -> dict
```

Chạy chu trình bảo trì hoàn chỉnh: compaction -> Z-order -> vacuum.

- **Parameters:**
  - `compact`: Chạy compaction.
  - `zorder_cols`: Cột Z-order (bỏ qua nếu `None`).
  - `vacuum`: Chạy vacuum.
  - `vacuum_retention_hours`: Retention cho vacuum.
  - `vacuum_dry_run`: Dry run cho vacuum (`True` mặc định).
- **Returns:** Dict với key `table_uri` và kết quả từng bước (`compaction`, `zorder`, `vacuum`). Mỗi bước có thể chứa `error` nếu thất bại.

---

## 7. Data Generators

Tạo dữ liệu giả lập cho testing và demo. Dữ liệu output theo Bronze schema (string timestamps, float64).

### `generate_sensor_batch`

```python
generate_sensor_batch(
    n_samples: int,
    line_id: str = "LINE_UHT_1",
    batch_id: str | None = None,
    start_time: datetime | None = None,
    seed: int | None = None,
) -> pl.DataFrame
```

Tạo batch dữ liệu IoT sensor telemetry (1 sample = 1 giây).

- **Returns:** DataFrame theo `BRONZE_SENSOR_TELEMETRY_SCHEMA`.
- **Raises:** `ValueError` nếu `n_samples <= 0`.

### `write_sensor_batch`

```python
write_sensor_batch(
    df: pl.DataFrame,
    table_uri: str | None = None,
    bronze_root: str | Path | None = None,
) -> int
```

Ghi sensor batch xuyên suốt Bronze -> Silver: gọi `write_bronze_batch` rồi `ingest_bronze_file`.

- **Returns:** Phiên bản Delta table sau khi ghi.

### `generate_mes_batch`

```python
generate_mes_batch(
    date: datetime,
    line_id: str = "LINE_UHT_1",
    sku_id: str = "UHT_PURE",
    batch_id: str | None = None,
    production_start: datetime | None = None,
    production_end: datetime | None = None,
    seed: int | None = None,
    include_defect: bool = False,
) -> pl.DataFrame
```

Tạo 1 batch MES/LIMS (1 dòng = 1 lô sản xuất).

- **Returns:** DataFrame theo `BRONZE_MES_LIMS_SCHEMA`.

### `generate_daily_production`

```python
generate_daily_production(
    date: datetime,
    line_id: str = "LINE_UHT_1",
    seed: int | None = None,
    defect_rate: float = 0.05,
) -> tuple[pl.DataFrame, pl.DataFrame, dict]
```

Tạo dữ liệu 1 ngày sản xuất (4 batches, 8 giờ).

- **Returns:** Tuple `(sensor_df, mes_df, batch_farm_map)`:
  - `sensor_df`: DataFrame sensor (28800 rows = 4 batches x 2h x 3600 samples/h).
  - `mes_df`: DataFrame MES (4 rows).
  - `batch_farm_map`: Dict mapping `batch_id` -> `{"farm_id": str, "line_id": str, "production_date": str}`.

### `write_mes_batch`

```python
write_mes_batch(
    df: pl.DataFrame,
    table_uri: str | None = None,
    bronze_root: str | Path | None = None,
) -> int
```

Ghi MES batch xuyên suốt Bronze -> Silver.

- **Returns:** Phiên bản Delta table sau khi ghi.
