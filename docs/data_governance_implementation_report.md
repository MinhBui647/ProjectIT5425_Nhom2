# Data Governance & Quality — Báo cáo triển khai

> Thời điểm đối chiếu: 19/10/2026 (Asia/Bangkok)  
> Nguồn yêu cầu: phạm vi Data Governance & Quality ban đầu của dự án  
> Nguồn kiểm chứng: code, test, fixture manifest, acceptance report và artifact hiện có trong workspace  
> Nhánh hiện tại: `feat/data-governance`  
> HEAD đã commit: `5126acd` (`docs: complete governance handoff`)

## 1. Kết luận ngắn

Pipeline Data Governance đã hoàn thành luồng **Bronze → canonical transform → quality validation → gate → artifact → Delta Silver → OpenLineage** cho 6 bảng MVP:

1. `sensor_telemetry`
2. `mes_lims`
3. `weather`
4. `market_prices`
5. `market_indices`
6. `food_recalls`

Pipeline đã được kiểm thử local và chạy end-to-end trên MinIO/Marquez bằng bộ Bronze fixtures cố định. Silver đã được đọc lại bằng Polars; riêng `sensor_telemetry` đã được kiểm tra thêm bằng **chDB**, đúng engine downstream analytics.

Các điểm chưa thể coi là production-complete gồm: chưa chạy toàn bộ dữ liệu crawl live ở quy mô thực, chưa hỗ trợ USDA `market_reports`, acceptance chDB mới kiểm tra một bảng, chưa có contract `v2` hoặc migration cho bảng legacy, artifact runtime phần lớn nằm trong `tmp/`/`data/` và các thay đổi bổ sung ngày 19/10 chưa được commit/push.

## 2. Phạm vi nhiệm vụ và phạm vi thực tế

### 2.1. Yêu cầu Data Governance ban đầu

Phạm vi Data Governance ban đầu gồm các nhóm việc chính:

- Nhận dữ liệu Bronze từ TV1 và TV2.
- Chuẩn hóa dữ liệu của từng nguồn sang canonical Silver schema.
- Tính THI và cờ stress nhiệt cho dữ liệu thời tiết.
- Xây dựng Data Contracts/Great Expectations theo các chiều chất lượng DAMA.
- Áp dụng quality gate 0,5% và quarantine dòng lỗi.
- Phát hiện bất thường cảm biến bằng Hampel/Rolling Z-score.
- Sinh Data Docs, Data Lineage và ba biểu đồ bắt buộc.
- Viết unit test và bàn giao Silver cho downstream analytics.

### 2.2. Phạm vi thực tế đã khóa

Phạm vi thực tế giữ 6 bảng MVP nêu trên. USDA `market_reports` không được đưa vào Silver vì các section của báo cáo USDA chưa có schema ổn định và đồng nhất. Code chủ động fail-fast nếu gọi bảng này thay vì tự suy đoán schema.

Mã nguồn chính nằm trong:

- `src/data_governance/`: logic governance của pipeline.
- `src/lakehouse_storage/`: schema registry, Bronze/Silver storage và Delta utilities dùng chung.
- `tests/test_data_governance_*.py`: test của pipeline governance.
- `tests/fixtures/bronze/`: bộ Bronze fixtures đã khóa checksum.
- `infra/docker-compose.minio.yml`: MinIO demo.
- `infra/docker-compose.marquez.yml`: Marquez/OpenLineage demo.

## 3. Kiến trúc input → output đã triển khai

```text
TV1 Sensor/MES + TV2 crawl/Drive
              |
              v
Bronze Parquet + .sha256 sidecar
              |
              v
Verify path / extension / checksum / non-empty
              |
              v
Canonical transform + UTC timestamp + ingestion_date
              |
              +---- Sensor: Hampel OR Rolling Z-score -> is_anomaly_flag
              |
              v
Operational Data Contract (Polars rule engine)
              |
              v
Quality gate theo tỷ lệ dòng lỗi duy nhất
      |                 |                    |
      | PASS            | <= 0,5%            | > 0,5% / hard failure
      |                 |                    |
      v                 v                    v
  ghi Silver      valid -> Silver        không ghi Silver
                  invalid -> quarantine   invalid -> quarantine nếu đã tới gate
      \                 |                    /
       \                v                   /
        +------ summary JSON / figures / GX Data Docs
                              |
                              v
                 OpenLineage -> Marquez hoặc local spool
                              |
                              v
             Delta Silver trên MinIO -> downstream/chDB
```

Nguyên tắc quan trọng: Bronze chỉ được đọc; Delta Silver là thao tác mutable cuối cùng. Nếu bước tạo artifact hoặc Data Docs thất bại thì pipeline không commit Silver.

## 4. Input Bronze đã sử dụng

### 4.1. Bộ fixture nghiệm thu

Manifest: `tests/fixtures/bronze/manifest.json`.

| Bảng | Nguồn fixture | Số dòng | Kích thước | Ghi chú |
|---|---|---:|---:|---|
| `sensor_telemetry` | Sensor demo generator, seed cố định | 60 | 8.449 byte | Dữ liệu giả lập Sensor |
| `mes_lims` | Generator TV1, seed 42 | 1 | 5.563 byte | Dữ liệu giả lập MES/LIMS |
| `weather` | Bronze TV2 tải từ Google Drive | 24 | 2.800 byte | Một batch 24 giờ, không phải toàn bộ 5 farm |
| `market_prices` | Bronze TV2 tải từ Google Drive | 3 | 3.804 byte | Mẫu GDT nhỏ, không phải toàn bộ lịch sử 5–10 năm |
| `market_indices` | Bronze TV2 tải từ Google Drive | 1 | 6.143 byte | FAO dạng long |
| `food_recalls` | Bronze TV2 tải từ Google Drive | 5 | 15.830 byte | Mẫu OpenFDA |

Mỗi fixture có `.sha256` sidecar và checksum được khóa trong manifest. Tổng thể đây là **bộ dữ liệu kiểm thử cố định**, không được mô tả là toàn bộ dữ liệu production hoặc toàn bộ kết quả crawl live.

### 4.2. Bronze loader và kiểm tra toàn vẹn

`bronze_loader.py` đã thực hiện:

- Chỉ nhận file `.parquet` nằm bên dưới Bronze root đã chỉ định.
- Chặn path traversal ra ngoài Bronze root.
- Yêu cầu `.sha256` sidecar tồn tại.
- Tính lại SHA-256 và từ chối file sai checksum.
- Từ chối file rỗng.
- Đọc Parquet bằng Polars.
- Gắn `_bronze_file`, `_bronze_sha256`, `_contract_version` để truy vết.
- Tạo `BronzeIdentity` gồm đường dẫn, checksum, table, row count và source timestamp.

SHA-256 ở đây là cơ chế **kiểm tra tính toàn vẹn/phát hiện thay đổi**, không phải chữ ký số hay cơ chế chống can thiệp tuyệt đối. Nếu cả Parquet và sidecar cùng bị thay bởi người có quyền thì checksum đơn thuần không chứng minh được tính xác thực.

## 5. Chuyển đổi canonical cho từng bảng

### 5.1. `sensor_telemetry`

- Parse `timestamp` về `Datetime(us, UTC)`.
- Ép các phép đo về `Float32`.
- Giữ nguyên `line_id`, `batch_id` và metadata Bronze.
- Sinh `ingestion_date` từ ngày của `timestamp`.
- Sau transform, bổ sung `is_anomaly_flag` ở bước outlier.

### 5.2. `mes_lims`

- Parse `production_start`, `production_end` về UTC.
- Ép `fat_in`, `protein_in`, `acidity_sh`, `yield_recovery_pct` về `Float32`.
- Ép `batch_defect_flag` về `Int8`.
- Sinh `ingestion_date` từ `production_start`.

### 5.3. `weather`

- Đổi `temperature_celsius` thành `temperature_c`.
- Parse `observed_at` và `_created_at` về UTC; `_created_at` trở thành `ingested_at`.
- Ép nhiệt độ, độ ẩm và lượng mưa về `Float32`.
- Tính:

  ```text
  THI = (1,8 × T + 32) − (0,55 − 0,0055 × RH) × (1,8 × T − 26)
  ```

- Sinh `is_heat_stress = true` khi `thi_index > 72`; giá trị đúng 72 không bị coi là stress.
- Nếu thiếu nhiệt độ hoặc độ ẩm thì THI/cờ stress giữ null.
- Sinh `ingestion_date` từ `observed_at`.

### 5.4. `market_prices` (GDT)

- `ProductGroupCode` → `product`.
- `EventDate` → `observed_at` UTC.
- `AveragePublishedPrice` dạng chuỗi → `price` `Float32`; parse lỗi được giữ null để validator xử lý.
- Gán canonical constants: `source = GDT`, `contract = EVENT_AVERAGE`, `currency = USD`, `unit = metric_ton`.
- `_created_at` → `ingested_at` UTC.
- Sinh `ingestion_date` từ `observed_at`.

### 5.5. `market_indices` (FAO)

Transformer hỗ trợ đồng thời hai thế hệ Bronze:

- **Dạng long từ Drive**: giữ `index_name`, `index_value`, `period_start`, `base_period` và chuẩn hóa kiểu.
- **Dạng wide từ crawler cũ**: unpivot 6 cột Food/Meat/Dairy/Cereals/Oils/Sugar thành `index_name` và `index_value`.

`source` được chuẩn hóa thành `FAO`; `period_start` thành `Date`; `index_value` thành `Float32`; `ingestion_date` lấy từ `period_start`.

### 5.6. `food_recalls` (OpenFDA)

- `recall_number` → `recall_id`.
- `report_date` → `published_at` UTC, hỗ trợ cả dạng `YYYYMMDD` và ISO datetime.
- `product_description` → `product`.
- `reason_for_recall` → `reason`.
- Gán `source = FDA`.
- `_base_api` → `source_url` nếu cột tồn tại.
- `_created_at` → `ingested_at` UTC.
- Sinh `ingestion_date` từ `published_at`.

## 6. Phát hiện bất thường Sensor

`outliers.py` triển khai hai thuật toán và lấy hợp của hai kết quả:

- Hampel Filter centered window: `window_radius = 5`, ngưỡng `3,0 × 1,4826 × MAD`.
- Rolling population Z-score trailing window: `window = 30`, ngưỡng tuyệt đối `3,0`.

Xử lý được tách theo `line_id`, sắp theo `timestamp`, mặc định chỉ áp dụng trên `uht_temp`. Kết quả được ghi vào `is_anomaly_flag`.

Hành vi thực tế:

- Không thay thế giá trị bất thường.
- Không xóa dòng bất thường.
- Không tự quarantine chỉ vì có anomaly.
- Giữ nguyên thứ tự và các giá trị đầu vào.
- Null không bị đánh dấu anomaly.
- `MAD = 0`, chuỗi hằng, window ngắn và nhiều line đã có test.

Như vậy anomaly flag hiện là thông tin cho downstream/phân tích, không phải quality rejection rule.

## 7. Data Contracts và rule engine

### 7.1. Operational validator

Nguồn quyết định PASS/FAIL là rule engine Polars trong `validator.py` và registry trong `contracts.py`. Engine:

- Kiểm tra bảng rỗng, cột bắt buộc và dtype trước.
- Phân biệt hard failure với row-level violation.
- Hợp các mask lỗi theo dòng; một dòng vi phạm nhiều rule vẫn chỉ tính là **một dòng lỗi** trong mẫu số 0,5%.
- Ghi metrics và danh sách mọi rule vi phạm của từng dòng.
- Hỗ trợ severity `ERROR` và `WARNING`.

Các dimension được định nghĩa gồm Completeness, Validity, Accuracy, Consistency, Uniqueness và Timeliness. Tuy nhiên registry hiện có rule gắn nhãn cho 5 dimension; chưa có rule riêng mang dimension `Accuracy` vì chưa có nguồn ground truth đối chiếu. Do đó mô tả trong DOCX rằng đã bao phủ đủ cả 6 chiều DAMA không nên hiểu là mỗi chiều đều đã có rule thực thi.

### 7.2. Các rule nghiệp vụ chính

| Bảng | Rule chính đã triển khai |
|---|---|
| Sensor | Required timestamp/line/UHT; unique `(line_id, timestamp)`; UHT 135–145; áp suất stage 1 150–250; stage 2 20–60; flow rate dương; null ratio các cột quan trọng không quá 1% |
| MES/LIMS | Required identity; unique `batch_id`; thời gian kết thúc không trước bắt đầu; SKU thuộc 3 mã UHT; microbiology thuộc PASSED/FAILED/PENDING; defect flag 0/1; fat/protein/yield trong 0–100 |
| Weather | Required farm/time; unique `(farm_id, observed_at)`; farm hợp lệ; nhiệt độ −10–45; độ ẩm 0–100; lượng mưa không âm; phải có THI khi đủ input; cảnh báo nếu farm/day không đủ 24 bản ghi |
| GDT | Required canonical values; unique `(source, product, observed_at, contract)`; product WMP/SMP/AMF; price dương; source/currency/unit cố định |
| FAO | Required canonical values; unique `(source, index_name, period_start)`; index dương; source FAO; cảnh báo khi chuỗi tháng có gap |
| OpenFDA | Required recall/date/classification; unique recall ID; classification Class I/II/III; source FDA; published time không sau crawl/ingestion time |

Weather 24 giờ và FAO month continuity hiện là **warning**, không reject dòng và không làm fail gate.

### 7.3. Great Expectations

Great Expectations Core 1.x được dùng thật trong `gx_context.py`:

- Tạo persisted File Data Context.
- Tạo 6 Expectation Suites, Validation Definitions và Checkpoints theo contract `v1`.
- Chạy checkpoint trên canonical dataframe.
- Sinh GX Data Docs HTML.
- Giữ `operational_rule_id` trong metadata để truy ngược về rule nội bộ.

GX không phải engine quyết định gate. Nó là lớp kiểm chứng/báo cáo song song. Một số rule operational chi tiết như Sensor null-ratio, Weather/FAO warning và một số range không được mirror toàn bộ sang GX. Test hiện chứng minh GX đồng thuận với validator trên các clean/failing scenario đã chọn, không chứng minh hai hệ rule tương đương tuyệt đối cho mọi trường hợp.

## 8. Quality gate 0,5%

Công thức đang dùng:

```text
error_ratio = số dòng lỗi duy nhất / tổng số dòng trong batch
```

Không dùng tổng số violation. Một dòng lỗi ba rule vẫn chỉ đóng góp một vào tử số.

| Điều kiện | Trạng thái | Ghi Silver | Quarantine |
|---|---|---|---|
| Không có dòng lỗi | `PASS` | Có | Không tạo file rỗng |
| `0 < error_ratio <= 0,005` | `PASS_WITH_REJECTIONS` | Chỉ dòng hợp lệ | Có |
| `error_ratio > 0,005` | `FAIL` | Không | Có nếu validation đã hoàn tất |
| Bảng rỗng/hard failure | `FAIL` | Không | Tùy loại failure và khả năng tạo invalid rows |
| Trùng idempotency key | `SKIPPED_DUPLICATE` | Không | Không |

Biên đúng 0,5% vẫn được chấp nhận với rejection. Với batch Sensor chỉ có 60 dòng, một dòng lỗi tương đương khoảng 1,67%, do đó sẽ FAIL ngay.

## 9. Quality artifacts và báo cáo

### 9.1. JSON summary

Mỗi run đi qua được validation/gate tạo `summary.json` bằng atomic replace. Nội dung gồm:

- run ID, table, contract version;
- Bronze identity và checksum;
- thời gian bắt đầu/kết thúc;
- tổng/valid/rejected rows;
- error ratio và gate decision;
- hard failures, per-rule metrics;
- đường dẫn và checksum của quarantine nếu có.

### 9.2. Quarantine

Khi có dòng lỗi, pipeline ghi `invalid_rows.parquet` theo:

```text
data/quality_quarantine/<table>/<UTC-date>/<run_id>/invalid_rows.parquet
```

Mỗi dòng giữ `_failed_rules`, `_quality_run_id`, `_bronze_file`, `_bronze_sha256`, `_contract_version`. File được ghi atomic và checksum được đưa vào summary.

Với FAIL do tỷ lệ lỗi `>0,5%`, full quality summary và quarantine vẫn được sinh trước khi Silver write bị từ chối. Với lỗi xảy ra sớm hơn như mất sidecar, checksum mismatch hoặc transform exception, pipeline hiện sinh một JSON failure summary tối thiểu gồm stage, category, reason, Bronze path và timestamps. Row counts/checksum để `null` vì dữ liệu chưa được đọc hoặc xác minh; Data Docs và quarantine không được tạo khi chưa có canonical dataframe.

### 9.3. Data Docs hiện có

Lần integration acceptance đã sinh GX Data Docs thật trong thư mục runtime bị ignore.

Hiện workspace còn index, 6 trang expectation và 6 trang validation. Đây là runtime artifact trong `tmp/`, bị `.gitignore` loại khỏi commit. Đường dẫn mặc định `reports/data_docs/` chỉ được tạo khi chạy CLI với `--build-data-docs`; Data Docs không tự sinh nếu bỏ cờ này.

### 9.4. Ba biểu đồ

Đã triển khai PNG headless:

- Missingness matrix heatmap.
- Sensor physical bands kèm anomaly.
- Data lineage DAG.

Integration acceptance đã sinh figures cho 6 run; Sensor có đủ cả ba, năm bảng còn lại có missingness và lineage. Các PNG nằm trong thư mục runtime `tmp/` và không được track.

## 10. Delta Silver và idempotency

### 10.1. Silver write

Chỉ `PASS` và `PASS_WITH_REJECTIONS` được phép ghi. `FAIL`, duplicate và `dry-run` không tạo Delta commit.

Mỗi Silver row có thêm:

- `_bronze_file`
- `_bronze_sha256`
- `_quality_status`
- `_quality_run_id`
- `_contract_version`

URI MinIO:

```text
s3://smart-dairy-lakehouse/02_silver_curated/<table>
```

Partition:

- Sensor/MES: `line_id`, `ingestion_date`.
- Weather: `farm_id`, `ingestion_date`.
- GDT/FAO/OpenFDA: `ingestion_date`.

### 10.2. Idempotency đã nâng cấp

Key hiện tại:

```text
target_table + bronze_sha256 + contract_version
```

Hành vi đã test:

1. Cùng table + SHA + contract version → `SKIPPED_DUPLICATE`.
2. Cùng table + SHA + contract version mới → được process.
3. Khác table + cùng SHA → được process.
4. Delta table cũ thiếu `_contract_version` → báo lỗi rõ ràng, không tự mặc định legacy là `v1`.

Chưa thêm `pipeline_version` theo quyết định phạm vi. Chưa có contract `v2` thật hoặc migration utility; việc cho phép reprocess `v2` mới chỉ được chứng minh bằng test writer.

## 11. OpenLineage và Marquez

Mỗi run có OpenLineage events:

```text
Bronze dataset
  -> bronze_to_silver.<table>
  -> s3://smart-dairy-lakehouse/02_silver_curated/<table>
```

- Gửi `START`, sau đó `COMPLETE` hoặc `FAIL`.
- Nếu Marquez tạm unavailable, event được lưu JSON vào spool local.
- Có cơ chế replay theo thứ tự; chỉ xóa event sau khi emit thành công.
- Lineage transport failure không thay đổi kết quả quality của pipeline.

Integration acceptance ghi nhận 6/6 job ở trạng thái `COMPLETED`, mỗi job có một input Bronze và một output Silver.

Bằng chứng API thật cho Sensor đã được lưu tại:

```text
reports/lineage/sensor_telemetry_marquez.json
```

Artifact hiện có ba node và hai edge thực:

```text
file://bronze/...sensor_telemetry....parquet
  -> job:smart-dairy-factory:bronze_to_silver.sensor_telemetry
  -> s3://smart-dairy-lakehouse/02_silver_curated/sensor_telemetry
```

Latest run trong artifact là `COMPLETED`. Không lưu screenshot PNG vì JSON API ổn định và audit được hơn. Exporter kiểm tra job có input/output và xác minh trực tiếp mọi cạnh input dataset → job → output dataset trong `lineage.graph`; thiếu node/edge bắt buộc sẽ fail trước khi ghi artifact. Strict validator cũng đã chạy PASS trên chính JSON Marquez thật đang lưu trong repo.

## 12. CLI/orchestration

Entry point:

```powershell
python -m data_governance.run_governance
```

Đã hỗ trợ:

- Một hoặc nhiều bảng bằng `--table`/`--tables`.
- Lọc `--date-from`/`--date-to`.
- `--dry-run`.
- Bật GX/Data Docs bằng `--build-data-docs`.
- Bật/tắt lineage và đổi Marquez endpoint.
- Replay lineage spool.
- Đổi reports/quarantine/figures/GX roots.
- Thứ tự discovery ổn định theo table rồi path.
- Tiếp tục xử lý file còn lại khi một file thất bại.
- JSON summary cuối run.

Exit code:

- `0`: không có quality/infrastructure failure; gồm PASS, PASS_WITH_REJECTIONS và duplicate.
- `2`: có quality FAIL.
- `3`: lỗi hạ tầng/cấu hình hoặc lỗi ngoài dự kiến.

## 13. Nghiệm thu end-to-end

### 13.1. MinIO + Marquez integration acceptance

| Bảng | Kết quả đầu tiên | Dòng Silver | Delta version | Partition |
|---|---:|---:|---:|---|
| `sensor_telemetry` | PASS | 60 | 0 | `line_id`, `ingestion_date` |
| `mes_lims` | PASS | 1 | 0 | `line_id`, `ingestion_date` |
| `weather` | PASS | 24 | 0 | `farm_id`, `ingestion_date` |
| `market_prices` | PASS | 3 | 0 | `ingestion_date` |
| `market_indices` | PASS | 1 | 0 | `ingestion_date` |
| `food_recalls` | PASS | 5 | 0 | `ingestion_date` |

`read_silver_table()` đã đọc lại đủ 6 bảng, đúng schema/lineage columns và `_quality_status = PASS`. Lần chạy lại cùng input trả 6/6 `SKIPPED_DUPLICATE`, không tăng row count hoặc Delta version.

MinIO và Marquez containers hiện đã dừng bằng `docker compose down`; volume demo được giữ nguyên.

### 13.2. Downstream Silver acceptance bằng chDB

Đã thêm smoke/acceptance dùng `chDB 4.4.0` (`chdb-core 26.9.0`) trong Linux container để đọc trực tiếp Delta trên MinIO qua `deltaLake(...)`.

Kiểm tra thực tế trên `sensor_telemetry`:

- Đọc được schema 20 cột.
- Có đủ `line_id`, `uht_temp`, `_bronze_sha256`, `_contract_version`, `_quality_status`.
- Chạy thành công `count(*)` và `avg(uht_temp) GROUP BY line_id`.
- `LINE_UHT_1`: 60 dòng.
- `avg(uht_temp) = 139.85112838745118`.

Đây là bằng chứng handoff tối thiểu, chưa phải tích hợp toàn bộ OLAP downstream và chưa smoke-test 5 bảng Silver còn lại.

## 14. Kiểm thử và chất lượng code

Kết quả mới nhất sau bổ sung idempotency versioning, chDB acceptance, failure audit và strict Marquez evidence validation:

- Toàn repository: **264 passed, 1 skipped**.
- Riêng governance: **94 passed, 1 skipped**.
- Coverage package `data_governance`: khoảng **92%**, vượt ngưỡng 85%.
- `pip check`: PASS.
- Python compile: PASS.
- `git diff --check`: PASS.

Test live chDB được skip mặc định vì cần Linux/chDB/MinIO; test này đã được chạy riêng trong Docker và PASS. Test suite bao phủ loader/checksum, transformer, contracts, GX, Hampel/Z-score, gate boundaries, artifact/quarantine, Delta/idempotency, figures, lineage/spool, CLI và local E2E.

## 15. Đối chiếu yêu cầu DOCX

| Yêu cầu trong `my_task_uptodate.docx` | Trạng thái thực tế | Ghi chú |
|---|---|---|
| Transform 6 bảng Bronze → Silver | Hoàn thành | Hỗ trợ cả FAO long và wide |
| THI và heat stress | Hoàn thành | Ngưỡng `THI > 72` |
| Data Contracts | Hoàn thành | Operational validator dùng Polars |
| Great Expectations/Data Docs | Hoàn thành có điều kiện | GX dùng thật; Data Docs sinh khi bật flag |
| Gate 0,5% | Hoàn thành | Tính theo unique invalid rows |
| Quarantine | Hoàn thành | Có cho invalid rows sau validation |
| Hampel Filter | Hoàn thành | Kết hợp thêm Rolling Z-score, chỉ gắn cờ |
| Data lineage | Hoàn thành | OpenLineage + Marquez + spool/replay |
| Ba biểu đồ | Hoàn thành | Sensor có đủ ba trong nghiệm thu |
| Unit/integration tests | Hoàn thành | 260 pass, 1 opt-in live skip |
| Bàn giao Silver cho downstream | Hoàn thành mức smoke | chDB đọc/query Sensor thành công |
| USDA `market_reports` | Chưa làm | Cố ý ngoài MVP do schema không ổn định |
| Toàn bộ dữ liệu crawl live | Chưa chứng minh | Nghiệm thu dùng fixture cố định |

## 16. Khác biệt giữa kế hoạch ban đầu và implementation

### 16.1. Trạng thái branch/crawler trong kế hoạch ban đầu đã cũ

Kế hoạch ban đầu mô tả code crawler ở `hotfix/crawling`, `main` chưa merge và `origin/feat/crawling` có biến thể khác. Đây chỉ là snapshot tại thời điểm lập kế hoạch; implementation governance hiện đã đồng bộ các dependency cần thiết trên nhánh riêng.

### 16.2. FAO không chỉ có một shape

DOCX mô tả FAO crawler dạng wide. Bronze bàn giao qua Drive lại là dạng long. Implementation giữ cả hai adapter để không phá dữ liệu cũ và dữ liệu bàn giao mới.

### 16.3. Metadata crawler không đồng nhất

DOCX nói mọi dòng đều có `_base_api` và `_created_at`. Một số Bronze Drive cũ thiếu `_base_api`; schema/transformer coi metadata này là optional để đọc được cả hai thế hệ. `_created_at` vẫn là mốc ingestion chính cho các nguồn crawl cần nó.

### 16.4. Great Expectations không trực tiếp quyết định gate

DOCX có thể tạo cảm giác mọi contract nằm hoàn toàn trong GX. Thực tế Polars validator là operational gate; GX mirror một tập rule để checkpoint/Data Docs. Cách này giúp định danh dòng lỗi và quarantine chính xác hơn nhưng cần trình bày đúng vai trò.

### 16.5. “Sau mỗi lần pipeline chạy” cần hiểu có điều kiện

Data Docs chỉ sinh khi dùng `--build-data-docs`. Với FAIL do tỷ lệ lỗi sau validation, full summary/quarantine/GX vẫn được sinh trước Silver write. Với checksum/loader/transform failure xảy ra sớm, pipeline sinh minimal failure summary nhưng không sinh Data Docs/quarantine vì chưa có canonical dataframe để validate hoặc phân loại dòng.

### 16.6. Bộ dữ liệu nghiệm thu không phải toàn bộ upstream

Các con số 60/1/24/3/1/5 là fixture counts. Không thể dùng chúng để khẳng định đã xử lý toàn bộ dữ liệu 5 farm, toàn bộ lịch sử GDT/FAO hay toàn bộ OpenFDA.

### 16.7. Số test và coverage trong acceptance cũ đã lỗi thời

Acceptance ban đầu ghi 253 test, 83 governance tests và 93,87% coverage. Sau các bổ sung ngày 19/10, số hiện tại là 264 pass, 94 governance pass và khoảng 92% governance coverage. Coverage giảm nhẹ do thêm code mới nhưng vẫn vượt ngưỡng 85%.

## 17. Điểm chưa hoàn thành, chưa chắc chắn hoặc cần nói rõ

### 17.1. Chưa hoàn thành/chưa mở rộng

- Chưa có Silver contract/transformer cho USDA `market_reports`.
- Chưa chạy E2E với toàn bộ file Bronze live hoặc dữ liệu quy mô lớn.
- Chưa benchmark hiệu năng, concurrency hoặc nhiều file/batch lớn.
- Chưa acceptance bằng chDB cho 5 bảng ngoài Sensor.
- Chưa tích hợp full query/dashboard/OLAP; đó là phạm vi downstream.
- Chưa có contract `v2`, schema evolution plan hoặc migration cho Delta legacy.
- Chưa có `pipeline_version` trong idempotency key; đây là quyết định phạm vi hiện tại.
- Chưa dùng chữ ký số/KMS để xác thực Bronze; SHA-256 chỉ kiểm tra integrity.
- Chưa lưu screenshot Marquez; đã có JSON API evidence cho Sensor.
- Early failure chỉ có minimal audit summary; không có full rule metrics/Data Docs/quarantine vì canonical dataframe chưa tồn tại. Đây là hành vi có chủ đích, không phải mất audit trail.
- Chưa có rule thực thi riêng thuộc DAMA `Accuracy` vì thiếu nguồn ground truth.

### 17.2. Artifact có nhưng không phải tất cả đều được track

- Data Docs, GX state, quality summaries và figures integration hiện nằm trong `tmp/`, bị ignore.
- Quarantine không tồn tại trong clean integration run vì 6 fixture đều PASS; behavior đã được test bằng dữ liệu lỗi synthetic/local.
- Artifact Marquez JSON trong `reports/lineage/` hiện là untracked file dự kiến đưa vào repo.
- MinIO Silver nằm trong Docker volume, không phải file Git.

### 17.3. Trạng thái Git chưa chốt

- Branch local `feat/data-governance` đang **35 commit trước `origin/main`**.
- Branch chưa có upstream remote tương ứng và chưa push.
- Các bổ sung ngày 19/10 về contract-version idempotency, chDB acceptance, Marquez evidence và test được đưa vào nhánh governance trước khi bàn giao.

Do đó trạng thái kỹ thuật là “đã implement và test local”, nhưng trạng thái giao nhận Git chưa hoàn tất cho đến khi review, commit và push branch.

## 18. Output bàn giao hiện tại

### Code

- Pipeline: `src/data_governance/run_governance.py`.
- Contracts/rules: `src/data_governance/contracts.py`, `validator.py`.
- Transform: `src/data_governance/transformers.py`.
- Outlier: `src/data_governance/outliers.py`.
- Gate: `src/data_governance/gatekeeper.py`.
- Reports/quarantine: `src/data_governance/reporting.py`.
- Delta/idempotency: `src/data_governance/silver_writer.py`.
- GX/Data Docs: `src/data_governance/gx_context.py`.
- OpenLineage: `src/data_governance/lineage.py`.
- Marquez evidence export: `src/data_governance/marquez_evidence.py`.
- Downstream chDB acceptance: `src/data_governance/chdb_silver_acceptance.py`.

### Data và artifact

- Fixture manifest: `tests/fixtures/bronze/manifest.json`.
- GX Data Docs đã sinh trong thư mục runtime dưới `tmp/`.
- Quality/figures integration: các thư mục runtime dưới `tmp/`.
- Marquez evidence: `reports/lineage/sensor_telemetry_marquez.json`.
- Silver Delta: `s3://smart-dairy-lakehouse/02_silver_curated/<table>` trong MinIO volume demo.

### Tài liệu

- Hướng dẫn vận hành: `src/data_governance/README.md`.
- Bằng chứng nghiệm thu: `src/data_governance/ACCEPTANCE_REPORT.md`.
- Báo cáo tổng hợp này: `docs/data_governance_implementation_report.md`.

## 19. Trạng thái bàn giao đề xuất

Có thể báo cáo pipeline Data Governance là **đã hoàn thành phạm vi MVP và đã qua nghiệm thu kỹ thuật local/MinIO**, với điều kiện nói rõ:

1. E2E dùng Bronze fixtures cố định, không phải toàn bộ upstream production.
2. USDA chưa thuộc Silver MVP.
3. Downstream acceptance mới chứng minh trực tiếp cho Sensor bằng chDB.
4. Một số runtime artifacts không được commit vì nằm trong thư mục ignore.
5. Các bổ sung cuối ngày 19/10 cần được review, commit và push trước khi coi là bàn giao qua Git hoàn tất.
