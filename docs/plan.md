> Historical planning material (2026-11-17): this file is not the status source for the implemented storage demo. Use [tasks/plan.md](../tasks/plan.md) and [tasks/todo.md](../tasks/todo.md).

# LAKEHOUSE STORAGE — COMPLETE IMPLEMENTATION PLAN

**Version:** 2.0  
**Scope:** Lakehouse Storage Module  
**Architecture:** Bronze → Silver → Storage Maintenance  
**Supporting Components:** Guard Rails Engine, Mock Server, E2E Testing

---

# 0. PROJECT OVERVIEW

## 0.1. Objectives

Hoàn thiện Lakehouse Storage Module để đáp ứng các mục tiêu:

1. Cải thiện độ tin cậy của Bronze và Silver Storage.
2. Đảm bảo Data Integrity, Schema Enforcement và Idempotency.
3. Xây dựng Guard Rails để phát hiện vi phạm, ngăn dữ liệu không hợp lệ và tạo yêu cầu xử lý.
4. Tự động hóa các hoạt động bảo trì Lakehouse.
5. Xây dựng Mock Server để mô phỏng SCADA/MES, bao gồm cả dữ liệu lỗi.
6. Kiểm thử toàn bộ Storage Pipeline trên MinIO/Delta Lake.
7. Đảm bảo các thay đổi không phá vỡ Data Contract và interface của downstream modules.

## 0.2. Project Scope

### In Scope

- Bronze Writer.
- Silver Manager.
- Schema Validation.
- Checksum và Data Integrity.
- Idempotency và Ingestion Ledger.
- Quarantine và Rejection.
- Guard Rails Engine.
- Remediation Management.
- Time Travel và Retention.
- Automatic Maintenance.
- Maintenance Scheduler.
- Storage Testing Mock Server.
- Fault Injection.
- Storage Observability.
- Integration Testing.
- MinIO E2E Testing.
- Recovery và Performance Testing.

### Out of Scope

- Gold Business Analytics.
- Business KPI.
- Dashboard.
- Machine Learning.
- Predictive Maintenance Models.
- Decision Support.
- Điều khiển trực tiếp thiết bị SCADA.
- Thay đổi Data Contract của toàn hệ thống nếu chưa được các module phụ thuộc chấp thuận.

## 0.3. Architectural Constraints

### Constraint 1 — Data Contract Stability

Không tự ý thay đổi:
- Field names.
- Data types.
- Required/nullable properties.
- Field semantics.
- Business identifiers.
- Timestamp semantics.
- Schema compatibility.

Nếu cần thay đổi, phải có sự thống nhất với các module downstream và có kế hoạch versioning.

### Constraint 2 — Interface Compatibility

Các public functions, classes, parameters và return types hiện có phải được giữ tương thích, trừ khi có quyết định thay đổi được phê duyệt.

### Constraint 3 — Metadata Separation

Các metadata vận hành mới như:
- Ingestion status.
- Checksum status.
- Guard Rails violations.
- Remediation history.
- Maintenance execution history.
- Pipeline run metadata.

Phải được tách khỏi business data contract.

### Constraint 4 — Storage Access

Mock Server không được ghi trực tiếp vào Delta tables.

Luồng dữ liệu phải đi qua ingestion interface đã thống nhất.

### Constraint 5 — Test-Driven Verification

Mỗi thay đổi phải có:
- Input.
- Output.
- Acceptance Criteria.
- Automated Tests.
- Checkpoint verification.

Không đánh giá một task hoàn thành chỉ vì code đã được viết.

---

# 1. TARGET ARCHITECTURE

## 1.1. Storage Data Flow

SCADA/MES hoặc Mock Server
    |
    v
Ingestion Interface
    |
    v
Bronze Writer
    |
    +--> Raw Parquet
    +--> SHA-256
    +--> Batch Metadata
    +--> Manifest
    |
    v
Guard Rails Engine
    |
    +--> Contract Validation
    +--> Data Quality Rules
    +--> Integrity Checks
    +--> Operational Rules
    |
    +--> ACCEPT
    +--> REJECT
    +--> QUARANTINE
    +--> ALERT
    +--> MANUAL REVIEW
    |
    v
Silver Manager
    |
    +--> Schema Enforcement
    +--> Type Casting
    +--> Deduplication
    +--> Data Quality Validation
    |
    v
Silver Delta Tables

Guard Rails cũng kiểm soát các hoạt động Storage Maintenance:

Maintenance Scheduler
    |
    v
Maintenance Guard Rails
    |
    +--> Policy Validation
    +--> Retention Safety
    +--> Concurrent Operation Check
    |
    v
Compaction / Z-Order / Vacuum

## 1.2. Module Responsibilities

| Module | Responsibility |
|---|---|
| Bronze Writer | Raw data ingestion và integrity |
| Silver Manager | Schema enforcement và data transformation |
| Guard Rails Engine | Phát hiện vi phạm và quyết định xử lý |
| Remediation Manager | Quản lý yêu cầu khắc phục |
| Maintenance Manager | Compaction, Z-Order, Vacuum |
| Maintenance Scheduler | Lập lịch maintenance |
| Mock Server | Sinh dữ liệu và mô phỏng lỗi |
| Ingestion Interface | Cổng tiếp nhận dữ liệu |
| Storage Observability | Logs, metrics và execution history |

---

# PART 1 — IMPROVE EXISTING STORAGE MODULES

## Objective

Cải thiện các module hiện tại để Storage Core có tính toàn vẹn, nhất quán và khả năng phục hồi tốt hơn.

Part 1 là nền tảng cho tất cả các phần còn lại.

## 1.1. Contract and Interface Audit

### Tasks

1. Kiểm tra `schemas.py`.
2. Kiểm tra `config.py`.
3. Xác định Data Contract hiện tại.
4. Xác định các public functions/classes.
5. Kiểm tra module downstream sử dụng interface nào.
6. Xây dựng Contract Tests.
7. Xác định các file được phép refactor và các interface cần giữ nguyên.

### Input

- Existing source code.
- Data Contract.
- Existing tests.
- Downstream module dependencies.

### Output

- Contract inventory.
- Public interface inventory.
- Contract test suite.
- Compatibility requirements.
- File-level change scope.

### Acceptance Criteria

- Field names, types, nullability và semantics được ghi nhận.
- Contract tests PASS.
- Public interfaces được xác định.
- Không có breaking changes chưa được phê duyệt.
- Có tài liệu về những ràng buộc cần giữ.

---

## 1.2. Bronze Writer — Atomic Publishing and Data Integrity

### Tasks

A. Improve Write Process

1. Validate input trước khi ghi.
2. Ghi dữ liệu vào staging location.
3. Tính SHA-256 checksum.
4. Verify file sau khi ghi.
5. Publish batch sau khi các bước cần thiết hoàn tất.
6. Ghi manifest/commit metadata.
7. Phân biệt dữ liệu đang ghi và dữ liệu đã commit.

B. Failure Handling

Xử lý các trường hợp:

- Parquet write failure.
- Checksum generation failure.
- Process crash trong lúc ghi.
- File tồn tại nhưng thiếu checksum.
- Checksum mismatch.
- Manifest creation failure.
- Retry sau lỗi.
- Partial publishing.

C. Storage Compatibility

- Kiểm tra local filesystem.
- Kiểm tra MinIO/S3 behavior.
- Không giả định rename trên object storage có tính atomic như local filesystem.
- Sử dụng cơ chế publish phù hợp với backend.

### Input

- Input DataFrame hoặc dataset.
- Batch ID.
- Bronze configuration.
- Storage destination.
- Schema information.

### Output

- Valid Parquet file.
- SHA-256 checksum.
- Manifest/commit metadata.
- Batch status.
- Error information.

### Acceptance Criteria

- Không publish batch chưa hoàn tất.
- Có thể phát hiện file thiếu hoặc checksum mismatch.
- Có thể xác định batch đã commit hay chưa.
- Có thể phục hồi sau interrupted write.
- Không làm thay đổi public API của Bronze Writer.
- Không ghi nhận batch thành công khi chưa có đủ bằng chứng commit.

---

## 1.3. Idempotency and Ingestion Ledger

### Tasks

1. Xác định batch identity.
2. Xác định payload fingerprint.
3. Xây dựng ingestion ledger.
4. Theo dõi trạng thái xử lý.
5. Phát hiện duplicate batch.
6. Phát hiện cùng batch ID nhưng payload khác nhau.
7. Xử lý retry.
8. Xử lý concurrent ingestion.
9. Đảm bảo ledger và trạng thái storage có thể reconcile sau crash.

### Suggested Batch States

- RECEIVED
- VALIDATING
- BRONZE_COMMITTED
- SILVER_COMMITTED
- COMPLETED
- REJECTED
- FAILED
- RETRYABLE

### Input

- Batch ID.
- Source identifier.
- Payload fingerprint.
- Ingestion request.
- Existing ledger state.

### Output

- Ledger record.
- Processing status.
- Duplicate detection result.
- Conflict report.
- Retry decision.

### Acceptance Criteria

- Retry cùng batch không tạo duplicate ngoài ý muốn.
- Cùng batch ID nhưng payload khác được phát hiện.
- Batch COMPLETED không bị xử lý lại không kiểm soát.
- Ledger phản ánh đúng trạng thái thực tế.
- Có recovery khi process dừng giữa chừng.
- Concurrent ingestion tests PASS.

---

## 1.4. Silver Manager — Strict Schema Enforcement

### Tasks

A. Schema Validation

1. Validate required fields.
2. Validate field names.
3. Validate data types.
4. Validate nullability.
5. Validate timestamp.
6. Validate type casting.
7. Validate schema compatibility.

B. Data Quality

1. Detect duplicate records.
2. Detect missing values.
3. Detect invalid values.
4. Detect inconsistent records.
5. Apply configured data quality rules.

C. Error Handling

Phân biệt:

REJECT:
- Thiếu field bắt buộc.
- Sai cấu trúc nghiêm trọng.
- Không thể đáp ứng contract.
- Bronze integrity failure.

QUARANTINE:
- Giá trị bất thường.
- Sensor value vượt operating range.
- Record cần điều tra.
- Dữ liệu không đủ tin cậy để đưa vào Silver.

WARNING:
- Vi phạm không nghiêm trọng và được policy cho phép tiếp tục.

D. Schema Evolution

- Không tự động thay đổi contract.
- Unknown fields được xử lý theo schema evolution policy.
- Không tự động drop field hoặc cast sai kiểu để ép dữ liệu đi tiếp.

### Input

- Bronze dataset.
- Expected schema.
- Validation rules.
- Batch metadata.

### Output

- Silver Delta table.
- Validation report.
- Rejected records.
- Quarantined records.
- Data quality metrics.

### Acceptance Criteria

- Không silently bỏ qua lỗi casting.
- Không giữ nguyên kiểu dữ liệu sai.
- NOT NULL được kiểm tra.
- Silver output tuân thủ contract.
- Có thể truy vết số lượng accepted/rejected/quarantined.
- Existing downstream consumers vẫn đọc được dữ liệu.

---

## 1.5. Checksum Verification

### Tasks

1. Chuẩn hóa checksum verification result.
2. Tách các trạng thái checksum.
3. Xây dựng verification report.
4. Tích hợp verification vào ingestion.
5. Tích hợp verification vào recovery.

### Checksum Status

- VALID
- MISSING_FILE
- MISSING_CHECKSUM
- CHECKSUM_MISMATCH
- VERIFICATION_ERROR

### Input

- File path.
- Expected checksum.
- Actual file content.
- Batch metadata.

### Output

- Verification status.
- Verification report.
- Error details.

### Acceptance Criteria

- Phân biệt đầy đủ các checksum status.
- Không chỉ trả về boolean.
- Có test cho file hợp lệ, file bị sửa, file bị xóa và checksum sidecar bị thiếu.
- Checksum mismatch không được xem là dữ liệu hợp lệ.

---

## 1.6. Time Travel and Retention

### Tasks

1. Kiểm tra Delta version history.
2. Kiểm tra historical reads.
3. Kiểm tra timestamp-based reads nếu được hỗ trợ.
4. Kiểm tra retention policy.
5. Kiểm tra VACUUM safety.
6. Kiểm tra hành vi sau VACUUM.
7. Xác minh retention configuration 168 giờ.
8. Xác minh khả năng tương thích trên MinIO.

### Input

- Delta table.
- Version/timestamp.
- Retention configuration.
- Vacuum configuration.

### Output

- Historical table snapshot.
- Version history.
- Retention verification report.

### Acceptance Criteria

- Đọc được các version còn được bảo vệ.
- VACUUM không xóa dữ liệu trong retention window.
- Dry-run cho phép preview file sẽ bị xóa.
- Không chạy unsafe vacuum ngoài chính sách.
- Có test trên MinIO thực tế.

---

## 1.7. Guard Rails Engine

### Objective

Xây dựng cơ chế kiểm soát dữ liệu và Storage operations.

Guard Rails không chỉ phát hiện lỗi mà còn phải đưa ra quyết định xử lý và tạo yêu cầu khắc phục.

### A. Rule Engine

Phân loại rule:

1. Contract Rules.
2. Completeness Rules.
3. Validity Rules.
4. Consistency Rules.
5. Integrity Rules.
6. Operational Rules.

Examples:

| Rule | Description |
|---|---|
| REQUIRED_FIELD | Required field không được thiếu |
| VALID_DATA_TYPE | Field phải đúng kiểu dữ liệu |
| NULLABILITY | Field NOT NULL không được null |
| SENSOR_RANGE | Sensor value phải theo configured range |
| TIMESTAMP_VALIDITY | Timestamp phải hợp lệ |
| BATCH_CONSISTENCY | Batch metadata phải nhất quán |
| DUPLICATE_DETECTION | Phát hiện duplicate |
| CHECKSUM_VALIDITY | File checksum phải hợp lệ |
| BATCH_SIZE_LIMIT | Batch không vượt giới hạn cấu hình |
| INGESTION_RATE_LIMIT | Ingestion rate không vượt policy |

Các rules phải có thể cấu hình, không hardcode toàn bộ trong Silver Manager.

### B. Severity Classification

| Severity | Meaning | Example action |
|---|---|---|
| INFO | Thông tin | Log |
| WARNING | Cần chú ý | Alert, tiếp tục nếu policy cho phép |
| ERROR | Vi phạm nghiêm trọng | Reject hoặc quarantine |
| CRITICAL | Rủi ro integrity/safety | Block operation, ưu tiên xử lý |

Severity phải được xác định theo rule và policy, không mặc định mọi sensor anomaly là CRITICAL.

### C. Action Policy

Hỗ trợ các action:

- ACCEPT
- REJECT
- QUARANTINE
- RETRY
- ALERT
- MANUAL_REVIEW

Action decision phải được tách khỏi rule evaluation để có thể thay đổi policy mà không sửa toàn bộ rule engine.

### D. Violation Tracking

Mỗi violation cần có:

- Violation ID.
- Batch ID.
- Record identifier nếu có.
- Rule ID.
- Field.
- Observed value hoặc thông tin tham chiếu an toàn.
- Severity.
- Action.
- Reason.
- Required action.
- Status.
- Created timestamp.

### Input

- Dataset hoặc batch metadata.
- Data Contract.
- Rule configuration.
- Threshold configuration.
- Processing context.

### Output

- Guard Rails evaluation report.
- Violation list.
- Severity.
- Action decision.
- Required actions.
- Processing status.

### Acceptance Criteria

- Có thể thêm rule mà không phải sửa toàn bộ Silver Manager.
- Mỗi violation có rule ID và severity.
- Có action decision.
- Không silently drop hoặc sửa raw data.
- Có thể truy vết violation về batch.
- Có unit tests cho từng rule.
- Không thay đổi Data Contract.

---

## 1.8. Remediation Management

### Objective

Quản lý yêu cầu xử lý được tạo ra từ Guard Rails.

### Tasks

1. Tạo remediation request.
2. Gán request cho nhóm/module chịu trách nhiệm.
3. Theo dõi trạng thái.
4. Ghi lại nguyên nhân.
5. Ghi lại hành động khắc phục.
6. Revalidate sau khi xử lý.
7. Ghi audit history.
8. Hỗ trợ escalation nếu quá thời hạn xử lý.

### Suggested States

- PENDING
- IN_PROGRESS
- RESOLVED
- VERIFIED
- REJECTED
- ESCALATED

### Input

- Guard Rails violation.
- Required action.
- Batch ID.
- Remediation result.
- Revalidation result.

### Output

- Remediation request.
- Action history.
- Resolution status.
- Verification result.

### Acceptance Criteria

- Violation cần xử lý có thể tạo remediation request.
- Request liên kết với batch và rule gốc.
- Không chuyển sang VERIFIED nếu chưa revalidate.
- Không tự động đưa quarantine data trở lại Silver khi chưa được xác minh.
- Có audit history.
- Có test cho toàn bộ lifecycle.

---

## 1.9. Part 1 — Checkpoint

### Required Tests

- Contract tests.
- Bronze integrity tests.
- Atomic publishing tests.
- Idempotency tests.
- Concurrent ingestion tests.
- Silver schema tests.
- Checksum tests.
- Time Travel tests.
- Retention tests.
- Guard Rails unit tests.
- Action policy tests.
- Remediation lifecycle tests.

### Checkpoint Acceptance Criteria

- Existing tests PASS.
- Contract tests PASS.
- Bronze integrity PASS.
- Idempotency PASS.
- Silver schema enforcement PASS.
- Checksum verification PASS.
- Guard Rails evaluation PASS.
- Remediation lifecycle PASS.
- Không có breaking interface changes chưa phê duyệt.

### Deliverables

1. Stable Storage Core.
2. Guard Rails Engine.
3. Remediation Management.
4. Contract/Compatibility Test Suite.
5. Part 1 Test Report.

**Gate:** Không chuyển sang tích hợp Mock Server nếu Storage Core và Guard Rails chưa đạt checkpoint.

---

# PART 2 — AUTOMATIC MAINTENANCE

## Objective

Tự động quản lý hiệu năng và vòng đời dữ liệu Lakehouse, đồng thời bảo đảm mọi thao tác maintenance đều được Guard Rails kiểm soát.

## 2.1. Maintenance Policy

### Tasks

1. Xác định policy cho từng Delta table.
2. Xác định small-file threshold.
3. Xác định target file size.
4. Xác định compact conditions.
5. Xác định Z-Order conditions.
6. Xác định retention policy.
7. Xác định schedule.
8. Xác định operation cần manual approval.

### Input

- Delta table metadata.
- File count.
- Average file size.
- Table size.
- Query workload.
- Retention configuration.

### Output

- Maintenance policies.
- Threshold configuration.
- Scheduled operation configuration.

### Acceptance Criteria

- Policy có thể cấu hình theo table.
- Không hardcode chung cho tất cả table.
- Operation nguy hiểm có preview/dry-run.
- Không VACUUM dưới retention threshold.
- Có tài liệu giải thích điều kiện kích hoạt.

---

## 2.2. Small File Detection

### Tasks

1. Thống kê số lượng file.
2. Tính average file size.
3. Xác định small files.
4. Tính small-file ratio.
5. Đánh giá có cần compact hay không.
6. Ghi nhận metrics.

### Input

- Delta table metadata.
- File statistics.
- Target file size.
- Small-file threshold.

### Output

- Small-file report.
- Compact recommendation.
- Maintenance metrics.

### Acceptance Criteria

- Phát hiện được table có nhiều small files.
- Không compact khi chưa đạt threshold.
- Threshold có thể cấu hình.
- Có test với table nhỏ, table lớn và table đã compact.

---

## 2.3. Automatic Compaction

### Tasks

1. Tích hợp compact function hiện tại.
2. Xây dựng trigger condition.
3. Kiểm tra target file size.
4. Kiểm tra concurrent ingestion.
5. Kiểm tra table state.
6. Thực hiện compaction.
7. Ghi maintenance result.
8. Xử lý lỗi và retry.

### Input

- Delta table.
- Compact policy.
- File statistics.
- Table state.

### Output

- Compacted Delta table.
- New Delta version.
- Maintenance execution report.

### Acceptance Criteria

- Small-file count giảm sau compact.
- Không mất record.
- Dữ liệu trước và sau compact tương đương.
- Schema không thay đổi.
- Có execution logs.
- Không gây xung đột không kiểm soát với ingestion.

---

## 2.4. Z-Order Optimization

### Tasks

1. Xác định columns theo query workload.
2. Kiểm tra Delta version và backend support.
3. Xây dựng trigger condition.
4. Thực hiện Z-Order khi cần.
5. Ghi optimization result.
6. Đánh giá query performance trước/sau nếu có thể.

### Input

- Delta table.
- Query workload.
- Z-Order columns.
- Table statistics.

### Output

- Optimized table.
- Delta version nếu operation thành công.
- Optimization report.

### Acceptance Criteria

- Không chạy Z-Order vô điều kiện.
- Column selection có lý do.
- Dữ liệu không thay đổi.
- Có fallback nếu backend không hỗ trợ.
- Có test operation success/failure.

---

## 2.5. Automatic Vacuum and Retention

### Tasks

1. Xây dựng vacuum schedule.
2. Kiểm tra retention policy.
3. Thực hiện dry-run.
4. Đánh giá các file sẽ bị xóa.
5. Chỉ thực hiện khi đạt điều kiện an toàn.
6. Ghi audit log.
7. Kiểm tra Time Travel sau VACUUM.

### Input

- Delta table.
- Retention configuration.
- Table versions.
- Vacuum dry-run report.

### Output

- Vacuum execution report.
- Deleted-file statistics.
- Retention verification result.

### Acceptance Criteria

- Không xóa dữ liệu trong retention window.
- Không unsafe vacuum mặc định.
- Có audit log.
- Time Travel hoạt động đúng với các version còn được bảo vệ.
- Có dry-run và actual vacuum tests.

---

## 2.6. Maintenance Scheduler

### Tasks

1. Xây dựng scheduler.
2. Hỗ trợ lịch chạy định kỳ.
3. Hỗ trợ enable/disable.
4. Kiểm tra operation đang chạy.
5. Ngăn duplicate maintenance jobs.
6. Tránh xung đột với ingestion.
7. Lưu execution history.
8. Retry job thất bại có kiểm soát.
9. Hỗ trợ scheduler restart/recovery.

Giai đoạn đầu có thể dùng cron hoặc scheduler đơn giản trong Docker. Chưa cần Airflow.

### Input

- Maintenance policies.
- Schedule configuration.
- Table list.
- Runtime configuration.

### Output

- Scheduled jobs.
- Execution history.
- Job status.
- Error logs.

### Acceptance Criteria

- Job chạy đúng lịch.
- Không chạy trùng operation trên cùng table.
- Job lỗi được ghi nhận.
- Có thể retry.
- Có thể disable scheduler.
- Ingestion không bị gián đoạn ngoài chính sách.

---

## 2.7. Maintenance Guard Rails

### Tasks

1. Validate maintenance request.
2. Kiểm tra table state.
3. Kiểm tra concurrent operations.
4. Kiểm tra retention.
5. Kiểm tra operation scope.
6. Kiểm tra configured thresholds.
7. Block unsafe operations.
8. Tạo violation report.
9. Tạo remediation request khi cần.
10. Verify kết quả sau maintenance.

### Input

- Maintenance request.
- Delta table metadata.
- Retention policy.
- Current table state.
- Maintenance policy.

### Output

- Permission decision.
- Maintenance execution result.
- Violation report.
- Required remediation action.

### Acceptance Criteria

- Không VACUUM khi vi phạm retention.
- Không chạy operation trên table state không an toàn.
- Có lý do rõ ràng khi operation bị block.
- Không mất dữ liệu do maintenance.
- Có pre-operation và post-operation logs.
- Có Guard Rails tests.

---

## 2.8. Part 2 — Checkpoint

### Required Tests

- Maintenance policy tests.
- Small-file detection tests.
- Compaction tests.
- Z-Order tests hoặc documented backend limitation.
- Vacuum dry-run tests.
- Retention safety tests.
- Scheduler tests.
- Duplicate job prevention tests.
- Concurrent ingestion/maintenance tests.
- Maintenance Guard Rails tests.
- Scheduler restart/recovery tests.

### Checkpoint Acceptance Criteria

- Automatic compaction PASS.
- Z-Order PASS hoặc có fallback được kiểm chứng.
- Vacuum safety PASS.
- Retention PASS.
- Scheduler PASS.
- Maintenance Guard Rails PASS.
- Không mất dữ liệu.
- Có execution history.

### Deliverables

1. Maintenance Policy Manager.
2. Automatic Maintenance Manager.
3. Maintenance Scheduler.
4. Maintenance Guard Rails.
5. Maintenance Execution Report.

---

# PART 3 — STORAGE TESTING MOCK SERVER

## Objective

Xây dựng một nguồn dữ liệu mô phỏng SCADA/MES để kiểm thử Storage trong cả điều kiện bình thường và điều kiện lỗi.

Mock Server phải tạo được dữ liệu hợp lệ, dữ liệu vi phạm contract, dữ liệu bất thường và các lỗi vận hành.

Mục tiêu đặc biệt: **Mock Server phải cung cấp dữ liệu lỗi có chủ đích để Guard Rails phát hiện và đưa ra yêu cầu xử lý.**

## 3.1. Mock Server Architecture

### Tasks

1. Xây dựng FastAPI application.
2. Tách Generator module.
3. Tách Scenario module.
4. Tách Fault Injection module.
5. Xây dựng ingestion client.
6. Tích hợp contract hiện tại.
7. Containerize bằng Docker.
8. Tách API layer khỏi generator logic.

### Input

- Existing Data Contract.
- Sensor/MES generators.
- Ingestion interface.
- Mock configuration.

### Output

- FastAPI application.
- Generator module.
- Scenario module.
- Fault Injection module.
- Ingestion client.
- Docker configuration.

### Acceptance Criteria

- Mock Server chạy độc lập.
- Không thay đổi Data Contract.
- Không ghi trực tiếp vào Delta.
- Có thể thay generator mà không sửa Storage Core.
- Có thể chạy local và Docker.

---

## 3.2. Schema-Driven Data Generator

### Tasks

1. Sinh dữ liệu theo Data Contract.
2. Sử dụng sensor distributions.
3. Hỗ trợ seed.
4. Hỗ trợ batch ID.
5. Hỗ trợ timestamp.
6. Hỗ trợ correlation giữa sensors.
7. Sinh dữ liệu SCADA và MES.
8. Liên kết dữ liệu theo batch/business identifiers.
9. Hỗ trợ dataset size tùy chỉnh.
10. Đảm bảo reproducibility.

### Input

- Data schema.
- Sensor distributions.
- MES configuration.
- Seed.
- Batch configuration.
- Dataset size.

### Output

- SCADA records.
- MES records.
- Batch metadata.
- Reproducible dataset.

### Acceptance Criteria

- Field names và types khớp contract.
- Cùng seed/config tạo dữ liệu tương đương.
- Timestamp có thể kiểm soát.
- Có correlation giữa các sensors.
- Có thể tạo dataset với kích thước tùy chỉnh.
- Có tests kiểm tra generator.

---

## 3.3. Fault Injection and Invalid Data Scenarios

### Objective

Chủ động tạo dữ liệu lỗi để kiểm tra Guard Rails.

Các scenario phải có expected results rõ ràng. Không chỉ tạo dữ liệu sai mà không xác định được lỗi đó cần được xử lý như thế nào.

### Group A — Contract Violations

| Scenario | Injected Fault | Expected Guard Rails Result |
|---|---|---|
| Missing field | Thiếu required field | REJECT |
| Wrong data type | String thay numeric | REJECT |
| Null required field | Field NOT NULL bị null | REJECT |
| Unknown field | Field ngoài contract | Theo schema evolution policy |
| Invalid timestamp | Timestamp sai format | REJECT |

### Group B — Data Quality Violations

| Scenario | Injected Fault | Expected Guard Rails Result |
|---|---|---|
| Missing readings | Thiếu một số sensor readings | WARNING/QUARANTINE |
| Out of range | Sensor vượt operating range | QUARANTINE/REVIEW |
| Duplicate record | Record bị gửi trùng | Deduplicate hoặc xử lý theo policy |
| Sudden spike | Sensor thay đổi đột ngột | WARNING/QUARANTINE |
| Inconsistent batch | Batch metadata không khớp | REJECT/QUARANTINE |
| Invalid value | Giá trị không hợp lệ theo rule | REJECT/QUARANTINE |

### Group C — Operational Faults

| Scenario | Injected Fault | Expected Guard Rails Result |
|---|---|---|
| High volume | Batch vượt configured limit | REJECT/THROTTLE |
| Burst ingestion | Nhiều batch đồng thời | Controlled ingestion |
| Late arrival | Dữ liệu đến muộn | Apply late-arrival policy |
| Sensor failure | Sensor ngừng gửi | ALERT |
| Corrupted file | File bị sửa sau khi tạo | Integrity failure |
| Partial failure | Process dừng giữa lúc ghi | Recovery required |
| MinIO unavailable | Object storage không truy cập được | RETRYABLE/ALERT |
| Duplicate batch | Gửi lại batch | Idempotency handling |

### Input

- Scenario name.
- Data Contract.
- Guard Rails configuration.
- Seed.
- Dataset size.
- Fault parameters.

### Output

- Valid dataset.
- Invalid dataset.
- Scenario metadata.
- Expected violations.
- Expected severity.
- Expected actions.
- Expected remediation requests.

### Acceptance Criteria

- Mỗi scenario tái lập được.
- Mỗi scenario có expected result.
- Có thể chạy riêng lẻ hoặc thành suite.
- Không vô tình thay đổi Data Contract.
- Dữ liệu lỗi được tạo đúng mục đích của từng rule.
- Có tests kiểm tra fault injection.

Lưu ý:
- Test checksum mismatch phải tạo file hợp lệ rồi thay đổi nội dung sau khi tính checksum.
- Test partial failure phải thực sự mô phỏng interruption.
- Test duplicate phải gửi lại cùng batch identity.
- Test concurrent ingestion phải tạo request đồng thời.

---

## 3.4. FastAPI Endpoints

### Proposed Endpoints

| Method | Endpoint | Responsibility |
|---|---|---|
| GET | /health | Health check |
| GET | /scenarios | List scenarios |
| POST | /generate | Generate dataset |
| POST | /ingest | Send data through ingestion interface |
| POST | /replay | Replay existing batch |
| GET | /batches | List batches |
| GET | /batches/{batch_id} | Get batch status |
| POST | /scenarios/{scenario_name}/run | Run scenario |

### Input

- HTTP request.
- Scenario configuration.
- Batch ID.
- Seed.
- Dataset size.
- Ingestion parameters.

### Output

- Generated batch.
- Ingestion result.
- Batch status.
- Scenario execution result.
- Error response.

### Acceptance Criteria

- API trả HTTP status phù hợp.
- Có request validation.
- Có error handling.
- Có batch status tracking.
- Duplicate request được xử lý theo idempotency policy.
- Không bypass ingestion interface.

---

## 3.5. Replay and Batch Management

### Tasks

1. Lưu batch metadata.
2. Replay batch cũ.
3. Kiểm soát replay theo batch ID.
4. Replay để kiểm tra idempotency.
5. Hỗ trợ nhiều batch liên tiếp.
6. Lưu scenario configuration.
7. Phân biệt original request và replay request.

### Input

- Batch ID.
- Scenario metadata.
- Replay configuration.

### Output

- Replayed batch.
- Replay execution result.
- Batch status.
- Replay history.

### Acceptance Criteria

- Replay tạo ra dữ liệu có thể tái lập.
- Có thể kiểm tra duplicate handling.
- Có thể phân biệt original và replay.
- Không tạo duplicate ngoài chính sách.

---

## 3.6. Guard Rails Integration

### Objective

Kiểm tra Mock Server và Guard Rails hoạt động cùng nhau.

### Tasks

1. Gửi valid dataset qua ingestion.
2. Gửi invalid dataset qua ingestion.
3. Thu thập Guard Rails evaluation.
4. Kiểm tra violation records.
5. Kiểm tra severity.
6. Kiểm tra action decision.
7. Kiểm tra remediation request.
8. Kiểm tra dữ liệu được lưu vào Bronze/Silver/quarantine.
9. Replay scenario để kiểm tra idempotency.
10. Revalidate dữ liệu sau remediation.

### Input

- Mock Server scenarios.
- Guard Rails configuration.
- Storage pipeline.
- Expected outcomes.

### Output

- Scenario execution report.
- Guard Rails evaluation report.
- Violation records.
- Remediation records.
- Actual versus expected results.

### Acceptance Criteria

- Mỗi scenario có actual result khớp expected result.
- Dữ liệu lỗi không vào Silver khi policy yêu cầu chặn.
- Dữ liệu hợp lệ không bị reject nhầm.
- Violation có đầy đủ thông tin truy vết.
- Remediation request được tạo đúng.
- Có revalidation tests.

---

## 3.7. Docker and Local Development

### Tasks

1. Dockerfile.
2. Docker Compose service.
3. Environment variables.
4. Secrets configuration.
5. Health check.
6. README.
7. Local execution.
8. Integration với MinIO/Storage environment.

### Input

- FastAPI application.
- Generator.
- Configuration.
- Docker environment.

### Output

- Docker image.
- Compose service.
- Environment template.
- Setup documentation.

### Acceptance Criteria

- Chạy được local.
- Chạy được Docker Compose.
- Có health check.
- Không hardcode credentials.
- Có hướng dẫn chạy scenarios.
- Có thể kết nối Storage environment.

---

## 3.8. Part 3 — Checkpoint

### Required Tests

- Generator tests.
- Contract compatibility tests.
- Seed reproducibility tests.
- API tests.
- Normal scenario tests.
- Contract violation tests.
- Data quality scenario tests.
- Operational fault tests.
- Replay tests.
- Guard Rails integration tests.
- Docker execution tests.

### Checkpoint Acceptance Criteria

- Generator PASS.
- API PASS.
- Contract compatibility PASS.
- Fault injection PASS.
- Expected Guard Rails action = Actual action.
- Remediation request creation PASS.
- Replay/idempotency PASS.
- Docker execution PASS.
- Mock Server không bypass ingestion.

### Deliverables

1. Storage Testing Mock Server.
2. Schema-Driven Generator.
3. Fault Injection Framework.
4. Scenario Test Suite.
5. Guard Rails Integration Tests.
6. Docker configuration.
7. Part 3 Test Report.

---

# PART 4 — END-TO-END INTEGRATION & VALIDATION

## Objective

Kiểm chứng toàn bộ Storage pipeline từ dữ liệu đầu vào đến Silver, bao gồm Guard Rails, Automatic Maintenance, MinIO và Recovery.

Part 4 là bước chứng minh hệ thống thực sự hoạt động, không chỉ chứng minh từng module riêng lẻ.

## 4.1. Storage Integration

### Tasks

1. Kết nối Mock Server với ingestion interface.
2. Kết nối ingestion với Bronze.
3. Kết nối Bronze với Guard Rails.
4. Kết nối Guard Rails với Silver.
5. Kết nối Maintenance Manager.
6. Kiểm tra batch status xuyên suốt.
7. Kiểm tra metadata và lineage.
8. Kiểm tra error propagation.

### Input

- Mock Server.
- Ingestion interface.
- Bronze Writer.
- Guard Rails Engine.
- Silver Manager.
- Maintenance Manager.

### Output

- Integrated Storage pipeline.
- Batch execution report.
- Pipeline status.
- Integration logs.

### Acceptance Criteria

- Dữ liệu đi qua đúng module.
- Không bypass ingestion.
- Contract không thay đổi.
- Batch ID truy vết được xuyên suốt.
- Lỗi tại từng bước được phản ánh đúng.
- Guard Rails action được thực thi đúng.

---

## 4.2. MinIO E2E Testing

### Tasks

1. Write Bronze vào MinIO.
2. Read lại Parquet.
3. Verify checksum.
4. Verify manifest.
5. Write Silver Delta.
6. Read Silver Delta.
7. Kiểm tra Delta version history.
8. Kiểm tra retry.
9. Kiểm tra concurrent writes.
10. Kiểm tra crash recovery.
11. Kiểm tra compaction.
12. Kiểm tra vacuum.
13. Kiểm tra time travel.

### Input

- MinIO service.
- Delta configuration.
- Mock Server datasets.
- Storage modules.
- Guard Rails configuration.

### Output

- MinIO E2E results.
- Integration logs.
- Storage verification report.

### Acceptance Criteria

- Read/write hoạt động trên MinIO thực tế.
- Checksum verification PASS.
- Delta commit/read PASS.
- Retry không tạo duplicate.
- Concurrent writes được kiểm chứng.
- Crash recovery có kết quả rõ ràng.
- Maintenance operations hoạt động hoặc có documented limitations.
- Không coi local filesystem test là bằng chứng MinIO compatibility.

---

## 4.3. Failure and Recovery Testing

### Tasks

Mô phỏng:

1. Process dừng trong lúc ghi Bronze.
2. Checksum sidecar bị thiếu.
3. Checksum mismatch.
4. Manifest creation failure.
5. Silver transformation failure.
6. Batch được gửi lại.
7. Hai batch ghi đồng thời.
8. MinIO tạm thời không truy cập được.
9. Maintenance thất bại.
10. Scheduler restart.
11. Guard Rails engine gặp lỗi.
12. Remediation chưa hoàn thành.
13. Replay sau khi xử lý lỗi.

### Input

- Failure scenarios.
- Test datasets.
- Storage configuration.
- Guard Rails configuration.

### Output

- Recovery report.
- Error logs.
- Retry results.
- Data integrity verification.
- Batch reconciliation report.

### Acceptance Criteria

- Không silently mất dữ liệu.
- Không publish dữ liệu chưa hoàn tất.
- Không tạo duplicate ngoài chính sách.
- Có thể xác định batch status sau recovery.
- Các lỗi không làm hỏng dữ liệu đã commit.
- Có thể xác định failure point.
- Có recovery tests tự động.

---

## 4.4. Guard Rails E2E Verification

### Tasks

Kiểm thử các luồng:

| Test Case | Input | Expected Result |
|---|---|---|
| Valid batch | Dữ liệu bình thường | ACCEPT → Silver |
| Invalid schema | Sai contract | REJECT |
| Missing required field | Thiếu field | REJECT |
| Sensor anomaly | Giá trị bất thường | QUARANTINE/REVIEW |
| Duplicate batch | Gửi lại batch | Idempotency handling |
| Corrupted file | Checksum sai | Integrity failure |
| Sensor unavailable | Thiếu readings kéo dài | ALERT |
| High-volume batch | Vượt giới hạn | Policy enforcement |
| Unsafe vacuum | Vi phạm retention | BLOCK |
| Remediation | Dữ liệu được xử lý | REVALIDATE |

### Input

- Mock Server scenarios.
- Guard Rails configuration.
- Storage modules.
- Maintenance Manager.
- Expected outcomes.

### Output

- Guard Rails E2E report.
- Violation report.
- Remediation audit.
- Recovery report.

### Acceptance Criteria

- Có positive cases và negative cases.
- Không false accept các test bắt buộc phải reject.
- Không false reject dữ liệu hợp lệ.
- Các yêu cầu xử lý có status và audit history.
- Remediation phải đi kèm revalidation.
- Maintenance Guard Rails được kiểm chứng trên MinIO/Delta.

---

## 4.5. Performance and Load Testing

### Tasks

1. Kiểm tra batch nhỏ.
2. Kiểm tra batch lớn.
3. Kiểm tra nhiều batch liên tiếp.
4. Kiểm tra burst ingestion.
5. Đo Bronze write duration.
6. Đo Silver transformation duration.
7. Đo throughput.
8. Theo dõi file count.
9. Theo dõi small-file ratio.
10. So sánh trước/sau compaction.
11. Kiểm tra overhead của Guard Rails.

### Input

- Configurable dataset size.
- Ingestion workload.
- Storage configuration.
- Guard Rails rules.

### Output

- Performance report.
- Throughput.
- Latency.
- Storage metrics.
- Guard Rails evaluation duration.

### Acceptance Criteria

- Có baseline.
- Có số liệu thực tế.
- Không mất dữ liệu trong load test.
- Không duplicate ngoài chính sách.
- Không có integrity errors.
- Guard Rails overhead được đo.
- Các giới hạn hiệu năng được ghi rõ.

---

## 4.6. Storage Observability

### Tasks

A. Ingestion Metrics

- Records received.
- Records accepted.
- Records rejected.
- Records quarantined.
- Ingestion duration.
- Retry count.
- Failed batch count.

B. Guard Rails Metrics

- Violations by rule.
- Violations by severity.
- Accepted/rejected/quarantined batches.
- Pending remediation count.
- Resolution time.
- Revalidation results.

C. Maintenance Metrics

- Compaction count.
- Vacuum count.
- Small-file ratio.
- File count.
- Maintenance duration.
- Failed jobs.
- Scheduler status.

D. Storage Metrics

- Bronze size.
- Silver size.
- Delta versions.
- File count.
- Storage availability.

### Input

- Pipeline logs.
- Guard Rails reports.
- Maintenance history.
- Storage metadata.

### Output

- Structured logs.
- Metrics.
- Operational reports.
- Monitoring dashboard hoặc report đơn giản.

### Acceptance Criteria

- Có thể truy vết batch.
- Có thể truy vết violation.
- Có thể xác định maintenance history.
- Có thể thống kê lỗi theo severity.
- Có thể xác định các job thất bại.
- Không cần triển khai monitoring stack phức tạp nếu chưa cần thiết.

---

## 4.7. Documentation and Final Handover

### Tasks

1. Cập nhật architecture documentation.
2. Tài liệu Bronze/Silver APIs.
3. Tài liệu Guard Rails rules.
4. Tài liệu remediation lifecycle.
5. Tài liệu Mock Server APIs.
6. Hướng dẫn tạo scenarios.
7. Hướng dẫn chạy maintenance.
8. Hướng dẫn cấu hình MinIO.
9. Hướng dẫn chạy E2E tests.
10. Ghi nhận known limitations.
11. Ghi nhận các interface cần giữ tương thích.

### Input

- Final source code.
- Test reports.
- Docker configuration.
- Architecture decisions.

### Output

- README.
- Architecture documentation.
- API documentation.
- Guard Rails documentation.
- Test report.
- Known limitations.

### Acceptance Criteria

- Thành viên khác có thể setup và chạy project theo README.
- Có thể tái lập E2E tests.
- Có thể hiểu cách xử lý lỗi.
- Có thể hiểu remediation lifecycle.
- Documentation khớp implementation thực tế.

---

## 4.8. Part 4 — Final Checkpoint

### Required Tests

- Storage integration tests.
- MinIO E2E tests.
- Concurrent ingestion tests.
- Crash recovery tests.
- Guard Rails E2E tests.
- Remediation tests.
- Maintenance integration tests.
- Performance tests.
- Observability tests.

### Checkpoint Acceptance Criteria

- Contract compatibility: PASS.
- Bronze integrity: PASS.
- Silver validation: PASS.
- Idempotency: PASS.
- Guard Rails: PASS.
- Remediation: PASS.
- MinIO E2E: PASS.
- Concurrent ingestion: PASS.
- Retry/recovery: PASS.
- Time Travel/retention: PASS.
- Automatic Maintenance: PASS.
- Mock Server: PASS.
- E2E tests: PASS.
- Documentation: COMPLETE.

### Deliverables

1. Integrated Lakehouse Storage Module.
2. Mock Server.
3. Guard Rails Engine.
4. Automatic Maintenance.
5. E2E Test Suite.
6. Final Verification Report.
7. Technical Documentation.

---

# 5. IMPLEMENTATION ORDER

Thứ tự implementation khuyến nghị:

## Phase 1 — Storage Core

Thực hiện Part 1.

Ưu tiên:
- Contract audit.
- Bronze integrity.
- Idempotency.
- Silver validation.
- Guard Rails.
- Remediation.

Checkpoint 1 phải PASS trước khi tích hợp các phần tiếp theo.

## Phase 2 — Mock Server

Thực hiện Part 3.

Lý do:
- Cần nguồn dữ liệu có kiểm soát.
- Cần tạo được dữ liệu lỗi.
- Cần kiểm tra Guard Rails bằng actual inputs.
- Cần có dataset tái lập cho các test tiếp theo.

Checkpoint 3 phải PASS trước khi dùng Mock Server làm nguồn kiểm thử maintenance và E2E.

## Phase 3 — Automatic Maintenance

Thực hiện Part 2.

Lý do:
- Có dữ liệu từ Mock Server để tạo small files.
- Có thể kiểm thử compaction trên workload thực tế.
- Có thể kiểm thử maintenance Guard Rails.
- Có thể kiểm thử scheduler với ingestion hoạt động.

Checkpoint 2 phải PASS.

## Phase 4 — E2E Integration

Thực hiện Part 4.

Kiểm thử toàn bộ Storage Pipeline và tạo final report.

Checkpoint 4 là checkpoint nghiệm thu toàn dự án.

---

# 6. GLOBAL CHANGE CONTROL

Mọi thay đổi cần có:

1. Task ID.
2. Objective.
3. Affected files.
4. Input.
5. Output.
6. Acceptance Criteria.
7. Unit tests.
8. Integration tests nếu có.
9. Contract compatibility verification.
10. Rollback strategy nếu có tác động tới dữ liệu hoặc configuration.

## Rules for Code Changes

Không merge nếu:

- Contract tests thất bại.
- Existing tests bị regression.
- Không có tests cho logic mới.
- Có breaking interface changes chưa được chấp thuận.
- Có destructive storage operations chưa được kiểm chứng.
- Guard Rails action không được xác định rõ.
- Dữ liệu lỗi có thể đi vào Silver ngoài policy.
- Remediation được đánh dấu VERIFIED mà không có revalidation.

## Definition of Done

Một task chỉ được đánh dấu COMPLETE khi:

- Code đã triển khai.
- Input/Output hoạt động đúng.
- Acceptance Criteria được kiểm chứng.
- Automated tests PASS.
- Không phá vỡ contract.
- Không phá vỡ downstream interface.
- Có documentation nếu cần.

---

# 7. FINAL PROJECT DEFINITION OF DONE

## A. Storage Core

- Bronze integrity.
- Checksum verification.
- Atomic publishing.
- Idempotency.
- Ingestion ledger.
- Silver schema enforcement.
- Data quality validation.
- Time Travel.
- Retention.

## B. Guard Rails

- Rule engine.
- Severity classification.
- Action policy.
- Violation tracking.
- Reject/quarantine handling.
- Remediation management.
- Revalidation.
- Audit history.
- Maintenance safety rules.

## C. Mock Server

- Schema-driven generator.
- Seed reproducibility.
- SCADA/MES data.
- Normal scenarios.
- Invalid schema scenarios.
- Data quality fault scenarios.
- Operational fault scenarios.
- Replay.
- Batch management.
- API tests.

## D. Automatic Maintenance

- Maintenance policy.
- Small-file detection.
- Automatic compaction.
- Z-Order policy.
- Vacuum safety.
- Retention enforcement.
- Scheduler.
- Execution history.
- Maintenance Guard Rails.

## E. Integration

- MinIO E2E.
- Concurrent ingestion.
- Crash recovery.
- Guard Rails E2E.
- Remediation verification.
- Performance tests.
- Observability.
- Documentation.

---

# 8. FINAL ARCHITECTURAL PRINCIPLE

Lakehouse Storage không chỉ chịu trách nhiệm lưu trữ dữ liệu.

Nó phải bảo đảm:

1. Dữ liệu được tiếp nhận đúng.
2. Dữ liệu được lưu trữ an toàn.
3. Dữ liệu được kiểm tra theo contract.
4. Vi phạm được phát hiện và phân loại.
5. Yêu cầu xử lý được tạo và theo dõi.
6. Dữ liệu chỉ được chuyển tiếp khi đáp ứng policy.
7. Storage được bảo trì an toàn.
8. Mọi hoạt động đều có thể truy vết và kiểm chứng.

**Mock Server cung cấp dữ liệu bình thường và dữ liệu lỗi. Guard Rails chứng minh khả năng phát hiện và yêu cầu xử lý. Storage Core thực thi các quyết định. E2E Tests chứng minh toàn bộ hệ thống hoạt động đúng.**
