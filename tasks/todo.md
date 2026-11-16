# Task list: Gói Lakehouse Storage tối thiểu

Cập nhật: 2026-11-17. Kế hoạch: [plan.md](plan.md). Branch: `feature/lakehouse-reliability-automation`.

Gói đã được người dùng chọn: mock server + ingestion worker tối thiểu + scheduler + benchmark/biểu đồ + hai sửa lỗi truy vết. Danh sách này thay thế 27 task cũ chưa triển khai. T01–T18 dưới đây được đánh số lại; không kế thừa trạng thái hoàn thành.

## Quy ước

- Mã mới trong lakehouse_storage; giữ nguyên mã/tests downstream, Silver/Bronze writer và schema.
- Mỗi task có 3 acceptance criteria, dependency rõ và tối đa 5 file chính. S = 1–2 file, M = 3–5 file; tách task nếu triển khai vượt một phiên tập trung.
- Các file/test/CLI mới là mục tiêu sẽ tạo, chưa phải khả năng hiện có. Lệnh pytest dùng .venv hiện tại; môi trường dịch vụ/container có thể dùng interpreter tương ứng.
- Chỉ đánh dấu hoàn thành sau evidence. Ghi pass/fail/skip và lý do; không sửa expected tests downstream để che regression.
- Skill yêu cầu review kế hoạch trước implementation. Người dùng đã đồng ý phạm vi; lượt này chỉ cập nhật tài liệu kế hoạch.

## Tiến độ tổng

- [x] T01: Baseline tương thích và môi trường dịch vụ
- [x] T02: Sửa lookup version theo thời điểm
- [x] T03: Truy vết mọi nguồn Bronze của batch
- [x] T04: Mock run và kho payload tái lập
- [ ] T05: Mock HTTP API có entry point
- [ ] T06: Mock scenarios và replay
- [ ] T07: Client và checkpoint Bronze tối thiểu
- [ ] T08: Adapter gọi P3 hiện có
- [ ] T09: Worker loop, retry và resume có giới hạn
- [ ] T10: Maintenance policy và runner one-shot
- [ ] T11: Scheduler cùng process với runner
- [ ] T12: Lịch sử maintenance và kiểm chứng logical rows
- [ ] T13: Docker Compose cho demo hai pha
- [ ] T14: MinIO E2E cho demo và failure cases
- [ ] T15: Benchmark harness có đối chiếu kết quả
- [ ] T16: Chạy thực nghiệm và ghi kết quả thật
- [ ] T17: Biểu đồ và diễn giải thực nghiệm
- [ ] T18: Demo, tài liệu và nghiệm thu tương thích

## Checklist chi tiết

## Giai đoạn A: Baseline và sửa truy vết

### T01: Baseline tương thích và môi trường dịch vụ

**Mô tả:** Ghi API đang được dùng, chạy baseline và chọn bộ dependencies riêng cho dịch vụ mới.

**Tiêu chí nghiệm thu:**

- [x] Baseline tests storage/downstream ghi pass/fail/skip và phiên bản thực; không sửa tests downstream.
- [x] FastAPI/Uvicorn/APScheduler 3.x và dependencies P3 cài được cùng constraints trong môi trường mới; pip check đạt.
- [x] Core import/signatures/schema/partition giữ nguyên; test smoke chạy được khi không có optional service packages.

**Kiểm chứng:**

- [x] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_storage_compatibility.py tests/test_schemas.py tests/test_bronze_writer.py tests/test_silver_manager.py tests/test_time_travel.py tests/test_maintenance.py tests/test_integration.py -q`
- [x] Chạy thêm tests/test_data_governance_*.py, tests/test_olap_build_marts.py và tests/test_manufacturing_spc.py nguyên trạng. Trong venv riêng cài requirements + service requirements + constraints; pip check. Không sửa môi trường chung khi chưa cần.
- [x] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** Không có.

**File dự kiến:**

- `tests/test_storage_compatibility.py`
- `infra/requirements-storage.txt`
- `infra/constraints-storage.txt`
- `docs/storage_baseline.md`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** `docs/storage_baseline.md`: core 104 pass; service suite 311 pass/1 skip; isolated constraints + pip check + compatibility smoke.

### T02: Sửa lookup version theo thời điểm

**Mô tả:** Sửa get_version_at_date để trả snapshot mới nhất trước/đúng mốc hỏi và giữ nghĩa timezone.

**Tiêu chí nghiệm thu:**

- [x] Với >=3 commit, thời điểm giữa/sau commits trả version đúng; trước commit đầu trả None.
- [x] Mốc có offset được quy đổi đúng; timestamp naive có quy ước UTC được ghi rõ; xử lý commits cùng timestamp theo version.
- [x] Chữ ký public và tests time-travel cũ không đổi; regression case thất bại trên code cũ và pass sau sửa.

**Kiểm chứng:**

- [x] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_time_travel.py tests/test_storage_compatibility.py -q`
- [x] So expected với Delta history fixture có timestamp xác định và một local Delta smoke; không dựa vào sleep để ép khoảng thời gian.
- [x] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T01

**File dự kiến:**

- `src/lakehouse_storage/time_travel.py`
- `tests/test_time_travel.py`

**Phạm vi:** S (2 file chính).

**Bằng chứng hoàn thành:** RED 5 failures; GREEN 22 pass (time_travel + compatibility), including local Delta, timezone and timestamp ties.

### T03: Truy vết mọi nguồn Bronze của batch

**Mô tả:** Bổ sung lineage_sources và kiểm tra integrity trên mọi file, giữ lineage cũ làm nguồn đại diện.

**Tiêu chí nghiệm thu:**

- [x] Batch hai file trả đủ path/hash và trạng thái mỗi nguồn; mỗi cặp nguồn được kiểm tra một lần.
- [x] Nguồn thứ hai hỏng/thiếu hoặc expected hash conflict khiến integrity tổng false, có lý do; không mất kết quả nguồn hợp lệ.
- [x] Giữ mọi khóa/chữ ký cũ và hành vi một file; mô tả lineage đại diện và danh sách đầy đủ trong API_REFERENCE.

**Kiểm chứng:**

- [x] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_traceability.py tests/test_time_travel.py tests/test_integration.py tests/test_storage_compatibility.py -q`
- [x] Dùng tmp_path chứa 2 Bronze files cùng batch; làm hỏng file thứ hai rồi trace. Không sửa Bronze dữ liệu nhóm.
- [x] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T01, T02

**File dự kiến:**

- `src/lakehouse_storage/time_travel.py`
- `tests/test_traceability.py`
- `tests/test_time_travel.py`
- `src/lakehouse_storage/API_REFERENCE.md`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** RED 7 failures; GREEN 30 pass (traceability, time_travel, integration, compatibility); API_REFERENCE documents corrected semantics.

### Checkpoint CP-A: sau T01–T03

- [ ] Hai lỗi truy vết được tái hiện và sửa; API/core import/schema giữ tương thích; môi trường dịch vụ dựng lại được.
- [ ] Focused tests và regression liên quan đạt; build/import hoặc smoke tương ứng có evidence.
- [ ] Review phạm vi diff: không thay downstream, schema hoặc write policy; mọi giới hạn runtime được ghi rõ.
- [ ] Cập nhật trạng thái/evidence trước khi bắt đầu phần phụ thuộc.

## Giai đoạn B: Mock server

### T04: Mock run và kho payload tái lập

**Mô tả:** Tái sử dụng generator S1/S2, đóng băng metadata và lưu payload run/sequence trên local volume.

**Tiêu chí nghiệm thu:**

- [x] Run có seed/start_time/generated_at cố định; sensor liên kết đúng MES batch_id/line_id/window.
- [x] Payload bytes lưu atomic và digest không đổi qua restart; không chỉ dựa vào RNG seed.
- [x] Giới hạn sample/batch/run được validate; generator API cũ giữ nguyên; không gọi helper ghi Silver.

**Kiểm chứng:**

- [x] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_mock_runs.py tests/test_mock_sensor_stream.py tests/test_generate_mes_data.py -q`
- [x] Đọc run bằng process mới và so bytes/digest; kiểm tra sample time 1Hz độc lập emission interval.
- [x] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T01

**File dự kiến:**

- `src/lakehouse_storage/mock_server/__init__.py`
- `src/lakehouse_storage/mock_server/models.py`
- `src/lakehouse_storage/mock_server/store.py`
- `src/lakehouse_storage/mock_server/generator.py`
- `tests/test_mock_runs.py`

**Phạm vi:** M (5 file chính).

**Bằng chứng hoàn thành:** `tests/test_mock_runs.py` + existing generators: 45 pass; persisted bytes read by a fresh subprocess.

### T05: Mock HTTP API có entry point

**Mô tả:** Cung cấp health, tạo run, lấy run và lấy batch qua FastAPI/Uvicorn.

**Tiêu chí nghiệm thu:**

- [ ] Bốn endpoints trả envelope/schema rõ; 404 cho unknown run/sequence, 422 cho control request sai.
- [ ] GET batch giữ bytes/digest qua HTTP và restart; mock không ghi Bronze/Silver hoặc cần credential MinIO.
- [ ] CLI/env cấu hình host/port/store, readiness kiểm tra store; import core không kéo dependencies API.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_mock_api.py tests/test_mock_runs.py tests/test_storage_compatibility.py -q`
- [ ] Chạy python -m lakehouse_storage.mock_server; gọi HTTP thật GET /health, POST /runs, GET batch trong smoke test.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T04

**File dự kiến:**

- `src/lakehouse_storage/mock_server/app.py`
- `src/lakehouse_storage/mock_server/__main__.py`
- `src/lakehouse_storage/service_config.py`
- `tests/test_mock_api.py`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T06: Mock scenarios và replay

**Mô tả:** Thêm normal, invalid-value/time, missing-field, delay, transient-500 và replay có điều khiển.

**Tiêu chí nghiệm thu:**

- [ ] Run/sequence quy định rõ lỗi và số attempts lỗi; cùng cấu hình tái hiện được, không ảnh hưởng run khác.
- [ ] Control API sai vẫn bị từ chối; raw scenario payload có lỗi cố ý được trả cho worker; missing-field có expected reject stage riêng.
- [ ] Replay giữ payload cũ; delay/500 có giới hạn, không làm hỏng payload store.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_mock_scenarios.py tests/test_mock_api.py -q`
- [ ] Viết bảng scenario -> HTTP outcome -> Bronze/P3 expected outcome, dùng lại ở E2E T14.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T05

**File dự kiến:**

- `src/lakehouse_storage/mock_server/scenarios.py`
- `src/lakehouse_storage/mock_server/models.py`
- `src/lakehouse_storage/mock_server/app.py`
- `tests/test_mock_scenarios.py`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### Checkpoint CP-B: sau T04–T06

- [ ] Mock HTTP API có normal/error scenarios và replay byte-identical sau restart; không ghi Silver.
- [ ] Focused tests và regression liên quan đạt; build/import hoặc smoke tương ứng có evidence.
- [ ] Review phạm vi diff: không thay downstream, schema hoặc write policy; mọi giới hạn runtime được ghi rõ.
- [ ] Cập nhật trạng thái/evidence trước khi bắt đầu phần phụ thuộc.

## Giai đoạn C: Ingestion worker tối thiểu

### T07: Client và checkpoint Bronze tối thiểu

**Mô tả:** Nhận một batch HTTP, kiểm tra digest, gọi writer hiện có và lưu đường dẫn/hash vào checkpoint.

**Tiêu chí nghiệm thu:**

- [ ] Dữ liệu hợp lệ giữ raw dtype/extra fields; không sửa nghiệp vụ hoặc thay writer; payload/checksum sai bị chặn trước P3.
- [ ] Checkpoint atomic lưu identity/run/table/sequence/path/hash; retry known artifact dùng cùng file; chỉ worker giữ single-instance guard được chạy.
- [ ] State PUBLISHING dở hoặc checkpoint hỏng chuyển NEEDS_REVIEW, không tự tạo file mới/xóa orphan/nhảy sequence.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_worker_state.py tests/test_ingestion_worker.py tests/test_bronze_writer.py -q`
- [ ] Kill one-shot worker sau Bronze write trước checkpoint; restart phải dừng NEEDS_REVIEW. Thử instance worker thứ hai trên cùng state root bị từ chối.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T06

**File dự kiến:**

- `src/lakehouse_storage/mock_client.py`
- `src/lakehouse_storage/worker_state.py`
- `src/lakehouse_storage/ingestion_worker.py`
- `tests/test_worker_state.py`
- `tests/test_ingestion_worker.py`

**Phạm vi:** M (5 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T08: Adapter gọi P3 hiện có

**Mô tả:** Nối worker với process_bronze_file qua adapter trong storage, dùng roots demo riêng.

**Tiêu chí nghiệm thu:**

- [ ] Normal đi qua P3 ghi đủ quality/lineage; quality FAIL không ghi; không sửa mã P3, append_to_delta hoặc ingest_bronze_file.
- [ ] Adapter kiểm tra status/result lỗi kể cả khi P3 không raise; DONE/REJECTED/FAILED phản ánh kết quả thực, không tự gán PASS.
- [ ] Replay cùng Bronze trong một writer tuần tự dùng duplicate gate của P3; schema/partition đầu ra giữ nguyên.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_governance_adapter.py tests/test_worker_pipeline.py tests/test_data_governance_local_e2e.py tests/test_data_governance_silver_writer.py tests/test_olap_build_marts.py -q`
- [ ] HTTP -> Bronze tmp_path -> P3 -> local Delta với normal/reject/replay. Giữ P3 thresholds; adapter lazy import tránh vòng import.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T07

**File dự kiến:**

- `src/lakehouse_storage/governance_adapter.py`
- `src/lakehouse_storage/ingestion_worker.py`
- `tests/test_governance_adapter.py`
- `tests/test_worker_pipeline.py`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T09: Worker loop, retry và resume có giới hạn

**Mô tả:** Thêm once/finite/continuous mode, retry transport hữu hạn và shutdown/resume cho một worker.

**Tiêu chí nghiệm thu:**

- [ ] Timeout/network/429/5xx có max attempts/backoff; schema/quality lỗi không retry vô hạn; stop/skip policy được ghi rõ.
- [ ] Resume BRONZE_READY/PROCESSING dùng cùng file; DONE không nhân rows; kết quả trước crash không rõ được ghi unknown, không suy ra counters.
- [ ] SIGTERM kết thúc hoặc giữ checkpoint; giới hạn in-flight=1, cấu hình env; summary có rows_written P3 báo, stage durations và trạng thái.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_worker_retry.py tests/test_worker_pipeline.py tests/test_ingestion_worker.py -q`
- [ ] Fake clock cho retry tests, subprocess thật cho shutdown. Dừng sau P3 đã ghi và trước worker DONE rồi retry cùng Bronze: không thêm rows trong điều kiện một writer.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T08

**File dự kiến:**

- `src/lakehouse_storage/ingestion_worker.py`
- `src/lakehouse_storage/mock_client.py`
- `src/lakehouse_storage/service_config.py`
- `tests/test_worker_retry.py`
- `tests/test_worker_pipeline.py`

**Phạm vi:** M (5 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### Checkpoint CP-C: sau T07–T09

- [ ] Một worker đi qua P3 nguyên trạng; known Bronze được tái dùng; state dở ngoài bảo đảm dừng NEEDS_REVIEW.
- [ ] Focused tests và regression liên quan đạt; build/import hoặc smoke tương ứng có evidence.
- [ ] Review phạm vi diff: không thay downstream, schema hoặc write policy; mọi giới hạn runtime được ghi rõ.
- [ ] Cập nhật trạng thái/evidence trước khi bắt đầu phần phụ thuộc.

## Giai đoạn D: Maintenance scheduler

### T10: Maintenance policy và runner one-shot

**Mô tả:** Thêm runner gọi maintenance hiện có với threshold/cooldown/allowlist; hỗ trợ compaction chính và Z-order tùy chọn.

**Tiêu chí nghiệm thu:**

- [ ] Policy validate namespace, file thresholds, target size, columns; runner trả RUN/SKIP/FAIL và lý do, không quét cả bảng chỉ để quyết định.
- [ ] Compaction truyền đúng target size xuống Delta; sửa cục bộ maintenance.py nếu cần, giữ chữ ký; Z-order chỉ dùng cột hợp lệ không phải partition.
- [ ] Vacuum chỉ dry-run với retention >=168h và enforce bật; mọi action cần maintenance-enabled/window, không thay policy global của API cũ.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_maintenance_policy.py tests/test_maintenance_runner.py tests/test_maintenance.py -q`
- [ ] Local Delta nhiều file nhỏ: chạy runner rồi so row count/query results. Kiểm chứng target size call bằng API cài trong T01; không đặt exact file-size assertion dễ flaky.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T01

**File dự kiến:**

- `src/lakehouse_storage/maintenance_policy.py`
- `src/lakehouse_storage/maintenance_runner.py`
- `src/lakehouse_storage/maintenance.py`
- `tests/test_maintenance_policy.py`
- `tests/test_maintenance_runner.py`

**Phạm vi:** M (5 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T11: Scheduler cùng process với runner

**Mô tả:** Thêm APScheduler entry point, lịch UTC và single-instance guard cho maintenance.

**Tiêu chí nghiệm thu:**

- [ ] Một scheduler gọi runner trực tiếp, không queue/worker container riêng; job ids ổn định, max concurrency=1, coalesce/misfire có giới hạn.
- [ ] Restart không tạo job trùng hoặc catch-up vô hạn; SIGTERM dừng sạch; scheduler mặc định disabled cho mutation khi chưa cấu hình phase/window.
- [ ] Không tự coi scheduler lock là lock P3; tài liệu/CLI nói rõ ingestion phải dừng trước maintenance trong demo.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_scheduler.py tests/test_maintenance_runner.py tests/test_storage_compatibility.py -q`
- [ ] Clock điều khiển cho test thời gian; smoke process lịch ngắn + shutdown; khởi động scheduler thứ hai trên cùng state root bị từ chối.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T10

**File dự kiến:**

- `src/lakehouse_storage/maintenance_scheduler.py`
- `src/lakehouse_storage/service_config.py`
- `tests/test_scheduler.py`
- `tests/test_maintenance_runner.py`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T12: Lịch sử maintenance và kiểm chứng logical rows

**Mô tả:** Lưu JSONL operation history/cooldown và kiểm tra bảng trước/sau tối ưu trong cửa sổ không ingest.

**Tiêu chí nghiệm thu:**

- [ ] Record có action/decision/error/duration/version/file count/active bytes; restart đọc cooldown, record dở ghi unknown không báo success.
- [ ] Compaction/Z-order giữ logical query results và time travel bản trước khi chưa vacuum; test scheduler skip khi không ở window.
- [ ] Preview vacuum không xóa file; metric phân biệt active bytes với physical bytes, không gọi active-byte delta là freed disk.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_maintenance_history.py tests/test_maintenance_invariants.py tests/test_scheduler.py tests/test_time_travel.py -q`
- [ ] Chạy một đợt ingest hoàn tất, maintenance, query trước/sau. Ghi rõ thử nghiệm này không chứng minh concurrency với writer bên ngoài.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T10, T11

**File dự kiến:**

- `src/lakehouse_storage/maintenance_history.py`
- `src/lakehouse_storage/maintenance_runner.py`
- `tests/test_maintenance_history.py`
- `tests/test_maintenance_invariants.py`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### Checkpoint CP-D: sau T10–T12

- [ ] Scheduler gọi runner cùng process; job không chồng; history đúng, logical rows giữ nguyên, vacuum chỉ preview.
- [ ] Focused tests và regression liên quan đạt; build/import hoặc smoke tương ứng có evidence.
- [ ] Review phạm vi diff: không thay downstream, schema hoặc write policy; mọi giới hạn runtime được ghi rõ.
- [ ] Cập nhật trạng thái/evidence trước khi bắt đầu phần phụ thuộc.

## Giai đoạn E: Docker, E2E và benchmark harness

### T13: Docker Compose cho demo hai pha

**Mô tả:** Đóng gói mock/worker/scheduler và MinIO với volume, roots và dependencies đã kiểm chứng.

**Tiêu chí nghiệm thu:**

- [ ] Image build được từ dependency constraints; mock và MinIO healthy; endpoints nội bộ dùng service hostname, không trùng host port với stack đang có.
- [ ] Worker và maintenance dùng profiles/pha riêng; Bronze/state/payload/reports persist; trace dùng cùng absolute Bronze mount path.
- [ ] Target demo root bắt buộc/allowlist, mock không cần MinIO secrets; instructions start-ingest-stop-maintain có command cụ thể, default vacuum preview.

**Kiểm chứng:**

- [ ] docker compose -f infra/docker-compose.storage.yml config; build; up theo profiles đã khai báo. Inspect health, ingest một run, stop worker sạch rồi mới bật maintenance. T01 constraints không được bỏ qua.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T03, T09, T12

**File dự kiến:**

- `infra/Dockerfile.storage`
- `infra/docker-compose.storage.yml`
- `infra/storage.env.example`
- `docs/storage_operations.md`
- `src/lakehouse_storage/service_config.py`

**Phạm vi:** M (5 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T14: MinIO E2E cho demo và failure cases

**Mô tả:** Kiểm chứng toàn bộ flow với P3 nguyên trạng trên stack demo, bao gồm replay và restart trong phạm vi hỗ trợ.

**Tiêu chí nghiệm thu:**

- [ ] Normal S1/S2 -> Bronze -> P3 -> MinIO có counts/lineage đúng; replay known Bronze không tăng rows; invalid payload bị chặn đúng stage.
- [ ] HTTP 500/timeout retry hữu hạn; worker restart ở state đã có path dùng lại file; PUBLISHING dở yêu cầu review và không âm thầm tiếp tục.
- [ ] Worker dừng trước scheduler; compaction giữ query results, trace hai nguồn và version lookup đúng; vacuum preview không xóa.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_storage_minio_demo.py -m minio -q`
- [ ] Đăng ký minio marker và fixture endpoint riêng trước chạy; dùng run-specific prefix, chỉ cleanup tài nguyên test sở hữu. Chạy nguyên trạng P3/OLAP/SPC suites hoặc ghi rõ skips môi trường.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T13

**File dự kiến:**

- `tests/test_storage_minio_demo.py`
- `tests/conftest.py`
- `pyproject.toml`
- `docs/storage_operations.md`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T15: Benchmark harness có đối chiếu kết quả

**Mô tả:** Viết harness tạo workload, nạp theo batch sizes, chạy query lặp và ghi raw metrics/config.

**Tiêu chí nghiệm thu:**

- [ ] Baseline/optimized cùng logical accepted dataset/schema/partition; query result equality được kiểm tra trước latency comparison.
- [ ] Có seed, scale, batch sizes, warm-up >=1 và repeats >=5; lưu median/dispersion/raw times/environment và đúng cache policy.
- [ ] Đo end-to-end throughput có ghi P3 overhead, file count/size, active bytes, compaction duration và query latency; không dùng shared namespace.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_storage_benchmark.py -q`
- [ ] Chạy harness quy mô nhỏ trên MinIO; tests kiểm tra correctness/schema metrics, không assert tốc độ. Không viết lại benchmark so sánh engine của P4.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T12, T14

**File dự kiến:**

- `src/lakehouse_storage/benchmark.py`
- `src/lakehouse_storage/benchmark_workloads.py`
- `tests/test_storage_benchmark.py`
- `docs/storage_benchmark.md`

**Phạm vi:** M (4 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### Checkpoint CP-E: sau T13–T15

- [ ] Demo MinIO hai pha chạy được; E2E đúng; benchmark harness kiểm tra kết quả trước đo thời gian.
- [ ] Focused tests và regression liên quan đạt; build/import hoặc smoke tương ứng có evidence.
- [ ] Review phạm vi diff: không thay downstream, schema hoặc write policy; mọi giới hạn runtime được ghi rõ.
- [ ] Cập nhật trạng thái/evidence trước khi bắt đầu phần phụ thuộc.

## Giai đoạn F: Thực nghiệm và bàn giao

### T16: Chạy thực nghiệm và ghi kết quả thật

**Mô tả:** Thực hiện các cấu hình batch/scale và layout đã chốt, lưu evidence đầy đủ cho báo cáo.

**Tiêu chí nghiệm thu:**

- [ ] Chạy scale 100k/1M nếu đủ tài nguyên; mọi cấu hình thực chạy/skip có lý do và cấu hình máy, seed, versions.
- [ ] Có baseline/compacted và Z-order tùy chọn cho query phù hợp, ít nhất 5 timed repetitions; correctness checks pass trước khi báo speedup.
- [ ] Lưu raw metrics/config/operation logs dưới reports/storage/<run_id>; không vacuum hoặc ghi lên bảng nhóm; physical-size chưa đo thì ghi unknown.

**Kiểm chứng:**

- [ ] Chạy python -m lakehouse_storage.benchmark theo CLI T15 và ghi nguyên command vào report. Kiểm tra metrics có đủ raw samples và không trộn local/MinIO hay cache regimes.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T15

**File dự kiến:**

- `docs/storage_benchmark.md`
- `docs/storage_results.md`

**Phạm vi:** S (2 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T17: Biểu đồ và diễn giải thực nghiệm

**Mô tả:** Tạo report generator và biểu đồ từ raw metrics, giải thích trade-off storage bằng số liệu đo.

**Tiêu chí nghiệm thu:**

- [ ] Xuất >=4 hình PNG/SVG: file counts, file-size distribution, latency và throughput/batch size; nhãn/đơn vị/dispersion rõ.
- [ ] Mỗi hình truy về run/config/raw metrics, script tái tạo được; không bịa kết quả khi Z-order/compaction không nhanh hơn.
- [ ] Báo cáo phân biệt active/retained bytes, end-to-end/storage time và giới hạn synthetic workload; plotting deps không vào core imports.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/test_benchmark_report.py tests/test_storage_compatibility.py -q`
- [ ] Chạy report generator trên dữ liệu T16, mở từng ảnh để kiểm tra nhãn/chồng chữ/trục. Report tests chỉ kiểm tra metric mapping/render smoke, không snapshot pixel toàn hình.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T16

**File dự kiến:**

- `src/lakehouse_storage/benchmark_report.py`
- `tests/test_benchmark_report.py`
- `docs/storage_results.md`

**Phạm vi:** M (3 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### T18: Demo, tài liệu và nghiệm thu tương thích

**Mô tả:** Hoàn thiện hướng dẫn chạy lại gói nhỏ và báo cáo phần đóng góp P1 theo bằng chứng.

**Tiêu chí nghiệm thu:**

- [ ] Demo có mock, ingest qua P3, replay/reject, scheduler sau ingest, trace và benchmark figures; lệnh chạy/roots/profile rõ ràng.
- [ ] README/API_REFERENCE ghi scope single worker, NEEDS_REVIEW window, metadata giữ nguyên và preview-only vacuum; docs cũ được đánh dấu không phải trạng thái gói mới.
- [ ] Full suite và downstream nguyên trạng có kết quả pass/fail/skip; review diff không sửa mã downstream, silver_manager hoặc bronze_writer; Docker/import checks đạt.

**Kiểm chứng:**

- [ ] `PYTHONPATH=src .venv/bin/python -m pytest tests/ -q`
- [ ] Chạy lại demo từ namespace mới, pip check, compose health, git diff --check và kiểm tra scope. Báo lỗi baseline riêng; không đánh dấu hoàn tất khi có regression do gói mới.
- [ ] Ghi kết quả, command thực chạy và artifact evidence vào mục hoàn thành.

**Phụ thuộc:** T17

**File dự kiến:**

- `src/lakehouse_storage/README.md`
- `src/lakehouse_storage/API_REFERENCE.md`
- `docs/storage_operations.md`
- `docs/storage_demo.md`
- `docs/overlap_analysis.md`

**Phạm vi:** M (5 file chính).

**Bằng chứng hoàn thành:** Chưa có.

### Checkpoint CP-F: sau T16–T18

- [ ] Số liệu thật và 4 biểu đồ tái tạo được; demo có docs; downstream không phải sửa và regression tests đạt.
- [ ] Focused tests và regression liên quan đạt; build/import hoặc smoke tương ứng có evidence.
- [ ] Review phạm vi diff: không thay downstream, schema hoặc write policy; mọi giới hạn runtime được ghi rõ.
- [ ] Cập nhật trạng thái/evidence trước khi bắt đầu phần phụ thuộc.

## Definition of Done chung

- [ ] Tất cả acceptance criteria đạt và evidence tái tạo được.
- [ ] Đã chạy tests downstream nguyên trạng, phân biệt lỗi baseline với regression.
- [ ] Demo dùng namespace riêng, worker dừng trước maintenance, vacuum preview không xóa.
- [ ] Worker không hứa exactly-once đa writer; crash window chưa hỗ trợ chuyển NEEDS_REVIEW.
- [ ] Báo cáo có số liệu thật và phân biệt throughput end-to-end với storage-only.
- [ ] Không commit dữ liệu lớn/secrets; không sửa hoặc xóa thay đổi ngoài phạm vi.

## Backlog để sau, không chặn gói này

- Atomic Bronze publisher/manifest, ingestion ledger và recovery toàn diện.
- Multi-worker/multi-host, khóa chung P3-writer-maintenance.
- Thay shared-write policy/strict enforcement toàn cục hoặc mở rộng schema.
- Destructive vacuum với retention audit, composite-key diff và remediation workflow.
