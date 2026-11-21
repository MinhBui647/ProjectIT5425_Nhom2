# Giải thích code triển khai kế hoạch Lakehouse

Tài liệu giải thích code mock đa nguồn, ingestion Bronze, P3/Silver, versioning
và orchestration Airflow. Các mã LW ở từng mục giúp đối chiếu nhóm task đã
triển khai; LW01–LW02 là phần nền, LW16–LW18 Gold còn ở backlog.
Lệnh chạy và kết quả mong đợi có trong [hướng dẫn demo](storage_demo.md).

## 1. Dữ liệu đi đâu và ai chịu trách nhiệm

Mock chỉ tạo raw handoff. Client kiểm identity/digest. Worker ghi Parquet và
checksum vào Bronze bất biến. P3 đọc nguyên file, transform, kiểm chất lượng với
ngưỡng hiện có 0.005, quarantine và ghi Delta. Pipeline lưu evidence rồi chốt bộ
Delta version. Airflow chỉ điều phối callable qua interpreter processing riêng.
USDA kết thúc ở Bronze; sáu bảng còn lại phải có evidence Silver.

## 2. LW03–LW07: mock đa nguồn và tạo run idempotent

`mock_server/models.py` định nghĩa `RunRequest`, kiểm giới hạn seed, số batch,
sample, nguồn trùng/lạ, khoảng thời gian UTC và FAO long/wide/both. Mặc định vẫn
MES trước và sensor sau để caller cũ chạy được. `sources` chọn bằng tên bảng;
`fault_table` xác định nguồn chịu lỗi thay vì làm hỏng mọi nguồn.

`generator.py` tái sử dụng generator sensor/MES. Sensor phát mẫu 1 Hz; production
window MES phủ toàn bộ mẫu. `_generated_at` được cố định theo window để không phụ
thuộc giờ replay. `source_generator.py` tạo GDT với giá string, FAO với hai shape,
weather với humidity integer, FDA report date YYYYMMDD, USDA với section prices
và commentary có schema khác nhau. P3 tự tính THI; mock không gán PASS.

`store.py` serialize JSON canonical (sort keys, không NaN) và SHA-256 trên danh
sách raw rows. Mỗi batch có sequence, table, source, variant, window, digest và
cờ Bronze-only trong manifest version 2. Payload nằm trong staging directory;
`os.replace` công bố cả directory khi mọi file và run.json đã ghi xong. GET chỉ
nhận run ID hex nên staging không lộ thành run hoàn chỉnh.

POST có `Idempotency-Key` tùy chọn. SHA-256 key chọn run ID ổn định; lock POSIX
serialize create trên một host. Retry sau mất response đọc lại run cũ; config
khác trả 409; staging tồn tại nhưng chưa publish yêu cầu review. Không có key
thì dùng UUID như trước. Key không được lưu dưới dạng plaintext trong manifest.

`app.py` cung cấp health, POST /runs, GET run và GET batch; OpenAPI ở /docs.
`scenarios.py` sửa field raw theo từng bảng cho missing-field, invalid-time,
invalid-value. Transient-500 có counter bền vững và delay bị giới hạn. Đây là
API handoff chung, không mô phỏng nguyên giao thức từng upstream.

Kiểm chứng: `test_mock_manifest.py` kiểm atomic visibility, concurrent create,
restart/config conflict; `test_mock_source_contracts.py` đưa raw qua transformer
và validator P3 nguyên trạng, kiểm mẫu lỗi thực sự không vượt gate.

## 3. LW09: client kiểm manifest trước Bronze

`mock_client.py` giới hạn response 32 MiB, tối đa 50.000 rows/batch và số batch
hữu hạn. `run()` kiểm manifest rồi cache metadata. `batch()` kiểm bounds và
`decode_batch()` so run/sequence/table/source/variant/window/digest với entry đã
chốt, sau đó tính lại digest rows. Payload tự khai digest đúng nhưng khác manifest
vẫn bị chặn. Manifest cũ không có version giữ đường tương thích S1/S2.

GET không theo redirect. Chỉ timeout, connection error, 429 và 5xx được retry với
exponential backoff hữu hạn. Schema/identity/digest/404 không retry nghiệp vụ.

## 4. LW08, LW10–LW13: source boundary, checkpoint, Bronze và P3

`lakehouse_pipeline/source_config.py` dùng model frozen cho mode, nguồn bắt buộc/
optional, start/end có timezone và tham số live. Window là [start,end), UTC.
`local-generator` chỉ mô phỏng S1/S2; live không tuyên bố có kết nối production
SCADA/MES. `sources.py` lazy import từng hàm P2, truyền dates thay runner hardcode;
API ngày inclusive nhận end trừ một microsecond. Riêng GDT so timestamp với
end_date ở 00:00 nên wrapper dùng ceil-ngày UTC của end: end đúng 00:00 giữ
ngày đó, end giữa ngày lấy ngày kế tiếp. Sau thu thập vẫn lọc `[start,end)`;
event đúng end bị loại. P2 không đổi, raw values giữ nguyên. P3 luôn nhận nguyên
file với `date_from/date_to=None`. Regression GDT chạy collector thật với HTTP
helper được stub, không phải bằng chứng kết nối API GDT thật.

`worker_state.py` lưu manifest bất biến cùng target binding. Khi có manifest mới,
load lấy table/source/variant/digest từ entry, không suy sequence thành sensor.
Thiếu evidence legacy hoặc mismatch không được tự sửa: ghi review riêng, giữ
checkpoint gốc. Atomic JSON dùng tempfile, fsync, replace. `single_instance`
dùng POSIX flock không chờ, chung file `demo-phase.lock` giữa CLI và task.

`ingestion_worker.py` khôi phục numeric widths của JSON theo đúng raw variant,
không sửa giá trị lỗi. Publish lưu FETCHING → PUBLISHING → BRONZE_READY. Crash
ở PUBLISHING chưa biết file nào đã công bố thì NEEDS_REVIEW. Khi biết path/hash,
retry kiểm lại checksum và tái sử dụng file. Chỉ USDA bypass schema; không gọi
P3 cho USDA. Dữ liệu rỗng ghi NO_DATA, để pipeline áp policy bắt buộc/optional.

P3 tiếp tục transform, quality gate, reports/quarantine, write Delta với governance
columns. `governance_adapter.committed_evidence()` đọc Delta thật, lọc checksum,
contract và Bronze path, kiểm quality status và lấy `_quality_run_id` gốc. Replay
có thể tạo audit run mới trong P3 nhưng evidence dữ liệu vẫn trỏ commit gốc.
Crash sau Delta commit trước checkpoint được phục hồi bằng duplicate gate; không
suy số rows đã ghi trước crash từ rows_written=0 của lần replay.

Tests: `test_pipeline_bronze.py`, `test_pipeline_governance.py`; các test worker
cũ vẫn kiểm crash process thật, target đổi, symlink và HTTP retry hữu hạn.

## 5. LW14–LW15, LW19: manifest, Delta snapshot và CLI

`run_manifest.py` băm identity thành tên file; config được allowlist, fingerprint
canonical JSON và ràng buộc bất biến cùng identity. Manifest version 1 có code/
image/dependency versions, stages, artifacts và Silver snapshot. Atomic updates
không thêm cột vận hành vào business schema. Git hash unknown được ghi đúng là
unknown khi image không cung cấp revision; không suy hash giả.

`silver_snapshot.py` nhận records DONE của từng nguồn, đọc latest Delta dưới lock
ở lần capture đầu, kiểm accepted rows/quality IDs/statuses theo Bronze hash/path/
contract và lưu URI/version/total_rows/batch evidence. `validate_snapshot()` dùng
cùng logic đối chiếu tại version đã lưu khi retry/finalize, không chốt lại latest.
`read_snapshot()` mở DeltaTable theo version cụ thể và kiểm version thực trước
khi đọc với storage_options. Append/compaction sau capture không thay map hoặc
pinned rows, kể cả rerun toàn pipeline; retention vẫn có thể xóa file lịch sử
nên versioning không thay backup.

`pipeline.py` cung cấp prepare, bronze, verify, govern, snapshot, finalize. Mọi
stage giữ chung local lock. Prepare ghi config trước khi POST mock, key phụ thuộc
identity bền vững; retry sau mất response không đổi run. Stage failure vẫn lưu
những artifact/P3 commit đã có, không hứa rollback nhiều bảng. Finalize kiểm đủ
checkpoint của từng sequence trong manifest, quality và pinned versions; thiếu
một batch, thiếu nguồn bắt buộc hoặc NO_DATA bắt buộc không được coi là success.
Nhánh thu thập artifact trong finally vẫn cho đọc run dở để giữ failure evidence.
Stage snapshot lưu `captured=true` ngay khi capture thành công; marker giữ qua
retry lỗi, nhận diện được cả map `{}` hợp lệ cho USDA-only/optional NO_DATA.
Stage SUCCESS của manifest cũ cũng được giữ dấu đã capture trước khi retry.
`__main__.py` dùng JSON stdin cho Airflow và arguments cho
CLI: exit 0 thành công, 2 lỗi nghiệp vụ/review/config, 3 lỗi infrastructure.

Tests: manifest corruption/config conflict, version đọc lại sau append, full
pipeline replay và partial failure giữ Silver đã commit. Pipeline không gọi Gold.

### Duplicate cùng bytes ở path Bronze khác

Hai run có thể nhận đúng cùng raw handoff và tạo Parquet bytes giống nhau ở hai
path khác nhau. P3 idempotency dựa checksum/contract/target nên lần sau trả
SKIPPED_DUPLICATE. Adapter phải lookup checksum/contract, lấy `bronze_paths` và
quality IDs từ rows đã commit. Nếu ép Silver `_bronze_file` bằng path mới thì
sẽ báo lỗi sai dù duplicate hợp lệ. Regression mới tái hiện rồi kiểm cả path
mới và provenance gốc; snapshot dùng danh sách path đã commit để đối chiếu rows.

## 6. LW20–LW22: Airflow và Compose

`airflow_tasks.py` chỉ import standard library khi parse. `invoke_processing()`
chạy `/opt/processing/bin/python -m lakehouse_pipeline --stdin`, gửi arguments
JSON nhỏ và nhận JSON ở dòng stdout cuối. Lỗi exit 2 thành `AirflowFailException`
(không retry); exit 3 thành `AirflowException` (retry hữu hạn). Timeout process
không xóa checkpoint hoặc suy commit chưa xảy ra. Task retry quay lại identity,
manifest và Bronze path cũ. Payload/DataFrame không qua XCom.

Prepare key là `dag_id/run_id`. Manual run bắt buộc window_start/window_end có
timezone; scheduled run lấy data interval không rỗng. DAG ingestion dùng rõ
`CronDataIntervalTimetable` UTC khi có LAKEHOUSE_CRON_UTC; mặc định schedule=None.
Không suy window từ giờ hệ thống hoặc cron có interval dài một ngày.

`dags/lakehouse_pipeline.py` tạo 25 task hữu hạn: prepare, ba stage cho mỗi trong
bảy bảng, snapshot, report, finalize. Mỗi task có cùng pool một slot; DAG chỉ có
một active run, catchup=False. Report dùng all_done nhưng không là leaf: finalize
all_success còn phụ thuộc trực tiếp snapshot, nên report thành công không che
lỗi upstream. Nguồn không được chọn trả NOT_SELECTED, không fake dữ liệu/gate.

`infra/docker-compose.lakehouse.yml` có PostgreSQL 18.1, Airflow 3.1.7 API,
scheduler LocalExecutor, DAG processor, init, MinIO và mock. Processing venv giữ
pins riêng vì NumPy/FastAPI của Airflow khác core. API cần asyncpg 0.31.0 ngoài
psycopg2; test image phát hiện thiếu driver trong slim image trước runtime fix.
Init migrate DB và set pool idempotently; không reset DB hoặc xóa volume.

Runtime, payloads, PostgreSQL, logs và MinIO có named volumes riêng. Cổng host
bind localhost. Các component dùng cùng artifact absolute path, giúp lineage
không trỏ path chỉ có trên host. Không mount Docker socket trong task. Giá trị
secret đi qua env; `.env.example` chỉ có local-demo defaults. Image không copy
notebook, dữ liệu người dùng, `.env` hoặc `.git` vào build context.

## 7. LW23–LW27: maintenance one-shot và chuyển lịch

`maintenance_cli.py` đọc env tạo policies rồi giữ phase lock suốt runner action.
Default disabled; operator phải xác nhận stopped-ingestion phase và ngừng writer
ngoài cơ chế này. Lock chỉ chứng minh độc quyền giữa process cùng volume/host.
Các task Airflow có thêm pool một slot, không tuyên bố distributed transaction.

Runner giữ policy/window/file thresholds/cooldown, history JSONL bền vững, kiểm
namespace và partition/nonpartition columns. Action chỉ compact, zorder hoặc
vacuum_preview. Vacuum luôn dry_run, retention floor 168 giờ. Record STARTED
không hoàn tất được coi là unknown/failed theo history hiện có; không tự xóa file.

`dags/lakehouse_maintenance.py` gọi CLI trong processing interpreter, dùng cùng
pool và lock với ingestion. Không tự bật enabled hoặc stopped phase. Scheduler
cũ chỉ được bỏ sau runtime gate; rollback dùng image cũ sau khi dừng Airflow
tasks để không có hai bộ lịch cùng target. History/checkpoints/volumes giữ lại.

## 8. Hướng dẫn đọc từng hàm và dữ liệu đầu ra

### Mock và transport

| Hàm/model | Đầu vào → đầu ra | Điều kiện lỗi đáng chú ý |
|---|---|---|
| RunRequest.validate_run | request → normalized UTC request | nguồn lạ/trùng, sample/window vượt giới hạn |
| generate_batches | request, run_id → iterator (table, raw rows) | chỉ finite batches, không tự ghi lake |
| external_batches | seed/window/sources → representative raw batches | FAO both và USDA nhiều section là nhiều entry |
| apply_payload_fault | raw rows, scenario, table → raw rows lỗi | không gán quality outcome |
| canonical_json | JSON-compatible value → bytes | NaN bị từ chối |
| RunStore.create/_create | request, optional key/factory → run metadata | lock serialize create; conflict/staging cần review |
| RunStore.run/batch | run_id, sequence → metadata/persisted bytes | ID phải 32 hex; sequence phải trong bounds |
| delivery_status | store/run/sequence → HTTP status | transient counter bền vững; delay bounded |
| MockClient.get/_get_once | route → bounded response bytes | timeout/network/429/5xx retry; redirect từ chối |
| MockClient.create | request, stable key → metadata | chỉ retry idempotent create; config conflict dừng |
| MockClient.run | run_id → validated metadata | manifest sai version/shape/identity là PayloadError |
| decode_batch | bytes, run, sequence, optional entry → envelope | self digest và manifest digest đều phải khớp |

Mock payload digest băm JSON rows; Bronze digest băm Parquet bytes. Chúng khác
nhau vì Parquet có schema, encoding và metadata. Không dùng cái này thay cái kia.

### State, Bronze và governance

| Hàm | Trách nhiệm | Artifact/result |
|---|---|---|
| atomic_json | tempfile cùng thư mục, flush/fsync rồi replace | JSON hoàn chỉnh hoặc bản cũ |
| single_instance | POSIX LOCK_EX/LOCK_NB trên file | context manager; process khác bị lỗi ngay |
| StateStore.bind_target | giữ Silver target bất biến theo run | target.json và review.json khi mismatch |
| StateStore.bind_manifest/entry | lưu danh mục bất biến và resolve sequence | manifest.json; thiếu/sai cần review |
| StateStore.load/save | kiểm status/attempts/digest/P3 identity | checkpoint nguyên bản được giữ khi corrupt |
| raw_frame | decode rows và restore width theo raw schema variant | float64, int64/int32; giữ extra columns |
| IngestionWorker.publish | fetch → raw frame → immutable writer → checksum | BRONZE_READY hoặc REJECTED/NO_DATA/review |
| IngestionWorker.process | nguyên Bronze → P3 → resolve commit evidence | DONE/REJECTED/FAILED; USDA DONE Bronze-only |
| IngestionWorker.run | duyệt hữu hạn, dừng/skip theo policy | list record; không coordinator Delta toàn cục |
| committed_evidence | đọc Delta theo hash/contract/target | accepted rows, quality IDs/statuses, original paths |

Old S1/S2 checkpoint có table/digest/target khớp manifest mới được bổ sung metadata
trong bộ nhớ khi đọc; không rewrite evidence gốc trong load. Legacy thiếu target
hoặc digest vẫn lỗi. Integer metadata nullable int32 của FDA được restore int32;
chỉ cast int64 mọi số nguyên sẽ làm raw schema validation sai ở trường này.

`bronze_writer.write_bronze_batch()` là writer hiện có: validate raw variants,
reserve tên duy nhất, stage Parquet/sidecar, publish sidecar trước và Parquet sau
bằng hardlink không replace, fsync. Không ghi đè file cũ. USDA bypass schema có
chủ đích nhưng worker yêu cầu published_date hợp lệ để không partition nhầm ngày.

### Orchestration và snapshot

| Hàm/model | Trách nhiệm |
|---|---|
| SourceConfig/utc_window | mode/source/optional/cadence parameters và [start,end) UTC |
| collect_live | gọi từng P2 collector theo window, giữ raw và NO_DATA |
| ManifestStore.path | SHA-256 identity thành tên file an toàn |
| fingerprint | canonical config digest; chặn secret fields kể cả nested |
| environment | Git hoặc image-provided revision, image ID, package versions |
| ManifestStore.prepare/load/save | immutable config binding, schema version, atomic updates |
| prepare_run | persist context trước create; key ổn định theo identity; trả manifest path |
| execute_stage | target check + phase lock + callable + stage duration + partial evidence |
| _records | snapshot/finalize yêu cầu đủ sequence; reporting vẫn thu checkpoint đang có |
| capture_snapshot | chốt lần đầu: đủ required sources, actual Delta version, rows/quality/lineage match |
| validate_snapshot | so map và evidence với checkpoints tại pinned version, không đọc latest |
| read_snapshot | Polars read_delta theo URI/version đã chốt và storage_options |
| run_pipeline | chạy cùng chuỗi stages mà Airflow điều phối |
| main CLI | parse JSON/arguments, exit 0/2/3, response metadata nhỏ |

`prepare_run` có thể đã POST thành công nhưng chưa lưu mock_run_id. Retry vẫn dùng
key tính từ manifest identity nên server trả run cũ. Khi nguồn live được thu thập,
raw payload cũng được publish vào collected store một lần; retry không gọi lại
upstream nếu run đã publish. Nếu crash ở staging live, dừng review thay vì lấy
snapshot upstream mới rồi gán cùng identity.

`execute_stage` lưu duration dù lỗi; artifacts phản ánh các checkpoint đã có.
Verify yêu cầu Bronze evidence tồn tại rồi kiểm checksum, không tự fetch dữ liệu
mới để làm bước verify xanh. Snapshot/finalize kiểm checksum lại; không chỉ tin
trạng thái task Airflow đã success trước đó.

### Airflow và maintenance

| Hàm/file | Trách nhiệm |
|---|---|
| resolve_window | manual bắt buộc conf window; scheduled dùng non-empty interval UTC |
| invoke_processing | subprocess input JSON, timeout, parse result, map business/infra errors |
| prepare_task | stable dag_id/run_id; request full-source default; XCom path/source list |
| stage_task | chọn table và gọi stage; NOT_SELECTED cho nguồn không chọn |
| report_task | all_done observer đọc manifest, không ghi PASS |
| maintenance_task | gọi one-shot processing CLI với action cho phép |
| maintenance_cli.run_once/main | env policy + held phase lock; RUN/SKIP/FAIL và exit code |
| maintenance_runner.run | policy/window/cooldown/metadata snapshots/action/history |
| volume-init/init_airflow.py | volume ownership trước user task; migrate DB/pool=1 |

Mọi mutation và capture đều cần cùng volume khóa. Named volume khác host hoặc
writer không dùng khóa nằm ngoài bảo đảm này. Airflow pool không thay distributed
lock. Vacuum preview không đồng nghĩa đã giải phóng retained/physical bytes.

## 9. Mapping task → implementation → verification

| Tasks | Code/tài liệu chính | Verification chính |
|---|---|---|
| LW01–LW02 có trước | Dockerfile/constraints hai interpreter | test_airflow_environment, pip check hai interpreter |
| LW03–LW04 | mock models/store/app | test_mock_manifest, test_mock_runs, test_mock_api |
| LW05–LW07 | source_generator/scenarios | test_mock_source_contracts, existing crawler tests |
| LW08 | source_config/sources | test_pipeline_sources, test_pipeline_contracts |
| LW09 | mock_client | test_mock_client_manifest, test_worker_retry |
| LW10–LW11 | worker_state/ingestion_worker | test_pipeline_bronze, test_worker_state, test_bronze_publication |
| LW12 | governance_adapter/P3 unchanged | test_pipeline_governance, P3 suites nguyên trạng |
| LW13 | worker state/recovery/phase lock | test_pipeline_recovery, test_worker_retry process kill |
| LW14–LW15 | run_manifest/silver_snapshot | test_run_manifest, test_silver_snapshot |
| LW16–LW18 | Gold backlog, không thêm code | không dùng làm điều kiện nghiệm thu |
| LW19 | pipeline/__main__ | CLI thật, test_lakehouse_pipeline_e2e/minio_e2e |
| LW20–LW22 | airflow_tasks, DAG, Compose | adapter/DAG/environment/Compose tests |
| LW23–LW24 | maintenance_cli/maintenance DAG | CLI/DAG/history/invariant tests |
| LW25 | runtime tests và sanitized evidence | normal/restart/clear/transient/schema/quality/checksum/locks |
| LW26–LW27 | cutover service/dependency cũ | Compose smoke, rebuild, pip check, regression |
| LW28–LW30 | mock/API/Airflow/operations/versioning/demo | HTTP/CLI walkthrough, links/commands |
| LW31 | acceptance/fresh walkthrough | core + MinIO + image + runtime, skip giải thích rõ |
| LW32 | backup/history grouping/handoff | 5 commit sau fixed base, so tree, test từng nhóm |

Tài liệu mô tả implementation và cách kiểm chứng; kết quả test phụ thuộc lần
chạy và môi trường. Test opt-in chưa chạy không được tính pass. Các giới hạn như local lock,
PUBLISHING orphan, retention và không multi-table rollback được giữ nguyên trong
mọi ví dụ và kết quả.

### USDA published_date có format riêng

Upstream USDA dùng `%m/%d/%Y %H:%M:%S`. Writer ưu tiên parse format này riêng
cho market_reports/published_date trước fallback ngày tổng quát. Nếu dùng DMY
fallback cho `01/02/2026 09:30:00` thì partition nhầm sang 2026-02-01; đúng phải
2026-01-02. Regression ghi/đọc raw để chứng minh chỉ thay cách chọn directory,
không đổi chuỗi trong Parquet. Các nguồn khác giữ DMY-before-MDY như trước.
