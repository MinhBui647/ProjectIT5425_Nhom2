# Version dữ liệu và run manifest

Git version mô tả code; contract `v1` mô tả schema/quality; Delta version là commit
của một bảng. Chúng không thay thế nhau. Một run có thể commit nhiều bảng trước
khi lỗi; version map là tập snapshot, không phải ACID transaction nhiều bảng.

`STORAGE_DEMO_ROOT/runs/<sha256(identity)>.json` là evidence bền vững. Schema 1:

```json
{
  "schema_version": 1,
  "identity": "lakehouse_pipeline/manual-demo",
  "config_digest": "<sha256 canonical allowlisted config>",
  "environment": {"git_commit":"<revision>","image":"<image id>","dependencies":{}},
  "status": "SUCCESS",
  "mock_run_id": "<32 hex>",
  "config": {"mode":"mock","sources":["weather"],"window":["<start UTC>","<end UTC>"]},
  "stages": {"snapshot":{"status":"SUCCESS","captured":true,"duration_seconds":0.1}},
  "artifacts": [],
  "silver_snapshot": {"weather":{"uri":"s3://smart-dairy-storage-demo/storage_demo/workflow/silver/weather","version":0,"total_rows":5,"batches":[]}}
}
```

Đây là mẫu rút gọn, không phải fixture nghiệm thu hoặc manifest đầy đủ có thể nạp.
Manifest thực lưu request normalized, roots, optional sources và evidence từng
artifact. Config fingerprint thay đổi thì cùng identity bị từ chối. Secrets lấy
qua env, không ghi credentials vào config/manifest. Khi không xác minh được Git
hoặc image digest, trường metadata ghi unknown thay vì suy đoán.

Bronze artifact ghi path, payload digest, Parquet SHA-256 và trạng thái. Sidecar
được kiểm trước P3 và khi finalize. P3 ghi `_bronze_file`, `_bronze_sha256`,
`_contract_version`, `_quality_status`, `_quality_run_id` trong Silver. Adapter
đọc rows thật để xác minh accepted count và quality IDs gốc. Với bytes giống
nhau ở path Bronze mới, duplicate evidence ghi `bronze_paths` của commit gốc;
manifest vẫn giữ file mới làm đầu vào hiện tại. Không đổi quality ID thành ID
của audit replay.

Snapshot và finalize đối chiếu checkpoint với **từng sequence** của batch
manifest. Thiếu một checkpoint cũng chặn chốt run, kể cả bảng đã có batch DONE.
Optional NO_DATA phải có checkpoint rõ ràng; USDA phải đủ mọi section DONE.
Lỗi ghi sequence/table còn thiếu, giữ nguyên Bronze và các commit Silver đã có.
Resume bước govern của bảng bị dở rồi chạy lại snapshot/finalize; không tạo
checkpoint DONE thủ công hoặc xóa artifact để bắt pipeline ghi lại.

Capture chỉ chạy lần đầu. Retry/clear/toàn pipeline cùng identity dùng lại map
đã lưu, kể cả crash sau capture nhưng trước finalize, hoặc run khác đã append/
compaction làm latest thay đổi. `validate_snapshot()` xác minh đủ bảng, URI,
version, tổng rows và lineage/contract/quality/counts tại **version đã chốt**.
Finalize dùng cùng validator; không capture latest để thay bằng chứng cũ.

`stages.snapshot.captured=true` giữ dấu capture qua cả lần retry lỗi. Manifest
schema vẫn là 1; manifest cũ có stage snapshot SUCCESS hoặc run SUCCESS được
nhận diện đã chốt trước khi retry. Map `{}` hợp lệ khi chỉ có USDA hoặc mọi
nguồn Silver optional đều NO_DATA; map rỗng không có nghĩa cần capture lại.
Map thiếu/sai bảng, URI hoặc evidence gây NEEDS_REVIEW và được giữ để kiểm tra.
Version/file lịch sử không còn gây lỗi đọc rõ ràng; không fallback latest.
Không tự sửa manifest lịch sử từng bị chốt sai/repin trước bản sửa này.

## Đọc version đã chốt

Chạy trong processing environment, `MANIFEST` là path trong volume:

```python
import json
from lakehouse_pipeline.silver_snapshot import read_snapshot
from lakehouse_storage.bronze_writer import verify_checksum
manifest = json.load(open(MANIFEST))
for artifact in manifest['artifacts']:
    if artifact.get('bronze_path'):
        assert verify_checksum(artifact['bronze_path'])
weather = read_snapshot(manifest['silver_snapshot'], 'weather')
```

Mọi Delta read/capture truyền storage_options. Capture dùng chung mutation lock
và Airflow pool một slot. Append/compaction sau đó không đổi version đã chốt,
nhưng vacuum có thể khiến version cũ mất file. Vacuum trong demo chỉ preview,
retention >=168 giờ; không cam kết giữ lịch sử vô hạn hoặc thay backup.

NO_DATA của nguồn optional được ghi riêng; NO_DATA nguồn bắt buộc, REJECTED,
FAILED, NEEDS_REVIEW và thiếu source chặn SUCCESS. USDA chỉ có Bronze evidence.
Gold pinned read/publish/rebuild là backlog LW16–LW18; không chạy trong pipeline.
