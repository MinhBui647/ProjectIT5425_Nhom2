# Review độc lập R01–R05

Phạm vi: diff `b0d9245..122af47` gồm năm commit `202f098`, `d75ad3e`,
`fe9b85f`, `64c442f`, `122af47`; đối chiếu section 11 của `tasks/plan.md`
và checklist R01–R05 trong `tasks/todo.md`.

**Kết luận:** không phát hiện finding Required/High/Medium trong phạm vi này.
Đã đọc regression tests trước, sau đó kiểm implementation và caller
worker/P3/maintenance. Guard S3 từ chối URI mơ hồ, so bucket và segment;
checkpoint gắn đích và chặn đổi đích trước skip/resume; sửa timezone và hai lỗi
Bronze phù hợp kế hoạch. Không có đề xuất cấu trúc bắt buộc.

Đã đọc thêm ba test MinIO R06 mới trong `tests/test_storage_minio_demo.py` ở
working tree tại thời điểm review; chưa thấy vấn đề qua đọc mã.

## Kiểm chứng độc lập

Lệnh chính xác, chạy tại repo root:

```bash
PYTHONPATH=src .venv/bin/python -B -m pytest tests/test_worker_state.py tests/test_storage_regressions.py tests/test_bronze_writer.py tests/test_maintenance_policy.py -q
```

Output cuối từ tool:

```text
....................................................................... [ 80%]
..................                                                       [100%]
90 passed in 1.98s
```

Lần chạy trước thiếu `PYTHONPATH=src`: 89 passed, 1 failed vì subprocess của
`test_only_one_instance_and_release_after_kill` không import được
`lakehouse_storage`. Chạy lại đúng môi trường cho kết quả trên; không thay
source hoặc test để xử lý lỗi môi trường này.

## Mutation test

Thay `_s3_parts` chỉ trong bộ nhớ của tiến trình Python, bỏ điều kiện chặn `%`,
rồi chạy nhóm regression URI. Không ghi source trên đĩa:

```bash
PYTHONPATH=src .venv/bin/python -B - <<'PY'
import inspect
import pytest
from lakehouse_storage import service_config
source = inspect.getsource(service_config._s3_parts)
source = source.replace("or '%' in uri", "or False")
exec(source, service_config.__dict__)
raise SystemExit(pytest.main(['tests/test_worker_state.py::test_s3_namespace_rejects_ambiguous_uri', '-q']))
PY
```

Output rút gọn từ tool:

```text
FFFFFF.....FF..                                                          [100%]
E       Failed: DID NOT RAISE ValueError
8 failed, 7 passed in 0.17s
```

Tám case thất bại như mong đợi: `%2E%2E/escaped/table`,
`%2e%2e/escaped/table`, `.%2e/escaped/table`, `%252e%252e/escaped/table`,
`%2f../escaped/table`, `%5c../escaped/table`, `bad%/table`, `bad%GG/table`.
Nhóm test phát hiện được mutation bỏ guard encoding.

## Giới hạn

- Reviewer không chạy HTTP worker, MinIO hoặc full suite; root agent xác minh
  riêng các kiểm tra này. Không suy diễn 90 test thành nghiệm thu R06.
- Review này chưa gồm tài liệu R07 cuối cùng.
- Reviewer không sửa source/test trên đĩa. Mutation chỉ tồn tại trong bộ nhớ
  tiến trình đã kết thúc; báo cáo này là artifact duy nhất reviewer tạo.

## Bổ sung review cuối: test và tài liệu R07

Đã đọc diff commit `5303e6c` của `tests/test_mock_scenarios.py`, các thay đổi
`docs/storage_operations.md`, `src/lakehouse_storage/API_REFERENCE.md` và báo cáo
`reports/storage/pr_fixes_20261117/README.md`. Không có finding
Required/High/Medium bổ sung. Test dựng URL batch đích rõ ràng, giữ run ID và
toàn bộ assertion cũ; hai ID prefix 1/2 đóng trường hợp từng gây lỗi ngẫu nhiên.
Tài liệu khớp guard URI, checkpoint legacy, timezone, fallback ngày và giới hạn
vận hành tuần tự; không tuyên bố đã sửa lỗi P3 hoặc production-ready.

Đã đối chiếu trực tiếp cuối log do root tạo:

```text
final_full_suite.log: 493 passed, 1 skipped in 100.88s (0:01:40)
r06_minio.log:        10 passed in 18.69s
```

Skip duy nhất được log chỉ rõ tại `test_data_governance_chdb_acceptance.py:51`,
cần opt-in `CHDB_SILVER_ACCEPTANCE=1` và MinIO. Không chạy lại suite trong bước
review cuối. Các con số tích hợp trên được xác minh bằng đọc evidence của root,
không phải lần chạy độc lập của reviewer. Không sửa production source/tests.
