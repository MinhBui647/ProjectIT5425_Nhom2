# Hướng dẫn chạy code (statistical_models)

Mọi lệnh chạy trong **PowerShell, tại thư mục gốc của repo** (`ProjectIT5425_Nhom2`).

## 1. Chuẩn bị môi trường (làm một lần)
Dùng môi trường chung của nhóm (Python 3.11, 64-bit; mục 2.2 kế hoạch nhóm):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1          # nếu bị chặn: Set-ExecutionPolicy Unrestricted -Scope Process
python -m pip install --upgrade pip
pip install -r requirements.txt
```

`requirements.txt` đã có đủ thư viện cho phần TV6: numpy, pandas, scipy, scikit-learn, matplotlib, pyarrow.

## 2. Chuẩn bị dữ liệu
Cần 2 file Gold do TV4 xuất ra, bản 180 ngày (01/01–29/06/2025, 1.440 lô):

| File | Dùng cho |
|---|---|
| `fact_sensor_telemetry.parquet` | cả 2 bài: trung bình theo phút của 7 cảm biến |
| `batch_features.parquet` | Bài 1: ngày của lô (chia Phase I/II), mã lô, dây chuyền, cờ lỗi để đối chiếu |

Có 2 cách:
- **Cách 1:** chép 2 file vào `data/03_gold_marts/` trong repo (thư mục `data/` đã nằm trong `.gitignore`), rồi chạy mà không cần tham số đường dẫn.
- **Cách 2:** để dữ liệu ở chỗ khác và truyền đường dẫn bằng `--sensor-path`, `--features-path` (xem mục 3).

## 3. Chạy 2 bài toán
```powershell
$env:PYTHONPATH = "src"
python -m statistical_models.run_models
```

Nếu dữ liệu nằm ngoài repo:

```powershell
$env:PYTHONPATH = "src"
$G = "D:\Downloads\Project IT5425\data\03_gold_marts"
python -m statistical_models.run_models `
    --sensor-path "$G\fact_sensor_telemetry.parquet" `
    --features-path "$G\batch_features.parquet"
```

Thời gian chạy khoảng 15 giây; phần lớn là đọc file cảm biến 10,4 triệu dòng. Khi xong, màn hình in tóm tắt kết quả, giống nội dung `reports/models/summary.json`.

Tùy chọn:

| Tham số | Ý nghĩa | Mặc định |
|---|---|---|
| `--tasks 1` | chỉ chạy một bài: 1 giám sát (lô cần kiểm tra), 2 điền dữ liệu cảm biến bị mất (EM) | `1,2` |
| `--out` | thư mục bảng và hình | `reports/models` |
| `--models-dir` | thư mục mô hình | `models` |
| `--no-figs` | không vẽ hình | có vẽ |

Đối chiếu nhanh với số mong đợi của bản mô tả (cần dữ liệu ở `data/03_gold_marts/`):

```powershell
$env:PYTHONPATH = "src"
python scripts/verify_statistical_models.py      # in từng mục ĐẠT / LỆCH, hiện tại 13/13 ĐẠT
```

## 4. Kết quả sinh ra
| File | Nội dung |
|---|---|
| `models/monitoring_t2.json` | Bài 1: µ, Σ (Phase I), ngưỡng χ², k, kết quả validate |
| `models/em_sensor_mvn.json` | Bài 2: µ̂, Σ̂ của EM (học trên toàn bộ dữ liệu), kết quả validate, cách điền từng cảm biến, quy tắc giữ lô |
| `reports/models/t2_bao_dong_theo_phut.csv` | tỉ lệ báo động giả theo phút: T² so với 3 sigma, Phase I và Phase II |
| `reports/models/t2_gan_co_theo_lo.csv` | tỉ lệ gắn cờ lô (lô đạt, lô lỗi) và AUC so với cờ lỗi |
| `reports/models/t2_loi_gia_lap.csv` | tỉ lệ bắt lỗi giả lập theo phút và theo lô |
| `reports/models/lo_can_kiem_tra_phase2.csv` | **danh sách lô cần kiểm tra** (Phase II), có lý do và cảm biến góp nhiều nhất |
| `reports/models/em_rmse_zscore.csv`, `em_rmse_original_units.csv` | RMSE của EM, điền trung bình và nội suy |
| `reports/models/em_cach_dien_tung_cam_bien.csv` | nên điền dữ liệu từng cảm biến bằng EM hay nội suy |
| `reports/models/em_hanh_dong_khi_mat_uht.csv` | lô nào phải giữ khi mất `uht_temp` |
| `reports/models/em_filled_*_zscore.csv` | giá trị đã điền trên tập valid (khoảng 7 MB mỗi file) |
| `reports/models/figures/*.png` | 4 hình: phân phối T², gắn cờ lô và bắt lỗi, log-likelihood EM, RMSE EM |
| `reports/models/summary.json` | tóm tắt toàn bộ |

Dùng kết quả đã lưu:

```python
import json, numpy as np
from statistical_models.hotelling import t2_scores
from statistical_models.em_missing import conditional_impute_sd, outage_action

m = json.load(open("models/monitoring_t2.json", encoding="utf-8"))
cols = m["variables"]
mu, sigma = np.array([m["mu_original"][c] for c in cols]), np.array(m["sigma_original"])
scores, contrib = t2_scores(X, mu, sigma)          # X: n phút × 7 cảm biến (theo thứ tự cols), được có NaN
lo_can_kiem_tra = scores["signal"].sum() >= m["rule_lo_can_kiem_tra"]["k"]

e = json.load(open("models/em_sensor_mvn.json", encoding="utf-8"))
mu_e, sigma_e = np.array([e["mu_hat_original"][c] for c in cols]), np.array(e["sigma_hat_original"])
filled, sd = conditional_impute_sd(X, mu_e, sigma_e)   # giá trị điền và độ lệch chuẩn (dải ±1,96 sd)
outage_action("uht_temp", 12)                           # {"action": "GIU_LO", ...}
```

## 5. Chạy kiểm thử
```powershell
python -m pytest tests/test_statistical_models.py -q     # 17 test, khoảng 10 giây
```

## 6. Lỗi thường gặp
| Lỗi | Cách xử lý |
|---|---|
| `ModuleNotFoundError: statistical_models` hoặc `manufacturing_spc` | chưa đặt `$env:PYTHONPATH = "src"`, hoặc không chạy tại thư mục gốc repo |
| `ModuleNotFoundError` khi chạy pytest | chưa cài đủ `requirements.txt` (`tests/conftest.py` của nhóm cần `polars`) |
| `FileNotFoundError` file parquet | kiểm tra lại mục 2, hoặc đường dẫn `--sensor-path`, `--features-path` |
| `ValueError: Cần dữ liệu ở cả Phase I và Phase II` | dữ liệu chỉ có tháng 01; Bài 1 cần thêm các tháng sau để validate |
| `UnicodeEncodeError` khi in ra console | `$env:PYTHONIOENCODING = "utf-8"` |
