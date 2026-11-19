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

`requirements.txt` đã có đủ thư viện cho phần TV6: numpy, pandas, scipy, statsmodels, scikit-learn, joblib, matplotlib, pyarrow.

## 2. Chuẩn bị dữ liệu
Cần 2 file Gold do TV4 xuất ra:

| File | Dùng cho |
|---|---|
| `batch_features.parquet` | Bài 1, Bài 2 |
| `fact_sensor_telemetry.parquet` | Bài 3, và biến `homo_press1_avg` của Bài 1 |

Có 2 cách:
- **Cách 1:** chép 2 file vào `data/03_gold_marts/` trong repo (thư mục `data/` đã nằm trong `.gitignore`), rồi chạy mà không cần tham số đường dẫn.
- **Cách 2:** để dữ liệu ở chỗ khác và truyền đường dẫn bằng `--sensor-path`, `--features-path` (xem mục 3).

## 3. Chạy 3 bài toán
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

Thời gian chạy khoảng 1 phút. Khi xong, màn hình in tóm tắt kết quả (giống nội dung `reports/models/summary.json`).

Tùy chọn:

| Tham số | Ý nghĩa | Mặc định |
|---|---|---|
| `--tasks 1,2` | chỉ chạy một số bài: 1 yield, 2 lô lỗi, 3 EM | `1,2,3` |
| `--out` | thư mục bảng và hình | `reports/models` |
| `--models-dir` | thư mục mô hình | `models` |
| `--no-figs` | không vẽ hình | có vẽ |

## 4. Kết quả sinh ra
| File | Nội dung |
|---|---|
| `models/yield_model.joblib` | Bài 1: Pipeline ElasticNet (điền trung vị → chuẩn hóa → ElasticNet) |
| `models/defect_model.joblib` | Bài 2: Pipeline Logistic (chuẩn hóa → LogisticRegression không phạt = MLE) |
| `models/yield_model_real.json` | β̂, R², MSE, KTC 95 % của OLS, kết quả kiểm định chéo, chẩn đoán phần dư |
| `models/defect_model_real.json` | β̂, Odds Ratio, AUC, ngưỡng Youden J, Hosmer–Lemeshow, so sánh IRLS và statsmodels |
| `models/em_sensor_mvn.json` | µ̂, Σ̂ của EM (học trên toàn bộ dữ liệu) và kết quả validate |
| `reports/models/*.csv` | bảng chi tiết của từng bài |
| `reports/models/figures/*.png` | 7 hình: phần dư, đường hệ số ElasticNet, dự báo CV, ROC, Hosmer–Lemeshow, log-likelihood EM, RMSE EM |
| `reports/models/summary.json` | tóm tắt toàn bộ |

Dùng mô hình đã lưu:

```python
import joblib, pandas as pd
m = joblib.load("models/defect_model.joblib")
p = m.predict_proba(pd.DataFrame({"uht_temp_min": [137.0], "flow_rate_avg": [5000.1]}))[:, 1]
```

Tên các cột đầu vào nằm ở trường `features` của `yield_model_real.json` và `defect_model_real.json`.
Điền dữ liệu cảm biến bị mất bằng µ̂, Σ̂ của EM: `statistical_models.em_missing.conditional_impute(X, mu, sigma)`,
với `mu = mu_hat_original`, `sigma = sigma_hat_original` lấy trong `em_sensor_mvn.json`.

## 5. Chạy kiểm thử
```powershell
python -m pytest tests/test_statistical_models.py -q     # 17 test, khoảng 30 giây
```

## 6. Lỗi thường gặp
| Lỗi | Cách xử lý |
|---|---|
| `ModuleNotFoundError: statistical_models` hoặc `manufacturing_spc` | chưa đặt `$env:PYTHONPATH = "src"`, hoặc không chạy tại thư mục gốc repo |
| `ModuleNotFoundError` khi chạy pytest | chưa cài đủ `requirements.txt` (`tests/conftest.py` của nhóm cần `polars`) |
| `FileNotFoundError` file parquet | kiểm tra lại mục 2, hoặc đường dẫn `--sensor-path`, `--features-path` |
| `UnicodeEncodeError` khi in ra console | `$env:PYTHONIOENCODING = "utf-8"` |
