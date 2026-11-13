# manufacturing_spc (TV5): Manufacturing EDA, SPC & Process Capability

Phân hệ 5 của đồ án IT5425, làm theo tài liệu nhiệm vụ "Manufacturing EDA, SPC & Process Capability".
Nhận dữ liệu Gold từ TV4, phân tích quá trình UHT, bàn giao kết quả cho TV6 (mô hình) và TV7 (dashboard).

## Cài vào repo (nhánh feature/olap_leading)
    src/manufacturing_spc/           <- thư mục manufacturing_spc/ trong gói này
    tests/test_manufacturing_spc.py

Thư viện: numpy, pandas, scipy, matplotlib; thêm pyarrow để đọc Parquet. (Không dùng statsmodels/polars/plotly.)

## Chạy
    pytest tests/test_manufacturing_spc.py                                   # 18 test
    python -m manufacturing_spc.run_spc --source synthetic --out reports/spc_demo
    python -m manufacturing_spc.run_spc --source parquet `
        --sensor-path <fact_sensor_telemetry.parquet> `
        --features-path <batch_features.parquet> --out reports/spc           # dữ liệu thật, không cần Docker
(Nếu gói nằm trong src/, đặt `$env:PYTHONPATH = "src"` trước. Đừng push thư mục demo lên GitHub.)

## Đối chiếu nhiệm vụ trong tài liệu
| Nhiệm vụ | Nơi thực hiện | Kết quả |
|---|---|---|
| Phạm vi: chỉ năm 2025 có batch_key | run_spc.filter_scope | |
| 1. EDA, histogram + Q-Q, ngoại lai ±3σ và IQR, boxplot theo nhóm | eda.py, figures.py | eda_summary.csv, fig01, fig02 |
| 2. X̄–S theo phút (n=60), I–MR theo lô, Phase I = tháng 1, Phase II = tháng 2–3 | spc_charts.py | spc_limits.json, fig03, fig04 |
| 3. CUSUM (k=0,5σ, h=5σ) và EWMA (λ=0,2, L=3) trên trung bình lô | spc_charts.py | fig05, fig06, spc_series_batch.csv |
| 4. 8 quy tắc Nelson trên X̄ và I; so với báo động giả kỳ vọng | nelson_rules.py | nelson_violations.csv, nelson_summary.csv |
| 5. Cp, Cpk, Cpm, Pp, Ppk (LSL 137, USL 142, Target 140; đạt khi Cpk ≥ 1,33) | capability.py | capability.csv, fig07 |
| 6. Pearson + Spearman kèm p-value, VIF > 5 loại dần | feature_screening.py | correlation_*.csv, vif.csv, selected_features.json, fig08 |
| 7. THI với fat_in, protein_in, yield; Mann–Whitney và Fisher | weather_analysis.py | correlation_thi.csv, thi_heat_stress_tests.csv, fig09 |
| 8. Cấy +1,5σ, +0,5σ, trôi dần vào chuỗi trung bình lô, ghi lô phát hiện đầu tiên | detection_test.py | detection_test.csv |

File ngoài danh sách của tài liệu (có thể bỏ nếu không cần): nelson_summary.csv, spc_series_batch.csv, spc_series.csv,
correlation_pearson*.csv, correlation_target.csv, correlation_thi.csv, thi_heat_stress_tests.csv, detection_montecarlo.csv.

## Bàn giao
- TV6: selected_features.json, correlation_target.csv (và vif.csv nếu cần xem lý do loại biến).
- TV7: spc_limits.json, nelson_violations.csv, spc_series.csv, spc_series_batch.csv, capability.csv.

## Lưu ý khi đọc kết quả
- Giới hạn Phase I tính từ tháng 01/2025, giám sát tháng 02–03. Giới hạn X̄ theo phút (n=60) rất hẹp vì chỉ chứa biến thiên
  trong một phút; biến thiên giữa các lô và tự tương quan nằm ngoài nên X̄ theo phút báo vi phạm nhiều hơn hẳn I–MR (I–MR dùng
  MR̄/d2, đã bao gồm biến thiên giữa các lô). Đây chính là điểm tài liệu yêu cầu giải thích.
- nelson_summary.csv: với biểu đồ I theo lô chỉ có vài trăm điểm nên số kỳ vọng thường < 1, tỉ lệ quan sát/kỳ vọng dao động mạnh;
  p-value Poisson chỉ mang tính tham khảo vì các cửa sổ chồng lấn.
- detection_test.csv chạy trên MỘT chuỗi thật nên lệch nhỏ (+0,5σ) có thể không bị phương pháp nào bắt trong khoảng Phase II;
  detection_montecarlo.csv chạy lặp nhiều lần để có thống kê ổn định.
- Boxplot theo sản phẩm/ca/trang trại tự lấy các cột sku_id/product_name, shift_code/shift, farm_id/farm_name nếu có trong batch_features.
- Dữ liệu mô phỏng nên tương quan cảm biến–hiệu suất ≈ 0 (đúng như ghi chú trong tài liệu): không kết luận nhân quả.
