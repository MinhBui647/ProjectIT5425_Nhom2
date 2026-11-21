# Báo cáo – Applied Statistical Modeling (Linear/Logistic, MLE & EM)

**Mã nguồn:** `src/statistical_models/` · **Kiểm thử:** `tests/test_statistical_models.py` (17 test) ·
**Hướng dẫn chạy:** `src/statistical_models/HUONG_DAN_CHAY.md`

## 1. Dữ liệu
- Đầu vào: Gold của TV4 (`batch_features.parquet`, `fact_sensor_telemetry.parquet`). Biến dự báo lỗi lô chọn theo kết quả sàng lọc của TV5 (`correlation_target.csv`).
- Phạm vi: 240 lô năm 2025. Các lô trùng 5 giá trị MES được gộp thành **63 lô độc lập** (`group_id`), trong đó **chỉ có 3 lô lỗi**.
- Dữ liệu cảm biến: trung bình theo phút của từng lô (28.800 phút). Gold không có ô khuyết, nên dữ liệu khuyết được tạo nhân tạo để đánh giá EM.

## 2. Kết quả theo từng bước của nhiệm vụ
| Bước | Yêu cầu | Kết quả |
|---|---|---|
| 1 | ElasticNet dự báo `yield_recovery_pct`; β̂, R², MSE | β̂ của cả 13 biến = 0 (alpha = 3,6). R² train = 0, MSE train = 5,80. R² CV = −0,06 ± 0,03, MSE CV = 6,13 ± 0,15 |
| 2 | Logistic dự báo lô lỗi; ROC, AUC, Odds Ratio | AUC train 0,86; AUC validate **0,84 ± 0,02**. OR theo 1 độ lệch chuẩn: `uht_temp_min` 3,85, `flow_rate_avg` 2,00. Ngưỡng cảnh báo (Youden J) 0,049 |
| 3 | EM bằng NumPy thuần cho dữ liệu cảm biến khuyết | RMSE trên các lô chưa thấy: **EM 0,890 so với điền trung bình 1,072 (−17 %)** khi cảm biến mất tín hiệu 10–15 phút; EM 0,898 so với 1,036 (−13 %) khi mất ngẫu nhiên 10 % ô |
| 4 | Đóng gói mô hình thành file joblib | `models/yield_model.joblib`, `models/defect_model.joblib` (kèm `*_real.json` ghi hệ số và chỉ số) |

Ngoài ra, theo tài liệu "Bài toán ML": đối chiếu với OLS (Bài 1); Logistic ước lượng bằng IRLS tự viết, khớp statsmodels
với sai khác ~1e-9; kiểm định Hosmer–Lemeshow (Bài 2).

## 3. Cách train và validate
| Bài | Validate | Lý do |
|---|---|---|
| ElasticNet | k-fold theo `group_id` (5 fold × 5 lần chia); alpha chọn bằng CV lồng bên trong tập train | chỉ 63 lô, tách 80/20 thì tập valid quá nhỏ |
| Logistic | StratifiedGroupKFold (3 fold × 5 lần chia), mỗi fold có 1 lô lỗi | chỉ 3 lô lỗi, tập valid 20 % sẽ không có lô lỗi |
| EM | tách 80/20 theo lô: µ, Σ học trên 192 lô, đánh giá trên 48 lô còn lại | dữ liệu cảm biến nhiều nên tách được tập valid riêng |

Bản sao của một lô không bao giờ nằm ở cả train và valid. Sau khi validate, cả ba mô hình được học lại trên toàn bộ dữ liệu.

## 4. Kết luận
- **Tỷ lệ thu hồi không dự báo được** từ thông số công nghệ, chất lượng sữa và thời tiết (R² CV ≤ 0), đúng như đề dự đoán:
  bộ sinh dữ liệu chỉ cho yield phụ thuộc cờ lỗi. Mô hình ElasticNet vì vậy dự báo hằng số (≈ 94,6 %).
- **Nguy cơ lỗi tăng khi `uht_temp_min` và `flow_rate_avg` cao.** Mô hình phân biệt tốt (AUC 0,84), nhưng chỉ dựa trên 3 lô lỗi
  nên Odds Ratio không vững: trên 63 lô độc lập, hai hệ số không có ý nghĩa thống kê (p = 0,15; 0,36).
- **EM điền dữ liệu cảm biến tốt hơn** điền trung bình và nội suy theo thời gian, nhờ tận dụng tương quan giữa các cảm biến.
  Cải thiện rõ ở `preheat_temp`, `uht_temp`, `conductivity`; gần như không cải thiện ở `flow_rate`, vì biến này không tương quan với biến khác.
