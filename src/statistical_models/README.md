# statistical_models: Mô hình thống kê – Linear/Logistic, MLE & EM

Nhận dữ liệu Gold từ TV4 và danh sách biến từ TV5.

- **Cách chạy:** [HUONG_DAN_CHAY.md](HUONG_DAN_CHAY.md)
- **Báo cáo ngắn:** [docs/bao_cao_tv6_statistical_models.md](../../docs/bao_cao_tv6_statistical_models.md)

Phần dưới đây là chi tiết kỹ thuật và kết quả đầy đủ.

## Cấu trúc
| File | Nội dung |
|---|---|
| config.py | biến của từng bài, tham số thuật toán |
| data.py | phạm vi 2025 + n_readings > 0, gán `group_id`, khử trùng, điền giá GDT, trung bình theo phút, chia fold / train–valid theo nhóm |
| linear_models.py | Bài 1: OLS, OLS SE cluster, ElasticNet + CV lồng nhau theo nhóm, MSE, chẩn đoán phần dư |
| logistic.py | Bài 2: IRLS tự viết (Newton–Raphson, giảm bước), SE cluster tự viết, ROC/AUC, Youden J, Hosmer–Lemeshow |
| em_missing.py | Bài 3: EM cho N(µ, Σ) có dữ liệu khuyết (NumPy thuần), kỳ vọng có điều kiện, thí nghiệm train/valid, kịch bản mất tín hiệu |
| figures.py, run_models.py | hình và pipeline ghi file |

## Bước chuẩn bị chung
240 lô năm 2025 → **63 `group_id`** (57 nhóm có 4 bản sao). Trong cùng một nhóm, các biến cảm biến theo lô cũng giống hệt nhau;
chỉ thời tiết và ngày là khác. **12 dòng lỗi thực chất chỉ là 3 lô lỗi độc lập.**
`fao_dairy_index` toàn NaN nên tự động bị loại. `gdt_smp_usd` thiếu 24 lô (01–06/01/2025, trước phiên GDT đầu tiên) nên lấy theo phiên gần nhất.

## Cách train và validate
| Bài | Train | Validate | Lý do |
|---|---|---|---|
| 1. ElasticNet / OLS | học β̂; alpha, l1_ratio chọn bằng CV bên trong tập train | k-fold theo `group_id`, 5 fold × **5 lần chia**; R², MSE dạng trung bình ± độ lệch chuẩn | chỉ 63 lô độc lập |
| 2. Logistic | IRLS học β̂ → Odds Ratio | StratifiedGroupKFold 3 fold (mỗi fold đúng 1 lô lỗi) × 5 lần chia; ngưỡng Youden lấy từ dự báo validate | chỉ 3 lô lỗi |
| 3. EM | µ, Σ học trên 3.000 phút của **192 lô train** (có 10 % ô bị xóa) | **48 lô valid** chưa thấy, 2 kịch bản: xóa ngẫu nhiên 10 % ô; mỗi lô có 2 cảm biến mất liền 10–15 phút | dữ liệu cảm biến nhiều |

Bản sao của một lô không bao giờ nằm ở cả train và valid. Sau khi validate, các mô hình được học lại trên toàn bộ dữ liệu:
ElasticNet và Logistic trên 240 lô, µ và Σ của EM trên 28.800 phút.

## Kết quả trên dữ liệu Gold thật

### Bài 1 – Tỷ lệ thu hồi (13 biến)
| Mô hình | n | R² train | MSE train | R² CV | MSE CV |
|---|---|---|---|---|---|
| OLS, lô độc lập | 63 | 0,106 (R² hiệu chỉnh −0,13; F p = 0,94) | 4,99 | −0,62 ± 0,14 | 9,06 ± 0,76 |
| OLS, SE cluster theo `group_id` | 240 | 0,097 | 5,23 | −0,52 ± 0,14 | 8,81 ± 0,84 |
| ElasticNet, CV lồng nhau | 63 | – | – | −0,06 ± 0,03 | 5,93 ± 0,17 |
| **ElasticNet, CV lồng nhau** | 240 | 0 (alpha = 3,6, l1_ratio = 0,1, cả 13 β̂ = 0) | 5,80 | **−0,06 ± 0,03** | **6,13 ± 0,15** |
| ElasticNet, KFold thường (rò rỉ, chỉ để so sánh) | 240 | – | – | −0,01 ± 0,02 | 5,83 ± 0,14 |

Phương sai của yield là 5,80; MSE ≈ phương sai nghĩa là mô hình không tốt hơn việc dự báo bằng trung bình.
Không hệ số OLS nào có ý nghĩa (mọi KTC 95 % chứa 0). Phần dư mô hình 63 lô xấp xỉ chuẩn (JB p = 0,77; Shapiro p = 0,80; BP p = 0,35).
**Đúng như đề dự đoán:** trong `generate_mes_data.py`, yield chỉ phụ thuộc cờ lỗi (95 ± 2 nếu đạt, 88 ± 3 nếu lỗi).
Vì ElasticNet loại hết biến, `yield_model.joblib` dự báo hằng số (≈ 94,6 %). Đây là kết quả trung thực của dữ liệu.

### Bài 2 – Lô lỗi (`uht_temp_min`, `flow_rate_avg`)
- IRLS tự viết **trùng statsmodels** (sai khác tối đa ~1e-9) về hệ số, SE MLE, SE cluster và log-likelihood. Hội tụ sau 7–8 vòng.
- Mẫu chính: 240 lô, SE cluster. OR theo 1 độ lệch chuẩn: `uht_temp_min` **3,85** (KTC 1,27–11,7; p = 0,017),
  `flow_rate_avg` **2,00** (1,03–3,86; p = 0,040).
- Kiểm tra độ nhạy: 63 lô độc lập, SE MLE. OR gần như giữ nguyên (3,60; 1,87) nhưng **không có ý nghĩa** (p = 0,15; 0,36).
- AUC train 0,86 (240 lô), 0,87 (63 lô). AUC validate: **0,84 ± 0,02** (240 lô), 0,85 ± 0,02 (63 lô).
- Ngưỡng Youden J = 0,049 → độ nhạy 1,0, độ đặc hiệu 0,71.
- Hosmer–Lemeshow trên 63 lô độc lập: p = 0,12 (train), 0,96 (validate) → không bác bỏ độ phù hợp.
- `defect_model.joblib` là LogisticRegression không phạt (C = ∞), tức đúng nghiệm MLE; hệ số trùng IRLS (đã có test).

**Cảnh báo:** chỉ có 3 lô lỗi độc lập. SE cluster khi có rất ít cụm chứa sự kiện thường bị ước lượng thấp, nên mức "có ý nghĩa" ở mẫu
240 lô không vững. Hosmer–Lemeshow trên 240 lô (p ≈ 0) không dùng được vì vi phạm giả định các quan sát độc lập.

### Bài 3 – EM điền dữ liệu cảm biến khuyết (7 biến, z-score theo tập train)
| Tập | Số ô bị xóa | EM | Điền trung bình | Nội suy theo thời gian | EM so với trung bình |
|---|---|---|---|---|---|
| train, MCAR 10 % | 2.140 | 0,920 | 1,018 | – | −9,6 % |
| valid, MCAR 10 % | 4.127 | **0,898** | 1,036 | 1,070 | −13,3 % |
| valid, mất tín hiệu 10–15 phút | 1.203 | **0,890** | 1,072 | 1,056 | −17,0 % |

EM hội tụ sau 6 vòng, log-likelihood không giảm. Kết quả trên tập valid không kém train, nên µ, Σ học được dùng tốt cho lô mới.
Cải thiện rõ nhất ở `preheat_temp`, `uht_temp`, `conductivity`; gần như không cải thiện ở `flow_rate`, `homo_press_stage1`, vì hai biến này
hầu như không tương quan với biến khác. Tương quan giữa các cảm biến theo phút (0,2–0,5) đến từ drift chung của từng lô trong bộ sinh dữ liệu.

## File kết quả
| File | Nội dung |
|---|---|
| `models/yield_model.joblib`, `models/defect_model.joblib` | mô hình đóng gói (Bước 4 kế hoạch) |
| `models/yield_model_real.json`, `models/defect_model_real.json`, `models/em_sensor_mvn.json` | tham số và chỉ số của từng bài |
| `reports/models/*.csv`, `reports/models/figures/`, `reports/models/summary.json` | bảng, hình, tóm tắt |