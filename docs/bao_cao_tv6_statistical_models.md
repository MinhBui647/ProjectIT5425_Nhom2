# Báo cáo – Giám sát dây chuyền (Hotelling T²) và điền dữ liệu cảm biến bị mất (EM)

**Mã nguồn:** `src/statistical_models/` · **Kiểm thử:** `tests/test_statistical_models.py` (17 test) và
`scripts/verify_statistical_models.py` (13/13 ĐẠT) · **Hướng dẫn chạy:** `src/statistical_models/HUONG_DAN_CHAY.md`

## 1. Dữ liệu
- Đầu vào: Gold của TV4, bản 180 ngày (01/01–29/06/2025), gồm 1.440 lô của 2 dây chuyền, trong đó 68 lô lỗi.
- Hai bài dùng trung bình theo phút của 7 cảm biến (172.800 phút).
- Gold không có ô khuyết, nên dữ liệu khuyết và lỗi dây chuyền đều được tạo nhân tạo để đánh giá.

## 2. Bài toán và kết quả
| Bài | Yêu cầu | Kết quả trên dữ liệu kiểm định |
|---|---|---|
| 1. Lô cần kiểm tra | Hotelling T² giám sát 7 cảm biến cùng lúc; gắn cờ lô cần kiểm tra | Báo động giả **0,26 %** số phút (kỳ vọng 0,27 %), quy tắc 3 sigma từng cảm biến báo sai 1,65 %. Ở cùng mức báo sai theo lô (≈ 4–5 %), T² bắt **46 %** lô có 1 cảm biến lệch 2 sd (3 sigma bắt 18 %) và **100 %** lô có tổ hợp lạ (3 sigma bắt 30 %). Phase II có 46/1.192 lô được gắn cờ cần kiểm tra |
| 2. Điền dữ liệu cảm biến bị mất | EM bằng NumPy thuần cho dữ liệu cảm biến bị mất | Khi mất tín hiệu 10–15 phút: **EM 0,843**, điền trung bình 1,008 (EM giảm 16 % sai số), nội suy 0,862. Dải 95 % chứa đúng 95,0 % giá trị thật. Mất `uht_temp` quá 5 phút thì giữ lô: 86/288 lô |

## 3. Cách train và validate
| Bài | Train | Validate |
|---|---|---|
| Lô cần kiểm tra | Phase I = tháng 01: µ, Σ học bằng EM trên 236 lô đạt. Chọn k = 2 (lô có ≥ 2 phút vượt ngưỡng thì gắn cờ) để ≤ 5 % lô ổn định bị gắn cờ | Phase II = tháng 02–06, 1.192 lô chưa thấy: báo động giả; báo động giả khi mất 2 cảm biến (0,25 %); chèn lỗi giả lập 15 phút vào lô thật |
| Điền dữ liệu cảm biến bị mất | µ, Σ học trên 1.152 lô train (80 %), có 10 % ô bị xóa | 288 lô valid chưa thấy: xóa ngẫu nhiên 10 % ô; mỗi lô có 2 cảm biến mất liền 10–15 phút |

Giám sát chia theo thời gian, giống Phase I/II của TV5: học giới hạn trên quá khứ rồi áp cho tương lai.
Các phút của cùng một lô không bao giờ nằm ở cả train và valid.

## 4. Kết luận
- **T² hợp với việc giám sát dây chuyền hơn 3 sigma từng cảm biến.** T² báo sai ít hơn khoảng 6 lần. T² còn bắt được tổ hợp lạ, tức trường hợp từng cảm biến vẫn trong ngưỡng nhưng đi cùng nhau thì bất thường. Mỗi lô bị gắn cờ có ghi cảm biến gây báo động.
- **T² không dự báo được lô lỗi** (AUC 0,50). Trong bộ sinh dữ liệu, lỗi do mức UHT thấp hơn rất ít (vẫn trong vùng bình thường) và do độ chua sữa vào, mà độ chua không phải cảm biến. Muốn dự báo lô lỗi cần một mô hình riêng có `acidity_sh`.
- **EM điền tốt hơn điền trung bình, và tốt nhất khi mất tín hiệu liền một đoạn.** Khi mất ô lẻ thì nội suy theo thời gian tốt hơn. Vì vậy quy tắc là:
  - khi đang mất tín hiệu thì dùng EM;
  - khi có tín hiệu lại thì nội suy cho `flow_rate`, `power_kw`, `preheat_temp`.
