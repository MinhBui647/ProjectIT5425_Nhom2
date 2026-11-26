# statistical_models: Giám sát dây chuyền (Hotelling T²) và điền dữ liệu cảm biến bị mất (EM)

Nhận dữ liệu Gold từ TV4 (180 ngày, 01/01–29/06/2025, 1.440 lô, 2 dây chuyền). Có 2 bài toán:

1. **Giám sát dây chuyền / lô cần kiểm tra:** dùng Hotelling T² trên 7 cảm biến cùng lúc để báo phút bất thường và gắn cờ lô cần kiểm tra.
2. **Điền dữ liệu cảm biến bị mất:** EM cho N(µ, Σ), viết bằng NumPy thuần. Đi kèm dải tin cậy 95 %, cách điền cho từng cảm biến và quy tắc giữ lô.

- **Cách chạy:** [HUONG_DAN_CHAY.md](HUONG_DAN_CHAY.md)
- **Báo cáo ngắn:** [docs/bao_cao_tv6_statistical_models.md](../../docs/bao_cao_tv6_statistical_models.md)

## Cấu trúc
| File | Nội dung |
|---|---|
| config.py | tham số thuật toán (α của T², Phase I, ngưỡng gắn cờ lô, lỗi giả lập, quy tắc giữ lô, tham số EM) |
| data.py | đọc Gold, trung bình theo phút của từng lô, chia train/valid theo lô |
| hotelling.py | Bài 1: T² và đóng góp từng cảm biến, T² khi thiếu cảm biến, mô phỏng, train Phase I / validate Phase II, danh sách lô cần kiểm tra |
| em_missing.py | Bài 2: EM, kỳ vọng có điều kiện, thí nghiệm train/valid; thêm `conditional_impute_sd`, `band_coverage`, `choose_fill_method`, `outage_action` |
| figures.py, run_models.py | hình và pipeline ghi file |
| `scripts/verify_statistical_models.py` | đối chiếu 13 con số với bản mô tả, in ĐẠT hoặc LỆCH |

## Cách train và validate
| Bài | Train | Validate |
|---|---|---|
| 1. Giám sát (T²) | **Phase I = tháng 01**: học µ, Σ bằng EM trên 236 lô đạt (28.320 phút; đã bỏ 12 lô lỗi). Chọn k = số phút vượt ngưỡng tối thiểu để gắn cờ lô, sao cho ≤ 5 % lô Phase I bị gắn cờ. Kết quả **k = 2** | **Phase II = tháng 02–06**: 1.192 lô chưa thấy (143.040 phút). Đo 4 thứ: (a) báo động giả theo phút và theo lô; (b) báo động giả khi mất cảm biến; (c) chèn lỗi giả lập vào lô thật rồi đo tỉ lệ bắt được, so với quy tắc 3 sigma từng cảm biến ở cùng mức báo sai; (d) đối chiếu với cờ lỗi lô |
| 2. Điền dữ liệu cảm biến bị mất (EM) | µ, Σ học trên 3.000 phút lấy từ **1.152 lô train** (80 %), có 10 % ô bị xóa | **288 lô valid** chưa thấy, 2 kịch bản: xóa ngẫu nhiên 10 % ô; mỗi lô có 2 cảm biến mất liền 10–15 phút |

Bài 1 chia theo thời gian (giống TV5: `PHASE1_MONTHS = (1,)`), vì giới hạn kiểm soát phải học từ quá khứ rồi áp cho tương lai.
Bài 2 chia ngẫu nhiên theo lô. Các phút của cùng một lô không bao giờ nằm ở cả train và valid.
Sau khi validate, µ, Σ của EM được học lại trên toàn bộ 172.800 phút để bàn giao. Giám sát giữ µ, Σ của Phase I, đúng cách làm của biểu đồ kiểm soát.

## Kết quả trên dữ liệu Gold

### Bài 1 – Giám sát dây chuyền / lô cần kiểm tra
Ngưỡng T² là χ²(7) ở α = 0,27 %, bằng 21,8.

**Báo động giả theo phút:**

| Tập | Hotelling T² | 3 sigma từng cảm biến |
|---|---|---|
| Phase I (train) | 0,16 % | 1,52 % |
| **Phase II, lô đạt** | **0,26 %** (kỳ vọng 0,27 %) | 1,65 % |
| Phase II, khi mất 2 cảm biến (T² tính trên 5 cảm biến còn lại) | 0,25 % | – |

Phân phối T² trên Phase II khớp χ²(7) (hình `fig01`). Quy tắc 3 sigma soi 7 cảm biến riêng lẻ nên báo sai nhiều gấp khoảng 6 lần.

**Gắn cờ "lô cần kiểm tra"**, có lỗi giả lập chèn vào 1.136 lô đạt của Phase II (đoạn 15 phút, lệch 2 độ lệch chuẩn).
Mỗi quy tắc chọn k riêng trên Phase I để báo sai ≤ 5 %, nên hai cột được so ở cùng mức báo sai:

| Tình huống | T², k = 2 | 3 sigma, k = 7 |
|---|---|---|
| Lô đạt không chèn lỗi (báo sai) | 4,0 % | 4,8 % |
| `uht_temp` −2 sd (lệch 1 cảm biến) | **46 %** | 18 % |
| `uht_temp` +2 sd, `preheat_temp` −2 sd (tổ hợp lạ, tương quan 0,59) | **100 %** | 30 % |

Mô phỏng từ N(µ, Σ) (`detection_check`) cho kết quả tương tự: báo sai 0,29 %; bắt tổ hợp lạ 68 % số phút, còn 3 sigma bắt 32 %.
Ở kịch bản lệch 1 cảm biến, 3 sigma bắt nhiều phút hơn (17,5 % so với 10,6 %). Nhưng vì nó báo sai nhiều, phải nâng k lên 7, nên tính theo lô thì T² vẫn tốt hơn.

**Danh sách lô cần kiểm tra (Phase II):** 46/1.192 lô (`lo_can_kiem_tra_phase2.csv`). Mỗi lô có ghi lý do và cảm biến góp nhiều nhất vào T².
Lô đầu danh sách là `BATCH_20250213_03_LINE_UHT_1` với 56 phút vượt ngưỡng, chủ yếu do `flow_rate`: lưu lượng cả lô lệch xa mức thường.
Tính trên các phút vượt ngưỡng, cảm biến góp nhiều nhất là `flow_rate` (91), `preheat_temp` (72) và `uht_temp` (64).

**Đối chiếu với cờ lỗi lô:** T² **không** phân biệt được lô lỗi (AUC 0,50). Chỉ 1,8 % lô lỗi bị gắn cờ, so với 4,0 % lô đạt.
Lý do nằm ở bộ sinh dữ liệu (`docs/ground_truth.md`). Lỗi do hai yếu tố gây ra:
- mức UHT của lô thấp hơn một chút: drift có sd 0,1 °C, vẫn nằm trong vùng vận hành bình thường;
- độ chua sữa vào `acidity_sh` cao: đây là chỉ tiêu phòng thí nghiệm, không phải cảm biến.

Vì vậy lô lỗi không phải là một trạng thái dây chuyền bất thường. T² dùng để phát hiện dây chuyền chạy khác thường; dự báo lô lỗi cần một mô hình riêng có `acidity_sh`.

### Bài 2 – Điền dữ liệu cảm biến bị mất (7 biến, z-score theo tập train)
| Tập | Số ô bị xóa | EM | Điền trung bình | Nội suy theo thời gian |
|---|---|---|---|---|
| train, MCAR 10 % | 2.140 | 0,865 | 1,039 | – |
| valid, MCAR 10 % | 24.147 | 0,855 | 1,001 | **0,810** |
| **valid, mất tín hiệu 10–15 phút** | 7.211 | **0,843** | 1,008 | 0,862 |

- **EM luôn tốt hơn điền trung bình**: sai số giảm 15 % khi mất ngẫu nhiên, 16 % khi mất tín hiệu. EM hội tụ sau 6 vòng, log-likelihood không giảm.
- **So với nội suy:** khi mất ô lẻ, nội suy giữa hai phút kề nhau tốt hơn EM. Khi mất tín hiệu liền 10–15 phút, EM tốt hơn.
  Với `flow_rate` và `power_kw`, nội suy gần như đúng tuyệt đối (RMSE 0,08 và 0,13), vì mức của hai biến này cố định theo từng lô (`flow_dev`, sản phẩm).
- **Cách điền cho từng cảm biến** (`em_cach_dien_tung_cam_bien.csv`):
  - Khi đang mất tín hiệu thì chỉ dùng được EM, vì nội suy cần số đo ở cả hai đầu đoạn mất.
  - Khi đã có tín hiệu lại thì nội suy cho `flow_rate`, `power_kw`, `preheat_temp`, và dùng EM cho 4 cảm biến còn lại.
- **Dải 95 % của giá trị điền** (±1,96 sd có điều kiện) chứa đúng **95,3 %** (MCAR) và **95,0 %** (mất tín hiệu) giá trị thật. Độ bất định EM báo ra là đáng tin.
- **Quy tắc giữ lô:** mất `uht_temp` quá 5 phút thì giữ lô và bắt buộc kiểm vi sinh. Lý do: nhiệt tiệt trùng là điểm kiểm soát tới hạn, không được cho xuất lô bằng số ước lượng.
  Trong kịch bản mất tín hiệu, 86/288 lô valid bị mất `uht_temp`, và cả 86 lô đều phải giữ vì đoạn mất dài 10–15 phút.

`scripts/verify_statistical_models.py` đạt **13/13** mục so với số mong đợi của bản mô tả.

## File kết quả
| File | Nội dung |
|---|---|
| `models/monitoring_t2.json` | µ, Σ Phase I, ngưỡng, k, kết quả validate của giám sát |
| `models/em_sensor_mvn.json` | µ̂, Σ̂ của EM (học trên toàn bộ dữ liệu), kết quả validate, cách điền, quy tắc giữ lô |
| `reports/models/t2_*.csv`, `lo_can_kiem_tra_phase2.csv` | bảng của bài giám sát |
| `reports/models/em_*.csv` | bảng của bài điền dữ liệu cảm biến bị mất |
| `reports/models/figures/` | `fig01` phân phối T², `fig02` gắn cờ lô và bắt lỗi, `fig03` log-likelihood EM, `fig04` RMSE EM |
| `reports/models/summary.json` | tóm tắt toàn bộ |
