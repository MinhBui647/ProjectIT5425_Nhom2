# Đáp án của bộ sinh dữ liệu (ground truth)

Bộ sinh `src/lakehouse_storage/generate_mes_data.py` tạo dữ liệu **mô phỏng** theo một cơ chế biết trước. Các hệ số dưới đây là tham chiếu để đánh giá mô hình trên dữ liệu mô phỏng. Ước lượng và khoảng tin cậy phụ thuộc cỡ mẫu, biến điều chỉnh, sai số đo và việc chặn yield ở 100; không yêu cầu mọi khoảng tin cậy của một lần chạy đều chứa hệ số thật. Kết quả không chứng minh nhà máy thật vận hành theo cơ chế này.

Giá trị trong code nằm ở hằng `GROUND_TRUTH`. Khi đổi hệ số, sửa cả code và tài liệu rồi khởi động tiến trình mới; không sửa dictionary giữa lúc chạy vì `defect_intercept()` có cache. Nhiễu từng cảm biến và hệ số drift còn được mô tả tại hàm sinh cảm biến, không phải mọi hằng số đều nằm trong dictionary này.

## Phạm vi dữ liệu

| Mục | Giá trị |
|---|---|
| Thời gian | 90 ngày, 2025-01-01 đến 2025-03-31 |
| Dây chuyền | `LINE_UHT_1`, `LINE_UHT_2` |
| Lô | 4 lô mỗi ngày mỗi dây chuyền, 2 giờ mỗi lô, bắt đầu 08:00, 10:00, 12:00, 14:00 giờ VN. Tổng 720 lô |
| Cảm biến | 1 mẫu mỗi giây, 7.200 mẫu mỗi lô |
| Seed | Với 90 ngày này: seed ngày = 5425 + d; seed lô = seed_ngày × 4 + i + 1.000.000 × chỉ số dây chuyền trong config. Cảm biến cộng 10.000.000, MES cộng 20.000.000 để dùng seed khác trong phạm vi này |

## Nguyên liệu (MES)

| Biến | Phân phối |
|---|---|
| Trang trại | Chọn đều trong 5 trang trại |
| `fat_in` (%) | N(3,5 + lệch trang trại, 0,1). Lệch: MOC_CHAU +0,08; LAM_DONG +0,04; BA_VI 0; NGHE_AN −0,04; CU_CHI −0,08 |
| `protein_in` (%) | N(3,2; 0,05) |
| `acidity_sh` (°SH) | N(6,5; 0,2) |
| `sku_id` | Chọn đều trong UHT_PURE, UHT_SWEETENED, UHT_LOWFAT |

## Cảm biến

Mỗi lô có biến ẩn `drift ~ N(0; 0,1)` tác động lên mức cảm biến theo các hệ số trong bảng. Trong một lô, khi giữ drift cố định, nhiễu từng mẫu được sinh độc lập; tương quan ở mức mẫu khi gộp nhiều lô có thể yếu. Không suy ra chất lượng phương pháp EM chỉ từ việc dùng chung drift; thí nghiệm missing-data cần được thiết kế riêng.

| Cảm biến | Mức của lô | Nhiễu trong lô (sd) |
|---|---|---|
| `uht_temp` (°C) | 140 + drift | 0,8 |
| `preheat_temp` (°C) | 80 + drift | 0,5 |
| `homo_press_stage1` (bar) | 180 + drift | 3,0 |
| `homo_press_stage2` (bar) | 40 + drift | 1,0 |
| `flow_rate` (L/h) | 5000 + flow_dev + 10 × drift, với flow_dev ~ N(0; 150) | 80 |
| `conductivity` (mS/cm) | 4,8 + 0,1 × drift | 0,1 |
| `power_kw` (kW) | 75 + drift + 0,01 × flow_dev + lệch sản phẩm. Lệch: UHT_PURE 0; UHT_SWEETENED +3; UHT_LOWFAT −2 | 2,0 |

Đơn vị `flow_rate` được quy ước là **lít/giờ**, nên một lô 2 giờ xử lý khoảng 10.000 lít sữa.

## Cờ lỗi lô

Cơ chế mô phỏng giả định mức UHT thấp hơn hoặc acidity cao hơn làm tăng xác suất lỗi; đây không phải mô hình an toàn thực phẩm đã hiệu chuẩn từ nhà máy:

```
uht_level = 140 + drift
logit p   = a − 8,0 × (uht_level − 140) + 1,5 × (acidity_sh − 6,5) / 0,2
lỗi       ~ Bernoulli(p)
```

Hệ số chặn `a` được tính sao cho xác suất lỗi trung bình theo phân phối drift và acidity của bộ sinh bằng `defect_rate` (mặc định 5 %). Tỷ lệ quan sát của một mẫu hữu hạn có thể khác 5 %. Với 5 % thì a ≈ −4,07. Hàm tính là `defect_intercept()`.

Quy về đơn vị gốc: hệ số của `uht_level` là **−8,0 mỗi °C**, hệ số của `acidity_sh` là **+7,5 mỗi °SH**.

Lô lỗi có `microbiology_status = FAILED`.

Lưu ý: `uht_temp_avg` trong Gold là trung bình 7.200 mẫu, nên bằng `uht_level` cộng một sai số rất nhỏ (sd khoảng 0,01 °C). `uht_temp_min` thì nhiễu hơn nhiều.

## Yield

```
yield = 95 + 8 × (fat_in − 3,5) + 5 × (protein_in − 3,2) − 0,5 × (uht_level − 140)
        − 6 × lỗi + lệch dây chuyền + N(0; 1),   tối đa 100
lệch dây chuyền: LINE_UHT_1 = 0; LINE_UHT_2 = −0,8
```

Ca (`shift_code`), sản phẩm (`sku_id`) và trang trại **không** ảnh hưởng trực tiếp lên yield. Trang trại chỉ ảnh hưởng qua độ béo.

## Những gì mô hình cần tìm lại

| Bài | Đáp án |
|---|---|
| 1B yield kỳ vọng | Hệ số sinh: fat = 8, protein = 5, UHT = −0,5, cờ lỗi = −6. Đánh giá khi điều chỉnh đúng các biến và dây chuyền; lưu ý chặn yield tại 100 |
| 1C so sánh nhóm | Hiệu ứng trực tiếp của LINE_UHT_2 là −0,8 điểm khi giữ các biến khác cố định. Ca và sản phẩm không có hiệu ứng trực tiếp trong cơ chế yield; chênh lệch mẫu vẫn có thể xuất hiện |
| 2 lô lỗi | Hệ số `uht_temp_avg` âm, gần −8 mỗi °C. Hệ số `acidity_sh` dương, gần +7,5 mỗi °SH |
| Năng lượng | Công suất có hệ số 0,01 kW cho mỗi L/h của biến ẩn `flow_dev`, không phải hệ số hồi quy bắt buộc trên lưu lượng từng giây. SKU offsets: SWEETENED +3 kW, LOWFAT −2 kW so với PURE |

## Điều kiện nghiệm thu dữ liệu

| Kiểm tra | Kết quả mong đợi | Kết quả khi sinh 90 ngày × 2 dây chuyền |
|---|---|---|
| Số lô khác nhau | Bằng số lô | 720 / 720 |
| Tương quan độ béo và UHT trung bình | Đối chiếu bộ seed cố định | −0,0470881682 |
| Tỉ lệ lô lỗi | Đối chiếu mẫu cố định này; 4–6 % là tham khảo, không phải bảo đảm với mọi seed | 4,7 %, 34 lô lỗi |
| Khóa lô và mapping | `batch_id` duy nhất; sensor IDs và mapping khớp MES | Đã đạt trên 720 lô từ file Bronze |

## Cách sinh lại

```bash
export PYTHONPATH=src
.venv/bin/python -m lakehouse_storage.generate_mes_data \
  --date 2025-01-01 --days 90 --seed 5425 --write-bronze \
  --bronze-root data/mes_mechanism_v1/bronze \
  --output-dir data/mes_mechanism_v1/synthetic
.venv/bin/python scripts/verify_mes_dataset.py \
  --bronze-root data/mes_mechanism_v1/bronze \
  --mapping-file data/mes_mechanism_v1/synthetic/batch_farm_map.json \
  --start-date 2025-01-01 --days 90 --seed 5425 \
  --output reports/mes_generator/acceptance.json
```

Chạy trong Linux/WSL với dependencies hiện có và root mới. Bronze append, không dedup theo batch ID. Nếu sinh tiếp bằng lệnh riêng, tăng seed đầu đợt đúng offset ngày; dùng cùng seed 5425 cho một ngày bắt đầu khác sẽ tái sử dụng các giá trị ngẫu nhiên. Công thức offset không bảo đảm tách seed toàn cục cho mọi line/khoảng ngày. Seed 0 vẫn hợp lệ; `_generated_at` dùng thời gian thực.

Cả ba chế độ raw/Bronze/Delta lưu mapping tại `--output-dir`. Gold mặc định đọc `data/synthetic/batch_farm_map.json`; với output khác, quy trình dựng marts phải gọi `load_batch_farm_map(path=...)` bằng đúng mapping và chọn Silver cùng bộ dữ liệu. Gold CLI hiện chưa có cờ chọn mapping riêng. Không tự di chuyển dữ liệu hoặc đổi target dùng chung.

`include_defect=True/False` tiếp tục ép cờ lỗi; None rút từ cơ chế. Khi không truyền `uht_level`, MES dùng 140; hiệu chuẩn tỷ lệ lỗi trung bình đầy đủ chỉ áp dụng khi UHT/drift và acidity có phân phối như daily generator. Caller mock server dùng shared MES có phân phối mới nhưng vẫn dùng generator cảm biến riêng, nên chưa có toàn bộ cơ chế daily.

## Evidence nghiệm thu 2026-11-24

Đã đọc bộ Bronze thực tại `data/mes_generator/20261124T113349Z/`: **180 file cảm biến + 180 file MES, 360 checksum hợp lệ, 5.184.000 hàng sensor, 720 MES và 720 mapping khớp**. Có 720/720 bộ giá trị chất lượng khác nhau, 34 lô lỗi (4,7222%), corr(fat, UHT avg) = −0,0470881682. Thời gian sinh đo được khoảng 30 giây trong môi trường này; đây không phải cam kết hiệu năng cho máy khác.

Report: [acceptance.json](../reports/mes_generator/20261124T113349Z/acceptance.json). Report ghi revision và SHA-256 của generator/verifier, seed khai báo, môi trường và phạm vi kiểm tra. PASS này chỉ áp dụng Bronze. [governed_sample.json](../reports/mes_generator/20261124T113349Z/governed_sample.json) ghi sample riêng một ngày hai line qua P3 vào local Delta; chưa nạp toàn bộ 90 ngày vào Silver và chưa nghiệm thu Gold/Airflow/MinIO.
