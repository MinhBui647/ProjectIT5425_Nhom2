# Thu thập USDA và lưu JSON

## 1. Chọn đúng dữ liệu

Nguồn: [USDA Dairy Mandatory Market Reporting](https://www.ams.usda.gov/rules-regulations/mmr/dmr).

National Dairy Products Sales Report (NDPSR) cung cấp giá khảo sát bán sản phẩm sữa theo tuần. Script lấy các mục `Final` của báo cáo 2993: bơ, cheddar khối 40 lb, cheddar thùng 500 lb, whey khô và sữa bột không béo. Đây không phải giá hợp đồng tương lai CME. Giá lưu theo USD/lb; không so sánh trực tiếp với GDT USD/tấn khi chưa đổi đơn vị và đối chiếu loại sản phẩm.

## 2. Kiểm tra nguồn trong trình duyệt

Mở https://mpr.datamart.ams.usda.gov/services/v1.1/reports/2993

JSON gồm `reportSections`, `stats` và `results`. Endpoint gốc là phần Summary, không phải bảng giá. Chọn một section trong `reportSections`, ví dụ:

```text
https://mpr.datamart.ams.usda.gov/services/v1.1/reports/2993/Final%20Butter%20Prices%20and%20Sales
```

Trong `results`, kiểm tra `week_ending_date`, `Butter_Price` và `published_date`. Tên cột giá thay đổi theo sản phẩm. Script yêu cầu đúng một cột kết thúc bằng `_Price` và loại các trường chứa `wtd`, tránh lấy nhầm số liệu.

## 3. Chạy script

Mở PowerShell tại thư mục gốc project:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_usda.ps1 -Product ALL -StartDate 2025-01-01 -EndDate 2025-06-29
```

Không cần Python hoặc Docker. Giữ `crawl_common.ps1` cùng thư mục với script. `ExecutionPolicy Bypass` chỉ áp dụng cho tiến trình này.

Mỗi section được tải rồi lọc cục bộ theo `week_ending_date`, tính cả ngày đầu và cuối. Kiểm tra `stats.totalRows:` bằng số dòng trả về; nếu thiếu dữ liệu, script dừng thay vì lưu như đã tải đủ.

Lấy riêng một sản phẩm bằng `-Product BUTTER`, `CHEDDAR_40LB`, `CHEDDAR_500LB`, `DRY_WHEY` hoặc `NONFAT_DRY_MILK`.

File mặc định:

```text
data/raw/usda/usda_ALL_2025-01-01_2025-06-29.json
```

Nếu file đã tồn tại, thêm `-OutputPath .\data\raw\usda\usda_lan2.json`. Script không ghi đè.

## 4. Đọc và kiểm tra JSON

```powershell
$usda = Get-Content -Raw -Encoding UTF8 .\data\raw\usda\usda_ALL_2025-01-01_2025-06-29.json | ConvertFrom-Json
$usda.record_count
$usda.rejected_count
$usda.records | Group-Object product | Select-Object Name,Count
$usda.records | Select-Object -First 5 | Format-Table product,observed_at,price,currency,unit
```

`records` chứa dữ liệu đã chuẩn hóa; `raw_data` giữ response của từng section kèm URL và thời điểm tải. `rejected` ghi giá thiếu/không hợp lệ trong khoảng chọn. Các giá không công bố không được thay bằng 0. Raw là JSON đã parse, không phải bản sao nguyên byte HTTP.

Lần tải mẫu 30/09/2026: 122 giá hợp lệ (BUTTER 26, CHEDDAR_40LB 26, CHEDDAR_500LB 18, DRY_WHEY 26, NONFAT_DRY_MILK 26). Có 8 dòng CHEDDAR_500LB bị loại và giữ trong `rejected`. Đã kiểm tra không trùng sản phẩm/tuần; không tự bù 8 giá này.

## 5. Ánh xạ vào market_prices

| Cột | Nguồn/quy ước |
|---|---|
| source | USDA_NDPSR |
| product | Mã nhóm sản phẩm của script |
| contract | WEEKLY_SURVEY, nhãn nội bộ project |
| observed_at | week_ending_date, quy ước 00:00 UTC |
| price | Cột giá tương ứng, chuyển số |
| currency / unit | USD / lb |
| ingested_at | Thời điểm tải UTC |
| _source_system / _generated_at | usda_ndpsr / thời điểm chuẩn hóa |

Ngày kết thúc tuần không phải ngày công bố. `published_date` được giữ trong raw; không được mặc định rằng giá đã được biết ngay ngày kết thúc tuần. Giá lịch sử lấy hôm nay có thể là số đã điều chỉnh.

## 6. Lỗi và bước tiếp theo

Nếu 404 hoặc schema đổi, kiểm tra lại `reportSections`. Nếu không có dòng trong khoảng ngày, xem phạm vi nguồn; không tạo giá giả. Script dừng khi kết nối lỗi, thiếu hàng hoặc gặp trùng sản phẩm/tuần. Với 429, chờ theo chỉ dẫn nguồn rồi thử lại.

JSON chưa được nạp vào MinIO/Delta. Bước sau là chuyển `records` thành Polars DataFrame đúng schema Bronze, gọi Bronze writer rồi Silver manager. Silver hiện append: kiểm tra trùng khóa `(source, product, contract, observed_at, currency, unit)` trước khi nạp lại.
