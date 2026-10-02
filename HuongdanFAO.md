# Thu thập FAO Dairy Price Index và lưu JSON

## 1. Hiểu dữ liệu

Nguồn: [FAO Food Price Index](https://www.fao.org/worldfoodsituation/foodpricesindex/en/).

Script lấy cột `Dairy` trong bộ chỉ số danh nghĩa theo tháng. Đây là chỉ số giá với kỳ cơ sở `2014-2016=100`, không phải USD/tấn hay giá giao dịch của một sản phẩm. Dữ liệu thuộc bảng `market_indices`, không phải `market_prices`.

## 2. Tìm file nguồn

Mở trang FAO, đến **Download datasets**, chọn **CSV: Nominal indices from 1990 onwards (monthly)**. Có thể xem request tải CSV trong F12 → Network.

Script tải HTML và tìm link `food_price_indices_data.csv` thay vì ghi cố định tham số phiên bản của file. Nguồn hiện có cấu trúc:

```text
FAO Food Price Index,...
2014-2016=100,...
Date,Food Price Index,Meat,Dairy,Cereals,Oils,Sugar,...
...
1990-01,...
```

Có dòng chú thích, dòng rỗng và nhiều cột rỗng cuối dòng. Không đọc dòng đầu như header dữ liệu. Script xác minh kỳ cơ sở và header, rồi lấy cột Dairy theo vị trí đã kiểm tra.

## 3. Chạy script

Tại PowerShell ở thư mục gốc project:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_fao.ps1 -StartDate 2025-01-01 -EndDate 2025-06-29
```

Không cần Python/Docker. Giữ `crawl_common.ps1` cùng thư mục. Chính sách chạy chỉ áp dụng cho tiến trình vừa mở.

File mặc định:

```text
data/raw/fao/fao_DAIRY_2025-01-01_2025-06-29.json
```

Script lọc theo **ngày đầu tháng**. Khoảng trên gồm tháng 1–6/2025; dù ngày kết thúc là 29/6, giá trị tháng 6 vẫn là chỉ số cả tháng, không phải số liệu riêng đến ngày 29. Nếu StartDate là giữa tháng, tháng đó bị loại vì ngày đầu tháng nằm ngoài khoảng. Nên chọn ngày đầu tháng khi thu thập theo tháng.

Muốn tải lại, thêm `-OutputPath .\data\raw\fao\fao_lan2.json` để giữ file cũ.

## 4. Kiểm tra JSON

```powershell
$fao = Get-Content -Raw -Encoding UTF8 .\data\raw\fao\fao_DAIRY_2025-01-01_2025-06-29.json | ConvertFrom-Json
$fao.record_count
$fao.records | Format-Table index_name,period_start,index_value,base_period
$fao.source_url
```

`records` chứa các tháng đã lọc. `raw_csv` giữ toàn bộ nội dung CSV dạng chuỗi, bao gồm các chỉ số và tháng khác để đối chiếu. Khi nhìn JSON sẽ thấy ký tự xuống dòng được escape; đó vẫn là chuỗi CSV hợp lệ. File còn có thời điểm tải, URL nguồn và các dòng giá trị bị loại.

Lần tải mẫu 30/09/2026: 6 bản ghi tháng 1–6/2025, không trùng tháng; chỉ số lần lượt 143.1, 147.7, 148.7, 151.7, 153.6, 155.5. Đây là giá trị nguồn cung cấp tại thời điểm tải.

## 5. Ánh xạ vào market_indices

| Cột | Nguồn/quy ước |
|---|---|
| source | FAO |
| index_name | DAIRY, nhãn nội bộ cho cột Dairy |
| period_start | Date dạng YYYY-MM chuyển thành YYYY-MM-01 |
| index_value | Giá trị Dairy dạng số |
| base_period | 2014-2016=100 |
| ingested_at | Thời điểm tải UTC |
| _source_system / _generated_at | fao_food_price_index / thời điểm chuẩn hóa |

`period_start` là chuỗi ngày cho Bronze; Silver dùng date32. Đây không phải ngày công bố. Không dùng chỉ số của cả tháng như thông tin đã biết từ ngày đầu tháng khi xây mô hình dự báo. CSV hiện tại có thể chứa giá trị lịch sử đã điều chỉnh.

## 6. Xử lý lỗi và nạp hạ tầng

Không tìm thấy CSV/header hoặc kỳ cơ sở thay đổi: kiểm tra trang FAO, cập nhật adapter; không tiếp tục với cột đoán. Không có bản ghi: kiểm tra khoảng tháng được nguồn cung cấp. Giá trị thiếu được giữ trong phần rejected, không thay bằng 0. Khi lỗi mạng hoặc 429, chờ và thử lại sau; script không tự retry.

JSON hiện chưa ghi Bronze/Silver. Để nạp, chuyển `records` thành DataFrame đúng schema; kiểm tra trùng `(source, index_name, period_start)` trước khi append. Ghi rõ FAO là nguồn trong báo cáo.
