# Thu thập GDT Product Results và lưu JSON

## 1. Xác định dữ liệu cần lấy

Trang nguồn: https://www.globaldairytrade.info/en/product-results/

Lấy giá bình quân có trọng số theo lượng giao dịch của từng nhóm sản phẩm tại mỗi phiên GDT Events. Đơn vị là USD/metric tonne (USD/tấn). Không nhầm giá với phần trăm thay đổi GDT Price Index, và không nhầm GDT Events với GDT Pulse.

Script `scripts/crawl_gdt.ps1` hỗ trợ:

| Mã | Tên | Ý nghĩa |
|---|---|---|
| WMP | Whole Milk Powder | Sữa bột nguyên kem |
| SMP | Skim Milk Powder | Sữa bột gầy |
| AMF | Anhydrous Milk Fat | Chất béo sữa khan |

Trong project, dữ liệu này thuộc `market_prices`, không phải dữ liệu chất lượng sữa của từng lô. Mỗi bản ghi là một sản phẩm tại một phiên, không phải một giờ hoặc một ngày. Không tự tạo bản ghi giá cho ngày không có phiên.

## 2. Xem cách website lấy dữ liệu bằng trình duyệt

1. Mở trang nguồn bằng Chrome hoặc Edge.
2. Nhấn F12, chọn Network.
3. Chọn Fetch/XHR, bật Preserve log nếu cần, tải lại trang bằng Ctrl+R.
4. Lọc theo `json`, `latest` hoặc `winning_prices`. Nếu không thấy, chuyển sang All. Mở mục sản phẩm WMP hoặc biểu đồ lịch sử để kích hoạt tải nếu trang tải dữ liệu theo nhu cầu.
5. Chọn request, xem Headers → Request URL và Response/Preview.
6. Xác nhận response là JSON kết quả, không phải HTML trang đăng nhập hay thông báo lỗi.

Không nên lấy số từ vị trí thẻ HTML cố định: giao diện hiển thị dữ liệu được tải riêng. Cấu trúc dưới đây đã được kiểm tra trực tiếp trong phiên làm việc ngày 30/09/2026; đây là đường dẫn nội bộ phục vụ website, không phải API công khai có cam kết giữ nguyên cấu trúc.

## 3. Tìm đường dẫn gốc trong HTML

HTML hiện có biến:

```javascript
var resultsPath = "https://s3.amazonaws.com/www-production.globaldairytrade.info/results/";
```

Script tải HTML và tìm biến này mỗi lần chạy. Không nhầm với `pulseResultsPath`. Amazon S3 ở đây là nơi GDT công bố dữ liệu, không phải MinIO của project.

## 4. Tìm phiên mới nhất

Gọi:

```text
{resultsPath}latest.json
```

Phản hồi có trường `latestEvent`. Giá trị này là mã để xây dựng URL tiếp theo, không nên ghi cố định trong script vì sẽ thay đổi theo phiên.

## 5. Tải lịch sử của một sản phẩm

Mẫu URL đang được website sử dụng:

```text
{resultsPath}{latestEvent}/product_group_winning_prices_5_years_WMP.json
```

Thay WMP bằng SMP hoặc AMF cho các sản phẩm được script hỗ trợ. File trả về chứa nhiều phiên lịch sử, không chỉ phiên mới nhất. Giới hạn lịch sử phụ thuộc dữ liệu GDT công bố; tên file không bảo đảm đáp ứng mọi khoảng ngày bạn yêu cầu.

Cấu trúc cần đọc:

```text
ProductGroup
  ProductGroupCode
  Events
    Event[]
      EventNumber
      EventDate
      ProductGroupName
      PriceIndexPercentageChange
      AveragePublishedPrice
```

`AveragePublishedPrice` là giá cần lấy. `PriceIndexPercentageChange` là phần trăm thay đổi chỉ số, không phải giá. Trường số có thể được biểu diễn bằng chuỗi nên cần chuyển kiểu.

## 6. Chạy script có sẵn

Mở PowerShell tại thư mục gốc ProjectIT5425:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_gdt.ps1 -Product WMP -StartDate 2025-01-01 -EndDate 2025-06-29
```

Không cần Python, Docker hoặc MinIO để tải JSON. `ExecutionPolicy Bypass` chỉ áp dụng cho tiến trình được mở, không đổi chính sách toàn máy.

File kết quả:

```text
data/raw/gdt/gdt_WMP_2025-01-01_2025-06-29.json
```

Script lấy toàn bộ lịch sử có trong response rồi lọc các bản ghi chuẩn hóa theo khoảng ngày, tính cả ngày đầu và ngày cuối. Khoảng ngày không phải tham số lọc của endpoint GDT này.

Tải sản phẩm khác:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_gdt.ps1 -Product SMP -StartDate 2025-01-01 -EndDate 2025-06-29
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_gdt.ps1 -Product AMF -StartDate 2025-01-01 -EndDate 2025-06-29
```

Mỗi lần chạy tạo một file cho một sản phẩm. Script từ chối ghi đè. Muốn tải lại, thêm tên file mới:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_gdt.ps1 -Product WMP -StartDate 2025-01-01 -EndDate 2025-06-29 -OutputPath .\data\raw\gdt\wmp_lan2.json
```

## 7. Cấu trúc JSON được lưu

- `source_url`: URL JSON thực tế đã tải.
- `page_url`, `latest_url`, `latest_event`: thông tin để kiểm tra luồng lấy dữ liệu.
- `fetched_at`: thời điểm tải UTC.
- `available_first_event`, `available_last_event`: phạm vi phiên trong response.
- `record_count`: số bản ghi chuẩn hóa trong khoảng ngày chọn.
- `skipped_missing_or_invalid_price`: số bản ghi trong khoảng chọn bị bỏ do giá thiếu, không hợp lệ hoặc không dương.
- `records`: dữ liệu đã chuẩn hóa và lọc ngày.
- `raw_data`: toàn bộ response sản phẩm đã parse, kể cả phiên ngoài khoảng ngày hoặc giá bị bỏ. Đây không phải bản sao nguyên byte của HTTP response.

Kiểm tra:

```powershell
$gdt = Get-Content -Raw -Encoding UTF8 .\data\raw\gdt\gdt_WMP_2025-01-01_2025-06-29.json | ConvertFrom-Json
$gdt.record_count
$gdt.skipped_missing_or_invalid_price
$gdt.records | Select-Object -First 5 | Format-Table product, observed_at, price, currency, unit
$gdt.raw_data.ProductGroup.ProductGroupCode
```

## 8. Ánh xạ sang cơ sở hạ tầng hiện tại

| Cột | Giá trị |
|---|---|
| source | GDT |
| product | WMP, SMP hoặc AMF |
| contract | EVENT_AVERAGE: nhãn do project quy ước, không phải mã hợp đồng GDT |
| observed_at | EventDate, giữ timestamp UTC nguồn cung cấp |
| price | AveragePublishedPrice chuyển sang số |
| currency | USD |
| unit | metric_ton |
| ingested_at | thời điểm tải |
| _source_system | gdt_product_results |
| _generated_at | thời điểm tạo dữ liệu chuẩn hóa |

Các tên cột khớp Bronze `market_prices`. Khi chuyển `records` thành Polars DataFrame, cần đặt đúng kiểu theo schema trước khi gọi `write_bronze_batch()`, rồi `ingest_bronze_file()` để ghi Silver. Script hiện chỉ lưu JSON, chưa tự nạp hai tầng này.

Silver manager hiện append, không tự upsert. Nếu nạp lại cùng sản phẩm và phiên, phải xử lý trùng trước; khóa nghiệp vụ có thể dùng `(source, product, contract, observed_at, currency, unit)`. Không dùng `ingested_at` làm khóa phiên.

Không gắn giá này như một phép đo của lô sản xuất. Nếu ghép với MES để phân tích, dùng phiên đã được công bố tại thời điểm phân tích; không lấy giá phiên tương lai để điền ngược cho ngày trước đó. EventDate không nhất thiết chứng minh thời điểm công bố chính xác.

## 9. Lỗi thường gặp và giới hạn

- Không tìm thấy `resultsPath`: kiểm tra lại Network/HTML; website có thể đã đổi cấu trúc.
- HTTP 404: kiểm tra mã sản phẩm và đường dẫn đang được website dùng; không đoán GUID cũ.
- HTTP 403/429: dừng, kiểm tra quyền truy cập hoặc chờ theo chỉ dẫn máy chủ; không gọi liên tục hay vượt kiểm soát truy cập.
- TLS/timeout: kiểm tra mạng, proxy và PowerShell. Không tắt xác minh chứng chỉ để khắc phục.
- Không có bản ghi: kiểm tra phạm vi lịch sử trả về và số giá thiếu; không thay `n.a.` bằng 0.
- Giá bằng 0/âm hoặc không parse được: script bỏ khỏi `records`, đếm lại và giữ nguyên trong `raw_data` để kiểm tra; không khẳng định mọi trường hợp đều có cùng ý nghĩa nghiệp vụ.
- Script dừng khi request lỗi, không tự retry. Có thể chạy lại sau khi khắc phục, với tên output mới nếu file đã tồn tại.

GDT cho phép tái sử dụng thông tin công bố trên trang Results khi ghi nhận Global Dairy Trade là nguồn. Điều này không đồng nghĩa mọi dữ liệu chi tiết của GDT đều miễn phí. Nguồn định nghĩa giá và điều kiện ghi nguồn: https://www.globaldairytrade.info/en/product-results/
