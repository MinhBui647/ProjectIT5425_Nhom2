# Thu thập openFDA Food Enforcement và lưu JSON

## 1. Xác định phạm vi

Nguồn: https://open.fda.gov/apis/food/enforcement/how-to-use-the-endpoint/

Endpoint `https://api.fda.gov/food/enforcement.json` trả dữ liệu thu hồi thực phẩm. Script mặc định lấy **toàn bộ thực phẩm** trong khoảng ngày, không chỉ sữa. Các bản ghi này là cảnh báo bên ngoài, không phải kết quả vi sinh của lô sản xuất trong project.

Ví dụ không cần API key. Nếu dùng thường xuyên, xem hướng dẫn xác thực và hạn mức chính thức. Script này chưa nhận API key.

## 2. Hiểu truy vấn

```text
search=report_date:[20250101 TO 20250629]
limit=100
skip=0
sort=report_date:asc
```

`report_date` là ngày báo cáo; khác `recall_initiation_date` là ngày bắt đầu thu hồi. Script dùng report_date để thống nhất với trường published_at của project, quy ước giờ 00:00 UTC do nguồn chỉ có ngày.

Tham số search được mã hóa bằng `EscapeDataString` trước khi tạo URL, tránh lỗi với dấu ngoặc và khoảng trắng. Xem trường nguồn: https://open.fda.gov/apis/food/enforcement/searchable-fields/

## 3. Phân trang để tránh mất dữ liệu

Mỗi phản hồi có `meta.results.total`, `meta.results.skip`, `meta.results.limit` và `results`. Mặc định API chỉ trả một bản ghi nếu không truyền limit. Giới hạn mỗi request là 1.000; script mặc định 100.

Script tải offset 0, 100, 200... cho đến đủ total, gộp vào một file; dừng nếu total thay đổi, xuất hiện recall_number trùng hoặc trang rỗng bất thường. Giới hạn skip là 25.000: khi vượt, script yêu cầu chia khoảng ngày, không lưu file như đã hoàn tất. Phân trang API đang cập nhật không bảo đảm snapshot cố định tuyệt đối; với dữ liệu lớn nên chia theo tháng/ngày và kiểm tra lại.

Một phản hồi 404 chỉ được hiểu là không có kết quả khi body ghi `error.code=NOT_FOUND` ngay trang đầu. Các lỗi khác được báo ra, không biến thành danh sách rỗng.

## 4. Chạy script

Tại PowerShell trong thư mục gốc project:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_openfda.ps1 -StartDate 2025-01-01 -EndDate 2025-06-29
```

Không cần Python/Docker. Giữ `crawl_common.ps1` cạnh script. ExecutionPolicy chỉ áp dụng cho tiến trình này. Có thể thêm `-PageSize 50` để quan sát nhiều trang, hoặc tối đa 1000.

File mặc định:

```text
data/raw/openfda/openfda_food_2025-01-01_2025-06-29.json
```

Nếu đã tồn tại, thêm `-OutputPath .\data\raw\openfda\openfda_lan2.json`.

## 5. Kiểm tra kết quả

```powershell
$fda = Get-Content -Raw -Encoding UTF8 .\data\raw\openfda\openfda_food_2025-01-01_2025-06-29.json | ConvertFrom-Json
$fda.record_count
$fda.expected_total
$fda.raw_pages.Count
$fda.records | Select-Object -First 5 | Format-Table recall_id,published_at,status
$fda.records[0] | Format-List
```

`record_count` phải bằng `expected_total`. `raw_pages` giữ từng response và URL/thời điểm tải; `records` là bản ghi chuẩn hóa. Các thông tin như classification, recalling_firm và recall_initiation_date vẫn có trong raw dù chưa có cột tương ứng ở schema project. Raw đã được parse, không phải bản sao nguyên byte HTTP.

Lần tải mẫu 30/09/2026: 677 bản ghi qua 7 trang (PageSize=100). Đã kiểm tra tổng raw bằng tổng records bằng expected_total, không trùng recall_number. Số lượng này có thể thay đổi khi nguồn cập nhật.

## 6. Ánh xạ vào food_recalls

| Cột | Nguồn/quy ước |
|---|---|
| source | FDA |
| recall_id | recall_number, không dùng event_id thay thế |
| published_at | report_date, quy ước 00:00 UTC |
| product | product_description |
| reason | reason_for_recall |
| status | status tại thời điểm tải |
| source_url | URL truy vấn theo recall_number |
| ingested_at | Thời điểm tải UTC |
| _source_system / _generated_at | openfda_food_enforcement / thời điểm chuẩn hóa |

Một sự kiện có thể có nhiều mã recall_number. Trạng thái hiện tại của một recall cũ không chứng minh trạng thái của nó trong quá khứ.

## 7. Nếu muốn lọc liên quan đến sữa

Có thể xem thử tập ứng viên bằng:

```powershell
$candidates = @($fda.records | Where-Object { $_.product -match '(?i)\b(milk|cheese|yogurt|butter|cream|dairy)\b' })
$candidates.Count
$candidates | Select-Object recall_id,product
```

Đây chỉ là bộ lọc từ khóa, không phải phân loại chính xác: almond milk có thể không chứa sữa, nhiều thực phẩm chứa sữa lại không ghi từ khóa trong tên. Cần quy tắc nghiệp vụ và rà soát trước khi gọi tập kết quả là thu hồi sản phẩm sữa. Script giữ đầy đủ thực phẩm làm dữ liệu gốc.

## 8. Lỗi và tích hợp

HTTP 400: kiểm tra cú pháp/mã hóa search; 429: chờ theo máy chủ rồi thử lại, không gọi dồn. Script không tự retry; nếu lỗi giữa chừng thì chưa tạo file kết quả. Muốn dữ liệu lớn hơn, chia khoảng ngày hoặc xem bulk downloads: https://open.fda.gov/apis/food/enforcement/download/

JSON chưa nạp vào Bronze/Silver. Bước sau chuyển records thành DataFrame đúng kiểu, ghi Bronze rồi Silver; xử lý khóa `(source, recall_id)` trước khi nạp lại vì Silver hiện append. Dữ liệu cảnh báo bên ngoài không được dùng để gán tự động nhãn lỗi vi sinh cho lô giả lập.
