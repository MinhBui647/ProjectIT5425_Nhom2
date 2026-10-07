# Crawl USDA và FAO bằng Python

Hai crawler bổ sung cho package crawling_ingestion trong thư mục Newest:

- usda.py: get_usda_marketprice_data(start_date, end_date, product_codes=None).
- fao.py: get_fao_dairyindex_data(start_date, end_date).

Cả hai nhận ngày YYYY-MM-DD, trả về polars.DataFrame; không tự ghi file khi gọi hàm. Runner ghi Parquet + SHA-256 vào Bronze theo từng ngày dữ liệu. Không cần bật MinIO để ghi Bronze local.

## 1. Chuẩn bị

Mở PowerShell tại thư mục Newest. Các file mẫu hiện có sử dụng cú pháp Python 3.12 trở lên; dùng Python 3.12 với requirements.txt hiện tại (numpy < 2).

~~~powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:PYTHONPATH = (Join-Path (Get-Location).Path 'src')
~~~

requirements.txt được bổ sung requests, thư viện HTTP mà cả ba crawler mẫu cũng đang dùng.

## 2. Chạy hai nguồn mới

~~~powershell
.\.venv\Scripts\python.exe -m crawling_ingestion.run_crawlers --sources usda fao --start-date 2025-01-01 --end-date 2025-06-29
~~~

Chỉ chạy một nguồn:

~~~powershell
.\.venv\Scripts\python.exe -m crawling_ingestion.run_crawlers --sources usda --start-date 2025-01-01 --end-date 2025-06-29
.\.venv\Scripts\python.exe -m crawling_ingestion.run_crawlers --sources fao --start-date 2025-01-01 --end-date 2025-06-29
~~~

Có thể thêm --bronze-root D:\dairy-data\bronze để đổi nơi lưu. Mặc định là Newest/data/01_bronze_vault, được xác định theo vị trí mã nguồn, không phụ thuộc thư mục terminal.

~~~text
data/01_bronze_vault/
  usda_market_prices/2025-01-04/*.parquet + *.sha256
  market_indices/2025-01-01/*.parquet + *.sha256
~~~

Chạy lại sẽ tạo batch Bronze mới theo nguyên tắc append-only; chưa có checkpoint hoặc chống trùng giữa các lần chạy trong runner này. Runner dừng khi một nguồn lỗi; các batch đã ghi trước đó vẫn tồn tại.

## 3. Gọi trong Python, xem nội dung hoặc lưu JSON

~~~python
from pathlib import Path
from crawling_ingestion import get_usda_marketprice_data, get_fao_dairyindex_data

usda = get_usda_marketprice_data(
    "2025-01-01", "2025-06-29", product_codes=["BUTTER", "NONFAT_DRY_MILK"]
)
fao = get_fao_dairyindex_data("2025-01-01", "2025-06-29")
print(usda.select("product", "observed_at", "price", "unit", "_price_status"))
print(fao.select("period_start", "index_value", "base_period"))

Path("data/raw/python_crawlers").mkdir(parents=True, exist_ok=True)
# Chọn tên mới nếu cần giữ các lần crawl cũ: write_json ghi đè nếu file đã tồn tại.
usda.write_json("data/raw/python_crawlers/usda_selected.json")
fao.write_json("data/raw/python_crawlers/fao_selected.json")
~~~

JSON xuất ở đây là mảng bản ghi DataFrame, không phải envelope của các script PowerShell cũ. Không đưa trực tiếp vào bộ chuyển đổi đòi hỏi envelope cũ; runner Python ghi DataFrame xuống Bronze trực tiếp.

## 4. Ý nghĩa dữ liệu và schema

| Nguồn | Dữ liệu | Mốc ngày | Đơn vị |
|---|---|---|---|
| USDA NDPSR, report 2993, các mục Final | Giá bán tuần của BUTTER, CHEDDAR_40LB, CHEDDAR_500LB, DRY_WHEY, NONFAT_DRY_MILK | week_ending_date, chuẩn hóa thành 00:00 UTC để lưu trữ | USD/lb |
| FAO Food Price Index | Chỉ số Dairy hằng tháng | Ngày đầu tháng, không phải ngày công bố | Điểm chỉ số, 2014-2016=100 |

USDA không phải giá giao dịch GDT hoặc hợp đồng CME. FAO không phải giá USD. Khoảng lấy dữ liệu năm 2025 không thay đổi kỳ cơ sở FAO.

Ở Newest, market_prices Bronze đang yêu cầu các cột GDT như EventNumber, EventDate, AveragePublishedPrice. USDA có cấu trúc khác nên bổ sung registry usda_market_prices và thư mục Bronze riêng; schema GDT được giữ nguyên. Registry mới khai báo đích Silver market_prices, nhưng runner chỉ ghi Bronze, chưa thực hiện ingest/merge vào Silver hoặc MinIO. FAO dùng schema market_indices có sẵn.

Các cột bổ sung: _created_at, _source_system, _generated_at, ingested_at, _source_url và _raw_record để truy vết. _raw_record giữ JSON bản ghi USDA gốc hoặc header/cells CSV FAO của dòng được chọn; không phải bản lưu toàn bộ phản hồi HTTP.

Giá trị thiếu/ẩn/không hợp lệ được giữ null, kèm _price_status hoặc _value_status = MISSING_OR_INVALID. Không thay bằng 0. Nếu chỉ phân tích giá hợp lệ, lọc status == OK ở bước Silver. Khác với script PowerShell cũ loại các hàng này khỏi records, crawler Python giữ chúng ở Bronze.

FAO lọc theo ngày đầu tháng: khoảng 2025-01-15 đến 2025-02-28 chỉ chọn tháng 2. Khoảng không có bản ghi trả DataFrame rỗng đủ schema và runner bỏ qua ghi file.

## 5. HTTP và kiểm tra dữ liệu

_http.py phục vụ hai crawler mới: tối đa 1 request/giây/nguồn trong một tiến trình; tối đa 5 lần thử với jitter khi timeout, lỗi kết nối, HTTP 408/429/500/502/503/504. Tôn trọng Retry-After; nếu máy chủ yêu cầu chờ hơn 120 giây thì báo lỗi để chạy lại sau. Không thử lại 401/403/404, lỗi chứng chỉ TLS hoặc lỗi parser.

USDA kiểm tra số bản ghi phản hồi, tên section, cột giá và tuần trùng. FAO tự tìm liên kết CSV trên trang chính thức, kiểm tra kỳ cơ sở, header và tháng trùng. Thay đổi cấu trúc nguồn sẽ báo lỗi để người dùng kiểm tra.

Bộ chạy chung có thể chọn cả năm nguồn bằng --sources openmeteo openfda gdt usda fao. Ba crawler mẫu vẫn giữ cách lấy dữ liệu cũ; helper retry mới chỉ được dùng bởi hai crawler mới và truy vấn tổng bản ghi openFDA của runner. Chưa tuyên bố đã kiểm thử live toàn bộ năm nguồn trong thay đổi này.

## 6. Kiểm thử

~~~powershell
$env:PYTHONPATH = (Join-Path (Get-Location).Path 'src')
.\.venv\Scripts\python.exe -m pytest tests/test_usda_fao.py -q
~~~

20 kiểm thử offline: parsing, đơn vị, kỳ cơ sở, khoảng ngày, null, cấu trúc nguồn lỗi, trùng, retry và Parquet/checksum Bronze. Chạy thử live cho 2025-01-01 đến 2025-06-29 đã lấy 130 dòng USDA (gồm các giá trị thiếu) và 6 dòng FAO, ghi thành 26 + 6 file Bronze trong thư mục kiểm thử. Kết quả nguồn có thể được sửa đổi về sau.

Nguồn chính thức:
- USDA: https://mpr.datamart.ams.usda.gov/services/v1.1/reports/2993
- FAO: https://www.fao.org/worldfoodsituation/foodpricesindex/en/
