# Thu thập dữ liệu Open-Meteo và lưu một file JSON

## 1. Dữ liệu cần lấy

Dùng Historical Weather API để lấy thời tiết quá khứ theo tọa độ trang trại. Đây là dữ liệu tái phân tích từ quan trắc kết hợp mô hình, không phải cảm biến đo trực tiếp trong trang trại và không phải dữ liệu ngẫu nhiên của generator.

Tài liệu: https://open-meteo.com/en/docs/historical-weather-api

Script dùng mô hình ERA5 và ba biến theo giờ:

| Tham số API | Ý nghĩa | Đơn vị | Trường tương ứng trong project |
|---|---|---|---|
| temperature_2m | Nhiệt độ không khí ở độ cao 2 m | °C | temperature_c |
| relative_humidity_2m | Độ ẩm tương đối ở độ cao 2 m | % | relative_humidity_pct |
| precipitation | Tổng lượng giáng thủy trong giờ | mm | precipitation_mm |

Đây là thời tiết ngoài trời, không phải nhiệt độ UHT, áp suất hay lưu lượng dây chuyền.

## 2. Chọn vị trí và thời gian

Tọa độ dưới đây lấy từ `src/lakehouse_storage/schemas.py`; chúng là điểm tham chiếu trong project, chưa phải tọa độ trang trại thực tế đã xác minh.

| FarmId | Vĩ độ | Kinh độ |
|---|---:|---:|
| MOC_CHAU | 20.84 | 104.63 |
| BA_VI | 21.08 | 105.37 |
| NGHE_AN | 18.67 | 105.68 |
| LAM_DONG | 11.94 | 108.45 |
| CU_CHI | 10.89 | 106.51 |

Ví dụ dùng 2025-01-01 đến 2025-01-07, tính cả hai ngày: 7 × 24 = 168 thời điểm. ERA5 có độ trễ cập nhật; script yêu cầu ngày kết thúc cách hiện tại ít nhất 6 ngày. Chọn ngày trùng thời gian sản xuất nếu muốn nối với sensor/MES.

Script dùng UTC. Ví dụ 00:00 UTC tương ứng 07:00 Việt Nam cùng ngày. Khoảng ngày được chọn là ngày UTC, không phải ngày địa phương Việt Nam.

## 3. Hiểu URL gọi API

```text
https://archive-api.open-meteo.com/v1/archive?latitude=20.84&longitude=104.63&start_date=2025-01-01&end_date=2025-01-07&hourly=temperature_2m,relative_humidity_2m,precipitation&timezone=UTC&models=era5&temperature_unit=celsius&precipitation_unit=mm
```

Có thể dán URL này vào trình duyệt để xem phản hồi. Các tham số gồm tọa độ, khoảng ngày, danh sách biến, múi giờ, mô hình và đơn vị. API công khai này không cần API key cho ví dụ học tập. Không cần Selenium hay phân tích HTML.

## 4. Chạy script và lưu JSON

Mở Terminal PowerShell tại thư mục gốc ProjectIT5425. Không cần khởi động Docker/MinIO hoặc kích hoạt Python `.venv` để tải JSON.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_open_meteo.ps1 -FarmId MOC_CHAU -StartDate 2025-01-01 -EndDate 2025-01-07
```

`-ExecutionPolicy Bypass` chỉ áp dụng cho tiến trình PowerShell vừa mở, không đổi thiết lập toàn máy. Có thể chạy trực tiếp `& .\scripts\crawl_open_meteo.ps1` nếu chính sách máy đã cho phép.

Kết quả mặc định là một file:

```text
data/raw/open_meteo/weather_MOC_CHAU_2025-01-01_2025-01-07.json
```

Đổi trang trại, khoảng ngày hoặc tên file:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\crawl_open_meteo.ps1 -FarmId BA_VI -StartDate 2025-01-01 -EndDate 2025-01-31 -OutputPath .\data\raw\open_meteo\weather_bavi_january.json
```

Mỗi lần chạy tải một trang trại và lưu một file. Script không ghi đè file có sẵn; để tải lại, truyền tên `-OutputPath` mới. Khi cần nhiều tháng, nên chạy theo từng tháng để dễ kiểm tra và tải lại phần lỗi.

## 5. Đọc cấu trúc file

File gồm metadata do script thêm và dữ liệu API ở khóa `data`:

- `farm_id`: mã trang trại do project quy định, API không cung cấp mã này.
- `requested_latitude`, `requested_longitude`: tọa độ gửi lên API.
- `source_url`: URL đã gọi, dùng để truy xuất nguồn.
- `fetched_at`: thời điểm tải UTC, khác thời điểm thời tiết được mô tả.
- `model`, `timezone`, `start_date`, `end_date`: cấu hình truy vấn.
- `record_count`: số thời điểm theo giờ.
- `data`: nội dung phản hồi API đã parse thành JSON, gồm tọa độ ô lưới trả về, đơn vị và các mảng theo giờ. Đây không phải bản sao nguyên byte của HTTP response.

Trong `data.hourly`, các phần tử cùng chỉ số thuộc cùng một thời điểm:

```text
time[0]                  -> thời điểm đầu tiên
temperature_2m[0]        -> nhiệt độ tại thời điểm đó
relative_humidity_2m[0]  -> độ ẩm tại thời điểm đó
precipitation[0]         -> lượng giáng thủy gắn với thời điểm đó
```

Ba biến × 168 thời điểm vẫn là 168 bản ghi khi chuyển thành bảng có ba cột đo lường.

## 6. Kiểm tra file bằng PowerShell

```powershell
$weather = Get-Content -Raw -Encoding UTF8 .\data\raw\open_meteo\weather_MOC_CHAU_2025-01-01_2025-01-07.json | ConvertFrom-Json
$weather.farm_id
$weather.record_count
$weather.data.hourly_units
$weather.data.hourly.time.Count
$weather.data.hourly.time[0]
$weather.data.hourly.temperature_2m[0]
$weather.data.hourly.relative_humidity_2m[0]
$weather.data.hourly.precipitation[0]
```

Với ví dụ 7 ngày, số thời điểm phải là 168. Script kiểm tra cả bốn mảng có đúng độ dài dự kiến trước khi lưu; nó cũng báo số giá trị null của từng biến. Giữ nguyên null, không thay bằng 0 vì 0 mm có nghĩa không có giáng thủy.

## 7. Quan hệ với Bronze và Silver

JSON này là dữ liệu đầu vào lưu cục bộ; script chưa nạp MinIO hay Delta Lake. `write_bronze_batch()` hiện nhận Polars DataFrame rồi ghi Parquet, không nhận trực tiếp file JSON này.

Để nối pipeline ở bước sau, chuyển từng chỉ số của các mảng thành một dòng:

| Cột Bronze weather | Nguồn giá trị |
|---|---|
| farm_id | metadata farm_id |
| observed_at | data.hourly.time[i], thêm thông tin UTC rõ ràng |
| temperature_c | data.hourly.temperature_2m[i] |
| relative_humidity_pct | data.hourly.relative_humidity_2m[i] |
| precipitation_mm | data.hourly.precipitation[i] |
| ingested_at | fetched_at |
| _source_system | open_meteo |
| _generated_at | thời điểm tạo bản ghi chuẩn hóa, dùng UTC |

Sau đó mới gọi Bronze writer và Silver manager. Dữ liệu thời tiết theo giờ, sensor theo giây: cần ghép theo trang trại và giờ tương ứng, không ghép trực tiếp hai timestamp có độ phân giải khác nhau. `batch_farm_map.json` hiện chỉ là ánh xạ giả lập.

## 8. Xử lý lỗi và ghi nguồn

- HTTP 400: kiểm tra định dạng ngày, tên biến và tọa độ.
- HTTP 429: dừng và chờ theo hướng dẫn máy chủ trước khi thử lại; không gọi liên tục.
- Timeout hoặc lỗi mạng: kiểm tra kết nối rồi chạy lại. Script dừng khi request lỗi và không coi đó là dữ liệu rỗng hợp lệ.
- File đã tồn tại: chọn `-OutputPath` khác để giữ bản tải trước.
- Ngày quá mới: chọn khoảng lịch sử cũ hơn do độ trễ của ERA5.

Ghi nguồn Open-Meteo và ERA5/Copernicus trong báo cáo. Free API dành cho mục đích phi thương mại, dữ liệu áp dụng CC BY 4.0; xem điều khoản tại https://open-meteo.com/en/terms và giấy phép tại https://open-meteo.com/en/licence.
