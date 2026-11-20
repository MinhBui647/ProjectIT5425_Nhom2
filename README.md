# Smart Dairy Factory Data Lakehouse

Dự án IT5425: Bronze raw bất biến → P3 quality governance → Silver Delta Lake trên
MinIO. Pipeline hiện hành được điều phối bởi Airflow; Gold/P4 nhận Silver qua
contract và không thuộc phạm vi orchestration lần này.

- [Bắt đầu demo](docs/storage_demo.md)
- [Giải thích toàn bộ code triển khai từng task](docs/lakehouse_code_walkthrough.md)
- [Mock API đa nguồn](docs/mock_server.md)
- [Airflow và vận hành](docs/lakehouse_airflow.md)
- [Version dữ liệu, manifest và historical reads](docs/data_versioning.md)
- [Storage API](src/lakehouse_storage/API_REFERENCE.md)
- [Vận hành và recovery](docs/storage_operations.md)
- [Chạy benchmark](docs/storage_benchmark.md)

Core tests: `PYTHONPATH=src .venv/bin/python -m pytest tests/ -q`.
Runtime/service tests có flags riêng; skip vì không bật dịch vụ không phải evidence
đã nghiệm thu MinIO hoặc Airflow. Xem cách chạy và kiểm tra kết quả trong
[hướng dẫn demo](docs/storage_demo.md).
