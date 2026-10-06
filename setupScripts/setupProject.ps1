$env:PYTHONPATH = (Join-Path (Get-Location).Path 'src')
$env:PYTHONIOENCODING = 'utf-8'
$env:MINIO_ENDPOINT = 'localhost:9000'
$env:MINIO_ACCESS_KEY = 'dairy'
$env:MINIO_SECRET_KEY = 'hustit5425'
$env:MINIO_BUCKET = 'smart-dairy-lakehouse'
$env:MINIO_SECURE = 'false'
$env:FACTORY_TIMEZONE = 'UTC'
$env:BRONZE_LOCAL_PATH = (Join-Path (Get-Location).Path 'data\01_bronze_vault')

Write-Host "[Done] Buoc 2 - Dat bien moi truong" -ForegroundColor Green

docker compose -f infra/docker-compose.minio.yml up -d
docker compose -f infra/docker-compose.minio.yml ps
docker compose -f infra/docker-compose.minio.yml logs --tail 50 minio

Write-Host "[Done] Buoc 3 - Chay MinIO va check health" -ForegroundColor Green

.\.venv\Scripts\python.exe infra/check_minio.py

Write-Host "[Done] Buoc 4 - Kiem tra MinIO connection" -ForegroundColor Green

.\.venv\Scripts\python.exe -m lakehouse_storage.mock_sensor_stream --batches 3 --samples 60 --interval 1 --line-id LINE_UHT_1 --batch-id BATCH_DEMO_001 --seed 42
.\.venv\Scripts\python.exe -m lakehouse_storage.generate_mes_data --days 1 --date 2026-01-15 --seed 5425 --line-id LINE_UHT_1 --write-delta

Write-Host "[Done] Buoc 5, 6 - Seed du lieu cam bien" -ForegroundColor Green

