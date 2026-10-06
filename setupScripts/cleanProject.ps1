Write-Host "[Warning] Stopping the container and deleting all the MinIO's data..." -ForegroundColor Yellow
docker compose -f infra/docker-compose.minio.yml down -v

Write-Host "[Done] Cleaned" -ForegroundColor Green

Write-Host "[Info] Check volume:" -ForegroundColor Green
docker volume ls
