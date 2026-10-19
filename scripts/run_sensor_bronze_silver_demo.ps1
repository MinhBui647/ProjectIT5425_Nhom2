param(
    [int]$Samples = 60,
    [string]$LineId = "LINE_UHT_1",
    [string]$BatchId = "BATCH_DEMO_001",
    [int]$Seed = 42,
    [string]$RunId = "",
    [string]$StartTimeUtc = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$composeFile = Join-Path $projectRoot "infra\docker-compose.minio.yml"

if (-not (Test-Path -LiteralPath $pythonExe)) {
    throw "Python environment not found: $pythonExe. Run the project environment setup first."
}

if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = "demo-" + [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ")
}
if ([string]::IsNullOrWhiteSpace($StartTimeUtc)) {
    $StartTimeUtc = [DateTime]::UtcNow.ToString("o")
}

Push-Location $projectRoot
try {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        $dockerDesktop = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
        if (-not (Test-Path -LiteralPath $dockerDesktop)) {
            throw "Docker daemon is unavailable and Docker Desktop was not found."
        }
        Start-Process -FilePath $dockerDesktop -WindowStyle Hidden

        $dockerReady = $false
        for ($attempt = 0; $attempt -lt 30; $attempt++) {
            Start-Sleep -Seconds 2
            docker info *> $null
            if ($LASTEXITCODE -eq 0) {
                $dockerReady = $true
                break
            }
        }
        if (-not $dockerReady) {
            throw "Docker daemon did not become ready within 60 seconds."
        }
    }

    docker compose -f $composeFile up -d | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Could not start MinIO."
    }

    $minioReady = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        try {
            $health = Invoke-WebRequest `
                -Uri "http://localhost:9000/minio/health/ready" `
                -UseBasicParsing `
                -TimeoutSec 2
            if ($health.StatusCode -eq 200) {
                $minioReady = $true
                break
            }
        }
        catch {
            Start-Sleep -Seconds 1
        }
    }
    if (-not $minioReady) {
        throw "MinIO did not become ready within 30 seconds."
    }

    if (-not $env:MINIO_ENDPOINT) { $env:MINIO_ENDPOINT = "localhost:9000" }
    if (-not $env:MINIO_ACCESS_KEY) { $env:MINIO_ACCESS_KEY = "dairy" }
    if (-not $env:MINIO_SECRET_KEY) { $env:MINIO_SECRET_KEY = "hustit5425" }
    if (-not $env:MINIO_BUCKET) { $env:MINIO_BUCKET = "smart-dairy-lakehouse" }
    if (-not $env:MINIO_SECURE) { $env:MINIO_SECURE = "false" }
    $env:PYTHONPATH = Join-Path $projectRoot "src"
    $env:PYTHONIOENCODING = "utf-8"

    & $pythonExe -m lakehouse_storage.run_sensor_demo `
        --run-id $RunId `
        --samples $Samples `
        --line-id $LineId `
        --batch-id $BatchId `
        --seed $Seed `
        --start-time $StartTimeUtc
    if ($LASTEXITCODE -ne 0) {
        throw "Sensor Bronze-to-Silver demo failed."
    }
}
finally {
    Pop-Location
}
