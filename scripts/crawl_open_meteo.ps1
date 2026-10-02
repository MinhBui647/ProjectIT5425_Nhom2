param(
    [ValidateSet('MOC_CHAU', 'BA_VI', 'NGHE_AN', 'LAM_DONG', 'CU_CHI')]
    [string]$FarmId = 'MOC_CHAU',
    [string]$StartDate = '2025-01-01',
    [string]$EndDate = '2025-01-07',
    [string]$OutputPath
)

$ErrorActionPreference = 'Stop'
$culture = [Globalization.CultureInfo]::InvariantCulture
$start = [datetime]::ParseExact($StartDate, 'yyyy-MM-dd', $culture)
$end = [datetime]::ParseExact($EndDate, 'yyyy-MM-dd', $culture)
if ($start -gt $end) { throw 'StartDate must not be after EndDate.' }
if ($end -gt [datetime]::UtcNow.Date.AddDays(-6)) {
    throw 'For ERA5, choose EndDate at least 6 days before today.'
}

# Coordinates from src/lakehouse_storage/schemas.py (project reference points).
$farms = @{
    MOC_CHAU = @('20.84', '104.63')
    BA_VI = @('21.08', '105.37')
    NGHE_AN = @('18.67', '105.68')
    LAM_DONG = @('11.94', '108.45')
    CU_CHI = @('10.89', '106.51')
}
$coords = $farms[$FarmId]
$url = 'https://archive-api.open-meteo.com/v1/archive?' +
    "latitude=$($coords[0])&longitude=$($coords[1])" +
    "&start_date=$StartDate&end_date=$EndDate" +
    '&hourly=temperature_2m,relative_humidity_2m,precipitation' +
    '&timezone=UTC&models=era5&temperature_unit=celsius&precipitation_unit=mm'

if (-not $OutputPath) {
    $projectRoot = Split-Path $PSScriptRoot -Parent
    $OutputPath = Join-Path $projectRoot "data/raw/open_meteo/weather_${FarmId}_${StartDate}_${EndDate}.json"
}
$fullPath = [IO.Path]::GetFullPath($OutputPath)
if (Test-Path -LiteralPath $fullPath) {
    throw "Output already exists; choose another -OutputPath: $fullPath"
}

Write-Host "Fetching $FarmId : $StartDate to $EndDate (UTC)"
$response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 60
$payload = $response.Content | ConvertFrom-Json
if ($payload.error) { throw "Open-Meteo: $($payload.reason)" }
$expected = [int](($end - $start).TotalDays + 1) * 24
foreach ($field in @('time', 'temperature_2m', 'relative_humidity_2m', 'precipitation')) {
    if (@($payload.hourly.$field).Count -ne $expected) {
        throw "Unexpected hourly.$field length; expected $expected. No file saved."
    }
}

$document = [ordered]@{
    source = 'Open-Meteo Historical Weather API'
    source_url = $url
    attribution = 'Weather data by Open-Meteo.com; ERA5 (Copernicus/ECMWF); CC BY 4.0'
    farm_id = $FarmId
    requested_latitude = [double]::Parse($coords[0], $culture)
    requested_longitude = [double]::Parse($coords[1], $culture)
    start_date = $StartDate
    end_date = $EndDate
    timezone = 'UTC'
    model = 'era5'
    fetched_at = [datetime]::UtcNow.ToString('o')
    record_count = $expected
    data = $payload
}
$json = $document | ConvertTo-Json -Depth 20
[IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($fullPath)) | Out-Null
# CreateNew protects previous downloads from being overwritten.
$stream = [IO.File]::Open($fullPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
$writer = New-Object IO.StreamWriter($stream, (New-Object Text.UTF8Encoding($false)))
try { $writer.Write($json) } finally { $writer.Dispose() }
Write-Host "Saved $expected hourly records to $fullPath"
foreach ($field in @('temperature_2m', 'relative_humidity_2m', 'precipitation')) {
    $missing = @($payload.hourly.$field | Where-Object { $null -eq $_ }).Count
    Write-Host "${field}: $missing null values (preserved, not replaced with zero)"
}
