param(
    [string]$StartDate = '2025-01-01',
    [string]$EndDate = '2025-06-29',
    [string]$UsdaDnsServer = '8.8.8.8'
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'crawl_common.ps1')
$null = Get-CrawlRange $StartDate $EndDate
$rawRoot = Join-Path (Split-Path $PSScriptRoot -Parent) 'data/raw'
$jobs = @()
foreach ($farm in @('MOC_CHAU', 'BA_VI', 'NGHE_AN', 'LAM_DONG', 'CU_CHI')) {
    $jobs += @{ Script='crawl_open_meteo.ps1'; Extra=@{FarmId=$farm}; File="open_meteo/weather_${farm}_${StartDate}_${EndDate}.json" }
}
foreach ($product in @('WMP', 'SMP', 'AMF')) {
    $jobs += @{ Script='crawl_gdt.ps1'; Extra=@{Product=$product}; File="gdt/gdt_${product}_${StartDate}_${EndDate}.json" }
}
$jobs += @{ Script='crawl_fao.ps1'; Extra=@{}; File="fao/fao_DAIRY_${StartDate}_${EndDate}.json" }
$jobs += @{ Script='crawl_openfda.ps1'; Extra=@{PageSize=1000}; File="openfda/openfda_food_${StartDate}_${EndDate}.json" }
$jobs += @{ Script='crawl_usda.ps1'; Extra=@{Product='ALL'; DnsServer=$UsdaDnsServer}; File="usda/ALL_${StartDate}_${EndDate}.json" }
$failures = @()
foreach ($job in $jobs) {
    $destination = Join-Path $rawRoot $job.File
    try {
        if (Test-Path -LiteralPath $destination) {
            $existing = Get-Content -LiteralPath $destination -Raw -Encoding UTF8 | ConvertFrom-Json
            if ($existing.start_date -ne $StartDate -or $existing.end_date -ne $EndDate -or $null -eq $existing.record_count) {
                throw "Existing file has unexpected metadata: $destination"
            }
            Write-Host "Already saved: $destination ($($existing.record_count) records)"
            continue
        }
        $arguments = @{ StartDate=$StartDate; EndDate=$EndDate; OutputPath=$destination }
        foreach ($key in $job.Extra.Keys) { $arguments[$key] = $job.Extra[$key] }
        & (Join-Path $PSScriptRoot $job.Script) @arguments
    } catch {
        $failures += $job.File
        Write-Warning "$($job.File): $($_.Exception.Message)"
    }
    Start-Sleep -Seconds 1
}
if ($failures.Count) { throw "Incomplete crawl. Failed: $($failures -join ', '). Rerun to retry missing files." }
Write-Host "All $($jobs.Count) files saved under $rawRoot."
