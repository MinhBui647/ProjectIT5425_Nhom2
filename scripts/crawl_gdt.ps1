param(
    [ValidateSet('WMP', 'SMP', 'AMF')]
    [string]$Product = 'WMP',
    [string]$StartDate = '2025-01-01',
    [string]$EndDate = '2025-06-29',
    [string]$OutputPath
)

$ErrorActionPreference = 'Stop'
$culture = [Globalization.CultureInfo]::InvariantCulture
$start = [datetime]::ParseExact($StartDate, 'yyyy-MM-dd', $culture)
$end = [datetime]::ParseExact($EndDate, 'yyyy-MM-dd', $culture)
if ($start -gt $end) { throw 'StartDate must not be after EndDate.' }
if (-not $OutputPath) {
    $root = Split-Path $PSScriptRoot -Parent
    $OutputPath = Join-Path $root "data/raw/gdt/gdt_${Product}_${StartDate}_${EndDate}.json"
}
$fullPath = [IO.Path]::GetFullPath($OutputPath)
if (Test-Path -LiteralPath $fullPath) { throw "Output exists; choose another -OutputPath: $fullPath" }

$pageUrl = 'https://www.globaldairytrade.info/en/product-results/'
Write-Host 'Discovering public GDT results location...'
$page = Invoke-WebRequest -Uri $pageUrl -UseBasicParsing -TimeoutSec 40
$match = [regex]::Match($page.Content, 'var\s+resultsPath\s*=\s*"([^"]+)"')
if (-not $match.Success) { throw 'GDT resultsPath not found. Inspect the website Network tab; update the adapter.' }
$base = $match.Groups[1].Value.TrimEnd('/') + '/'
$latestUrl = $base + 'latest.json'
$latest = Invoke-RestMethod -Uri $latestUrl -TimeoutSec 40
if (-not $latest.latestEvent) { throw 'Missing latestEvent in GDT response.' }
$url = $base + $latest.latestEvent + "/product_group_winning_prices_5_years_${Product}.json"
$payload = Invoke-RestMethod -Uri $url -TimeoutSec 40
$fetchedAt = [datetime]::UtcNow.ToString('o')
if ($payload.ProductGroup.ProductGroupCode -ne $Product) { throw 'Unexpected product code in response.' }
$events = @($payload.ProductGroup.Events.Event)
if (-not $events.Count -or $null -eq $events[0]) { throw 'No events found. Check GDT response structure.' }

$records = @()
$skipped = 0
$dates = @()
foreach ($event in $events) {
    $date = [DateTimeOffset]::Parse($event.EventDate, $culture).UtcDateTime
    $dates += $date
    if ($date.Date -lt $start -or $date.Date -gt $end) { continue }
    $price = 0.0
    $valid = [double]::TryParse([string]$event.AveragePublishedPrice,
        [Globalization.NumberStyles]::Float, $culture, [ref]$price)
    if (-not $valid -or [double]::IsNaN($price) -or [double]::IsInfinity($price) -or $price -le 0) {
        $skipped++
        continue
    }
    $records += [ordered]@{
        source = 'GDT'
        product = $Product
        contract = 'EVENT_AVERAGE'
        observed_at = $date.ToString('o')
        price = $price
        currency = 'USD'
        unit = 'metric_ton'
        ingested_at = $fetchedAt
        _source_system = 'gdt_product_results'
        _generated_at = $fetchedAt
    }
}
$sortedDates = @($dates | Sort-Object)
$document = [ordered]@{
    source = 'Global Dairy Trade'
    attribution = 'Source: Global Dairy Trade, GDT Events Results'
    page_url = $pageUrl
    latest_url = $latestUrl
    source_url = $url
    latest_event = $latest.latestEvent
    fetched_at = $fetchedAt
    product = $Product
    start_date = $StartDate
    end_date = $EndDate
    available_first_event = $sortedDates[0].ToString('o')
    available_last_event = $sortedDates[-1].ToString('o')
    record_count = $records.Count
    skipped_missing_or_invalid_price = $skipped
    records = $records
    raw_data = $payload
}
$json = $document | ConvertTo-Json -Depth 30
[IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($fullPath)) | Out-Null
$stream = [IO.File]::Open($fullPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
$writer = New-Object IO.StreamWriter($stream, (New-Object Text.UTF8Encoding($false)))
try { $writer.Write($json) } finally { $writer.Dispose() }
Write-Host "Saved $($records.Count) prices; skipped $skipped missing/invalid prices in selected range."
Write-Host $fullPath
if ($records.Count -eq 0) { Write-Warning 'No valid prices in selected range; inspect available dates and raw_data.' }
if ($start -lt $sortedDates[0].Date -or $end -gt $sortedDates[-1].Date) {
    Write-Warning 'Requested range extends beyond returned event dates; this file does not prove full coverage.'
}
