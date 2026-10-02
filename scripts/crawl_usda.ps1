param(
    [ValidateSet('ALL','BUTTER','CHEDDAR_40LB','CHEDDAR_500LB','DRY_WHEY','NONFAT_DRY_MILK')]
    [string]$Product = 'ALL',
    [string]$StartDate = '2025-01-01',
    [string]$EndDate = '2025-06-29',
    [string]$OutputPath
)
. (Join-Path $PSScriptRoot 'crawl_common.ps1')
$first, $last = Get-CrawlRange $StartDate $EndDate
$path = Get-CrawlOutput $OutputPath "data/raw/usda/usda_${Product}_${StartDate}_${EndDate}.json"
$sections = [ordered]@{
    BUTTER = 'Final Butter Prices and Sales'
    CHEDDAR_40LB = 'Final 40 Pound Block Cheddar Cheese Prices and Sales'
    CHEDDAR_500LB = 'Final 500 Pound Barrel Cheddar Cheese Prices, Sales, and Moisture Content'
    DRY_WHEY = 'Final Dry Whey Prices and Sales'
    NONFAT_DRY_MILK = 'Final Nonfat Dry Milk Prices and Sales'
}
$records = @(); $raw = @(); $rejected = @(); $seen = @{}
foreach ($key in $sections.Keys) {
    if ($Product -ne 'ALL' -and $Product -ne $key) { continue }
    Write-Host "Fetching USDA $key..."
    $url = 'https://mpr.datamart.ams.usda.gov/services/v1.1/reports/2993/' + [uri]::EscapeDataString($sections[$key])
    $payload = Invoke-RestMethod -Uri $url -TimeoutSec 60
    $fetched = [datetime]::UtcNow.ToString('o')
    if (-not $payload.results) { throw 'Missing USDA results.' }
    if ($null -eq $payload.stats.'totalRows:' -or [int]$payload.stats.'totalRows:' -ne @($payload.results).Count) {
        throw 'USDA response missing total count or truncated; no output saved.'
    }
    $raw += [ordered]@{ product=$key; source_url=$url; fetched_at=$fetched; data=$payload }
    $matched = 0
    foreach ($row in $payload.results) {
        $day = [datetime]::ParseExact($row.week_ending_date, 'MM/dd/yyyy', $CrawlCulture)
        if ($day -lt $first -or $day -gt $last) { continue }
        $matched++
        $fields = @($row.PSObject.Properties.Name | Where-Object { $_ -match '_price$' -and $_ -notmatch 'wtd' })
        if ($fields.Count -ne 1) { throw 'USDA price column missing or ambiguous.' }
        $value = Convert-CrawlNumber $row.($fields[0])
        if ($null -eq $value -or $value -le 0) {
            $rejected += [ordered]@{product=$key; week_ending_date=$row.week_ending_date; value=$row.($fields[0]); reason='Missing or invalid price'}
            continue
        }
        $id = $key + ':' + $day.ToString('yyyy-MM-dd')
        if ($seen.ContainsKey($id)) { throw "Duplicate USDA product/week: $id; inspect source before ingesting." }
        $seen[$id] = $true
        $records += [ordered]@{
            source='USDA_NDPSR'; product=$key; contract='WEEKLY_SURVEY'
            observed_at=$day.ToString('yyyy-MM-dd') + 'T00:00:00Z'
            price=$value; currency='USD'; unit='lb'; ingested_at=$fetched
            _source_system='usda_ndpsr'; _generated_at=$fetched
        }
    }
    if (-not $matched) { Write-Warning "No USDA $key weeks in selected range." }
}
Save-CrawlJson ([ordered]@{
    source='USDA AMS National Dairy Products Sales Report'; start_date=$StartDate; end_date=$EndDate
    date_basis='week_ending_date; midnight UTC is a normalization convention, not publication time'
    record_count=$records.Count; rejected_count=$rejected.Count; records=$records; rejected=$rejected; raw_data=$raw
}) $path
