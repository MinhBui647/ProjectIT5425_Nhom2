param(
    [string]$StartDate = '2025-01-01',
    [string]$EndDate = '2025-06-29',
    [ValidateRange(1,1000)][int]$PageSize = 100,
    [string]$OutputPath
)
. (Join-Path $PSScriptRoot 'crawl_common.ps1')
$first, $last = Get-CrawlRange $StartDate $EndDate
$path = Get-CrawlOutput $OutputPath "data/raw/openfda/openfda_food_${StartDate}_${EndDate}.json"
$query = 'report_date:[' + $first.ToString('yyyyMMdd') + ' TO ' + $last.ToString('yyyyMMdd') + ']'
$base = 'https://api.fda.gov/food/enforcement.json?search=' + [uri]::EscapeDataString($query)
$records = @(); $pages = @(); $seen = @{}; $skip = 0; $total = $null
do {
    if ($skip -gt 25000) { throw 'openFDA skip limit reached; use smaller date ranges. No output saved.' }
    $url = $base + "&limit=$PageSize&skip=$skip&sort=report_date:asc"
    Write-Host "Fetching openFDA offset $skip..."
    try { $payload = Invoke-RestMethod -Uri $url -TimeoutSec 60 }
    catch {
        $status = $_.Exception.Response.StatusCode
        $apiError = $null
        try { $apiError = $_.ErrorDetails.Message | ConvertFrom-Json } catch { }
        if ([int]$status -eq 404 -and $apiError.error.code -eq 'NOT_FOUND' -and $skip -eq 0) {
            $total=0
            $pages += [ordered]@{source_url=$url; fetched_at=[datetime]::UtcNow.ToString('o'); data=$apiError}
            break
        }
        throw
    }
    $fetched = [datetime]::UtcNow.ToString('o')
    if ($null -eq $payload.meta.results.total) { throw 'Missing openFDA total count.' }
    if ($null -eq $total) { $total=[int]$payload.meta.results.total }
    elseif ($total -ne [int]$payload.meta.results.total) { throw 'openFDA result count changed while paging; retry later.' }
    $items = @($payload.results)
    if (-not $items.Count -or $null -eq $items[0]) { throw 'Unexpected empty openFDA page.' }
    $pages += [ordered]@{source_url=$url; fetched_at=$fetched; data=$payload}
    foreach ($row in $items) {
        if (-not $row.recall_number) { throw 'Missing recall_number.' }
        if ($seen.ContainsKey($row.recall_number)) { throw 'Duplicate recall while paging; retry or split date range.' }
        $seen[$row.recall_number] = $true
        $day = [datetime]::ParseExact($row.report_date, 'yyyyMMdd', $CrawlCulture)
        if ($day -lt $first -or $day -gt $last) { throw 'openFDA returned date outside requested range.' }
        $records += [ordered]@{
            source='FDA'; recall_id=$row.recall_number; published_at=$day.ToString('yyyy-MM-dd') + 'T00:00:00Z'
            product=$row.product_description; reason=$row.reason_for_recall; status=$row.status
            source_url=('https://api.fda.gov/food/enforcement.json?search=' + [uri]::EscapeDataString('recall_number:"' + $row.recall_number + '"'))
            ingested_at=$fetched; _source_system='openfda_food_enforcement'; _generated_at=$fetched
        }
    }
    $skip += $items.Count
    if ($skip -lt $total) { Start-Sleep -Milliseconds 350 }
} while ($skip -lt $total)
if ($records.Count -ne $total) { throw 'openFDA result count mismatch; no output saved.' }
Save-CrawlJson ([ordered]@{
    source='openFDA Food Enforcement'; start_date=$StartDate; end_date=$EndDate; query=$query
    scope='All food recalls; not dairy-only'
    date_basis='report_date at conventional midnight UTC; not recall_initiation_date'
    expected_total=$total; record_count=$records.Count; records=$records; raw_pages=$pages
}) $path
