param(
    [string]$StartDate = '2025-01-01',
    [string]$EndDate = '2025-06-29',
    [string]$OutputPath
)
. (Join-Path $PSScriptRoot 'crawl_common.ps1')
$first, $last = Get-CrawlRange $StartDate $EndDate
$path = Get-CrawlOutput $OutputPath "data/raw/fao/fao_DAIRY_${StartDate}_${EndDate}.json"
$pageUrl = 'https://www.fao.org/worldfoodsituation/foodpricesindex/en/'
Write-Host 'Discovering FAO monthly CSV...'
$page = Invoke-WebRequest -Uri $pageUrl -UseBasicParsing -TimeoutSec 60
$link = @($page.Links | Where-Object href -Match 'food_price_indices_data\.csv' | Select-Object -First 1)
if (-not $link.Count) { throw 'FAO CSV link changed; inspect Download datasets on the source page.' }
$url = ([uri]::new([uri]$pageUrl, [Net.WebUtility]::HtmlDecode($link[0].href))).AbsoluteUri
$response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 60
$text = [string]$response.Content
$fetched = [datetime]::UtcNow.ToString('o')
if ($text -notmatch '2014-2016=100') { throw 'FAO base period changed; update adapter.' }
$lines = $text -split '\r?\n'
$header = -1
for ($i=0; $i -lt $lines.Count; $i++) { if ($lines[$i] -match '^Date,Food Price Index,Meat,Dairy,') { $header=$i; break } }
if ($header -lt 0) { throw 'FAO CSV header changed.' }
# Source has many empty trailing columns. Assign unique positional names to all columns.
$columnCount = ($lines[$header] -split ',').Count
$headers = @(0..($columnCount-1) | ForEach-Object { "col$_" })
$rows = ($lines[($header+1)..($lines.Count-1)] -join "`n") | ConvertFrom-Csv -Header $headers
$records = @(); $rejected = @(); $seen = @{}
foreach ($row in $rows) {
    if ([string]::IsNullOrWhiteSpace($row.col0)) { continue }
    if ($row.col0 -notmatch '^\d{4}-\d{2}$') { throw "Unexpected FAO period: $($row.col0)" }
    $day = [datetime]::ParseExact($row.col0 + '-01', 'yyyy-MM-dd', $CrawlCulture)
    if ($day -lt $first -or $day -gt $last) { continue }
    $value = Convert-CrawlNumber $row.col3
    if ($null -eq $value -or $value -le 0) { $rejected += $row; continue }
    if ($seen.ContainsKey($row.col0)) { throw 'Duplicate FAO month.' }
    $seen[$row.col0] = $true
    $records += [ordered]@{
        source='FAO'; index_name='DAIRY'; period_start=$day.ToString('yyyy-MM-dd')
        index_value=$value; base_period='2014-2016=100'; ingested_at=$fetched
        _source_system='fao_food_price_index'; _generated_at=$fetched
    }
}
Save-CrawlJson ([ordered]@{
    source='FAO Food Price Index'; page_url=$pageUrl; source_url=$url; fetched_at=$fetched
    start_date=$StartDate; end_date=$EndDate; date_basis='month start; not publication date'
    record_count=$records.Count; rejected_count=$rejected.Count; records=$records; rejected=$rejected
    raw_csv=$text
}) $path
if (-not $records.Count) { Write-Warning 'No FAO monthly values in selected range.' }
