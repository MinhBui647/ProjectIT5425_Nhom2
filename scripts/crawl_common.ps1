# Shared helpers for USDA, FAO and openFDA. Dot-source this file.
$ErrorActionPreference = 'Stop'
$CrawlCulture = [Globalization.CultureInfo]::InvariantCulture
function Get-CrawlRange([string]$StartDate, [string]$EndDate) {
    $first = [datetime]::ParseExact($StartDate, 'yyyy-MM-dd', $CrawlCulture)
    $last = [datetime]::ParseExact($EndDate, 'yyyy-MM-dd', $CrawlCulture)
    if ($first -gt $last) { throw 'StartDate must not be after EndDate.' }
    return @($first, $last)
}
function Get-CrawlOutput([string]$OutputPath, [string]$DefaultName) {
    if (-not $OutputPath) { $OutputPath = Join-Path (Split-Path $PSScriptRoot -Parent) $DefaultName }
    $full = [IO.Path]::GetFullPath($OutputPath)
    if (Test-Path -LiteralPath $full) { throw "Output exists; choose another -OutputPath: $full" }
    return $full
}
function Save-CrawlJson($Document, [string]$Path) {
    $json = $Document | ConvertTo-Json -Depth 50
    [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($Path)) | Out-Null
    $stream = [IO.File]::Open($Path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
    $writer = New-Object IO.StreamWriter($stream, (New-Object Text.UTF8Encoding($false)))
    try { $writer.Write($json) } finally { $writer.Dispose() }
    Write-Host "Saved $($Document.record_count) records to $Path"
}
function Convert-CrawlNumber($Value) {
    $number = 0.0
    if ([double]::TryParse([string]$Value, [Globalization.NumberStyles]::Float, $CrawlCulture, [ref]$number) -and
        -not [double]::IsNaN($number) -and -not [double]::IsInfinity($number)) { return $number }
    return $null
}
