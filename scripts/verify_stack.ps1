<#
.SYNOPSIS
    Kiểm tra sức khỏe toàn diện cụm Lakehouse: container, cổng Web UI, DAG, dữ liệu và doanh thu.
.DESCRIPTION
    Đầu vào : không có.
    Xử lý   : chạy lần lượt 5 nhóm kiểm tra, mỗi mục in PASS hoặc FAIL kèm giá trị đo được.
    Đầu ra  : bảng kết quả; mã thoát 0 nếu tất cả PASS, 1 nếu có bất kỳ mục nào FAIL.

    Đây là "bài kiểm tra 60 giây" nên chạy sau mỗi lần bật cụm hoặc sau khi sửa cấu hình,
    trước khi bắt tay vào bất kỳ việc gì khác.
.EXAMPLE
    .\scripts\verify_stack.ps1
#>
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"
. (Join-Path $PSScriptRoot "_compose.ps1")
$composeArgs = Get-ComposeArgs

$script:Failures = 0
function Write-Check {
    param([string]$Name, [bool]$Ok, [string]$Detail = "")
    if ($Ok) {
        Write-Host ("  [PASS] {0,-44} {1}" -f $Name, $Detail) -ForegroundColor Green
    } else {
        Write-Host ("  [FAIL] {0,-44} {1}" -f $Name, $Detail) -ForegroundColor Red
        $script:Failures++
    }
}

Write-Output "`n=== 1. TRANG THAI CONTAINER ==="
$psLines = & docker @($composeArgs + @("ps", "--format", "{{.Service}};{{.State}};{{.Health}}"))
foreach ($line in $psLines) {
    if (-not $line) { continue }
    $parts = $line -split ";"
    $healthy = ($parts[1] -eq "running") -and (($parts[2] -eq "healthy") -or (-not $parts[2]))
    Write-Check -Name $parts[0] -Ok $healthy -Detail "$($parts[1]) $($parts[2])"
}

Write-Output "`n=== 2. CONG WEB UI ==="
$endpoints = [ordered]@{
    "Airflow"    = "http://localhost:8083/health"
    "Trino"      = "http://localhost:8082/v1/info"
    "Superset"   = "http://localhost:8088/health"
    "MinIO"      = "http://localhost:9000/minio/health/live"
    "Prometheus" = "http://localhost:9090/-/healthy"
    "Grafana"    = "http://localhost:3000/api/health"
    "MailHog"    = "http://localhost:8025/api/v2/messages?limit=1"
}
foreach ($name in $endpoints.Keys) {
    try {
        $response = Invoke-WebRequest -Uri $endpoints[$name] -TimeoutSec 10 -UseBasicParsing
        Write-Check -Name $name -Ok ($response.StatusCode -eq 200) -Detail "HTTP $($response.StatusCode)"
    } catch {
        Write-Check -Name $name -Ok $false -Detail "khong ket noi duoc"
    }
}

Write-Output "`n=== 3. AIRFLOW DAGS ==="
$dagOutput = & docker exec olist-lakehouse-airflow-scheduler-1 airflow dags list 2>$null | Out-String
foreach ($dag in @("olist_daily_pipeline", "olist_transformation", "olist_quality", "olist_maintenance")) {
    Write-Check -Name $dag -Ok ($dagOutput -match [regex]::Escape($dag)) -Detail "da nap"
}
$importErrors = & docker exec olist-lakehouse-airflow-scheduler-1 airflow dags list-import-errors 2>$null | Out-String
Write-Check -Name "Khong co loi import DAG" -Ok ($importErrors -match "No data found") -Detail ""

Write-Output "`n=== 4. DOI SOAT DU LIEU QUA TRINO ==="
$sql = "SELECT count(*), sum(item_revenue), sum(freight_total), sum(payment_total) FROM lakehouse.gold.fact_orders"
$row = (& docker exec olist-lakehouse-trino-1 trino --execute $sql --output-format CSV 2>$null | Select-Object -First 1)
if ($row) {
    $values = $row -replace '"', '' -split ","
    Write-Check -Name "fact_orders: so don hang" -Ok ($values[0] -eq "99441") -Detail "$($values[0]) (ky vong 99441)"
    Write-Check -Name "fact_orders: doanh thu hang" -Ok ($values[1] -eq "13591643.70") -Detail "$($values[1]) (ky vong 13591643.70)"
    Write-Check -Name "fact_orders: cuoc van chuyen" -Ok ($values[2] -eq "2251909.54") -Detail "$($values[2]) (ky vong 2251909.54)"
    Write-Check -Name "fact_orders: tong thanh toan" -Ok ($values[3] -eq "16008872.12") -Detail "$($values[3]) (ky vong 16008872.12)"
} else {
    Write-Check -Name "Truy van Trino" -Ok $false -Detail "khong tra ve ket qua"
}

Write-Output "`n=== 5. PROMETHEUS SCRAPE TARGETS ==="
try {
    $targets = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/targets" -TimeoutSec 10
    $active = $targets.data.activeTargets
    $up = ($active | Where-Object { $_.health -eq "up" }).Count
    Write-Check -Name "Scrape targets UP" -Ok ($up -eq $active.Count) -Detail "$up/$($active.Count)"
} catch {
    Write-Check -Name "Prometheus targets" -Ok $false -Detail "khong doc duoc API"
}

Write-Output ""
if ($script:Failures -eq 0) {
    Write-Host "KET QUA: TAT CA KIEM TRA DEU PASS." -ForegroundColor Green
    exit 0
}
Write-Host "KET QUA: $($script:Failures) muc FAIL - xem chi tiet o tren." -ForegroundColor Red
exit 1
