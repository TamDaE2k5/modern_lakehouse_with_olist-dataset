<#
.SYNOPSIS
    Khởi động toàn bộ 13 container của Olist Data Lakehouse và chờ tới khi tất cả khỏe mạnh.
.DESCRIPTION
    Đầu vào : không bắt buộc. Tham số -Build để build lại image, -TimeoutSeconds để đổi hạn chờ.
    Xử lý   : gọi `docker compose up -d --wait` với đủ 5 file compose của dự án.
    Đầu ra  : bảng trạng thái các dịch vụ kèm danh mục cổng truy cập Web UI.

    Cờ --wait yêu cầu Docker Compose tự chờ tới khi MỌI healthcheck chuyển xanh rồi mới
    trả quyền điều khiển. Nhờ vậy không cần tự viết vòng lặp thăm dò như bản script cũ.
.EXAMPLE
    .\scripts\up.ps1
    .\scripts\up.ps1 -Build
#>
[CmdletBinding()]
param(
    [switch]$Build,
    [ValidateRange(60, 3600)][int]$TimeoutSeconds = 900
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_compose.ps1")

$composeArgs = Get-ComposeArgs
$upArgs = $composeArgs + @("up", "-d", "--wait", "--wait-timeout", "$TimeoutSeconds")
if ($Build) { $upArgs += "--build" }

Write-Output "Dang khoi dong Olist Data Lakehouse (han cho $TimeoutSeconds giay)..."
& docker @upArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "Khoi dong that bai (ma loi $LASTEXITCODE). Xem log: .\scripts\logs.ps1 <ten-dich-vu>"
    exit $LASTEXITCODE
}

Write-Output ""
& docker @($composeArgs + @("ps", "--format", "table {{.Service}}`t{{.Status}}"))

Write-Output @"

--- CONG TRUY CAP WEB UI ---
  Airflow     http://localhost:8083    admin / admin
  Trino       http://localhost:8082    user: trino
  Superset    http://localhost:8088    olist_admin / admin
  MinIO       http://localhost:9001    root / admin_root
  Grafana     http://localhost:3000    admin / admin
  Prometheus  http://localhost:9090
  MailHog     http://localhost:8025    (hop thu canh bao su co)
  Spark       http://localhost:8084
"@
