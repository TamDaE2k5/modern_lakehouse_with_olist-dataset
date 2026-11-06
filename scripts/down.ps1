<#
.SYNOPSIS
    Dừng toàn bộ container của Lakehouse, BẢO TOÀN 100% dữ liệu trong Docker volume.
.DESCRIPTION
    Đầu vào : -RemoveVolumes nếu thực sự muốn XÓA SẠCH dữ liệu (có hỏi xác nhận).
    Xử lý   : `docker compose down --remove-orphans`, chỉ thêm -v khi được yêu cầu rõ ràng.
    Đầu ra  : thông báo trạng thái sau khi dừng.

    CẢNH BÁO: file bash.txt của dự án ghi lệnh `down -v` — cờ -v sẽ XÓA VĨNH VIỄN các volume
    minio-data và postgres-data, tức mất toàn bộ Lakehouse đã dựng. Script này mặc định KHÔNG
    dùng -v, và nếu người dùng chủ động yêu cầu thì vẫn bắt gõ chữ XOA để xác nhận.
.EXAMPLE
    .\scripts\down.ps1
#>
[CmdletBinding()]
param([switch]$RemoveVolumes)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_compose.ps1")

$downArgs = (Get-ComposeArgs) + @("down", "--remove-orphans")

if ($RemoveVolumes) {
    Write-Warning "Thao tac nay se XOA VINH VIEN toan bo du lieu Lakehouse (minio-data, postgres-data)."
    $confirm = Read-Host "Go dung chu XOA de xac nhan"
    if ($confirm -ne "XOA") {
        Write-Output "Da huy. Khong co gi bi xoa."
        exit 0
    }
    $downArgs += "-v"
}

Write-Output "Dang dung cac container..."
& docker @downArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($RemoveVolumes) {
    Write-Output "Da dung va XOA volume. Lan chay sau se dung lai Lakehouse tu dau."
} else {
    Write-Output "Da dung. Du lieu van nguyen ven trong volume - bat lai bang .\scripts\up.ps1"
}
