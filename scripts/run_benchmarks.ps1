<#
.SYNOPSIS
    Chạy trọn bộ chuỗi benchmark Giai đoạn 2 trên cả 6 bộ dữ liệu, rồi khôi phục trạng thái gốc.
.DESCRIPTION
    Đầu vào : -Only để chạy chọn lọc một vài bài; -SkipRestore để giữ lại dữ liệu sau khi đo.
    Xử lý   : lần lượt gọi scripts/run_benchmark.py cho từng bài theo đúng thứ tự thiết kế.
    Đầu ra  : các file JSON trong docs/reports/benchmarks/ + bảng tổng kết in ra màn hình.

    THỨ TỰ CÁC BÀI ĐO ĐƯỢC THIẾT KẾ CÓ CHỦ ĐÍCH:
      0. baseline (full)    - dựng lại trạng thái chuẩn và lấy mốc so sánh gốc.
      1. offpeak            - lô nhỏ, đo chi phí cố định và kiểm tra nạp tăng dần không nhân đôi.
      2. flash_sale         - lô lớn dồn dập, đo khả năng chịu tải và điều tiết ghi.
      3. catchup            - nạp bù, đo cơ chế upsert MERGE INTO.
      4-6. ba bộ chaos      - đo khả năng phòng vệ trước dữ liệu bẩn.
      7. restore (full)     - nạp lại dữ liệu gốc, trả hệ thống về đúng trạng thái đã nghiệm thu.

    VÌ SAO BẮT BUỘC CÓ BƯỚC KHÔI PHỤC CUỐI CÙNG? Các bài đo tăng dần ghi thêm dữ liệu tổng hợp
    vào Lakehouse, làm con số doanh thu khác đi. Chạy lại ở chế độ `full` với dữ liệu gốc sẽ
    ghi đè toàn bộ (create_or_replace), đưa hệ thống về đúng mốc 16.008.872,12 đã nghiệm thu.
.EXAMPLE
    .\scripts\run_benchmarks.ps1
    .\scripts\run_benchmarks.ps1 -Only 01_offpeak,02_flash_sale
#>
[CmdletBinding()]
param(
    [string[]]$Only,
    [switch]$SkipRestore
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"
Set-Location (Split-Path -Parent $PSScriptRoot)

# Mỗi bài đo: Nhan (ten file ket qua) | ThuMuc du lieu | Che do nap
$suite = @(
    @{ Nhan = "00_baseline_full";        ThuMuc = "data/raw";                            Mode = "full" },
    @{ Nhan = "01_offpeak";              ThuMuc = "data/synthetic/01_offpeak";           Mode = "incremental" },
    @{ Nhan = "02_flash_sale";           ThuMuc = "data/synthetic/02_flash_sale";        Mode = "incremental" },
    @{ Nhan = "03_catchup";              ThuMuc = "data/synthetic/03_catchup";           Mode = "incremental" },
    @{ Nhan = "04_dirty_metadata";       ThuMuc = "data/synthetic/04_dirty_metadata";    Mode = "incremental" },
    @{ Nhan = "05_integrity_violations"; ThuMuc = "data/synthetic/05_integrity_violations"; Mode = "incremental" },
    @{ Nhan = "06_duplicates";           ThuMuc = "data/synthetic/06_duplicates";        Mode = "incremental" }
)
if (-not $SkipRestore) {
    $suite += @{ Nhan = "07_baseline_restore"; ThuMuc = "data/raw"; Mode = "full" }
}
if ($Only) {
    $suite = $suite | Where-Object { $Only -contains $_.Nhan }
}

$ketQua = @()
foreach ($bai in $suite) {
    Write-Host "`n>>> BAI DO: $($bai.Nhan)" -ForegroundColor Cyan
    $batDau = Get-Date
    & python scripts/run_benchmark.py --dataset $bai.ThuMuc --label $bai.Nhan --mode $bai.Mode
    $ma = $LASTEXITCODE
    $giay = [math]::Round(((Get-Date) - $batDau).TotalSeconds, 1)
    $ketQua += [pscustomobject]@{
        Bai       = $bai.Nhan
        Mode      = $bai.Mode
        KetQua    = $(if ($ma -eq 0) { "SUCCESS" } else { "FAILED" })
        GiayTongCong = $giay
    }
    if ($ma -ne 0) {
        Write-Warning "Bai do $($bai.Nhan) ket thuc voi ma loi $ma. Van tiep tuc cac bai con lai de thu du so lieu."
    }
}

Write-Host "`n=== TONG KET CHUOI BENCHMARK ===" -ForegroundColor Cyan
$ketQua | Format-Table -AutoSize
Write-Host "Chi tiet tung bai: docs/reports/benchmarks/*.json"
