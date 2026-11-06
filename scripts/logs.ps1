<#
.SYNOPSIS
    Xem log của một dịch vụ trong cụm Lakehouse.
.DESCRIPTION
    Đầu vào : -Service tên dịch vụ (vd: airflow-scheduler, spark-worker, trino);
              -Follow để bám đuôi log theo thời gian thực; -Tail số dòng cuối cần xem.
    Đầu ra  : log in trực tiếp ra màn hình.
.EXAMPLE
    .\scripts\logs.ps1 -Service airflow-scheduler -Tail 100
    .\scripts\logs.ps1 -Service spark-worker -Follow
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Service,
    [switch]$Follow,
    [ValidateRange(1, 10000)][int]$Tail = 200
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_compose.ps1")

$logArgs = (Get-ComposeArgs) + @("logs", "--tail", "$Tail", $Service)
if ($Follow) { $logArgs += "--follow" }
& docker @logArgs
