# =====================================================================================
# Tham số Docker Compose dùng chung cho các script vận hành PowerShell.
#
# VÌ SAO CHỈ CÓ BẤY NHIÊU ĐÂY?
# Bản tiền nhiệm của bộ script này có một file common.ps1 dài 406 dòng, tự chế cả cơ chế
# dò tìm đường dẫn Docker, tự viết bộ phân tích tham số và lớp bọc bắt lỗi riêng. Đó chính
# là kiểu over-engineering mà dự án đã quyết loại bỏ.
#
# Thực tế, thứ duy nhất mà mọi lệnh vận hành đều cần dùng chung chỉ là **danh sách 5 file
# compose**. Mọi việc còn lại Docker Compose đã làm tốt hơn bất kỳ lớp bọc tự chế nào.
#
# CÁCH DÙNG: dot-source từ script khác rồi gọi Get-ComposeArgs
#     . (Join-Path $PSScriptRoot "_compose.ps1")
#     & docker @(Get-ComposeArgs) ps
# =====================================================================================

function Get-ComposeArgs {
    <#
    .SYNOPSIS
        Trả về mảng tham số đứng trước mọi lệnh `docker compose` của dự án.
    .DESCRIPTION
        Đầu vào : không có (tên project lấy từ biến môi trường COMPOSE_PROJECT_NAME nếu có).
        Đầu ra  : mảng chuỗi dạng @('compose','--project-name','olist-lakehouse','-f',...).
        Lưu ý   : hàm tự chuyển thư mục làm việc về gốc dự án (thư mục cha của scripts/)
                  để các đường dẫn tương đối trong file compose luôn phân giải đúng,
                  bất kể người dùng đang đứng ở thư mục nào khi gọi script.
    #>
    $projectRoot = Split-Path -Parent $PSScriptRoot
    Set-Location $projectRoot

    $projectName = $env:COMPOSE_PROJECT_NAME
    if (-not $projectName) { $projectName = "olist-lakehouse" }

    return @(
        "compose",
        "--project-name", $projectName,
        "-f", "docker-compose.yml",              # Hạ tầng lõi: MinIO, PostgreSQL, Spark
        "-f", "docker-compose.query.yml",        # Tầng truy vấn: Trino
        "-f", "docker-compose.orchestration.yml",# Tầng điều phối: Airflow
        "-f", "docker-compose.bi.yml",           # Tầng BI: Superset
        "-f", "docker-compose.monitoring.yml"    # Giám sát: Prometheus, Grafana, MailHog
    )
}
