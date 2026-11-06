"""Module phát cảnh báo sự cố Lakehouse qua giao thức SMTP tiêu chuẩn (Hỗ trợ Gmail & SMTP Server).

ĐẦU VÀO (Input):
    - ``subject``     : Tiêu đề cảnh báo ngắn gọn.
    - ``body_text``   : Nội dung chi tiết sự cố (traceback, thông tin task, nguyên nhân).
    - ``severity``    : Mức độ nghiêm trọng ('INFO' | 'WARN' | 'ERROR' | 'CRITICAL').
    - ``html_content``: (Tùy chọn) Nội dung định dạng HTML đẹp mắt hiển thị trên ứng dụng Gmail/Outlook.
    - Cấu hình môi trường: ``ALERT_SMTP_HOST``, ``ALERT_SMTP_PORT``, ``ALERT_SMTP_USER``,
      ``ALERT_SMTP_PASSWORD``, ``ALERT_ADMIN_EMAIL``, ``ALERT_SMTP_STARTTLS``, ``ALERT_SMTP_SSL``.

XỬ LÝ (Logic):
    1. Kiểm tra cờ bật/tắt ``ALERT_EMAIL_ENABLED``.
    2. Tự động nhận diện cấu hình Gmail nếu host là ``smtp.gmail.com`` hoặc user là ``@gmail.com``.
    3. Hỗ trợ cả 2 chuẩn mã hóa bảo mật phổ biến:
       - SSL Direct (Cổng 465): dùng ``smtplib.SMTP_SSL`` kèm SSLContext an toàn.
       - STARTTLS (Cổng 587): dùng ``smtplib.SMTP`` rồi nâng cấp bảo mật bằng ``server.starttls()``.
       - Plain SMTP (Cổng 1025 như MailHog): dùng ``smtplib.SMTP`` thông thường cho môi trường dev.
    4. Xây dựng email đa phần (MIME Multipart): vừa có văn bản thuần (plain text), vừa có HTML hiển thị thẻ bảng màu.
    5. Bắt và phân loại lỗi thông minh:
       - Nếu gặp ``SMTPAuthenticationError`` trên Gmail: in hướng dẫn chi tiết cách tạo Mật khẩu ứng dụng (App Password).
       - Không bao giờ ném ngoại lệ làm gián đoạn pipeline chính của Airflow/Spark.

ĐẦU RA (Output):
    - ``True`` nếu gửi thành công vào máy chủ thư, ``False`` nếu thất bại hoặc tính năng bị tắt.

HƯỚNG DẪN CẤU HÌNH GMAIL THẬT:
-----------------------------
1. Bật xác thực 2 bước (2-Step Verification) trên tài khoản Google:
   https://myaccount.google.com/signinoptions/two-step-verification
2. Tạo Mật khẩu ứng dụng (App Password 16 ký tự):
   https://myaccount.google.com/apppasswords
   - Chọn ứng dụng: Khác (Tên tùy chỉnh: 'Olist Lakehouse Alert')
   - Nhận mật khẩu 16 ký tự (ví dụ: 'abcd efgh ijkl mnop').
3. Cập nhật vào file ``.env``:
   ALERT_EMAIL_ENABLED=true
   ALERT_SMTP_HOST=smtp.gmail.com
   ALERT_SMTP_PORT=587
   ALERT_SMTP_STARTTLS=true
   ALERT_SMTP_USER=email_cua_ban@gmail.com
   ALERT_SMTP_PASSWORD=abcd efgh ijkl mnop
   ALERT_FROM_EMAIL=email_cua_ban@gmail.com
   ALERT_ADMIN_EMAIL=email_nhan_canh_bao@gmail.com
"""

from __future__ import annotations

import argparse
from email.message import EmailMessage
import logging
import os
import smtplib
import socket
import ssl
import sys


LOGGER = logging.getLogger(__name__)


def _is_truthy(val: str | None) -> bool:
    """Kiểm tra giá trị chuỗi có mang ý nghĩa True (1, true, yes, on)."""
    return str(val or "").strip().lower() in {"1", "true", "yes", "on"}


def is_email_alert_enabled() -> bool:
    """Đọc cờ bật/tắt tính năng cảnh báo email từ biến môi trường ``ALERT_EMAIL_ENABLED``."""
    return _is_truthy(os.environ.get("ALERT_EMAIL_ENABLED", "true"))


def _build_html_template(subject: str, body_text: str, severity: str) -> str:
    """Tạo giao diện email HTML chuyên nghiệp, thân thiện khi đọc trên Gmail điện thoại/máy tính."""
    # Màu sắc nhận diện theo mức độ nghiêm trọng
    color_map = {
        "CRITICAL": "#d93025",  # Đỏ đậm
        "ERROR": "#ea4335",     # Đỏ tươi
        "WARN": "#fbbc04",      # Vàng cam
        "INFO": "#1a73e8",      # Xanh lam
    }
    badge_color = color_map.get(severity.upper(), "#ea4335")
    escaped_body = (
        body_text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br/>")
    )

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="utf-8">
      <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f6f9; margin: 0; padding: 20px; }}
        .card {{ max-width: 650px; margin: 0 auto; background: #ffffff; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); overflow: hidden; border: 1px solid #e1e4e8; }}
        .header {{ background-color: {badge_color}; color: #ffffff; padding: 18px 24px; }}
        .header h2 {{ margin: 0; font-size: 18px; font-weight: 600; }}
        .badge {{ display: inline-block; padding: 4px 10px; font-size: 11px; font-weight: bold; background: rgba(255,255,255,0.25); border-radius: 4px; text-transform: uppercase; margin-bottom: 6px; }}
        .content {{ padding: 24px; color: #24292e; line-height: 1.6; font-size: 14px; }}
        .code-box {{ background-color: #f6f8fa; border: 1px solid #e1e4e8; border-radius: 6px; padding: 14px; font-family: SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace; font-size: 12px; color: #24292e; word-break: break-word; overflow-x: auto; margin-top: 15px; }}
        .footer {{ padding: 14px 24px; background: #fafbfc; border-top: 1px solid #e1e4e8; font-size: 12px; color: #586069; text-align: center; }}
        .btn {{ display: inline-block; padding: 8px 16px; background-color: #1a73e8; color: #ffffff; text-decoration: none; border-radius: 4px; font-weight: 500; margin-top: 15px; }}
      </style>
    </head>
    <body>
      <div class="card">
        <div class="header">
          <span class="badge">{severity.upper()}</span>
          <h2>Olist Lakehouse Alert: {subject}</h2>
        </div>
        <div class="content">
          <p>Hệ thống <strong>Olist Modern Data Lakehouse</strong> phát hiện một sự cố cần can thiệp:</p>
          <div class="code-box">
            {escaped_body}
          </div>
          <div style="text-align: center;">
            <a href="http://localhost:8083" class="btn" style="color: #ffffff;">Mở Airflow Web UI</a>
            &nbsp;
            <a href="http://localhost:3000" class="btn" style="background-color: #f39c12; color: #ffffff;">Mở Grafana Monitoring</a>
          </div>
        </div>
        <div class="footer">
          Thông báo tự động từ Olist DataOps Platform &bull; Thời gian: {os.environ.get('TZ', 'UTC')}
        </div>
      </div>
    </body>
    </html>
    """


def send_alert_email(
    subject: str,
    body_text: str,
    severity: str = "ERROR",
    html_content: str | None = None,
    timeout: int = 15,
) -> bool:
    """Gửi email cảnh báo tới Quản trị viên (Admin) qua giao thức SMTP tiêu chuẩn.

    Hỗ trợ kết nối Gmail thật (cổng 587 STARTTLS hoặc cổng 465 SSL) và máy chủ nội bộ (MailHog).
    Tự động phòng vệ không để ném ngoại lệ làm gián đoạn luồng xử lý dữ liệu.
    """
    if not is_email_alert_enabled():
        LOGGER.info("Tính năng cảnh báo email đang bị tắt (ALERT_EMAIL_ENABLED=false)")
        return False

    # 1. Đọc và chuẩn hóa cấu hình SMTP từ biến môi trường
    host = os.environ.get("ALERT_SMTP_HOST", "smtp.gmail.com").strip()
    port_str = os.environ.get("ALERT_SMTP_PORT", "").strip()
    user = os.environ.get("ALERT_SMTP_USER", "").strip()
    password = os.environ.get("ALERT_SMTP_PASSWORD", "").strip()

    # Nhận diện Gmail: nếu host là smtp.gmail.com hoặc user là gmail
    is_gmail = "gmail.com" in host.lower() or "gmail.com" in user.lower()
    if is_gmail and not host:
        host = "smtp.gmail.com"

    # Xác định cổng kết nối mặc định
    if not port_str:
        port = 587 if is_gmail else 1025
    else:
        port = int(port_str)

    # Người gửi: Gmail yêu cầu From phải trùng khớp với tài khoản đăng nhập
    sender = os.environ.get("ALERT_FROM_EMAIL", "").strip() or user or "lakehouse-alert@olist.local"
    recipient = os.environ.get("ALERT_ADMIN_EMAIL", "").strip() or user or "admin@olist.local"

    # Xác định chế độ bảo mật: SSL (cổng 465) hoặc STARTTLS (cổng 587 hoặc cờ cấu hình)
    use_ssl = (port == 465) or _is_truthy(os.environ.get("ALERT_SMTP_SSL", "false"))
    use_starttls = (port == 587) or _is_truthy(os.environ.get("ALERT_SMTP_STARTTLS", "true" if is_gmail else "false"))

    # 2. Xây dựng đối tượng EmailMessage (MIME Multipart chuẩn UTF-8)
    message = EmailMessage()
    message["Subject"] = f"[{severity.upper()}] Olist Lakehouse Alert: {subject}"
    message["From"] = sender
    message["To"] = recipient

    # Thiết lập nội dung văn bản thuần (Fallback)
    message.set_content(body_text, charset="utf-8")

    # Thiết lập giao diện HTML nếu có hoặc dùng template mặc định
    rendered_html = html_content or _build_html_template(subject, body_text, severity)
    message.add_alternative(rendered_html, subtype="html", charset="utf-8")

    # 3. Thực thi kết nối và gửi thư
    try:
        if use_ssl:
            # Chế độ SSL Direct (thường dùng cho cổng 465)
            ssl_context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=ssl_context, timeout=timeout) as server:
                if user and password:
                    server.login(user, password)
                server.send_message(message)
        else:
            # Chế độ thông thường hoặc nâng cấp STARTTLS (cổng 587 hoặc 1025)
            with smtplib.SMTP(host, port, timeout=timeout) as server:
                server.ehlo()
                if use_starttls:
                    ssl_context = ssl.create_default_context()
                    server.starttls(context=ssl_context)
                    server.ehlo()
                if user and password:
                    server.login(user, password)
                server.send_message(message)

    except smtplib.SMTPAuthenticationError as auth_err:
        LOGGER.error(
            "XÁC THỰC SMTP THẤT BẠI: %s. "
            "Nếu bạn đang dùng Gmail, Google BẮT BUỘC bạn phải sử dụng 'Mật khẩu ứng dụng' (App Password 16 ký tự "
            "tạo tại https://myaccount.google.com/apppasswords), KHÔNG DÙNG mật khẩu đăng nhập Google thông thường!",
            auth_err,
        )
        return False
    except (socket.timeout, TimeoutError) as timeout_err:
        LOGGER.warning("Kết nối SMTP tới %s:%s bị hết hạn thời gian (Timeout): %s", host, port, timeout_err)
        return False
    except socket.gaierror as dns_err:
        LOGGER.warning("Không thể phân giải tên miền máy chủ SMTP '%s': %s", host, dns_err)
        return False
    except Exception as error:  # noqa: BLE001 - tuyệt đối không để kênh cảnh báo làm sập pipeline
        LOGGER.warning("Không gửi được email cảnh báo tới %s: %s", recipient, error)
        return False

    LOGGER.info("Đã gửi email cảnh báo thành công [%s] '%s' tới %s", severity.upper(), subject, recipient)
    return True


def main() -> int:
    """CLI kiểm thử nhanh việc gửi email cảnh báo từ dòng lệnh."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Kiểm thử gửi email cảnh báo sự cố Lakehouse")
    parser.add_argument("--subject", default="Kiểm tra kết nối email cảnh báo Olist Lakehouse", help="Tiêu đề email")
    parser.add_argument("--body", default="Đây là email kiểm thử kết nối SMTP từ module email_alert.py.", help="Nội dung email")
    parser.add_argument("--severity", default="INFO", choices=["INFO", "WARN", "ERROR", "CRITICAL"], help="Mức độ")
    parser.add_argument("--recipient", default=None, help="Địa chỉ email nhận (ghi đè cấu hình ALERT_ADMIN_EMAIL)")
    args = parser.parse_args()

    if args.recipient:
        os.environ["ALERT_ADMIN_EMAIL"] = args.recipient

    print("=" * 70)
    print("THÔNG SỐ CẤU HÌNH SMTP HIỆN TẠI TRONG MÔI TRƯỜNG:")
    print(f"  ALERT_EMAIL_ENABLED   : {os.environ.get('ALERT_EMAIL_ENABLED', 'true')}")
    print(f"  ALERT_SMTP_HOST       : {os.environ.get('ALERT_SMTP_HOST', 'smtp.gmail.com')}")
    print(f"  ALERT_SMTP_PORT       : {os.environ.get('ALERT_SMTP_PORT', '587')}")
    print(f"  ALERT_SMTP_USER       : {os.environ.get('ALERT_SMTP_USER', '(chưa cấu hình)')}")
    print(f"  ALERT_SMTP_PASSWORD   : {'******' if os.environ.get('ALERT_SMTP_PASSWORD') else '(chưa cấu hình)'}")
    print(f"  ALERT_ADMIN_EMAIL     : {os.environ.get('ALERT_ADMIN_EMAIL', '(chưa cấu hình)')}")
    print("=" * 70)

    success = send_alert_email(subject=args.subject, body_text=args.body, severity=args.severity)
    if success:
        print("[SUCCESS] Email cảnh báo đã được gửi thành công!")
        return 0
    else:
        print("[FAILED] Gửi email thất bại. Hãy kiểm tra các thông báo log ở trên để xử lý.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
