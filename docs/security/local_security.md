# Bảo mật cục bộ

Bản triển khai bind các cổng đã công khai vào `127.0.0.1`, xác minh URL endpoint cục bộ, giữ `.env` trong ignore, dùng source mount chỉ đọc khi khả thi, đồng thời chặn thao tác phá hủy bằng label chính xác của Compose project và xác nhận tường minh.

> [!NOTE]
> Cơ chế **maintenance approval secret** (ký HMAC báo cáo dry-run) đã được **loại bỏ hoàn toàn**
> cùng thư mục `secrets/`. Nó không mang lại giá trị an ninh thực tế — kẻ tấn công đã vào được
> container Airflow thì cũng đọc được chính file secret đó — trong khi lại là nguyên nhân gây lỗi
> treo DAG. Việc kiểm soát rủi ro xóa nhầm dữ liệu nay dựa vào **allowlist tường minh** các bảng
> được phép bảo trì, cộng với ngưỡng thời gian lưu giữ tối thiểu, khai báo ngay trong
> `transformation/maintenance/job.py`.

Các giới hạn này tạo ra rủi ro đáng kể: traffic giữa các dịch vụ không có TLS; biến môi trường Compose làm lộ thông tin xác thực cho quản trị viên Docker cục bộ; MinIO root vẫn được cấp cho thao tác khởi tạo và sao lưu/khôi phục tường minh, trong khi client upload, xử lý và truy vấn dùng các policy có phạm vi riêng; basic authentication của Airflow và tài khoản admin Superset/Grafana cục bộ không cung cấp enterprise identity; image được pin nhưng không được ký tại đây; host bị xâm nhập sẽ vô hiệu hóa mọi ranh giới container. MinIO chỉ dùng một volume cục bộ, không có replication, versioning hoặc bản sao lưu độc lập. PostgreSQL chỉ có một instance metadata/catalog.

Container **MailHog** (cổng 8025) là máy chủ SMTP giả lập **không có xác thực và lưu thư trong RAM**. Mọi email cảnh báo, bao gồm cả tên DAG/task và tóm tắt lỗi, đều đọc được bởi bất kỳ ai truy cập được cổng 8025 trên máy. Chấp nhận được khi chạy cục bộ; khi lên production bắt buộc phải thay bằng SMTP thật có TLS và xác thực (đặt `ALERT_SMTP_STARTTLS=true` cùng thông tin đăng nhập lấy từ secret manager).

Không được tái sử dụng các thông tin xác thực này, công khai cổng ra ngoài loopback, commit `.env` hoặc xem Docker secret cục bộ như vault cho production. Đưa hệ thống lên production bắt buộc phải có TLS, workload identity được quản lý bên ngoài, secret manager, vòng đời S3 policy được audit tập trung, network policy, image provenance, lưu giữ audit, object storage có replication, PostgreSQL HA/sao lưu và disaster recovery đã được kiểm thử.
