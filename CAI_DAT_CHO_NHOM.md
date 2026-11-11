# CÀI ĐẶT & CHẠY BTL OLIST DATA LAKEHOUSE — ĐỌC TRƯỚC KHI LÀM GÌ

> Gói này chứa **toàn bộ** thứ cần thiết: mã nguồn, dữ liệu, tài liệu, và cả 12 image Docker đã
> build sẵn. Bạn **không cần tải gì thêm từ mạng** ngoài một image nhỏ 9 MB.
>
> Làm theo đúng 5 bước dưới đây là chạy được. Tổng thời gian khoảng **25 phút**, trong đó phần lớn
> là ngồi chờ máy chạy.

---

## BƯỚC 0 — Kiểm tra máy có chạy nổi không (làm trước, kẻo mất công)

| Hạng mục | Yêu cầu tối thiểu | Cách kiểm tra |
|:---|:---|:---|
| RAM của máy | **16 GB** (máy 8 GB sẽ rất chật vật) | Ctrl+Shift+Esc → tab Performance → Memory |
| Dung lượng trống | **35 GB** | Ưu tiên ổ không phải ổ C: |
| Docker Desktop | Đã cài, bật WSL2 backend | Mở Docker Desktop, góc dưới trái phải có chấm xanh "Engine running" |
| Hệ điều hành | Windows 10/11 | PowerShell có sẵn, không cần cài gì thêm |

**Cấp RAM cho Docker:** Docker Desktop → **Settings → Resources** → kéo **Memory** lên **ít nhất 8 GB**
→ **Apply & Restart**. Đây là bước hay bị bỏ qua nhất, và bỏ qua thì Spark sẽ chết giữa chừng.

> [!TIP]
> **Nên để dữ liệu Docker ở ổ khác ổ C:** Docker Desktop → **Settings → Resources → Advanced →
> Disk image location** → chọn một thư mục trên ổ D: (hoặc ổ nào còn nhiều chỗ) → **Apply & Restart**.
> Bộ image này nặng ~13 GB, để ở ổ C: rất dễ làm đầy ổ hệ thống.

---

## BƯỚC 1 — Giải nén

Giải nén gói vào một thư mục **không có dấu tiếng Việt và không có khoảng trắng** trong đường dẫn.

- ✅ Tốt: `D:\BTL\lakehouse`
- ❌ Tránh: `C:\Users\Nguyễn Văn A\Tài liệu\bài tập lớn`

Sau khi giải nén, trong thư mục phải thấy các tệp sau. **Nếu thiếu tệp nào thì gói bị lỗi, báo lại
người gửi ngay, đừng chạy tiếp:**

```
docker-compose.yml
.env                              ← TỆP NÀY QUAN TRỌNG NHẤT, chứa mật khẩu
olist-lakehouse-images.tar        ← 5,4 GB
scripts\up.ps1
data\raw\                         ← 9 tệp CSV
```

> [!WARNING]
> Tệp `.env` bắt đầu bằng dấu chấm nên Windows Explorer có thể ẩn đi. Bật hiển thị: tab **View** →
> tích **Hidden items**. Thiếu `.env` thì toàn bộ container sẽ chết ngay giây đầu tiên.

---

## BƯỚC 2 — Nạp 12 image Docker (~10 phút)

Mở **PowerShell** (không phải Command Prompt), chuyển vào thư mục vừa giải nén, rồi chạy:

```powershell
docker load -i .\olist-lakehouse-images.tar
```

Màn hình sẽ in ra hàng loạt dòng `Loaded image: ...`. Chạy xong, kiểm tra lại:

```powershell
docker images | findstr olist-lakehouse
```

Phải thấy **5 dòng**: `airflow`, `spark`, `superset`, `minio`, `mc`.

> **Vì sao phải làm bước này?** Vì 5 image đó được build riêng cho dự án (Spark 3.5.9 + Iceberg
> 1.11.0 + JDK + hàng chục thư viện Python), **không tải sẵn ở đâu trên mạng được**. Bỏ qua bước này
> thì Docker sẽ phải tự build lại, mất 30–60 phút và rất dễ hỏng giữa chừng.

---

## BƯỚC 3 — Bật hệ thống (~4 phút)

```powershell
.\scripts\up.ps1
```

Script tự chờ đến khi **cả 13 container đều khỏe** rồi mới trả quyền điều khiển, và in ra bảng địa
chỉ truy cập. Nếu máy chậm thì tăng hạn chờ:

```powershell
.\scripts\up.ps1 -TimeoutSeconds 1800
```

Lúc này máy cần có mạng khoảng 10 giây để tải một image nhỏ tên `mailhog` (9 MB) — đây là image duy
nhất không nằm trong gói.

---

## BƯỚC 4 — Nạp dữ liệu vào Lakehouse (~8 phút)

Bật xong thì hệ thống mới chỉ là cái nhà trống, chưa có dữ liệu. Chạy pipeline để nạp:

```powershell
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags unpause olist_daily_pipeline
docker exec olist-lakehouse-airflow-scheduler-1 airflow dags trigger olist_daily_pipeline
```

Mở http://localhost:8083 (tài khoản `admin` / `admin`) → bấm vào `olist_daily_pipeline` → tab
**Graph** để xem các ô task chuyển xanh dần. Chờ đến khi ô cuối cùng xanh.

---

## BƯỚC 5 — Kiểm tra đã thành công chưa

```powershell
.\scripts\verify_stack.ps1
```

Phải ra dòng màu xanh:

```
KET QUA: TAT CA KIEM TRA DEU PASS.
```

Và **4 con số này phải khớp tuyệt đối** — đây là chữ ký của hệ thống, sai một chữ số nghĩa là dữ
liệu chưa đúng:

| Chỉ số | Giá trị đúng |
|:---|---:|
| Số đơn hàng | `99441` |
| Doanh thu hàng | `13591643.70` |
| Cước vận chuyển | `2251909.54` |
| Tổng thanh toán | `16008872.12` |

**Đến đây là xong.** Máy bạn giờ có Lakehouse giống hệt máy người gửi.

---

## Địa chỉ truy cập

Lệnh `.\scripts\up.ps1` ở bước 3 **tự in ra bảng địa chỉ và tài khoản** ngay sau khi chạy xong — cứ
nhìn vào màn hình là thấy.

Bảng đầy đủ 8 giao diện cũng nằm ở **mục 2 của [`huong_dan_su_dung.md`](huong_dan_su_dung.md)**.
Tài liệu đó là **nguồn duy nhất** cho thông tin này; cố tình không chép lại ở đây để mật khẩu không
bao giờ lệch nhau giữa hai tệp.

---

## Tắt hệ thống khi không dùng

```powershell
.\scripts\down.ps1
```

Dữ liệu **vẫn còn nguyên**, lần sau chỉ cần `.\scripts\up.ps1` là có lại ngay, không phải làm lại
bước 2 và bước 4.

> [!CAUTION]
> **Tuyệt đối không chạy `./scripts/down.sh -v` (hoặc `.\scripts\down.ps1 -RemoveVolumes`)** và **không chạy
> `docker compose down -v`** nếu chỉ muốn tạm tắt máy nghỉ ngơi. Cờ `-v` sẽ **xóa
> sạch toàn bộ dữ liệu Lakehouse**, và dựng lại mất 15–20 phút.

---

## Bốn lỗi hay gặp nhất

| Hiện tượng | Nguyên nhân | Cách xử lý |
|:---|:---|:---|
| `up.ps1` báo **port is already allocated** | Máy đang chạy sẵn thứ khác chiếm cổng 8083/8088/9000/3000 | `docker ps` xem đang chạy gì, `docker stop <tên>`. Nếu là phần mềm khác (XAMPP, SQL Server…) thì tắt nó đi |
| Container bật rồi **tự chết**, hoặc báo thiếu biến môi trường | **Thiếu tệp `.env`** khi giải nén | Xin lại tệp `.env` từ người gửi, đặt vào thư mục gốc (cùng chỗ với `docker-compose.yml`) |
| Task Spark chạy một lúc rồi **chết, log ghi OOM / exit code 137** | Docker được cấp quá ít RAM | Docker Desktop → Settings → Resources → Memory ≥ 8 GB → Apply & Restart, rồi chạy lại bước 4 |
| Trigger DAG rồi mà **không thấy chạy** | DAG có lịch `@daily` và chỉ cho 1 lần chạy đồng thời, nên lần chạy theo lịch đang chiếm chỗ | Chờ lần chạy đó xong (~8 phút) rồi trigger lại |

Bốn lỗi trên là lỗi **lúc cài đặt**. Các lỗi gặp trong quá trình sử dụng về sau nằm ở mục 7 của
`huong_dan_su_dung.md`.

---

## Cài xong rồi thì đọc gì tiếp

Mở **[`huong_dan_su_dung.md`](huong_dan_su_dung.md)** — đó là tài liệu vận hành chính: cách chạy
pipeline, viết SQL truy vấn, xem dashboard, bảng tra sự cố đầy đủ, và cả kịch bản demo 10 phút.
Trong đó có sẵn **bản đồ dẫn tới mọi tài liệu còn lại** (kiến trúc, báo cáo nghiệm thu, runbook),
nên bạn chỉ cần nhớ đúng một tệp này.

Từ giờ trở đi, tệp cài đặt bạn đang đọc không còn cần đến nữa.
