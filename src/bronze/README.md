# Bronze Layer

Bronze đọc đúng 10 file `data/raw/real_estate/shard_*.parquet`, giữ nguyên
19 cột nguồn và thêm 5 cột metadata. Không clean, cast, deduplicate hoặc áp
dụng Data Quality ở lớp này.

## Chạy trên Windows

Project nằm trong đường dẫn có Unicode và khoảng trắng. Spark/Hadoop trên
Windows cần alias đường dẫn ASCII và một local filesystem implementation để
ghi file mà không phụ thuộc vào `winutils.exe` không chính thức.

```powershell
.\scripts\setup_spark_windows.ps1
.\scripts\run_bronze.ps1 -SmokeTest
.\scripts\run_bronze.ps1
.\.venv\Scripts\python.exe -X utf8 .\src\bronze\02_validate_bronze.py
```

Launcher tạo ổ `T:` tạm thời, chạy Spark rồi gỡ ổ trong `finally`. Nếu `T:` đã
được sử dụng, truyền ký tự khác, ví dụ `-DriveLetter U`.

Full run dùng `local[2]`, driver heap 4 GiB và tắt Parquet vectored I/O để giới
hạn bộ nhớ khi đọc đồng thời các shard lớn trên máy Windows local.

## Đầu ra

- Dữ liệu: `data/bronze/real_estate/`
- Báo cáo: `docs/quality/bronze_ingestion_summary.json`
- Báo cáo từng lần chạy: `docs/quality/bronze_ingestion_<batch_id>.json`
- Expected: 3.500.744 dòng, 24 cột, Parquet Snappy.

Pipeline ghi vào thư mục staging trước, kiểm tra schema, row count theo từng
shard, metadata, fingerprint 19 cột nghiệp vụ, codec Snappy và read-back. Chỉ
khi tất cả đều đạt, staging mới được chuyển thành đầu ra chính thức. Mọi sai
lệch làm pipeline trả về trạng thái `FAIL` và giữ nguyên đầu ra tốt trước đó.
