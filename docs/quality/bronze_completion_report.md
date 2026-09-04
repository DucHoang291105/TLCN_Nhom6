# Báo cáo hoàn thành lớp Bronze — Người 1

Ngày hoàn thành: 2026-09-04 20:49 (Asia/Saigon)

## Kết luận

Lớp Bronze cho dữ liệu bất động sản đã hoàn thành và vượt qua cả kiểm tra trong
pipeline Spark lẫn kiểm tra độc lập bằng DuckDB/PyArrow.

- Batch chính thức: `real_estate_20260904T134803402485Z_dd5b0a8c`
- Trạng thái: `PASS`
- Thời gian chạy: 89,433 giây
- Input: 10 Raw shard, 3.500.744 dòng, 19 cột
- Output: 10 part Parquet, 3.500.744 dòng, 24 cột
- Kích thước Raw: 1.669.244.898 byte (1,555 GiB)
- Kích thước Bronze: 1.666.891.099 byte (1,552 GiB)
- Compression thực tế: `SNAPPY`

## Đã thực hiện

1. Khóa phiên bản môi trường trong `requirements.txt`: DuckDB, Pandas,
   PyArrow, OpenPyXL và PySpark 4.2.0.
2. Tạo script setup Spark local trên Windows, gồm kiểm tra checksum JAR local
   filesystem dùng để không phụ thuộc vào bản `winutils.exe` không chính thức.
3. Tạo launcher dùng ổ `SUBST` tạm thời để Spark xử lý được đường dẫn project
   có Unicode/khoảng trắng; launcher luôn gỡ ổ trong `finally`.
4. Tạo và chạy smoke test: đọc đúng shard mẫu 350.075 dòng/19 cột, chạy Python
   worker, ghi và đọc lại 1.000 dòng thành công.
5. Tạo pipeline Raw → Bronze:
   - Chỉ đọc đúng 10 file `shard_0000.parquet` đến `shard_0009.parquet`.
   - Giữ nguyên thứ tự và kiểu của toàn bộ 19 cột nguồn.
   - Thêm 5 cột metadata: `source_name`, `source_file`, `ingested_at`,
     `ingestion_date`, `batch_id`.
   - Ghi Parquet Snappy vào staging, kiểm định xong mới promote sang output
     chính thức; output tốt trước đó được bảo vệ nếu lần chạy mới lỗi.
   - Ghi báo cáo JSON riêng theo batch và một báo cáo `latest` ổn định.
6. Giới hạn Spark ở `local[2]`, driver heap 4 GiB và tắt Hadoop Parquet
   vectored I/O để phù hợp với các row group lớn trên máy Windows local.
7. Tạo validator độc lập để đối chiếu toàn bộ dữ liệu Raw ↔ Bronze bằng một
   thuật toán fingerprint khác với pipeline Spark.
8. Viết hướng dẫn chạy lại tại `src/bronze/README.md`.

## Kết quả kiểm định

Tất cả kiểm tra sau đều `PASS`:

- Danh sách 10 Raw shard đúng specification.
- Schema Raw đúng 19 cột; schema Bronze đúng 24 cột và đúng thứ tự.
- Tổng số dòng input bằng output: 3.500.744.
- Số dòng theo nguồn khớp tuyệt đối:
  - `shard_0000`–`shard_0003`: mỗi file 350.075 dòng.
  - `shard_0004`–`shard_0009`: mỗi file 350.074 dòng.
- Fingerprint 19 cột nghiệp vụ khớp Raw ↔ Bronze trên cả 10 shard.
- 5 cột metadata không có dòng null; source, batch, timestamp và ngày ingest
  đồng nhất.
- `ingested_at` lưu đúng UTC: `2026-09-04T13:48:03.402485+00:00`.
- Tất cả column chunk vật lý dùng codec `SNAPPY`.
- Đọc lại output thành công và có `_SUCCESS`.
- Không còn thư mục staging/backup; ổ `SUBST` tạm đã được gỡ.
- Raw vẫn gồm đúng 10 file/1.669.244.898 byte và fingerprint khớp output; pipeline
  không sửa dữ liệu Raw.

## Sự cố đã xử lý

- Lần full đầu tiên dùng `local[*]`, chạy 20 task đồng thời với heap mặc định
  nên hết Java heap. Pipeline dừng trước khi ghi output và cleanup staging đúng
  thiết kế. Đã sửa bằng `local[2]`, heap 4 GiB và tắt Parquet vectored I/O.
- Lần PASS kế tiếp phát hiện trường timestamp trong JSON bị hiển thị lệch +7 giờ
  do chuyển đổi datetime từ JVM sang Python. Dữ liệu Parquet không sai. Pipeline
  đã đổi sang kiểm tra epoch-microseconds trong JVM và batch chính thức phía trên
  xác nhận UTC chính xác.

Các JSON theo từng lần chạy được giữ local để chẩn đoán và không đưa lên Git.
Báo cáo có thẩm quyền được chia sẻ trong repository là
`docs/quality/bronze_ingestion_summary.json`.

## Cách chạy lại

```powershell
.\scripts\setup_spark_windows.ps1
.\scripts\run_bronze.ps1 -SmokeTest
.\scripts\run_bronze.ps1
.\.venv\Scripts\python.exe -X utf8 .\src\bronze\02_validate_bronze.py
```

Không chạy đồng thời hai tiến trình Bronze vì phiên bản hiện tại chưa có
execution lock.

## Phần chưa làm

- Chưa clean, chuẩn hóa địa chỉ, cast trường nghiệp vụ, deduplicate hoặc áp dụng
  Data Quality; các bước này thuộc lớp Silver theo đúng phân lớp đã thống nhất.
- Dữ liệu `data/bronze/**` được `.gitignore`; thành viên khác cần chạy pipeline
  để sinh dữ liệu local hoặc nhận dữ liệu bằng một kênh lưu trữ riêng.
