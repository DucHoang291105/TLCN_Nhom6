# TLCN Nhóm 6 — Data Lakehouse bất động sản Việt Nam

Đề tài xây dựng Data Lakehouse phục vụ phân tích dữ liệu thị trường bất động
sản tại Việt Nam. Dữ liệu chính là hơn 3,5 triệu tin đăng bất động sản; vì đây
là dữ liệu listing, các chỉ số giá được hiểu là **giá đăng/giá chào bán**, không
phải giá giao dịch thành công.

Các nguồn đã chốt cho TLCN:

- Listing chính: [vduydong/vietnam-real-estates-2](https://huggingface.co/datasets/vduydong/vietnam-real-estates-2).
- GIS/reference: [adminvsrm/GISData](https://github.com/adminvsrm/GISData).
- Administrative master: [thanglequoc/vietnamese-provinces-database](https://github.com/thanglequoc/vietnamese-provinces-database).

NSO hiện chỉ là nguồn mở rộng tùy chọn; Google Maps/Places chưa được duyệt và
không nằm trong pipeline hiện tại.

## Kiến trúc mục tiêu

```text
Real Estate + GIS/Administrative Reference
                    ↓
Raw → Bronze → Silver → Gold → PostgreSQL/DWH → Dashboard
```

| Lớp/hạng mục | Trạng thái |
|---|---|
| Raw và Data Profiling | Hoàn thành, đã bổ sung profiling count fields/P99 |
| Bronze PySpark | Hoàn thành, full dataset đã PASS |
| Source Catalog + Silver Core schema/DQ | Hoàn thành, phiên bản v1 đã triển khai |
| Silver Listing Core | Hoàn thành, full dataset và validator độc lập đã PASS |
| Location Mapping/Location Master | Đang phát triển độc lập từ SRC02/SRC03; chưa tích hợp vào Silver Listing Core |
| Gold, DWH và Dashboard | Chưa thực hiện |

Bronze hiện đọc 10 Raw shard, giữ nguyên 19 cột nguồn, thêm 5 cột metadata và
ghi Parquet Snappy. Batch đã xác minh gần nhất có 3.500.744 dòng và 24 cột.

Kết quả Bronze đã kiểm định:

| Chỉ số | Kết quả |
|---|---:|
| Batch | `real_estate_20260904T134803402485Z_dd5b0a8c` |
| Trạng thái | `PASS` |
| Input / output | 3.500.744 / 3.500.744 dòng |
| Raw / Bronze columns | 19 / 24 |
| Source / part files | 10 / 10 |
| Kích thước output | 1.666.891.099 byte (khoảng 1,552 GiB) |
| Compression | `SNAPPY` |
| Thời gian chạy | 89,433 giây |

## Cấu trúc chính

```text
data/
├── raw/                 # Dữ liệu nguồn, không đưa lên Git
├── bronze/              # Dữ liệu ingest, không đưa lên Git
├── silver/              # Dữ liệu chuẩn hóa, không đưa lên Git
└── gold/                # Dữ liệu tổng hợp, không đưa lên Git
docs/
├── profiling/           # Kết quả khảo sát dữ liệu
├── quality/             # Báo cáo kiểm định Bronze và Silver
├── silver/              # Silver Core schema/design contract
├── data_sources.xlsx    # Danh mục nguồn và scope
├── data_dictionary.xlsx # Data Dictionary Silver Core 58 cột
└── data_quality_rules.xlsx # DQ01-DQ14 và threshold/version
scripts/
├── setup_spark_windows.ps1
├── run_bronze.ps1
└── run_silver.ps1
src/
├── bronze/              # Smoke test, ingestion và validator Bronze
├── quality/             # Profiling và sinh tài liệu chất lượng
└── silver/              # Build và validator Silver Listing Core
```

## Yêu cầu môi trường

- Windows PowerShell.
- Python 3.12 (Python 3.10+ tương thích với PySpark hiện tại).
- Java 17 và lệnh `java` có trong `PATH`.
- Tối thiểu 7 GiB dung lượng trống ngoài dữ liệu Raw khi build Silver.
- Không cần cài một bản `winutils.exe` không chính thức.

Kiểm tra nhanh:

```powershell
python --version
java -version
```

## Chuẩn bị dữ liệu Raw

Dữ liệu lớn không nằm trong GitHub. Sau khi clone, đặt đúng 10 file sau vào
`data/raw/real_estate/`:

```text
shard_0000.parquet ... shard_0009.parquet
```

Specification hiện tại:

- 10 file.
- 3.500.744 dòng.
- 19 cột Raw.
- Tổng dung lượng khoảng 1,555 GiB.

## Cài môi trường Bronze

Từ thư mục gốc repository:

```powershell
.\scripts\setup_spark_windows.ps1
```

Script sẽ:

1. Tạo `.venv` nếu chưa có.
2. Cài các phiên bản trong `requirements.txt`.
3. Chuẩn bị local filesystem JAR cho Spark trên Windows và xác minh SHA-256.

Chỉ cần chạy lại setup khi tạo môi trường mới hoặc thay đổi dependency.

## Chạy lớp Bronze

### 1. Smoke test

```powershell
.\scripts\run_bronze.ps1 -SmokeTest
```

Smoke test đọc `shard_0000.parquet`, kiểm tra 350.075 dòng/19 cột, chạy Python
worker và ghi–đọc lại 1.000 dòng. Kết quả đúng sẽ có:

```text
SMOKE_TEST: PASS
```

### 2. Chạy full ingestion

```powershell
.\scripts\run_bronze.ps1
```

Kết quả mong đợi ở cuối console:

```text
[BRONZE] input_rows=3500744
[BRONZE] output_rows=3500744
[BRONZE] output_columns=24
[BRONZE] part_files=10
[BRONZE] status=PASS
```

Không gọi trực tiếp `01_ingest_real_estate.py` trên Windows. Launcher tạo một
ổ đĩa ASCII tạm để Spark xử lý được đường dẫn project có Unicode/khoảng trắng,
sau đó luôn gỡ ổ trong `finally`. Nếu ổ `T:` đang được dùng:

```powershell
.\scripts\run_bronze.ps1 -DriveLetter U
```

Không chạy đồng thời hai tiến trình Bronze vì pipeline hiện chưa có execution
lock.

## Bronze xử lý những gì?

Pipeline `src/bronze/01_ingest_real_estate.py`:

1. Chỉ nhận đúng 10 Raw shard theo specification.
2. Giữ nguyên 19 cột và kiểu dữ liệu nguồn; không clean, cast business field,
   deduplicate hoặc loại outlier tại Bronze.
3. Thêm `source_name`, `source_file`, `ingested_at`, `ingestion_date` và
   `batch_id`.
4. Ghi Parquet Snappy vào staging.
5. Kiểm tra schema, tổng số dòng, số dòng từng shard, metadata, fingerprint 19
   cột nghiệp vụ, codec, `_SUCCESS` và khả năng đọc lại.
6. Chỉ promote staging thành output chính thức khi tất cả kiểm tra đều PASS;
   output tốt trước đó được giữ nếu batch mới thất bại.

Spark local được cấu hình `local[2]`, driver heap 4 GiB và tắt Hadoop Parquet
vectored I/O để giới hạn bộ nhớ khi xử lý row group lớn trên Windows.

## Output và cách kiểm tra

Dữ liệu Bronze:

```text
data/bronze/real_estate/
├── part-*.snappy.parquet
└── _SUCCESS
```

Báo cáo ổn định:

```text
docs/quality/bronze_ingestion_summary.json
docs/quality/bronze_completion_report.md
```

Chạy kiểm định độc lập bằng DuckDB và PyArrow:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\src\bronze\02_validate_bronze.py
```

Validator quét lại toàn bộ Raw và Bronze, đối chiếu số dòng/fingerprint 19 cột,
metadata, timestamp UTC, schema, codec và thư mục staging. Kết quả hợp lệ:

```json
{
  "status": "PASS",
  "rows": 3500744,
  "columns": 24,
  "source_files": 10,
  "part_files": 10,
  "metadata_null_rows": 0,
  "parquet_codecs": ["SNAPPY"],
  "raw_bronze_business_match": true
}
```

## Chạy Silver Listing Core

Silver Core v1 chỉ đọc `data/bronze/real_estate/` của SRC01 và tập trung chuẩn hóa
dữ liệu tin đăng. Dữ liệu GIS, danh mục hành chính và Location Master được xử lý
trong pipeline Location Mapping riêng, sau đó mới tích hợp ở Silver Enriched.

Chạy kiểm tra 10.000 dòng trước:

```powershell
.\scripts\run_silver.ps1 -SmokeTest
```

Chạy toàn bộ dữ liệu:

```powershell
.\scripts\run_silver.ps1
```

Pipeline thực hiện:

1. Kiểm tra Bronze PASS, schema, source/batch, property type và threshold contract.
2. Chuẩn hóa text, giá, diện tích, số tầng/phòng và thời gian đăng.
3. Tạo `listing_id` SHA-256 có `source_id` + `source_name` để sẵn sàng cho đa nguồn.
4. Giữ đủ audit rows, đánh `is_canonical=false` cho 50 bản sao exact duplicate.
5. Tạo `price_per_m2`, các cờ field-validity và áp dụng DQ01–DQ14.
6. Ghi 58 cột Parquet Snappy theo `partition_year_month`, read-back và chỉ promote
   khi toàn bộ quality gate PASS.

Kết quả full run đã xác nhận:

| Chỉ số | Kết quả |
|---|---:|
| Trạng thái | `PASS` |
| Input / output | 3.500.744 / 3.500.744 dòng |
| Output columns | 58 |
| Canonical / noncanonical | 3.500.694 / 50 dòng |
| Exact duplicate groups | 36 |
| DQ status VALID / REVIEW / REJECTED | 2.698.781 / 744.590 / 57.373 |
| Part files | 10 |
| Kích thước | 2.179.352.949 byte (khoảng 2,03 GiB) |
| Compression | `SNAPPY` |
| Thời gian full run | 312,05 giây |

Output và báo cáo:

```text
data/silver/real_estate_core/
docs/quality/silver_core_summary.json
docs/quality/silver_core_validation.json
docs/quality/silver_core_completion_report.md
```

Chạy lại kiểm định độc lập bằng DuckDB/PyArrow:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\src\silver\02_validate_silver_core.py
```

Khi phân tích, luôn lọc `is_canonical=true` để tránh đếm trùng, sau đó thêm cờ
hợp lệ đúng với KPI (`is_price_valid`, `is_area_valid`, `is_published_at_valid`,
v.v.). Không nên loại mọi dòng khác `dq_status='VALID'` vì một dòng thiếu ward vẫn
có thể dùng cho KPI giá hoặc cấp tỉnh.

Chi tiết riêng nằm tại [`src/silver/README.md`](src/silver/README.md).

## Xuất danh sách địa danh để bàn giao

Khi Raw đã có đầy đủ, chạy:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\src\quality\04_export_location_values.py
```

Đầu ra nhỏ, được lưu trong Git để hỗ trợ xây Location Mapping:

```text
docs/profiling/provinces.csv
docs/profiling/district_distinct.csv
docs/profiling/ward_distinct.csv
```

Chi tiết riêng của Bronze nằm tại [`src/bronze/README.md`](src/bronze/README.md).
