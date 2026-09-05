# Silver Listing Core — SRC01

> Trạng thái: **IMPLEMENTED AND VALIDATED — PASS**
>
> Phiên bản schema: `silver_listing_core_v1`
>
> Phiên bản DQ: `src01_silver_core_dq_v1`
>
> Output: `data/silver/real_estate_core/`.

## 1. Phạm vi đã khóa

- Input duy nhất: `data/bronze/real_estate/` của `SRC01`.
- Giữ đủ `3.500.744` dòng trong Silver Audit, kể cả dòng lỗi và duplicate.
- Không join SRC02/SRC03, không gán mã hành chính và không geocoding trong Silver Core.
- Không dùng SRC04 và không gọi Google Maps/Places API.
- Chỉ exact dedup nội SRC01; chưa fuzzy matching và chưa matching xuyên nguồn.
- Output: Parquet Snappy tại `data/silver/real_estate_core/`.
- Tập canonical logic dùng `is_canonical = true`, có `3.500.694` dòng.

Luồng hiện tại:

```text
SRC01 Raw -> Bronze Listing -> Silver Listing Core

SRC02 GIS + SRC03 Admin -> Location Master -> Silver Listing Enriched (làm sau)
```

Không cần tải SRC02/SRC03 để duyệt hoặc xây Silver Core. Người 2 chỉ cần tải các
snapshot đã pin khi bắt đầu Location Master.

## 2. Schema đã triển khai

Schema đầy đủ 58 cột nằm tại
[`silver_core_schema.csv`](silver_core_schema.csv). Các nhóm cột gồm:

| Nhóm | Mục đích |
|---|---|
| Identity | `source_id`, `source_name`, `listing_id`, khóa nguồn tương lai |
| Duplicate | `duplicate_group_id`, `duplicate_count`, `duplicate_rank`, `is_canonical` |
| Business | Nội dung, loại BĐS, địa danh raw/clean, giá, diện tích, thuộc tính |
| Time | Raw timestamp, `timestamp_ntz`, ngày/tháng/năm |
| Lineage | File/batch Bronze và batch/time/schema Silver |
| Quality | Trạng thái, mảng mã lỗi và cờ hợp lệ theo từng nhóm KPI |
| Partition | `partition_year_month`, dùng `unknown` nếu ngày không hợp lệ |

`nullable` trong schema là khả năng lưu vật lý ở Silver Audit, không có nghĩa
trường đó hợp lệ để dùng cho mọi KPI.

## 3. Contract của `listing_id`

`listing_id` của SRC01 là SHA-256 từ một JSON có thứ tự field cố định và giữ rõ
giá trị `null`. Payload gồm:

```text
id_contract = src_content_sha256_v1
source_id
source_name
name
description
property_type_name
province_name
district_name
ward_name
street_name
project_name
price
area
floor_count
frontage_width
house_depth
road_width
bedroom_count
bathroom_count
house_direction
balcony_direction
published_at
```

Quy tắc:

1. Bắt buộc có cả `source_id` và `source_name` trong input hash.
2. Hash raw values; không trim/cast/normalize trước khi hash.
3. Dùng JSON struct với `ignoreNullFields=false`; không dùng `concat_ws`.
4. Output là 64 ký tự hexadecimal lowercase.
5. Đổi payload/serialization phải tạo `listing_id_version` mới.
6. Exact duplicate chia sẻ `listing_id`; ID chỉ unique trên tập
   `is_canonical=true`.
7. Nguồn listing tương lai ưu tiên native ID/URL và có ID contract riêng.
   Matching cùng một BĐS giữa nhiều website là một khóa khác, không dùng
   `listing_id` để khẳng định hai tin là một.

## 4. DQ11 — chiến lược duplicate đã làm rõ

- Duplicate group = cùng `source_id`, `source_name` và toàn bộ 19 raw business
  fields.
- `duplicate_count = COUNT(*) OVER (PARTITION BY duplicate_group_id)`.
- `duplicate_rank = ROW_NUMBER()`, ưu tiên `source_file` tăng dần.
- Rank 1: `is_canonical=true`, không nhận DQ11.
- Rank lớn hơn 1: `is_canonical=false`, thêm DQ11 và row status `REJECTED`.
- Không xóa dòng khỏi Silver Audit. Consumer phải lọc `is_canonical=true` khi
  tính aggregate để tránh đếm lặp.

Baseline đã kiểm tra trên SRC01:

| Chỉ số | Giá trị |
|---|---:|
| Exact duplicate groups | 36 |
| Bản sao dư / noncanonical rows | 50 |
| Canonical rows đã xác nhận | 3.500.694 |
| Group cùng nằm trong một file | 21 |
| Group trải qua nhiều file | 15 |
| Kích thước group lớn nhất | 6 |

Bronze không có raw row ordinal. Vì vậy, với các bản sao giống hệt trong cùng
file, không thể hứa physical copy nào luôn là rank 1 qua mọi execution. Tuy
nhiên mọi business value của chúng giống nhau, nên canonical business record và
kết quả aggregate vẫn ổn định; không cần refactor Bronze chỉ vì điểm này.

## 5. Profiling bổ sung và threshold

### Ba trường số đếm

| Field | Non-null | P50 | P95 | Global P99 fallback | Max | Âm | Không nguyên |
|---|---:|---:|---:|---:|---:|---:|---:|
| `floor_count` | 808.762 | 4 | 7 | 8 | 255 | 0 | 0 |
| `bedroom_count` | 1.900.383 | 3 | 6 | 14 | 200 | 0 | 0 |
| `bathroom_count` | 1.779.770 | 2 | 5 | 13 | 255 | 0 | 0 |

Không dùng ba ngưỡng global cho năm loại BĐS, vì phân phối khác nhau rõ rệt;
ví dụ P99 `floor_count` của căn hộ là 40. DQ12–DQ14 dùng
`quantile_disc(P99)` theo `property_type`, và chỉ fallback global với loại chưa
có threshold.

### Threshold đã triển khai

| Property type | P99 area m² | P99 price/m² | P99 floor | P99 bedroom | P99 bathroom |
|---|---:|---:|---:|---:|---:|
| Biệt thự/Nhà liền kề | 894,252 | 625.000.000,00 | 7 | 10 | 10 |
| Căn hộ chung cư | 240 | 261.864.406,78 | 40 | 4 | 4 |
| Nhà | 544 | 735.000.000,00 | 8 | 19 | 18 |
| Shophouse | 535 | 552.310.374,89 | 7 | 10 | 10 |
| Đất | 10.716,35 | 347.947.879,12 | 9 | 20 | 15 |
| Global fallback | 2.222 | 607.692.307,69 | 8 | 14 | 13 |

- DQ05 dùng population `area > 0`, không phụ thuộc `price`.
- DQ06 dùng `price > 0 AND area > 0`, so ngưỡng bằng tỷ lệ chưa làm tròn như
  profiling; cột `price_per_m2` xuất ra vẫn là `DECIMAL(24,2)`.
- DQ12–DQ14 dùng P99 discrete và toán tử strict `>`; bằng threshold không bị
  flag, null cũng không kích hoạt rule.
- Threshold version: `src01_raw_20260904_p99_v1`.

Expected hits trên snapshot hiện tại:

| Rule | Hit rows |
|---|---:|
| DQ05 | 34.807 |
| DQ06 | 32.219 |
| DQ12 | 6.511 |
| DQ13 | 14.530 |
| DQ14 | 11.636 |

Các tập có thể overlap, không được cộng chúng để suy ra tổng dòng lỗi.

## 6. DQ logic đã triển khai

| Rule | Điều kiện tóm tắt | Status | Phạm vi không nên dùng |
|---|---|---|---|
| DQ01 | `price_raw` null/blank | REVIEW | KPI giá |
| DQ02 | `price_vnd <= 0` | REJECTED | KPI giá, price/m² |
| DQ03 | Giá có dữ liệu nhưng cast thất bại | REJECTED | KPI giá, price/m² |
| DQ04 | `area_m2` null hoặc `<= 0` | REJECTED | KPI diện tích, price/m² |
| DQ05 | Area lớn hơn P99 cùng loại | REVIEW | Phân phối diện tích sạch |
| DQ06 | Price/m² lớn hơn P99 cùng loại | REVIEW | Phân phối price/m² sạch |
| DQ07 | Province null/blank | REJECTED | KPI địa lý |
| DQ08 | District null/blank | REVIEW | KPI huyện/xã |
| DQ09 | Ward null/blank | REVIEW | KPI xã |
| DQ10 | Timestamp thiếu/blank/parse lỗi | REJECTED | KPI thời gian |
| DQ11 | `duplicate_rank > 1` | REJECTED | Mọi aggregate canonical |
| DQ12 | Floor count lớn hơn P99 cùng loại | REVIEW | Phân phối số tầng sạch |
| DQ13 | Bedroom count lớn hơn P99 cùng loại | REVIEW | Phân phối phòng ngủ sạch |
| DQ14 | Bathroom count lớn hơn P99 cùng loại | REVIEW | Phân phối phòng tắm sạch |

Chi tiết điều kiện/action nằm trong `docs/data_quality_rules.xlsx`.

Status precedence:

```text
có REJECTED -> REJECTED
không có REJECTED nhưng có REVIEW -> REVIEW
không có rule hit -> VALID
```

Không dùng một filter toàn cục `dq_status='VALID'` cho mọi Gold. Ví dụ:

- Đếm listing: `is_canonical=true`.
- KPI giá: canonical + `is_price_valid`.
- KPI price/m²: canonical + `is_price_valid` + `is_area_valid`; clean view có
  thể loại thêm DQ06.
- KPI tỉnh: canonical + `is_province_valid`.
- KPI huyện: canonical + `is_province_valid` + `is_district_valid`.
- KPI thời gian: canonical + `is_published_at_valid`.
- Phân phối count sạch: loại đúng DQ12, DQ13 hoặc DQ14 theo metric.

## 7. Timestamp và location

Raw `published_at` không có UTC offset. Proposal giữ `published_at_raw` và parse
thành Spark `timestamp_ntz`; không tự gắn UTC hay cộng/trừ 7 giờ. Các cột
date/year/month được lấy trực tiếp từ timestamp local-naive. Nếu xác nhận được
timezone nguồn sau này, phải đổi contract có version thay vì diễn giải lại âm
thầm.

Trong Silver Core, location chỉ trim và chuyển blank thành null. Không bỏ dấu,
không lowercase display name, không thêm mã hành chính và không inner join với
master. Silver Enriched sau này dùng left join, chọn master version theo
`published_date` và bổ sung các cột như `location_id`, administrative codes,
`location_match_status`, `location_match_method`, `location_master_version`,
`valid_from` và `valid_to`.

## 8. Output và validator

```text
data/silver/real_estate_core/
├── partition_year_month=2025-06/
├── ...
├── partition_year_month=2026-03/
└── partition_year_month=unknown/
```

Trước khi ghi, pipeline phải kiểm tra Bronze đang PASS, input đúng SRC01,
3.500.744 dòng, schema 19 business + 5 metadata columns, đủ threshold cho năm
property type và các trường count không âm/không nguyên.

Sau khi ghi/read-back, validator phải kiểm tra:

- Input rows = output rows = 3.500.744 và schema/type đúng đặc tả.
- Technical fields bắt buộc không null; `listing_id` đúng 64 hex chars.
- Không có hash collision giữa hai raw payload khác nhau.
- Đúng 36 duplicate groups, 50 noncanonical rows và mỗi group đúng một
  canonical row.
- DQ11 xuất hiện khi và chỉ khi `duplicate_rank > 1`.
- `dq_error_codes` chỉ chứa DQ01–DQ14, không lặp và có thứ tự ổn định.
- `dq_status`, validity flags và `price_per_m2` đúng contract.
- Partition counts cộng lại đúng tổng; Parquet dùng Snappy và đọc lại được.
- Ghi qua staging rồi promote; sinh `docs/quality/silver_core_summary.json`.

## 9. Quyết định áp dụng trong phiên bản v1

1. Schema 58 cột trong CSV.
2. `listing_id` là source-scoped content hash và chỉ unique trong canonical
   subset.
3. DQ11 exact full-row: giữ toàn bộ audit rows, loại 50 bản sao khỏi canonical.
4. P99 theo property type với global fallback.
5. `published_at` dùng `timestamp_ntz`, chưa quy đổi UTC.
6. Partition theo `partition_year_month`, lỗi ngày vào `unknown`.
7. Gold lọc theo metric-specific validity, không lọc toàn cục theo `dq_status`.
8. DQ15 chưa được đưa vào v1. Hiện có 1.868 dòng mà title và description đều blank;
   quyết định bổ sung DQ15 được hoãn sang phiên bản DQ sau.
9. Location Mapping nằm ngoài Silver Core và chỉ thêm ở Silver Enriched.

Pipeline full và validator độc lập đã PASS trên 3.500.744 dòng. Kết quả ổn định nằm tại
`docs/quality/silver_core_summary.json` và `docs/quality/silver_core_validation.json`.
