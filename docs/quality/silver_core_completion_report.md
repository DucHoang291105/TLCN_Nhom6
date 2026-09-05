# Báo cáo hoàn thành Silver Listing Core

Ngày hoàn thành: 2026-09-05 (Asia/Saigon)

## Kết luận

Silver Listing Core cho SRC01 đã hoàn thành và PASS cả quality gate trong PySpark
lẫn kiểm định độc lập bằng DuckDB/PyArrow. Phiên bản này chỉ xử lý listing SRC01;
Location Mapping/Location Master thuộc một pipeline độc lập.

- Silver batch: `silver_core_20260905T013354855842Z_d2629a94`
- Bronze batch đầu vào: `real_estate_20260904T134803402485Z_dd5b0a8c`
- Input/output: 3.500.744 / 3.500.744 dòng
- Output schema: 58 cột, phiên bản `silver_listing_core_v1`
- Output vật lý: 10 file Parquet Snappy, 2.179.352.949 byte (khoảng 2,03 GiB)
- Thời gian full run: 312,05 giây
- Output: `data/silver/real_estate_core/`

## Đã thực hiện

1. Khóa input chỉ gồm Bronze SRC01 và kiểm tra trạng thái Bronze, schema 24 cột,
   source, batch, metadata, property type và threshold version trước khi xử lý.
2. Tạo schema canonical 58 cột và chuẩn hóa text, giá, diện tích, số tầng/phòng,
   chiều rộng/chiều sâu và thời gian đăng.
3. Tạo `listing_id` SHA-256 có version, `source_id` và `source_name`; ID vì vậy đã
   sẵn sàng phân biệt nguồn khi bổ sung listing source mới.
4. Thực hiện exact duplicate audit: giữ đủ mọi dòng, tạo group/rank/count và cờ
   `is_canonical`; không xóa âm thầm dữ liệu.
5. Tạo `price_per_m2`, các trường lịch, lineage Bronze/Silver và partition tháng.
6. Áp dụng DQ01–DQ14, lưu `dq_error_codes`, `dq_status` và sáu cờ field-validity.
7. Ghi qua staging, read-back kiểm định rồi mới promote; output tốt trước đó được
   bảo vệ nếu một lần chạy mới thất bại.
8. Tạo launcher Windows, smoke test 10.000 dòng, pipeline full và validator độc lập.
9. Cập nhật README, Source Catalog, Data Dictionary, schema contract và DQ workbook
   sang trạng thái `IMPLEMENTED_AND_VALIDATED`.

## Thu được gì

### Tập dữ liệu dùng cho phân tích

- Tổng audit rows: 3.500.744.
- Canonical rows: 3.500.694.
- Noncanonical duplicate rows: 50.
- Exact duplicate groups: 36; group lớn nhất có 6 dòng.
- 10 partition tháng từ `2025-06` đến `2026-03`.

### Trạng thái chất lượng

| DQ status | Số dòng |
|---|---:|
| VALID | 2.698.781 |
| REVIEW | 744.590 |
| REJECTED | 57.373 |

Các status có overlap rule theo từng dòng; không cộng số hit của các DQ rule để suy
ra số dòng lỗi. Gold phải lọc theo `is_canonical` và field-validity phù hợp với KPI,
không dùng một filter `dq_status='VALID'` cho mọi trường hợp.

| Rule | Hit rows |
|---|---:|
| DQ01 | 218.494 |
| DQ02 | 57.320 |
| DQ03 | 0 |
| DQ04 | 4 |
| DQ05 | 34.807 |
| DQ06 | 32.219 |
| DQ07 | 0 |
| DQ08 | 101.933 |
| DQ09 | 496.716 |
| DQ10 | 0 |
| DQ11 | 50 |
| DQ12 | 6.511 |
| DQ13 | 14.530 |
| DQ14 | 11.636 |

## Kiểm định đã PASS

- Schema và đúng thứ tự 58 cột.
- Bảo toàn tổng số dòng Bronze → Silver.
- Đúng SRC01 và chỉ một Bronze/Silver batch.
- ID 64 hex, duplicate contract, canonical count và DQ11.
- Ngữ nghĩa từng DQ01–DQ14 và mức ưu tiên REJECTED/REVIEW/VALID.
- Các cờ field-validity và điều kiện sinh `price_per_m2`.
- Tổng partition khớp tổng dòng; codec vật lý là Snappy; có `_SUCCESS`.
- Không còn staging/backup và không có cột Location Master trong Silver Core.

## Sự cố đã xử lý

Lần full đầu tiên bị quality gate chặn vì DQ06 có 32.227 hit thay vì baseline 32.219.
Nguyên nhân là so P99 sau khi `price_per_m2` đã làm tròn 2 chữ số. Pipeline đã được
sửa để DQ06 so tỷ lệ chưa làm tròn giống profiling, trong khi cột output vẫn giữ
`DECIMAL(24,2)`. Staging lỗi được dọn sạch và không tạo output chính. Lần chạy sau
và validator độc lập đều PASS đúng 32.219 hit.

## Cách chạy lại và xem kết quả

```powershell
.\scripts\run_silver.ps1 -SmokeTest
.\scripts\run_silver.ps1
.\.venv\Scripts\python.exe -X utf8 .\src\silver\02_validate_silver_core.py
```

Báo cáo máy đọc:

```text
docs/quality/silver_core_summary.json
docs/quality/silver_core_validation.json
```

## Phần chưa làm

- DQ15 cho 1.868 dòng trống cả title/description chưa được duyệt nên không nằm
  trong DQ v1.
- Chưa tích hợp Location Master/mã hành chính. Việc này thuộc bước
  `Silver Listing Enriched` và phải dùng left join sau khi mapping được kiểm định.
- Chưa xây Gold/DWH/dashboard.
