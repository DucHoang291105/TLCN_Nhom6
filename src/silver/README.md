# Silver Listing Core (SRC01)

Silver Core chuẩn hóa dữ liệu listing từ Bronze của `SRC01`. Việc chuẩn hóa mã
hành chính và geometry thuộc pipeline Location Mapping độc lập.

## Phạm vi

```text
data/bronze/real_estate
        -> Silver Listing Core
        -> data/silver/real_estate_core
```

- Input duy nhất là Bronze `vduydong/vietnam-real-estates-2`.
- Không đọc hoặc join GIS, Administrative Database hay Location Master.
- Giữ đủ 3.500.744 dòng để audit; dùng `is_canonical=true` để lấy tập không trùng.
- Chuẩn hóa kiểu dữ liệu, thêm ID/lineage, trường thời gian, `price_per_m2`, cờ hợp lệ
  và DQ01–DQ14.
- Ghi 58 cột dưới dạng Parquet Snappy, partition theo `partition_year_month`.

Schema đầy đủ nằm tại `docs/silver/silver_core_schema.csv`; điều kiện DQ nằm tại
`docs/data_quality_rules.xlsx`.

## Cách chạy

Từ thư mục gốc repository:

```powershell
# Kiểm tra logic trên 10.000 dòng, không tạo output chính
.\scripts\run_silver.ps1 -SmokeTest

# Chạy toàn bộ 3.500.744 dòng
.\scripts\run_silver.ps1
```

Nếu ổ `T:` đang được sử dụng:

```powershell
.\scripts\run_silver.ps1 -DriveLetter U
```

Không chạy đồng thời hai tiến trình Silver. Pipeline dùng staging và chỉ thay output
chính khi các kiểm tra read-back đã PASS.

## Output và kiểm định

```text
data/silver/real_estate_core/
├── partition_year_month=YYYY-MM/
├── partition_year_month=unknown/
└── _SUCCESS

docs/quality/silver_core_summary.json
docs/quality/silver_core_validation.json
```

Chạy kiểm định độc lập bằng DuckDB/PyArrow:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\src\silver\02_validate_silver_core.py
```

Kết quả hợp lệ phải có `"status": "PASS"`, 3.500.744 dòng, 58 cột,
3.500.694 dòng canonical, 36 nhóm duplicate và 50 dòng noncanonical.

## Dùng dữ liệu

- Đếm listing không trùng: lọc `is_canonical=true`.
- KPI giá: thêm `is_price_valid=true`.
- KPI giá/m²: thêm `is_price_valid=true AND is_area_valid=true`; tùy mục đích có
  thể loại DQ06.
- KPI tỉnh/huyện/xã: dùng các cờ location tương ứng. Tên ở Silver Core chỉ được
  trim/đổi blank thành null, chưa phải tên/mã hành chính từ Location Master.
- Dữ liệu location đã mapping sẽ được left join ở `Silver Listing Enriched` sau này.
