import os
import duckdb
import pandas as pd

BASE_DIR=r"D:\Code\Năm 4\TLCN"
DATA_PATH=rf"{BASE_DIR}\data\raw\real_estate\*.parquet"
OUTPUT_DIR=rf"{BASE_DIR}\docs\profiling"

os.makedirs(OUTPUT_DIR,exist_ok=True)

con=duckdb.connect()

# 1. Tổng quan
overview=con.execute(f"""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT property_type_name) AS property_types,
    COUNT(DISTINCT province_name) AS provinces,
    MIN(TRY_CAST(published_at AS TIMESTAMP)) AS min_date,
    MAX(TRY_CAST(published_at AS TIMESTAMP)) AS max_date
FROM read_parquet('{DATA_PATH}')
""").df()

# 2. NULL
nulls=con.execute(f"""
SELECT
    COUNT(*) AS total_rows,
    COUNT(*)-COUNT(name) AS null_name,
    COUNT(*)-COUNT(description) AS null_description,
    COUNT(*)-COUNT(property_type_name) AS null_property_type,
    COUNT(*)-COUNT(province_name) AS null_province,
    COUNT(*)-COUNT(district_name) AS null_district,
    COUNT(*)-COUNT(ward_name) AS null_ward,
    COUNT(*)-COUNT(street_name) AS null_street,
    COUNT(*)-COUNT(project_name) AS null_project,
    COUNT(*)-COUNT(price) AS null_price,
    COUNT(*)-COUNT(area) AS null_area,
    COUNT(*)-COUNT(floor_count) AS null_floor,
    COUNT(*)-COUNT(frontage_width) AS null_frontage,
    COUNT(*)-COUNT(house_depth) AS null_depth,
    COUNT(*)-COUNT(road_width) AS null_road,
    COUNT(*)-COUNT(bedroom_count) AS null_bedroom,
    COUNT(*)-COUNT(bathroom_count) AS null_bathroom,
    COUNT(*)-COUNT(house_direction) AS null_house_direction,
    COUNT(*)-COUNT(balcony_direction) AS null_balcony_direction,
    COUNT(*)-COUNT(published_at) AS null_published_at
FROM read_parquet('{DATA_PATH}')
""").df().T.reset_index()

nulls.columns=["metric","value"]

# 3. Area
area_stats=con.execute(f"""
SELECT
    MIN(area) AS min_area,
    AVG(area) AS avg_area,
    MEDIAN(area) AS median_area,
    QUANTILE_CONT(area,0.95) AS p95_area,
    QUANTILE_CONT(area,0.99) AS p99_area,
    MAX(area) AS max_area
FROM read_parquet('{DATA_PATH}')
WHERE area IS NOT NULL
""").df()

# 4. Price
price_stats=con.execute(f"""
SELECT
    COUNT(*) FILTER(WHERE price IS NULL) AS null_price,
    COUNT(*) FILTER(WHERE TRY_CAST(price AS DOUBLE)=0) AS zero_price,
    COUNT(*) FILTER(
        WHERE price IS NOT NULL
        AND TRY_CAST(price AS DOUBLE) IS NULL
    ) AS invalid_price,
    MIN(TRY_CAST(price AS DOUBLE)) AS min_price,
    AVG(TRY_CAST(price AS DOUBLE)) AS avg_price,
    MEDIAN(TRY_CAST(price AS DOUBLE)) AS median_price,
    QUANTILE_CONT(TRY_CAST(price AS DOUBLE),0.95) AS p95_price,
    QUANTILE_CONT(TRY_CAST(price AS DOUBLE),0.99) AS p99_price,
    MAX(TRY_CAST(price AS DOUBLE)) AS max_price
FROM read_parquet('{DATA_PATH}')
""").df()

# 5. Price/m2
price_m2=con.execute(f"""
SELECT
    QUANTILE_CONT(TRY_CAST(price AS DOUBLE)/area,0.01) AS p01,
    QUANTILE_CONT(TRY_CAST(price AS DOUBLE)/area,0.25) AS p25,
    QUANTILE_CONT(TRY_CAST(price AS DOUBLE)/area,0.50) AS p50,
    QUANTILE_CONT(TRY_CAST(price AS DOUBLE)/area,0.75) AS p75,
    QUANTILE_CONT(TRY_CAST(price AS DOUBLE)/area,0.95) AS p95,
    QUANTILE_CONT(TRY_CAST(price AS DOUBLE)/area,0.99) AS p99,
    MAX(TRY_CAST(price AS DOUBLE)/area) AS max_price_m2
FROM read_parquet('{DATA_PATH}')
WHERE TRY_CAST(price AS DOUBLE)>0
AND area>0
""").df()

# 6. Property type
property_types=con.execute(f"""
SELECT
    property_type_name,
    COUNT(*) AS records
FROM read_parquet('{DATA_PATH}')
GROUP BY property_type_name
ORDER BY records DESC
""").df()

# 7. Province
provinces=con.execute(f"""
SELECT
    province_name,
    COUNT(*) AS records
FROM read_parquet('{DATA_PATH}')
GROUP BY province_name
ORDER BY records DESC
""").df()

# 8. Theo từng loại BĐS
by_type=con.execute(f"""
SELECT
    property_type_name,
    COUNT(*) AS records,
    MEDIAN(area) AS median_area,
    QUANTILE_CONT(area,0.99) AS p99_area,
    MEDIAN(TRY_CAST(price AS DOUBLE)/area) AS median_price_m2,
    QUANTILE_CONT(
        TRY_CAST(price AS DOUBLE)/area,0.99
    ) AS p99_price_m2
FROM read_parquet('{DATA_PATH}')
WHERE TRY_CAST(price AS DOUBLE)>0
AND area>0
GROUP BY property_type_name
ORDER BY records DESC
""").df()

# 9. Duplicate
duplicates=con.execute(f"""
SELECT
    COUNT(*) AS duplicate_groups,
    COALESCE(SUM(cnt-1),0) AS duplicate_records
FROM (
    SELECT
        name,
        description,
        property_type_name,
        province_name,
        district_name,
        price,
        area,
        published_at,
        COUNT(*) AS cnt
    FROM read_parquet('{DATA_PATH}')
    GROUP BY ALL
    HAVING COUNT(*)>1
)
""").df()

# 10. Outlier lớn nhất
top_area=con.execute(f"""
SELECT
    name,
    property_type_name,
    province_name,
    district_name,
    price,
    area
FROM read_parquet('{DATA_PATH}')
ORDER BY area DESC
LIMIT 20
""").df()

top_price=con.execute(f"""
SELECT
    name,
    property_type_name,
    province_name,
    district_name,
    price,
    area
FROM read_parquet('{DATA_PATH}')
WHERE TRY_CAST(price AS DOUBLE) IS NOT NULL
ORDER BY TRY_CAST(price AS DOUBLE) DESC
LIMIT 20
""").df()

# Save
overview.to_csv(rf"{OUTPUT_DIR}\overview.csv",index=False)
nulls.to_csv(rf"{OUTPUT_DIR}\null_profile.csv",index=False)
area_stats.to_csv(rf"{OUTPUT_DIR}\area_stats.csv",index=False)
price_stats.to_csv(rf"{OUTPUT_DIR}\price_stats.csv",index=False)
price_m2.to_csv(rf"{OUTPUT_DIR}\price_per_m2_stats.csv",index=False)
property_types.to_csv(rf"{OUTPUT_DIR}\property_types.csv",index=False)
provinces.to_csv(rf"{OUTPUT_DIR}\provinces.csv",index=False)
by_type.to_csv(rf"{OUTPUT_DIR}\property_type_stats.csv",index=False)
duplicates.to_csv(rf"{OUTPUT_DIR}\duplicates.csv",index=False)
top_area.to_csv(rf"{OUTPUT_DIR}\top_area_outliers.csv",index=False)
top_price.to_csv(rf"{OUTPUT_DIR}\top_price_outliers.csv",index=False)

print("DATA PROFILING COMPLETED")
print()
print(overview.to_string(index=False))
print()
print("Output:",OUTPUT_DIR)

con.close()