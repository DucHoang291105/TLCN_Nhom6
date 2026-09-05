import math
from pathlib import Path

import duckdb
import pandas as pd


PROJECT_ROOT = Path(__file__).absolute().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "real_estate"
RAW_GLOB = (RAW_DIR / "shard_*.parquet").as_posix()
OUTPUT_DIR = PROJECT_ROOT / "docs" / "profiling"
COUNT_STATS_PATH = OUTPUT_DIR / "count_field_stats.csv"
THRESHOLDS_PATH = OUTPUT_DIR / "silver_dq_thresholds.csv"
EXPECTED_HITS_PATH = OUTPUT_DIR / "silver_dq_expected_hits.csv"

SOURCE_ID = "SRC01"
THRESHOLD_VERSION = "src01_raw_20260904_p99_v1"
EXPECTED_FILES = [f"shard_{index:04d}.parquet" for index in range(10)]
EXPECTED_TOTAL_ROWS = 3_500_744
COUNT_FIELDS = ("floor_count", "bedroom_count", "bathroom_count")


def validate_raw_files():
    actual_files = sorted(path.name for path in RAW_DIR.glob("shard_*.parquet"))
    if actual_files != EXPECTED_FILES:
        raise RuntimeError(
            f"Raw shards do not match SRC01 snapshot: {actual_files}"
        )


def profile_count_fields(connection):
    query = f"""
        WITH field_values AS (
            SELECT property_type_name, 'floor_count' AS field, floor_count AS value
            FROM read_parquet('{RAW_GLOB}')
            UNION ALL
            SELECT property_type_name, 'bedroom_count', bedroom_count
            FROM read_parquet('{RAW_GLOB}')
            UNION ALL
            SELECT property_type_name, 'bathroom_count', bathroom_count
            FROM read_parquet('{RAW_GLOB}')
        ),
        global_stats AS (
            SELECT
                'ALL' AS scope,
                CAST(NULL AS VARCHAR) AS property_type,
                field,
                count(*) AS input_rows,
                count(value) AS non_null_rows,
                count(*) - count(value) AS null_rows,
                count(*) FILTER (WHERE value = 0) AS zero_rows,
                count(*) FILTER (WHERE value < 0) AS negative_rows,
                count(*) FILTER (WHERE value != trunc(value)) AS non_integer_rows,
                min(value) AS min_value,
                quantile_cont(value, 0.50) AS p50,
                quantile_cont(value, 0.95) AS p95,
                quantile_cont(value, 0.99) AS p99_cont,
                quantile_disc(value, 0.99) AS p99_disc,
                max(value) AS max_value
            FROM field_values
            GROUP BY field
        ),
        property_type_stats AS (
            SELECT
                'PROPERTY_TYPE' AS scope,
                property_type_name AS property_type,
                field,
                count(*) AS input_rows,
                count(value) AS non_null_rows,
                count(*) - count(value) AS null_rows,
                count(*) FILTER (WHERE value = 0) AS zero_rows,
                count(*) FILTER (WHERE value < 0) AS negative_rows,
                count(*) FILTER (WHERE value != trunc(value)) AS non_integer_rows,
                min(value) AS min_value,
                quantile_cont(value, 0.50) AS p50,
                quantile_cont(value, 0.95) AS p95,
                quantile_cont(value, 0.99) AS p99_cont,
                quantile_disc(value, 0.99) AS p99_disc,
                max(value) AS max_value
            FROM field_values
            GROUP BY property_type_name, field
        )
        SELECT * FROM global_stats
        UNION ALL
        SELECT * FROM property_type_stats
        ORDER BY field, scope, property_type
    """
    result = connection.execute(query).df()
    result.insert(0, "source_id", SOURCE_ID)
    result.insert(1, "threshold_version", THRESHOLD_VERSION)
    result["recommended_review_threshold"] = result["p99_disc"]
    return result


def build_silver_thresholds(connection):
    query = f"""
        WITH property_type_thresholds AS (
            SELECT
                'PROPERTY_TYPE' AS threshold_scope,
                property_type_name AS property_type,
                count(*) AS total_rows,
                count(*) FILTER (WHERE area > 0) AS valid_area_rows,
                quantile_cont(area, 0.99) FILTER (WHERE area > 0) AS p99_area_m2,
                count(*) FILTER (
                    WHERE try_cast(price AS DECIMAL(20, 0)) > 0 AND area > 0
                ) AS valid_price_per_m2_rows,
                quantile_cont(
                    try_cast(price AS DOUBLE) / area,
                    0.99
                ) FILTER (
                    WHERE try_cast(price AS DECIMAL(20, 0)) > 0 AND area > 0
                ) AS p99_price_per_m2,
                count(floor_count) AS floor_count_non_null_rows,
                quantile_disc(floor_count, 0.99) AS p99_floor_count,
                count(bedroom_count) AS bedroom_count_non_null_rows,
                quantile_disc(bedroom_count, 0.99) AS p99_bedroom_count,
                count(bathroom_count) AS bathroom_count_non_null_rows,
                quantile_disc(bathroom_count, 0.99) AS p99_bathroom_count
            FROM read_parquet('{RAW_GLOB}')
            GROUP BY property_type_name
        ),
        global_fallback AS (
            SELECT
                'GLOBAL_FALLBACK' AS threshold_scope,
                '__GLOBAL_FALLBACK__' AS property_type,
                count(*) AS total_rows,
                count(*) FILTER (WHERE area > 0) AS valid_area_rows,
                quantile_cont(area, 0.99) FILTER (WHERE area > 0) AS p99_area_m2,
                count(*) FILTER (
                    WHERE try_cast(price AS DECIMAL(20, 0)) > 0 AND area > 0
                ) AS valid_price_per_m2_rows,
                quantile_cont(
                    try_cast(price AS DOUBLE) / area,
                    0.99
                ) FILTER (
                    WHERE try_cast(price AS DECIMAL(20, 0)) > 0 AND area > 0
                ) AS p99_price_per_m2,
                count(floor_count) AS floor_count_non_null_rows,
                quantile_disc(floor_count, 0.99) AS p99_floor_count,
                count(bedroom_count) AS bedroom_count_non_null_rows,
                quantile_disc(bedroom_count, 0.99) AS p99_bedroom_count,
                count(bathroom_count) AS bathroom_count_non_null_rows,
                quantile_disc(bathroom_count, 0.99) AS p99_bathroom_count
            FROM read_parquet('{RAW_GLOB}')
        )
        SELECT
            *
        FROM global_fallback
        UNION ALL
        SELECT
            *
        FROM property_type_thresholds
        ORDER BY threshold_scope, property_type
    """
    result = connection.execute(query).df()
    result.insert(0, "source_id", SOURCE_ID)
    result.insert(1, "threshold_version", THRESHOLD_VERSION)
    return result


def build_expected_hits(connection):
    query = f"""
        WITH thresholds AS (
            SELECT
                property_type_name AS property_type,
                quantile_cont(area, 0.99) FILTER (WHERE area > 0) AS p99_area_m2,
                quantile_cont(
                    try_cast(price AS DOUBLE) / area,
                    0.99
                ) FILTER (
                    WHERE try_cast(price AS DECIMAL(20, 0)) > 0 AND area > 0
                ) AS p99_price_per_m2,
                quantile_disc(floor_count, 0.99) AS p99_floor_count,
                quantile_disc(bedroom_count, 0.99) AS p99_bedroom_count,
                quantile_disc(bathroom_count, 0.99) AS p99_bathroom_count
            FROM read_parquet('{RAW_GLOB}')
            GROUP BY property_type_name
        ),
        joined AS (
            SELECT source.*, thresholds.*
            FROM read_parquet('{RAW_GLOB}') AS source
            JOIN thresholds
              ON source.property_type_name IS NOT DISTINCT FROM thresholds.property_type
        )
        SELECT 'DQ05' AS rule_id, 'area' AS field,
               count(*) FILTER (WHERE area > 0 AND area > p99_area_m2) AS expected_hit_rows
        FROM joined
        UNION ALL
        SELECT 'DQ06', 'price_per_m2',
               count(*) FILTER (
                   WHERE try_cast(price AS DECIMAL(20, 0)) > 0
                     AND area > 0
                     AND try_cast(price AS DOUBLE) / area > p99_price_per_m2
               )
        FROM joined
        UNION ALL
        SELECT 'DQ12', 'floor_count',
               count(*) FILTER (
                   WHERE floor_count IS NOT NULL AND floor_count > p99_floor_count
               )
        FROM joined
        UNION ALL
        SELECT 'DQ13', 'bedroom_count',
               count(*) FILTER (
                   WHERE bedroom_count IS NOT NULL AND bedroom_count > p99_bedroom_count
               )
        FROM joined
        UNION ALL
        SELECT 'DQ14', 'bathroom_count',
               count(*) FILTER (
                   WHERE bathroom_count IS NOT NULL AND bathroom_count > p99_bathroom_count
               )
        FROM joined
        ORDER BY rule_id
    """
    result = connection.execute(query).df()
    result.insert(0, "source_id", SOURCE_ID)
    result.insert(1, "threshold_version", THRESHOLD_VERSION)
    result["threshold_scope"] = "P99_BY_PROPERTY_TYPE"
    result["comparison"] = "STRICT_GREATER_THAN"
    return result


def validate_results(count_stats, thresholds, expected_hits):
    global_rows = count_stats[count_stats["scope"] == "ALL"]
    actual_fields = set(global_rows["field"])
    if actual_fields != set(COUNT_FIELDS):
        raise RuntimeError(f"Missing count-field statistics: {actual_fields}")
    if set(global_rows["input_rows"]) != {EXPECTED_TOTAL_ROWS}:
        raise RuntimeError("Global count-field input rows do not match SRC01")
    if global_rows["negative_rows"].sum() != 0:
        raise RuntimeError("Negative count values found; review DQ policy")
    if global_rows["non_integer_rows"].sum() != 0:
        raise RuntimeError("Non-integer count values found; review cast policy")

    expected_global_p99 = {
        "floor_count": 8.0,
        "bedroom_count": 14.0,
        "bathroom_count": 13.0,
    }
    actual_global_p99 = dict(zip(global_rows["field"], global_rows["p99_disc"]))
    if actual_global_p99 != expected_global_p99:
        raise RuntimeError(
            "Global P99 changed from reviewed SRC01 snapshot: "
            f"{actual_global_p99}"
        )
    property_thresholds = thresholds[
        thresholds["threshold_scope"] == "PROPERTY_TYPE"
    ]
    global_threshold = thresholds[
        thresholds["threshold_scope"] == "GLOBAL_FALLBACK"
    ]
    if (
        len(property_thresholds) != 5
        or property_thresholds["total_rows"].sum() != EXPECTED_TOTAL_ROWS
        or len(global_threshold) != 1
        or int(global_threshold.iloc[0]["total_rows"]) != EXPECTED_TOTAL_ROWS
    ):
        raise RuntimeError("Property-type thresholds do not cover all SRC01 rows")
    global_count_p99 = global_threshold.iloc[0][
        ["p99_floor_count", "p99_bedroom_count", "p99_bathroom_count"]
    ].tolist()
    if global_count_p99 != [8.0, 14.0, 13.0]:
        raise RuntimeError(f"Unexpected global count thresholds: {global_count_p99}")

    expected_continuous_p99 = {
        "__GLOBAL_FALLBACK__": (2_222.0, 607_692_307.6923077),
        "Biệt thự/Nhà liền kề": (894.2520000000252, 625_000_000.0),
        "Căn hộ chung cư": (240.0, 261_864_406.77966103),
        "Nhà": (544.0, 735_000_000.0),
        "Shophouse": (535.0, 552_310_374.89102),
        "Đất": (10_716.349999999977, 347_947_879.1170806),
    }
    if set(thresholds["property_type"]) != set(expected_continuous_p99):
        raise RuntimeError("Unexpected property types in Silver thresholds")
    for row in thresholds.itertuples(index=False):
        expected_area, expected_price_m2 = expected_continuous_p99[
            row.property_type
        ]
        if not math.isclose(
            row.p99_area_m2,
            expected_area,
            rel_tol=1e-12,
            abs_tol=1e-6,
        ):
            raise RuntimeError(
                f"Unexpected P99 area for {row.property_type}: {row.p99_area_m2}"
            )
        if not math.isclose(
            row.p99_price_per_m2,
            expected_price_m2,
            rel_tol=1e-12,
            abs_tol=1e-6,
        ):
            raise RuntimeError(
                "Unexpected P99 price/m2 for "
                f"{row.property_type}: {row.p99_price_per_m2}"
            )

    expected_hit_counts = {
        "DQ05": 34_807,
        "DQ06": 32_219,
        "DQ12": 6_511,
        "DQ13": 14_530,
        "DQ14": 11_636,
    }
    actual_hit_counts = dict(
        zip(expected_hits["rule_id"], expected_hits["expected_hit_rows"])
    )
    if actual_hit_counts != expected_hit_counts:
        raise RuntimeError(f"Unexpected DQ hit counts: {actual_hit_counts}")


def main():
    validate_raw_files()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    connection.execute("SET enable_progress_bar = false")
    try:
        count_stats = profile_count_fields(connection)
        thresholds = build_silver_thresholds(connection)
        expected_hits = build_expected_hits(connection)
    finally:
        connection.close()

    validate_results(count_stats, thresholds, expected_hits)
    count_stats.to_csv(COUNT_STATS_PATH, index=False)
    thresholds.to_csv(THRESHOLDS_PATH, index=False)
    expected_hits.to_csv(EXPECTED_HITS_PATH, index=False)

    print("COUNT FIELD PROFILING: PASS")
    print(f"Count stats: {COUNT_STATS_PATH}")
    print(f"Silver thresholds: {THRESHOLDS_PATH}")
    print(f"Expected DQ hits: {EXPECTED_HITS_PATH}")
    print(
        count_stats[count_stats["scope"] == "ALL"]
        [["field", "p50", "p95", "p99_disc", "max_value"]]
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
