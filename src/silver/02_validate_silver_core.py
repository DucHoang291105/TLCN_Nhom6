import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).absolute().parents[2]
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "real_estate_core"
SUMMARY_PATH = PROJECT_ROOT / "docs" / "quality" / "silver_core_summary.json"
REPORT_PATH = PROJECT_ROOT / "docs" / "quality" / "silver_core_validation.json"
SCHEMA_PATH = PROJECT_ROOT / "docs" / "silver" / "silver_core_schema.csv"
THRESHOLD_PATH = PROJECT_ROOT / "docs" / "profiling" / "silver_dq_thresholds.csv"

EXPECTED_ROWS = 3_500_744
EXPECTED_COLUMNS = 58
EXPECTED_CANONICAL_ROWS = 3_500_694
EXPECTED_DUPLICATE_GROUPS = 36
EXPECTED_DUPLICATE_EXCESS_ROWS = 50
EXPECTED_MAX_DUPLICATE_COUNT = 6
EXPECTED_SOURCE_ID = "SRC01"
EXPECTED_SOURCE_NAME = "vduydong/vietnam-real-estates-2"
EXPECTED_SCHEMA_VERSION = "silver_listing_core_v1"
EXPECTED_DQ_VERSION = "src01_silver_core_dq_v1"
EXPECTED_THRESHOLD_VERSION = "src01_raw_20260904_p99_v1"
EXPECTED_DQ_COUNTS = {
    "DQ01": 218_494,
    "DQ02": 57_320,
    "DQ03": 0,
    "DQ04": 4,
    "DQ05": 34_807,
    "DQ06": 32_219,
    "DQ07": 0,
    "DQ08": 101_933,
    "DQ09": 496_716,
    "DQ10": 0,
    "DQ11": 50,
    "DQ12": 6_511,
    "DQ13": 14_530,
    "DQ14": 11_636,
}
REJECTED_RULES = ("DQ02", "DQ03", "DQ04", "DQ07", "DQ10", "DQ11")
REVIEW_RULES = ("DQ01", "DQ05", "DQ06", "DQ08", "DQ09", "DQ12", "DQ13", "DQ14")
FORBIDDEN_LOCATION_COLUMNS = {
    "location_id",
    "province_code",
    "district_code",
    "ward_code",
    "master_province_name",
    "master_district_name",
    "master_ward_name",
    "location_match_level",
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sql_path(path):
    return path.absolute().as_posix().replace("'", "''")


def or_contains(rule_ids):
    return " OR ".join(
        f"list_contains(dq_error_codes, '{rule_id}')" for rule_id in rule_ids
    )


def validate_codecs(part_files):
    codecs = set()
    for part_file in part_files:
        metadata = pq.ParquetFile(part_file).metadata
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                codecs.add(row_group.column(column_index).compression)
    require(codecs == {"SNAPPY"}, f"Unexpected Parquet codecs: {sorted(codecs)}")
    return sorted(codecs)


def main():
    part_files = sorted(SILVER_DIR.rglob("part-*.parquet"))
    require(SUMMARY_PATH.is_file(), f"Missing Silver summary: {SUMMARY_PATH}")
    require(SCHEMA_PATH.is_file(), f"Missing schema contract: {SCHEMA_PATH}")
    require(THRESHOLD_PATH.is_file(), f"Missing DQ thresholds: {THRESHOLD_PATH}")
    require((SILVER_DIR / "_SUCCESS").is_file(), "Missing Silver _SUCCESS")
    require(part_files, "No Silver part Parquet files")

    summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    require(summary.get("status") == "PASS", "Latest Silver summary is not PASS")
    require(summary.get("location_enrichment") == "NOT_INCLUDED", "Location scope changed")

    with SCHEMA_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        expected_names = [row["column_name"] for row in csv.DictReader(handle)]
    require(len(expected_names) == EXPECTED_COLUMNS, "Schema contract is not 58 columns")

    silver_glob = sql_path(SILVER_DIR / "**" / "part-*.parquet")
    threshold_csv = sql_path(THRESHOLD_PATH)
    connection = duckdb.connect()
    try:
        connection.execute(
            f"""
            CREATE TEMP VIEW silver AS
            SELECT * FROM read_parquet(
                '{silver_glob}', hive_partitioning=true, hive_types_autocast=false
            )
            """
        )
        actual_names = [
            row[0] for row in connection.execute("DESCRIBE SELECT * FROM silver").fetchall()
        ]
        require(actual_names == expected_names, "Silver column names/order mismatch")
        require(
            not (set(actual_names) & FORBIDDEN_LOCATION_COLUMNS),
            "Silver Core unexpectedly contains Location Master columns",
        )

        connection.execute(
            f"""
            CREATE TEMP VIEW thresholds AS
            SELECT * FROM read_csv_auto('{threshold_csv}', header=true)
            WHERE threshold_scope = 'PROPERTY_TYPE'
            """
        )
        fallback = connection.execute(
            f"""
            SELECT p99_area_m2, p99_price_per_m2, p99_floor_count,
                   p99_bedroom_count, p99_bathroom_count
            FROM read_csv_auto('{threshold_csv}', header=true)
            WHERE threshold_scope = 'GLOBAL_FALLBACK'
            """
        ).fetchone()
        require(fallback is not None, "Missing global threshold fallback")

        connection.execute(
            f"""
            CREATE TEMP VIEW checked AS
            SELECT s.*,
                coalesce(t.p99_area_m2, {fallback[0]}) AS area_limit,
                coalesce(t.p99_price_per_m2, {fallback[1]}) AS price_m2_limit,
                coalesce(t.p99_floor_count, {fallback[2]}) AS floor_limit,
                coalesce(t.p99_bedroom_count, {fallback[3]}) AS bedroom_limit,
                coalesce(t.p99_bathroom_count, {fallback[4]}) AS bathroom_limit
            FROM silver s
            LEFT JOIN thresholds t ON s.property_type = t.property_type
            """
        )

        rule_conditions = {
            "DQ01": "price_raw IS NULL OR trim(price_raw) = ''",
            "DQ02": "price_vnd <= 0",
            "DQ03": "price_raw IS NOT NULL AND trim(price_raw) <> '' AND price_vnd IS NULL",
            "DQ04": "area_m2 IS NULL OR area_m2 <= 0",
            "DQ05": "area_m2 > area_limit",
            "DQ06": "CAST(price_vnd AS DOUBLE) / area_m2 > price_m2_limit",
            "DQ07": "NOT is_province_valid",
            "DQ08": "NOT is_district_valid",
            "DQ09": "NOT is_ward_valid",
            "DQ10": "NOT is_published_at_valid",
            "DQ11": "NOT is_canonical",
            "DQ12": "floor_count > floor_limit",
            "DQ13": "bedroom_count > bedroom_limit",
            "DQ14": "bathroom_count > bathroom_limit",
        }
        rule_mismatch_sql = ",\n".join(
            "count(*) FILTER (WHERE "
            f"coalesce(list_contains(dq_error_codes, '{rule_id}'), false) "
            f"IS DISTINCT FROM coalesce(({condition}), false)) AS {rule_id}_mismatch"
            for rule_id, condition in rule_conditions.items()
        )
        rule_count_sql = ",\n".join(
            f"count(*) FILTER (WHERE list_contains(dq_error_codes, '{rule_id}')) AS {rule_id}"
            for rule_id in EXPECTED_DQ_COUNTS
        )
        rejected_expression = or_contains(REJECTED_RULES)
        review_expression = or_contains(REVIEW_RULES)
        metrics_cursor = connection.execute(
            f"""
            SELECT
                count(*) AS rows,
                count(*) FILTER (WHERE is_canonical) AS canonical_rows,
                count(*) FILTER (WHERE NOT is_canonical) AS noncanonical_rows,
                count(*) FILTER (WHERE duplicate_count > 1 AND duplicate_rank = 1) AS duplicate_groups,
                max(duplicate_count) AS max_duplicate_count,
                count(DISTINCT source_id) AS source_ids,
                min(source_id) AS source_id_min,
                max(source_id) AS source_id_max,
                count(DISTINCT source_name) AS source_names,
                min(source_name) AS source_name_min,
                max(source_name) AS source_name_max,
                count(DISTINCT bronze_batch_id) AS bronze_batches,
                count(DISTINCT silver_batch_id) AS silver_batches,
                count(*) FILTER (WHERE listing_id IS NULL OR NOT regexp_matches(listing_id, '^[0-9a-f]{{64}}$')) AS invalid_ids,
                count(*) FILTER (WHERE duplicate_group_id <> listing_id) AS invalid_duplicate_group_ids,
                count(*) FILTER (WHERE duplicate_rank < 1 OR duplicate_rank > duplicate_count) AS invalid_duplicate_ranks,
                count(*) FILTER (WHERE len(list_distinct(dq_error_codes)) <> len(dq_error_codes)) AS duplicate_dq_codes,
                count(*) FILTER (WHERE is_price_valid IS DISTINCT FROM coalesce(price_vnd > 0, false)) AS invalid_price_flags,
                count(*) FILTER (WHERE is_area_valid IS DISTINCT FROM coalesce(area_m2 > 0, false)) AS invalid_area_flags,
                count(*) FILTER (WHERE is_published_at_valid IS DISTINCT FROM (published_at IS NOT NULL)) AS invalid_time_flags,
                count(*) FILTER (WHERE price_per_m2 IS NOT NULL AND (NOT is_price_valid OR NOT is_area_valid)) AS invalid_price_per_m2,
                count(*) FILTER (WHERE silver_schema_version <> '{EXPECTED_SCHEMA_VERSION}') AS invalid_schema_versions,
                count(*) FILTER (WHERE dq_rule_version <> '{EXPECTED_DQ_VERSION}') AS invalid_dq_versions,
                count(*) FILTER (WHERE dq_threshold_version <> '{EXPECTED_THRESHOLD_VERSION}') AS invalid_threshold_versions,
                count(*) FILTER (
                    WHERE dq_status IS DISTINCT FROM CASE
                        WHEN {rejected_expression} THEN 'REJECTED'
                        WHEN {review_expression} THEN 'REVIEW'
                        ELSE 'VALID'
                    END
                ) AS invalid_dq_status,
                {rule_count_sql},
                {rule_mismatch_sql}
            FROM checked
            """
        )
        metric_names = [item[0] for item in connection.description]
        metrics_row = metrics_cursor.fetchone()
        metrics = dict(zip(metric_names, metrics_row))
        partition_counts = dict(
            connection.execute(
                "SELECT partition_year_month, count(*) FROM silver GROUP BY 1 ORDER BY 1"
            ).fetchall()
        )
    finally:
        connection.close()

    require(metrics["rows"] == EXPECTED_ROWS, "Silver total row count mismatch")
    require(metrics["canonical_rows"] == EXPECTED_CANONICAL_ROWS, "Canonical count mismatch")
    require(metrics["noncanonical_rows"] == EXPECTED_DUPLICATE_EXCESS_ROWS, "Duplicate excess mismatch")
    require(metrics["duplicate_groups"] == EXPECTED_DUPLICATE_GROUPS, "Duplicate groups mismatch")
    require(metrics["max_duplicate_count"] == EXPECTED_MAX_DUPLICATE_COUNT, "Max duplicate count mismatch")
    require(
        metrics["source_ids"] == 1
        and metrics["source_id_min"] == EXPECTED_SOURCE_ID
        and metrics["source_id_max"] == EXPECTED_SOURCE_ID,
        "source_id is inconsistent",
    )
    require(
        metrics["source_names"] == 1
        and metrics["source_name_min"] == EXPECTED_SOURCE_NAME
        and metrics["source_name_max"] == EXPECTED_SOURCE_NAME,
        "source_name is inconsistent",
    )
    require(metrics["bronze_batches"] == 1 and metrics["silver_batches"] == 1, "Batch IDs are inconsistent")
    zero_metrics = [
        "invalid_ids",
        "invalid_duplicate_group_ids",
        "invalid_duplicate_ranks",
        "duplicate_dq_codes",
        "invalid_price_flags",
        "invalid_area_flags",
        "invalid_time_flags",
        "invalid_price_per_m2",
        "invalid_schema_versions",
        "invalid_dq_versions",
        "invalid_threshold_versions",
        "invalid_dq_status",
        *[f"{rule_id}_mismatch" for rule_id in EXPECTED_DQ_COUNTS],
    ]
    failed_metrics = {name: metrics[name] for name in zero_metrics if metrics[name]}
    require(not failed_metrics, f"Silver semantic validation failed: {failed_metrics}")
    actual_dq_counts = {rule_id: metrics[rule_id] for rule_id in EXPECTED_DQ_COUNTS}
    require(actual_dq_counts == EXPECTED_DQ_COUNTS, f"DQ counts changed: {actual_dq_counts}")
    require(sum(partition_counts.values()) == EXPECTED_ROWS, "Partition counts do not reconcile")
    require(summary.get("output_rows") == EXPECTED_ROWS, "Summary output row count mismatch")
    require(summary.get("dq_counts") == EXPECTED_DQ_COUNTS, "Summary DQ counts mismatch")
    require(not list(SILVER_DIR.parent.glob("_staging_*")), "A Silver staging directory remains")
    require(not list(SILVER_DIR.parent.glob("_backup_*")), "A Silver backup directory remains")

    codecs = validate_codecs(part_files)
    report = {
        "status": "PASS",
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "silver_batch_id": summary["silver_batch_id"],
        "bronze_batch_id": summary["bronze_batch_id"],
        "rows": metrics["rows"],
        "columns": len(expected_names),
        "canonical_rows": metrics["canonical_rows"],
        "noncanonical_rows": metrics["noncanonical_rows"],
        "duplicate_groups": metrics["duplicate_groups"],
        "dq_counts": actual_dq_counts,
        "partition_counts": partition_counts,
        "part_files": len(part_files),
        "output_size_bytes": sum(path.stat().st_size for path in part_files),
        "parquet_codecs": codecs,
        "location_enrichment": "NOT_INCLUDED",
        "checks": {
            "schema_contract": "PASS",
            "row_preservation": "PASS",
            "source_scope": "PASS",
            "duplicate_contract": "PASS",
            "field_validity": "PASS",
            "dq_rule_semantics": "PASS",
            "dq_counts": "PASS",
            "dq_status_precedence": "PASS",
            "partitions": "PASS",
            "compression": "PASS",
            "location_enrichment_not_included": "PASS",
        },
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
