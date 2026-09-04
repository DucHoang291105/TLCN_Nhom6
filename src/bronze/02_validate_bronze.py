import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).absolute().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "real_estate"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "real_estate"
SUMMARY_PATH = PROJECT_ROOT / "docs" / "quality" / "bronze_ingestion_summary.json"

SOURCE_NAME = "vduydong/vietnam-real-estates-2"
EXPECTED_SOURCE_ROWS = {
    "shard_0000.parquet": 350_075,
    "shard_0001.parquet": 350_075,
    "shard_0002.parquet": 350_075,
    "shard_0003.parquet": 350_075,
    "shard_0004.parquet": 350_074,
    "shard_0005.parquet": 350_074,
    "shard_0006.parquet": 350_074,
    "shard_0007.parquet": 350_074,
    "shard_0008.parquet": 350_074,
    "shard_0009.parquet": 350_074,
}
BUSINESS_COLUMNS = [
    "name",
    "description",
    "property_type_name",
    "province_name",
    "district_name",
    "ward_name",
    "street_name",
    "project_name",
    "price",
    "area",
    "floor_count",
    "frontage_width",
    "house_depth",
    "road_width",
    "bedroom_count",
    "bathroom_count",
    "house_direction",
    "balcony_direction",
    "published_at",
]
METADATA_COLUMNS = [
    "source_name",
    "source_file",
    "ingested_at",
    "ingestion_date",
    "batch_id",
]


def sql_path(path):
    return path.absolute().as_posix().replace("'", "''")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def source_metrics(connection, relation, source_expression):
    columns = ", ".join(f'"{column}"' for column in BUSINESS_COLUMNS)
    rows = connection.execute(
        f"""
        SELECT
            {source_expression} AS source_file,
            count(*) AS rows,
            sum(CAST(hash({columns}) AS HUGEINT)) AS business_fingerprint
        FROM {relation}
        GROUP BY source_file
        ORDER BY source_file
        """
    ).fetchall()
    return {
        source_file: {
            "rows": row_count,
            "business_fingerprint": str(fingerprint),
        }
        for source_file, row_count, fingerprint in rows
    }


def main():
    part_files = sorted(BRONZE_DIR.glob("part-*.parquet"))
    require(SUMMARY_PATH.is_file(), f"Missing summary: {SUMMARY_PATH}")
    require((BRONZE_DIR / "_SUCCESS").is_file(), "Missing Bronze _SUCCESS")
    require(part_files, "No Bronze part Parquet files")

    summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    raw_glob = sql_path(RAW_DIR / "shard_*.parquet")
    bronze_glob = sql_path(BRONZE_DIR / "part-*.parquet")
    connection = duckdb.connect()

    try:
        raw_metrics = source_metrics(
            connection,
            f"read_parquet('{raw_glob}', filename=true)",
            "parse_filename(filename)",
        )
        bronze_metrics = source_metrics(
            connection,
            f"read_parquet('{bronze_glob}')",
            "source_file",
        )

        bronze_columns = [
            row[0]
            for row in connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{bronze_glob}')"
            ).fetchall()
        ]
        metadata = connection.execute(
            f"""
            SELECT
                count(*) AS rows,
                count(*) FILTER (
                    WHERE source_name IS NULL
                       OR source_file IS NULL
                       OR ingested_at IS NULL
                       OR ingestion_date IS NULL
                       OR batch_id IS NULL
                ) AS metadata_null_rows,
                count(DISTINCT source_name) AS source_names,
                min(source_name) AS source_name_min,
                max(source_name) AS source_name_max,
                count(DISTINCT batch_id) AS batch_ids,
                min(batch_id) AS batch_id_min,
                max(batch_id) AS batch_id_max,
                count(DISTINCT ingested_at) AS ingestion_timestamps,
                min(ingested_at) AS ingested_at_min,
                max(ingested_at) AS ingested_at_max,
                count(DISTINCT ingestion_date) AS ingestion_dates,
                min(ingestion_date) AS ingestion_date_min,
                max(ingestion_date) AS ingestion_date_max,
                count(*) FILTER (
                    WHERE CAST(ingested_at AS DATE) != ingestion_date
                ) AS ingestion_date_mismatch_rows
            FROM read_parquet('{bronze_glob}')
            """
        ).fetchone()
    finally:
        connection.close()

    codecs = set()
    for part_file in part_files:
        parquet_metadata = pq.ParquetFile(part_file).metadata
        for row_group_index in range(parquet_metadata.num_row_groups):
            row_group = parquet_metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                codecs.add(row_group.column(column_index).compression)

    expected_total_rows = sum(EXPECTED_SOURCE_ROWS.values())
    expected_started_at = (
        datetime.fromisoformat(summary["started_at_utc"])
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )

    require(summary["status"] == "PASS", "Latest ingestion summary is not PASS")
    require(
        {key: value["rows"] for key, value in raw_metrics.items()}
        == EXPECTED_SOURCE_ROWS,
        "Raw row counts do not match specification",
    )
    require(raw_metrics == bronze_metrics, "Raw and Bronze business data differ")
    require(metadata[0] == expected_total_rows, "Bronze total row count mismatch")
    require(metadata[1] == 0, "Bronze contains null ingestion metadata")
    require(
        metadata[2:5] == (1, SOURCE_NAME, SOURCE_NAME),
        "source_name is inconsistent",
    )
    require(
        metadata[5:8] == (1, summary["batch_id"], summary["batch_id"]),
        "batch_id is inconsistent",
    )
    require(
        metadata[8] == 1 and metadata[9] == metadata[10],
        "ingested_at is inconsistent",
    )
    require(
        metadata[9] == expected_started_at,
        "Stored ingested_at does not match batch start time in UTC",
    )
    require(
        metadata[11] == 1 and metadata[12] == metadata[13],
        "ingestion_date is inconsistent",
    )
    require(metadata[14] == 0, "ingestion_date does not match ingested_at")
    require(
        bronze_columns == BUSINESS_COLUMNS + METADATA_COLUMNS,
        "Bronze column names/order mismatch",
    )
    require(codecs == {"SNAPPY"}, f"Unexpected Parquet codecs: {codecs}")
    require(
        not list(BRONZE_DIR.parent.glob("_staging_*")),
        "A Bronze staging directory remains",
    )
    require(
        not list(BRONZE_DIR.parent.glob("_backup_*")),
        "A Bronze backup directory remains",
    )

    result = {
        "status": "PASS",
        "batch_id": summary["batch_id"],
        "rows": metadata[0],
        "columns": len(bronze_columns),
        "source_files": len(bronze_metrics),
        "part_files": len(part_files),
        "output_size_bytes": sum(path.stat().st_size for path in part_files),
        "metadata_null_rows": metadata[1],
        "ingested_at_utc": metadata[9].replace(tzinfo=timezone.utc).isoformat(),
        "parquet_codecs": sorted(codecs),
        "raw_bronze_business_match": True,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
