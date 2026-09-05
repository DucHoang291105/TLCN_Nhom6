import argparse
import csv
import json
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

import pyarrow.parquet as pq
import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).absolute().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "real_estate"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "real_estate_core"
QUALITY_DIR = PROJECT_ROOT / "docs" / "quality"
BRONZE_SUMMARY_PATH = QUALITY_DIR / "bronze_ingestion_summary.json"
SILVER_SUMMARY_PATH = QUALITY_DIR / "silver_core_summary.json"
THRESHOLD_PATH = PROJECT_ROOT / "docs" / "profiling" / "silver_dq_thresholds.csv"

SOURCE_ID = "SRC01"
SOURCE_NAME = "vduydong/vietnam-real-estates-2"
EXPECTED_ROWS = 3_500_744
EXPECTED_CANONICAL_ROWS = 3_500_694
EXPECTED_DUPLICATE_GROUPS = 36
EXPECTED_DUPLICATE_EXCESS_ROWS = 50
EXPECTED_MAX_DUPLICATE_COUNT = 6

LISTING_ID_VERSION = "src_content_sha256_v1"
SILVER_SCHEMA_VERSION = "silver_listing_core_v1"
DQ_RULE_VERSION = "src01_silver_core_dq_v1"
DQ_THRESHOLD_VERSION = "src01_raw_20260904_p99_v1"

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
REVIEW_RULES = (
    "DQ01",
    "DQ05",
    "DQ06",
    "DQ08",
    "DQ09",
    "DQ12",
    "DQ13",
    "DQ14",
)

PROPERTY_TYPES = (
    "Biệt thự/Nhà liền kề",
    "Căn hộ chung cư",
    "Nhà",
    "Shophouse",
    "Đất",
)

RAW_COLUMNS = [
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

EXPECTED_BRONZE_SCHEMA = [
    ("name", "string"),
    ("description", "string"),
    ("property_type_name", "string"),
    ("province_name", "string"),
    ("district_name", "string"),
    ("ward_name", "string"),
    ("street_name", "string"),
    ("project_name", "string"),
    ("price", "string"),
    ("area", "double"),
    ("floor_count", "double"),
    ("frontage_width", "double"),
    ("house_depth", "double"),
    ("road_width", "double"),
    ("bedroom_count", "double"),
    ("bathroom_count", "double"),
    ("house_direction", "string"),
    ("balcony_direction", "string"),
    ("published_at", "string"),
    ("source_name", "string"),
    ("source_file", "string"),
    ("ingested_at", "timestamp"),
    ("ingestion_date", "date"),
    ("batch_id", "string"),
]

EXPECTED_SILVER_SCHEMA = [
    ("source_id", "string"),
    ("source_name", "string"),
    ("source_listing_id", "string"),
    ("source_record_url", "string"),
    ("listing_id", "string"),
    ("listing_id_version", "string"),
    ("duplicate_group_id", "string"),
    ("duplicate_count", "bigint"),
    ("duplicate_rank", "int"),
    ("is_canonical", "boolean"),
    ("title", "string"),
    ("description", "string"),
    ("property_type_raw", "string"),
    ("property_type", "string"),
    ("province_name_raw", "string"),
    ("province_name", "string"),
    ("district_name_raw", "string"),
    ("district_name", "string"),
    ("ward_name_raw", "string"),
    ("ward_name", "string"),
    ("street_name", "string"),
    ("project_name", "string"),
    ("price_raw", "string"),
    ("price_vnd", "decimal(20,0)"),
    ("area_m2", "double"),
    ("price_per_m2", "decimal(24,2)"),
    ("floor_count", "int"),
    ("frontage_width_m", "double"),
    ("house_depth_m", "double"),
    ("road_width_m", "double"),
    ("bedroom_count", "int"),
    ("bathroom_count", "int"),
    ("house_direction", "string"),
    ("balcony_direction", "string"),
    ("published_at_raw", "string"),
    ("published_at", "timestamp_ntz"),
    ("published_date", "date"),
    ("published_year", "int"),
    ("published_month", "int"),
    ("year_month", "string"),
    ("source_file", "string"),
    ("bronze_batch_id", "string"),
    ("bronze_ingested_at", "timestamp"),
    ("bronze_ingestion_date", "date"),
    ("silver_batch_id", "string"),
    ("silver_processed_at", "timestamp"),
    ("silver_schema_version", "string"),
    ("dq_rule_version", "string"),
    ("dq_threshold_version", "string"),
    ("dq_status", "string"),
    ("dq_error_codes", "array<string>"),
    ("is_price_valid", "boolean"),
    ("is_area_valid", "boolean"),
    ("is_published_at_valid", "boolean"),
    ("is_province_valid", "boolean"),
    ("is_district_valid", "boolean"),
    ("is_ward_valid", "boolean"),
    ("partition_year_month", "string"),
]

BARE_LOCAL_FS_CLASS = "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
BARE_LOCAL_FS_JAR = "hadoop-bare-naked-local-fs-0.1.0.jar"
SPARK_MASTER = "local[2]"
SPARK_DRIVER_MEMORY = "4g"
SHUFFLE_PARTITIONS = "12"
MIN_FREE_SPACE_BYTES = 7 * 1024**3


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke-test", action="store_true")
    return parser.parse_args()


def schema_signature(dataframe):
    return [
        (field.name, field.dataType.simpleString())
        for field in dataframe.schema.fields
    ]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def configure_windows_runtime():
    if os.name != "nt":
        return

    python_dir = Path(sys.executable).parent
    spark_home = Path(pyspark.__file__).parent
    try:
        str(spark_home).encode("ascii")
    except UnicodeEncodeError as exc:
        raise RuntimeError(
            "Run Silver through scripts/run_silver.ps1 on Windows."
        ) from exc

    if not (spark_home / "jars" / BARE_LOCAL_FS_JAR).is_file():
        raise RuntimeError(
            f"Missing {BARE_LOCAL_FS_JAR}; run scripts/setup_spark_windows.ps1."
        )

    os.environ["PATH"] = str(python_dir) + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = "python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python.exe"
    os.environ["SPARK_HOME"] = str(spark_home)
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--driver-memory {SPARK_DRIVER_MEMORY} pyspark-shell",
    )


def create_spark_session(app_name):
    builder = (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false")
    )
    if os.name == "nt":
        builder = builder.config("spark.hadoop.fs.file.impl", BARE_LOCAL_FS_CLASS)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_thresholds():
    require(THRESHOLD_PATH.is_file(), f"Missing thresholds: {THRESHOLD_PATH}")
    with THRESHOLD_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    require(len(rows) == 6, "Threshold file must contain five types plus fallback")
    require({row["source_id"] for row in rows} == {SOURCE_ID}, "Threshold source mismatch")
    require(
        {row["threshold_version"] for row in rows} == {DQ_THRESHOLD_VERSION},
        "Threshold version mismatch",
    )

    fallback_rows = [
        row for row in rows if row["threshold_scope"] == "GLOBAL_FALLBACK"
    ]
    type_rows = [row for row in rows if row["threshold_scope"] == "PROPERTY_TYPE"]
    require(len(fallback_rows) == 1, "Missing unique global fallback threshold")
    require(
        {row["property_type"] for row in type_rows} == set(PROPERTY_TYPES),
        "Property-type thresholds are incomplete",
    )

    numeric_columns = (
        "p99_area_m2",
        "p99_price_per_m2",
        "p99_floor_count",
        "p99_bedroom_count",
        "p99_bathroom_count",
    )

    def converted(row):
        return {
            "property_type": row["property_type"],
            **{column: float(row[column]) for column in numeric_columns},
        }

    return [converted(row) for row in type_rows], converted(fallback_rows[0])


def clean_text(column):
    value = F.trim(column)
    return F.when(column.isNull() | (value == ""), F.lit(None).cast("string")).otherwise(value)


def any_condition(conditions):
    return reduce(lambda left, right: left | right, conditions)


def build_silver_dataframe(
    bronze_df,
    threshold_df,
    fallback,
    silver_batch_id,
    processed_at_text,
):
    source_scoped = bronze_df.withColumn("source_id", F.lit(SOURCE_ID))
    id_payload = F.struct(
        F.lit(LISTING_ID_VERSION).alias("id_contract"),
        F.col("source_id"),
        F.col("source_name"),
        *[F.col(column) for column in RAW_COLUMNS],
    )
    listing_id = F.sha2(
        F.to_json(id_payload, options={"ignoreNullFields": "false"}),
        256,
    )

    working = (
        source_scoped
        .withColumn("listing_id", listing_id)
        .withColumn("title", clean_text(F.col("name")))
        .withColumn("description_clean", clean_text(F.col("description")))
        .withColumn("property_type_raw", F.col("property_type_name"))
        .withColumn("property_type", clean_text(F.col("property_type_name")))
        .withColumn("province_name_raw", F.col("province_name"))
        .withColumn("province_name_clean", clean_text(F.col("province_name")))
        .withColumn("district_name_raw", F.col("district_name"))
        .withColumn("district_name_clean", clean_text(F.col("district_name")))
        .withColumn("ward_name_raw", F.col("ward_name"))
        .withColumn("ward_name_clean", clean_text(F.col("ward_name")))
        .withColumn("street_name_clean", clean_text(F.col("street_name")))
        .withColumn("project_name_clean", clean_text(F.col("project_name")))
        .withColumn("price_raw", F.col("price"))
        .withColumn(
            "price_vnd",
            F.expr("try_cast(trim(price) AS DECIMAL(20,0))"),
        )
        .withColumn("area_m2", F.col("area"))
        .withColumn("floor_count_clean", F.col("floor_count").cast("int"))
        .withColumn("bedroom_count_clean", F.col("bedroom_count").cast("int"))
        .withColumn("bathroom_count_clean", F.col("bathroom_count").cast("int"))
        .withColumn("house_direction_clean", clean_text(F.col("house_direction")))
        .withColumn("balcony_direction_clean", clean_text(F.col("balcony_direction")))
        .withColumn("published_at_raw", F.col("published_at"))
        .withColumn(
            "published_at_clean",
            F.expr("try_cast(published_at AS TIMESTAMP_NTZ)"),
        )
        .withColumn(
            "is_price_valid",
            F.coalesce(F.col("price_vnd") > 0, F.lit(False)),
        )
        .withColumn(
            "is_area_valid",
            F.coalesce(F.col("area_m2") > 0, F.lit(False)),
        )
        .withColumn("is_published_at_valid", F.col("published_at_clean").isNotNull())
        .withColumn("is_province_valid", F.col("province_name_clean").isNotNull())
        .withColumn("is_district_valid", F.col("district_name_clean").isNotNull())
        .withColumn("is_ward_valid", F.col("ward_name_clean").isNotNull())
        .withColumn(
            "price_per_m2",
            F.when(
                F.col("is_price_valid") & F.col("is_area_valid"),
                (F.col("price_vnd") / F.col("area_m2")).cast("decimal(24,2)"),
            ).otherwise(F.lit(None).cast("decimal(24,2)")),
        )
    )

    duplicate_window = Window.partitionBy("listing_id")
    duplicate_rank_window = duplicate_window.orderBy(F.col("source_file").asc_nulls_last())
    working = (
        working
        .withColumn("duplicate_count", F.count(F.lit(1)).over(duplicate_window).cast("long"))
        .withColumn("duplicate_rank", F.row_number().over(duplicate_rank_window))
        .withColumn("is_canonical", F.col("duplicate_rank") == 1)
        .join(F.broadcast(threshold_df), on="property_type", how="left")
    )

    threshold_columns = {
        "area": F.coalesce(F.col("p99_area_m2"), F.lit(fallback["p99_area_m2"])),
        "price_m2": F.coalesce(
            F.col("p99_price_per_m2"),
            F.lit(fallback["p99_price_per_m2"]),
        ),
        "floor": F.coalesce(F.col("p99_floor_count"), F.lit(fallback["p99_floor_count"])),
        "bedroom": F.coalesce(
            F.col("p99_bedroom_count"),
            F.lit(fallback["p99_bedroom_count"]),
        ),
        "bathroom": F.coalesce(
            F.col("p99_bathroom_count"),
            F.lit(fallback["p99_bathroom_count"]),
        ),
    }

    price_not_blank = F.col("price_raw").isNotNull() & (F.trim("price_raw") != "")
    price_per_m2_unrounded = F.col("price_vnd").cast("double") / F.col("area_m2")
    rules = {
        "DQ01": F.col("price_raw").isNull() | (F.trim("price_raw") == ""),
        "DQ02": F.col("price_vnd") <= 0,
        "DQ03": price_not_blank & F.col("price_vnd").isNull(),
        "DQ04": F.col("area_m2").isNull() | (F.col("area_m2") <= 0),
        "DQ05": F.col("area_m2") > threshold_columns["area"],
        # Compare the unrounded ratio used during profiling. The published
        # price_per_m2 column remains decimal(24,2) for consumption.
        "DQ06": price_per_m2_unrounded > threshold_columns["price_m2"],
        "DQ07": ~F.col("is_province_valid"),
        "DQ08": ~F.col("is_district_valid"),
        "DQ09": ~F.col("is_ward_valid"),
        "DQ10": ~F.col("is_published_at_valid"),
        "DQ11": ~F.col("is_canonical"),
        "DQ12": F.col("floor_count_clean") > threshold_columns["floor"],
        "DQ13": F.col("bedroom_count_clean") > threshold_columns["bedroom"],
        "DQ14": F.col("bathroom_count_clean") > threshold_columns["bathroom"],
    }
    error_array = F.filter(
        F.array(
            *[
                F.when(rules[rule_id], F.lit(rule_id)).otherwise(
                    F.lit(None).cast("string")
                )
                for rule_id in EXPECTED_DQ_COUNTS
            ]
        ),
        lambda value: value.isNotNull(),
    )
    rejected = any_condition([rules[rule_id] for rule_id in REJECTED_RULES])
    review = any_condition([rules[rule_id] for rule_id in REVIEW_RULES])

    working = (
        working
        .withColumn("dq_error_codes", error_array)
        .withColumn(
            "dq_status",
            F.when(rejected, F.lit("REJECTED"))
            .when(review, F.lit("REVIEW"))
            .otherwise(F.lit("VALID")),
        )
        .withColumn("published_date", F.to_date("published_at_clean"))
        .withColumn("published_year", F.year("published_at_clean"))
        .withColumn("published_month", F.month("published_at_clean"))
        .withColumn("year_month", F.date_format("published_at_clean", "yyyy-MM"))
        .withColumn(
            "partition_year_month",
            F.coalesce(F.col("year_month"), F.lit("unknown")),
        )
    )

    processed_at = F.to_timestamp(
        F.lit(processed_at_text),
        "yyyy-MM-dd HH:mm:ss.SSSSSS",
    )
    return working.select(
        F.col("source_id"),
        F.col("source_name"),
        F.lit(None).cast("string").alias("source_listing_id"),
        F.lit(None).cast("string").alias("source_record_url"),
        F.col("listing_id"),
        F.lit(LISTING_ID_VERSION).alias("listing_id_version"),
        F.col("listing_id").alias("duplicate_group_id"),
        F.col("duplicate_count"),
        F.col("duplicate_rank"),
        F.col("is_canonical"),
        F.col("title"),
        F.col("description_clean").alias("description"),
        F.col("property_type_raw"),
        F.col("property_type"),
        F.col("province_name_raw"),
        F.col("province_name_clean").alias("province_name"),
        F.col("district_name_raw"),
        F.col("district_name_clean").alias("district_name"),
        F.col("ward_name_raw"),
        F.col("ward_name_clean").alias("ward_name"),
        F.col("street_name_clean").alias("street_name"),
        F.col("project_name_clean").alias("project_name"),
        F.col("price_raw"),
        F.col("price_vnd"),
        F.col("area_m2"),
        F.col("price_per_m2"),
        F.col("floor_count_clean").alias("floor_count"),
        F.col("frontage_width").alias("frontage_width_m"),
        F.col("house_depth").alias("house_depth_m"),
        F.col("road_width").alias("road_width_m"),
        F.col("bedroom_count_clean").alias("bedroom_count"),
        F.col("bathroom_count_clean").alias("bathroom_count"),
        F.col("house_direction_clean").alias("house_direction"),
        F.col("balcony_direction_clean").alias("balcony_direction"),
        F.col("published_at_raw"),
        F.col("published_at_clean").alias("published_at"),
        F.col("published_date"),
        F.col("published_year"),
        F.col("published_month"),
        F.col("year_month"),
        F.col("source_file"),
        F.col("batch_id").alias("bronze_batch_id"),
        F.col("ingested_at").alias("bronze_ingested_at"),
        F.col("ingestion_date").alias("bronze_ingestion_date"),
        F.lit(silver_batch_id).alias("silver_batch_id"),
        processed_at.alias("silver_processed_at"),
        F.lit(SILVER_SCHEMA_VERSION).alias("silver_schema_version"),
        F.lit(DQ_RULE_VERSION).alias("dq_rule_version"),
        F.lit(DQ_THRESHOLD_VERSION).alias("dq_threshold_version"),
        F.col("dq_status"),
        F.col("dq_error_codes"),
        F.col("is_price_valid"),
        F.col("is_area_valid"),
        F.col("is_published_at_valid"),
        F.col("is_province_valid"),
        F.col("is_district_valid"),
        F.col("is_ward_valid"),
        F.col("partition_year_month"),
    )


def validate_bronze(bronze_df, bronze_summary):
    require(
        schema_signature(bronze_df) == EXPECTED_BRONZE_SCHEMA,
        "Bronze schema does not match the validated 24-column contract",
    )
    require(bronze_summary.get("status") == "PASS", "Bronze summary is not PASS")

    count_invalid = lambda column: F.sum(
        F.when(
            F.col(column).isNotNull()
            & ((F.col(column) < 0) | (F.col(column) != F.floor(F.col(column)))),
            1,
        ).otherwise(0)
    ).alias(f"invalid_{column}")
    metrics = bronze_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("source_name").alias("source_names"),
        F.min("source_name").alias("source_name_min"),
        F.max("source_name").alias("source_name_max"),
        F.countDistinct("batch_id").alias("batch_ids"),
        F.min("batch_id").alias("batch_id_min"),
        F.max("batch_id").alias("batch_id_max"),
        F.sum(
            F.when(
                F.col("source_name").isNull()
                | F.col("source_file").isNull()
                | F.col("ingested_at").isNull()
                | F.col("ingestion_date").isNull()
                | F.col("batch_id").isNull(),
                1,
            ).otherwise(0)
        ).alias("metadata_null_rows"),
        F.sort_array(F.collect_set(F.trim("property_type_name"))).alias("property_types"),
        count_invalid("floor_count"),
        count_invalid("bedroom_count"),
        count_invalid("bathroom_count"),
    ).first()

    require(metrics["rows"] == EXPECTED_ROWS, "Bronze row count mismatch")
    require(
        metrics["source_names"] == 1
        and metrics["source_name_min"] == SOURCE_NAME
        and metrics["source_name_max"] == SOURCE_NAME,
        "Bronze source_name mismatch",
    )
    require(
        metrics["batch_ids"] == 1
        and metrics["batch_id_min"] == bronze_summary["batch_id"]
        and metrics["batch_id_max"] == bronze_summary["batch_id"],
        "Bronze batch_id mismatch",
    )
    require(metrics["metadata_null_rows"] == 0, "Bronze metadata contains nulls")
    require(set(metrics["property_types"]) == set(PROPERTY_TYPES), "Property types changed")
    for column in ("floor_count", "bedroom_count", "bathroom_count"):
        require(metrics[f"invalid_{column}"] == 0, f"{column} is negative/non-integer")
    return metrics.asDict(recursive=True)


def validate_silver_output(dataframe):
    require(
        schema_signature(dataframe) == EXPECTED_SILVER_SCHEMA,
        f"Silver schema mismatch: {schema_signature(dataframe)}",
    )

    technical_columns = [
        "source_id",
        "source_name",
        "listing_id",
        "listing_id_version",
        "duplicate_group_id",
        "duplicate_count",
        "duplicate_rank",
        "is_canonical",
        "source_file",
        "bronze_batch_id",
        "bronze_ingested_at",
        "bronze_ingestion_date",
        "silver_batch_id",
        "silver_processed_at",
        "silver_schema_version",
        "dq_rule_version",
        "dq_threshold_version",
        "dq_status",
        "dq_error_codes",
        "is_price_valid",
        "is_area_valid",
        "is_published_at_valid",
        "is_province_valid",
        "is_district_valid",
        "is_ward_valid",
        "partition_year_month",
    ]
    technical_null = reduce(
        lambda left, right: left | right,
        [F.col(column).isNull() for column in technical_columns],
    )
    dq_aggregates = [
        F.sum(
            F.when(F.array_contains("dq_error_codes", rule_id), 1).otherwise(0)
        ).alias(rule_id)
        for rule_id in EXPECTED_DQ_COUNTS
    ]
    metrics = dataframe.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("is_canonical"), 1).otherwise(0)).alias("canonical_rows"),
        F.sum(F.when(~F.col("is_canonical"), 1).otherwise(0)).alias("noncanonical_rows"),
        F.sum(
            F.when((F.col("duplicate_count") > 1) & (F.col("duplicate_rank") == 1), 1).otherwise(0)
        ).alias("duplicate_groups"),
        F.max("duplicate_count").alias("max_duplicate_count"),
        F.sum(
            F.when(
                (F.col("duplicate_rank") < 1)
                | (F.col("duplicate_rank") > F.col("duplicate_count")),
                1,
            ).otherwise(0)
        ).alias("invalid_duplicate_rank_rows"),
        F.sum(
            F.when(
                F.array_contains("dq_error_codes", "DQ11") != (~F.col("is_canonical")),
                1,
            ).otherwise(0)
        ).alias("dq11_mismatch_rows"),
        F.sum(F.when(~F.col("listing_id").rlike("^[0-9a-f]{64}$"), 1).otherwise(0)).alias("invalid_id_rows"),
        F.sum(F.when(technical_null, 1).otherwise(0)).alias("technical_null_rows"),
        F.sum(
            F.when(
                F.col("price_per_m2").isNotNull()
                & (~F.col("is_price_valid") | ~F.col("is_area_valid")),
                1,
            ).otherwise(0)
        ).alias("invalid_price_per_m2_rows"),
        F.sum(
            F.when(F.size("dq_error_codes") != F.size(F.array_distinct("dq_error_codes")), 1).otherwise(0)
        ).alias("duplicate_dq_code_rows"),
        F.sum(F.when(F.col("dq_status") == "VALID", 1).otherwise(0)).alias("valid_rows"),
        F.sum(F.when(F.col("dq_status") == "REVIEW", 1).otherwise(0)).alias("review_rows"),
        F.sum(F.when(F.col("dq_status") == "REJECTED", 1).otherwise(0)).alias("rejected_rows"),
        *dq_aggregates,
    ).first().asDict()

    require(metrics["rows"] == EXPECTED_ROWS, "Silver row count mismatch")
    require(metrics["canonical_rows"] == EXPECTED_CANONICAL_ROWS, "Canonical row count mismatch")
    require(metrics["noncanonical_rows"] == EXPECTED_DUPLICATE_EXCESS_ROWS, "Noncanonical row count mismatch")
    require(metrics["duplicate_groups"] == EXPECTED_DUPLICATE_GROUPS, "Duplicate group count mismatch")
    require(metrics["max_duplicate_count"] == EXPECTED_MAX_DUPLICATE_COUNT, "Max duplicate count mismatch")
    require(metrics["invalid_duplicate_rank_rows"] == 0, "Invalid duplicate ranks")
    require(metrics["dq11_mismatch_rows"] == 0, "DQ11 does not match canonical flag")
    require(metrics["invalid_id_rows"] == 0, "Invalid listing_id format")
    require(metrics["technical_null_rows"] == 0, "Required technical fields contain nulls")
    require(metrics["invalid_price_per_m2_rows"] == 0, "Invalid price_per_m2 derivation")
    require(metrics["duplicate_dq_code_rows"] == 0, "Duplicate DQ codes found")
    require(
        metrics["valid_rows"] + metrics["review_rows"] + metrics["rejected_rows"] == EXPECTED_ROWS,
        "DQ status counts do not reconcile",
    )
    actual_dq_counts = {rule_id: metrics[rule_id] for rule_id in EXPECTED_DQ_COUNTS}
    require(actual_dq_counts == EXPECTED_DQ_COUNTS, f"DQ counts changed: {actual_dq_counts}")
    metrics["dq_counts"] = actual_dq_counts
    for rule_id in EXPECTED_DQ_COUNTS:
        del metrics[rule_id]
    return metrics


def validate_parquet_compression(part_files):
    codecs = set()
    for part_file in part_files:
        metadata = pq.ParquetFile(part_file).metadata
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                codecs.add(row_group.column(column_index).compression)
    require(codecs == {"SNAPPY"}, f"Unexpected Parquet codecs: {sorted(codecs)}")
    return sorted(codecs)


def ensure_silver_child(path):
    expected_parent = PROJECT_ROOT / "data" / "silver"
    require(path.parent == expected_parent, f"Unsafe generated Silver path: {path}")


def remove_generated_directory(path):
    ensure_silver_child(path)
    if path.exists():
        shutil.rmtree(path)


def promote_staging(staging_dir, batch_id):
    ensure_silver_child(staging_dir)
    ensure_silver_child(SILVER_DIR)
    backup_dir = SILVER_DIR.parent / f"_backup_real_estate_core_{batch_id}"
    ensure_silver_child(backup_dir)
    require(not backup_dir.exists(), f"Backup already exists: {backup_dir}")

    had_previous = SILVER_DIR.exists()
    if had_previous:
        SILVER_DIR.rename(backup_dir)
    try:
        staging_dir.rename(SILVER_DIR)
    except Exception:
        if had_previous and backup_dir.exists() and not SILVER_DIR.exists():
            backup_dir.rename(SILVER_DIR)
        raise
    if backup_dir.exists():
        remove_generated_directory(backup_dir)


def write_summary(summary, batch_id):
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    run_path = QUALITY_DIR / f"silver_core_{batch_id}.run.json"
    for path in (run_path, SILVER_SUMMARY_PATH):
        with path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    return run_path


def main():
    args = parse_args()
    started_clock = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    silver_batch_id = (
        "silver_core_"
        f"{started_at.strftime('%Y%m%dT%H%M%S%fZ')}_"
        f"{uuid.uuid4().hex[:8]}"
    )
    processed_at_text = started_at.strftime("%Y-%m-%d %H:%M:%S.%f")
    staging_dir = SILVER_DIR.parent / f"_staging_real_estate_core_{silver_batch_id}"
    summary = {
        "pipeline": "bronze_to_silver_listing_core_src01",
        "status": "RUNNING",
        "source_id": SOURCE_ID,
        "source_name": SOURCE_NAME,
        "silver_batch_id": silver_batch_id,
        "started_at_utc": started_at.isoformat(),
        "input_path": "data/bronze/real_estate",
        "output_path": "data/silver/real_estate_core",
        "location_enrichment": "NOT_INCLUDED",
        "schema_version": SILVER_SCHEMA_VERSION,
        "dq_rule_version": DQ_RULE_VERSION,
        "dq_threshold_version": DQ_THRESHOLD_VERSION,
        "compression": "snappy",
        "partition_column": "partition_year_month",
        "smoke_test": args.smoke_test,
    }
    spark = None

    try:
        configure_windows_runtime()
        require(BRONZE_SUMMARY_PATH.is_file(), "Missing Bronze PASS summary")
        require((BRONZE_DIR / "_SUCCESS").is_file(), "Bronze output is incomplete")
        bronze_summary = json.loads(BRONZE_SUMMARY_PATH.read_text(encoding="utf-8"))
        threshold_rows, fallback = read_thresholds()

        if not args.smoke_test:
            disk = shutil.disk_usage(PROJECT_ROOT)
            require(
                disk.free >= MIN_FREE_SPACE_BYTES,
                f"Not enough free disk space: {disk.free} bytes",
            )
            ensure_silver_child(staging_dir)
            require(not staging_dir.exists(), f"Staging already exists: {staging_dir}")
            summary["available_disk_bytes_before"] = disk.free
            summary["required_free_bytes"] = MIN_FREE_SPACE_BYTES
            summary["previous_output_existed"] = SILVER_DIR.exists()

        spark = create_spark_session(
            "SilverCoreSmokeTest" if args.smoke_test else "SilverListingCoreSRC01"
        )
        bronze_df = spark.read.parquet(BRONZE_DIR.absolute().as_uri())
        threshold_df = spark.createDataFrame(threshold_rows)

        if args.smoke_test:
            require(
                schema_signature(bronze_df) == EXPECTED_BRONZE_SCHEMA,
                "Bronze schema mismatch",
            )
            sample_df = bronze_df.limit(10_000)
            silver_df = build_silver_dataframe(
                sample_df,
                threshold_df,
                fallback,
                silver_batch_id,
                processed_at_text,
            )
            require(
                schema_signature(silver_df) == EXPECTED_SILVER_SCHEMA,
                f"Smoke schema mismatch: {schema_signature(silver_df)}",
            )
            sample_metrics = silver_df.agg(
                F.count("*").alias("rows"),
                F.sum(F.when(F.col("listing_id").isNull(), 1).otherwise(0)).alias("null_ids"),
                F.sum(F.when(F.col("dq_error_codes").isNull(), 1).otherwise(0)).alias("null_dq"),
            ).first().asDict()
            require(sample_metrics == {"rows": 10_000, "null_ids": 0, "null_dq": 0}, "Smoke metrics failed")
            print("SILVER_CORE_SMOKE_TEST: PASS")
            print(json.dumps(sample_metrics, indent=2))
            return

        bronze_metrics = validate_bronze(bronze_df, bronze_summary)
        summary["bronze_validation"] = bronze_metrics
        print(f"[SILVER] batch_id={silver_batch_id}", flush=True)
        print(f"[SILVER] input_rows={bronze_metrics['rows']}", flush=True)
        print("[SILVER] location_enrichment=NOT_INCLUDED", flush=True)

        silver_df = build_silver_dataframe(
            bronze_df,
            threshold_df,
            fallback,
            silver_batch_id,
            processed_at_text,
        )
        require(
            schema_signature(silver_df) == EXPECTED_SILVER_SCHEMA,
            f"Silver pre-write schema mismatch: {schema_signature(silver_df)}",
        )

        print(f"[SILVER] writing_staging={staging_dir}", flush=True)
        (
            silver_df.repartition("partition_year_month")
            .write.mode("errorifexists")
            .option("compression", "snappy")
            .partitionBy("partition_year_month")
            .parquet(staging_dir.absolute().as_uri())
        )

        output_df = spark.read.parquet(staging_dir.absolute().as_uri())
        output_metrics = validate_silver_output(output_df)
        partition_counts = {
            row["partition_year_month"]: row["count"]
            for row in output_df.groupBy("partition_year_month").count().collect()
        }
        require(sum(partition_counts.values()) == EXPECTED_ROWS, "Partition counts mismatch")

        success_marker = staging_dir / "_SUCCESS"
        part_files = sorted(staging_dir.rglob("part-*.parquet"))
        require(success_marker.is_file(), "Silver staging output is missing _SUCCESS")
        require(part_files, "Silver staging output has no Parquet files")
        codecs = validate_parquet_compression(part_files)
        output_size_bytes = sum(path.stat().st_size for path in part_files)

        spark.stop()
        spark = None
        promote_staging(staging_dir, silver_batch_id)
        final_parts = sorted(SILVER_DIR.rglob("part-*.parquet"))
        require((SILVER_DIR / "_SUCCESS").is_file(), "Promoted Silver lacks _SUCCESS")
        require(len(final_parts) == len(part_files), "Part-file count changed during promote")

        summary.update(
            {
                "status": "PASS",
                "bronze_batch_id": bronze_summary["batch_id"],
                "input_rows": EXPECTED_ROWS,
                "output_rows": output_metrics["rows"],
                "output_columns": len(EXPECTED_SILVER_SCHEMA),
                "canonical_rows": output_metrics["canonical_rows"],
                "noncanonical_rows": output_metrics["noncanonical_rows"],
                "duplicate_groups": output_metrics["duplicate_groups"],
                "max_duplicate_count": output_metrics["max_duplicate_count"],
                "dq_status_counts": {
                    "VALID": output_metrics["valid_rows"],
                    "REVIEW": output_metrics["review_rows"],
                    "REJECTED": output_metrics["rejected_rows"],
                },
                "dq_counts": output_metrics["dq_counts"],
                "partition_counts": dict(sorted(partition_counts.items())),
                "part_file_count": len(final_parts),
                "output_size_bytes": output_size_bytes,
                "parquet_codecs": codecs,
                "validations": {
                    "bronze_pass_gate": "PASS",
                    "bronze_schema": "PASS",
                    "bronze_metadata": "PASS",
                    "threshold_contract": "PASS",
                    "silver_schema": "PASS",
                    "row_preservation": "PASS",
                    "listing_id_format": "PASS",
                    "duplicate_contract": "PASS",
                    "dq_rule_counts": "PASS",
                    "dq_status_reconciliation": "PASS",
                    "field_validity": "PASS",
                    "partition_reconciliation": "PASS",
                    "read_back": "PASS",
                    "parquet_compression": "PASS",
                    "staging_promotion": "PASS",
                    "location_not_joined": "PASS",
                },
            }
        )
        print(f"[SILVER] output_rows={output_metrics['rows']}", flush=True)
        print(f"[SILVER] canonical_rows={output_metrics['canonical_rows']}", flush=True)
        print(f"[SILVER] output_columns={len(EXPECTED_SILVER_SCHEMA)}", flush=True)
        print(f"[SILVER] part_files={len(final_parts)}", flush=True)
        print("[SILVER] status=PASS", flush=True)
    except Exception as exc:
        summary["status"] = "FAIL"
        summary["error_type"] = type(exc).__name__
        summary["error_message"] = str(exc)
        print(f"[SILVER] status=FAIL error={exc}", file=sys.stderr, flush=True)
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as stop_error:
                summary["spark_stop_warning"] = str(stop_error)
        if not args.smoke_test and staging_dir.exists():
            try:
                remove_generated_directory(staging_dir)
            except Exception as cleanup_error:
                summary["staging_cleanup_warning"] = str(cleanup_error)
        if not args.smoke_test:
            completed_at = datetime.now(timezone.utc)
            summary["completed_at_utc"] = completed_at.isoformat()
            summary["duration_seconds"] = round(time.perf_counter() - started_clock, 3)
            try:
                run_path = write_summary(summary, silver_batch_id)
                print(f"[SILVER] summary={run_path.relative_to(PROJECT_ROOT)}", flush=True)
            except Exception as summary_error:
                print(f"[SILVER] summary_write_failed={summary_error}", file=sys.stderr)
                if summary.get("status") == "PASS":
                    raise


if __name__ == "__main__":
    main()
