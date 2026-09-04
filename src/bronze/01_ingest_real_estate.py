import json
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyspark
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).absolute().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "real_estate"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "real_estate"
SUMMARY_DIR = PROJECT_ROOT / "docs" / "quality"
LATEST_SUMMARY_PATH = SUMMARY_DIR / "bronze_ingestion_summary.json"

SOURCE_NAME = "vduydong/vietnam-real-estates-2"
EXPECTED_TOTAL_ROWS = 3_500_744
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

EXPECTED_RAW_SCHEMA = [
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
]

EXPECTED_METADATA_SCHEMA = [
    ("source_name", "string"),
    ("source_file", "string"),
    ("ingested_at", "timestamp"),
    ("ingestion_date", "date"),
    ("batch_id", "string"),
]

BARE_LOCAL_FS_CLASS = "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
BARE_LOCAL_FS_JAR = "hadoop-bare-naked-local-fs-0.1.0.jar"
MIN_FREE_SPACE_BYTES = 5 * 1024**3
SPARK_MASTER = "local[2]"
SPARK_DRIVER_MEMORY = "4g"
MIN_DRIVER_HEAP_BYTES = (7 * 1024**3) // 2
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def schema_signature(dataframe):
    return [
        (field.name, field.dataType.simpleString())
        for field in dataframe.schema.fields
    ]


def epoch_micros(value):
    delta = value.astimezone(timezone.utc) - UTC_EPOCH
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000
        + delta.microseconds
    )


def get_raw_files():
    raw_files = sorted(RAW_DIR.glob("shard_*.parquet"))
    actual_names = [path.name for path in raw_files if path.is_file()]
    expected_names = list(EXPECTED_SOURCE_ROWS)

    if actual_names != expected_names:
        raise RuntimeError(
            "Danh sách raw shard không đúng specification. "
            f"Expected={expected_names}, actual={actual_names}"
        )

    return raw_files


def configure_windows_runtime():
    if os.name != "nt":
        return

    python_dir = Path(sys.executable).parent
    spark_home = Path(pyspark.__file__).parent

    try:
        str(spark_home).encode("ascii")
    except UnicodeEncodeError as exc:
        raise RuntimeError(
            "Spark trên Windows phải được chạy qua alias đường dẫn ASCII. "
            "Hãy dùng scripts/run_bronze.ps1 thay vì gọi file Python trực tiếp."
        ) from exc

    bare_local_fs_jar = spark_home / "jars" / BARE_LOCAL_FS_JAR
    if not bare_local_fs_jar.is_file():
        raise RuntimeError(
            f"Thiếu {BARE_LOCAL_FS_JAR}. "
            "Hãy chạy scripts/setup_spark_windows.ps1 trước."
        )

    os.environ["PATH"] = str(python_dir) + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = "python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python.exe"
    os.environ["SPARK_HOME"] = str(spark_home)
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--driver-memory {SPARK_DRIVER_MEMORY} pyspark-shell",
    )


def create_spark_session():
    builder = (
        SparkSession.builder.master(SPARK_MASTER)
        .appName("BronzeRealEstateIngestion")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false")
    )

    if os.name == "nt":
        builder = builder.config(
            "spark.hadoop.fs.file.impl",
            BARE_LOCAL_FS_CLASS,
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_schema(actual_schema, expected_schema, layer_name):
    if actual_schema != expected_schema:
        raise RuntimeError(
            f"{layer_name} schema không đúng. "
            f"Expected={expected_schema}, actual={actual_schema}"
        )


def collect_source_metrics(dataframe):
    business_columns = [F.col(column) for column, _ in EXPECTED_RAW_SCHEMA]
    rows = (
        dataframe.groupBy("source_file")
        .agg(
            F.count("*").alias("rows"),
            F.sum(
                F.xxhash64(*business_columns).cast("decimal(38,0)")
            ).alias("business_fingerprint"),
        )
        .collect()
    )

    return {
        row["source_file"]: {
            "rows": row["rows"],
            "business_fingerprint": str(row["business_fingerprint"]),
        }
        for row in sorted(rows, key=lambda item: item["source_file"] or "")
    }


def source_counts(source_metrics):
    return {
        source_file: metrics["rows"]
        for source_file, metrics in source_metrics.items()
    }


def source_fingerprints(source_metrics):
    return {
        source_file: metrics["business_fingerprint"]
        for source_file, metrics in source_metrics.items()
    }


def validate_source_counts(actual_counts, layer_name):
    if actual_counts != EXPECTED_SOURCE_ROWS:
        raise RuntimeError(
            f"{layer_name} row count theo shard không đúng. "
            f"Expected={EXPECTED_SOURCE_ROWS}, actual={actual_counts}"
        )

    total_rows = sum(actual_counts.values())
    if total_rows != EXPECTED_TOTAL_ROWS:
        raise RuntimeError(
            f"{layer_name} total rows không đúng. "
            f"Expected={EXPECTED_TOTAL_ROWS}, actual={total_rows}"
        )

    return total_rows


def validate_output_metadata(
    dataframe,
    batch_id,
    ingestion_date,
    expected_ingested_at_micros,
):
    business_columns = [F.col(column) for column, _ in EXPECTED_RAW_SCHEMA]
    grouped_rows = (
        dataframe.groupBy("source_file")
        .agg(
            F.count("*").alias("rows"),
            F.sum(
                F.xxhash64(*business_columns).cast("decimal(38,0)")
            ).alias("business_fingerprint"),
            F.count("source_name").alias("source_name_non_null"),
            F.min("source_name").alias("source_name_min"),
            F.max("source_name").alias("source_name_max"),
            F.count("batch_id").alias("batch_id_non_null"),
            F.min("batch_id").alias("batch_id_min"),
            F.max("batch_id").alias("batch_id_max"),
            F.count("ingested_at").alias("ingested_at_non_null"),
            F.min(F.unix_micros("ingested_at")).alias(
                "ingested_at_min_micros"
            ),
            F.max(F.unix_micros("ingested_at")).alias(
                "ingested_at_max_micros"
            ),
            F.count("ingestion_date").alias("ingestion_date_non_null"),
            F.min("ingestion_date").alias("ingestion_date_min"),
            F.max("ingestion_date").alias("ingestion_date_max"),
            F.sum(
                F.when(
                    F.to_date("ingested_at") != F.col("ingestion_date"),
                    1,
                ).otherwise(0)
            ).alias("date_mismatch_rows"),
        )
        .collect()
    )

    output_metrics = {}
    timestamps = set()

    for row in grouped_rows:
        source_file = row["source_file"]
        row_count = row["rows"]
        output_metrics[source_file] = {
            "rows": row_count,
            "business_fingerprint": str(row["business_fingerprint"]),
        }

        if not source_file:
            raise RuntimeError("Bronze có source_file null/rỗng")
        if row["source_name_non_null"] != row_count:
            raise RuntimeError(f"source_name bị null trong {source_file}")
        if row["source_name_min"] != SOURCE_NAME or row["source_name_max"] != SOURCE_NAME:
            raise RuntimeError(f"source_name không đồng nhất trong {source_file}")
        if row["batch_id_non_null"] != row_count:
            raise RuntimeError(f"batch_id bị null trong {source_file}")
        if row["batch_id_min"] != batch_id or row["batch_id_max"] != batch_id:
            raise RuntimeError(f"batch_id không đồng nhất trong {source_file}")
        if row["ingested_at_non_null"] != row_count:
            raise RuntimeError(f"ingested_at bị null trong {source_file}")
        if row["ingested_at_min_micros"] != row["ingested_at_max_micros"]:
            raise RuntimeError(f"ingested_at không đồng nhất trong {source_file}")
        if row["ingested_at_min_micros"] != expected_ingested_at_micros:
            raise RuntimeError(f"ingested_at không đúng batch trong {source_file}")
        if row["ingestion_date_non_null"] != row_count:
            raise RuntimeError(f"ingestion_date bị null trong {source_file}")
        if row["ingestion_date_min"] != ingestion_date:
            raise RuntimeError(f"ingestion_date không đúng trong {source_file}")
        if row["ingestion_date_max"] != ingestion_date:
            raise RuntimeError(f"ingestion_date không đồng nhất trong {source_file}")
        if row["date_mismatch_rows"] != 0:
            raise RuntimeError(
                f"ingestion_date không khớp ingested_at trong {source_file}"
            )

        timestamps.add(row["ingested_at_min_micros"])

    if len(timestamps) != 1:
        raise RuntimeError("ingested_at không đồng nhất trên toàn bộ batch")

    return output_metrics, next(iter(timestamps))


def validate_parquet_compression(part_files):
    codecs = set()

    for part_file in part_files:
        metadata = pq.ParquetFile(part_file).metadata
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                codecs.add(row_group.column(column_index).compression)

    if codecs != {"SNAPPY"}:
        raise RuntimeError(f"Bronze Parquet codec không đúng: {sorted(codecs)}")

    return sorted(codecs)


def ensure_bronze_child(path):
    expected_parent = PROJECT_ROOT / "data" / "bronze"
    if path.parent != expected_parent:
        raise RuntimeError(f"Unsafe generated Bronze path: {path}")


def remove_generated_directory(path):
    ensure_bronze_child(path)
    if path.exists():
        shutil.rmtree(path)


def promote_staging(staging_dir, batch_id):
    ensure_bronze_child(staging_dir)
    ensure_bronze_child(BRONZE_DIR)

    backup_dir = BRONZE_DIR.parent / f"_backup_real_estate_{batch_id}"
    ensure_bronze_child(backup_dir)

    if backup_dir.exists():
        raise RuntimeError(f"Backup path đã tồn tại: {backup_dir}")

    had_previous_output = BRONZE_DIR.exists()
    if had_previous_output:
        BRONZE_DIR.rename(backup_dir)

    try:
        staging_dir.rename(BRONZE_DIR)
    except Exception:
        if had_previous_output and backup_dir.exists() and not BRONZE_DIR.exists():
            backup_dir.rename(BRONZE_DIR)
        raise

    if backup_dir.exists():
        remove_generated_directory(backup_dir)


def write_summary(summary, batch_id):
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    run_summary_path = SUMMARY_DIR / f"bronze_ingestion_{batch_id}.json"

    for summary_path in (run_summary_path, LATEST_SUMMARY_PATH):
        with summary_path.open("w", encoding="utf-8") as output_file:
            json.dump(summary, output_file, ensure_ascii=False, indent=2)
            output_file.write("\n")

    return run_summary_path


def main():
    started_clock = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    expected_ingested_at_micros = epoch_micros(started_at)
    ingested_at_text = started_at.strftime("%Y-%m-%d %H:%M:%S.%f")
    ingestion_date = started_at.date()
    batch_id = (
        "real_estate_"
        f"{started_at.strftime('%Y%m%dT%H%M%S%fZ')}_"
        f"{uuid.uuid4().hex[:8]}"
    )

    summary = {
        "pipeline": "raw_to_bronze_real_estate",
        "status": "RUNNING",
        "source_name": SOURCE_NAME,
        "batch_id": batch_id,
        "started_at_utc": started_at.isoformat(),
        "input_glob": "data/raw/real_estate/shard_*.parquet",
        "output_path": "data/bronze/real_estate",
        "compression": "snappy",
    }
    spark = None
    staging_dir = BRONZE_DIR.parent / f"_staging_real_estate_{batch_id}"

    try:
        configure_windows_runtime()
        raw_files = get_raw_files()
        input_uris = [path.absolute().as_uri() for path in raw_files]
        raw_size_bytes = sum(path.stat().st_size for path in raw_files)
        disk_usage = shutil.disk_usage(PROJECT_ROOT)
        required_free_bytes = max(MIN_FREE_SPACE_BYTES, raw_size_bytes * 2)

        if disk_usage.free < required_free_bytes:
            raise RuntimeError(
                "Không đủ dung lượng đĩa để ghi và kiểm tra Bronze. "
                f"Required={required_free_bytes}, available={disk_usage.free}"
            )

        ensure_bronze_child(staging_dir)
        if staging_dir.exists():
            raise RuntimeError(f"Staging path đã tồn tại: {staging_dir}")

        summary.update(
            {
                "raw_size_bytes": raw_size_bytes,
                "available_disk_bytes_before": disk_usage.free,
                "required_free_bytes": required_free_bytes,
                "previous_output_existed": BRONZE_DIR.exists(),
            }
        )

        print(f"[BRONZE] batch_id={batch_id}", flush=True)
        print(f"[BRONZE] input_files={len(raw_files)}", flush=True)
        print(f"[BRONZE] raw_size_bytes={raw_size_bytes}", flush=True)

        spark = create_spark_session()
        spark_version = spark.version
        spark_master = spark.sparkContext.master
        driver_max_heap_bytes = (
            spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()
        )
        parquet_vectored_io = (
            spark.sparkContext._jsc.hadoopConfiguration().get(
                "parquet.hadoop.vectored.io.enabled"
            )
        )

        if spark_master != SPARK_MASTER:
            raise RuntimeError(
                f"Spark master không đúng: expected={SPARK_MASTER}, "
                f"actual={spark_master}"
            )
        if driver_max_heap_bytes < MIN_DRIVER_HEAP_BYTES:
            raise RuntimeError(
                "Spark driver heap thấp hơn mức an toàn. "
                f"Minimum={MIN_DRIVER_HEAP_BYTES}, "
                f"actual={driver_max_heap_bytes}"
            )
        if parquet_vectored_io.lower() != "false":
            raise RuntimeError(
                "parquet.hadoop.vectored.io.enabled phải là false, "
                f"actual={parquet_vectored_io}"
            )
        print(f"[BRONZE] spark_version={spark_version}", flush=True)
        print(f"[BRONZE] spark_master={spark_master}", flush=True)
        print(
            f"[BRONZE] driver_max_heap_bytes={driver_max_heap_bytes}",
            flush=True,
        )

        raw_df = spark.read.parquet(*input_uris)
        validate_schema(
            schema_signature(raw_df),
            EXPECTED_RAW_SCHEMA,
            "Raw",
        )

        bronze_df = (
            raw_df.select(*[column for column, _ in EXPECTED_RAW_SCHEMA])
            .withColumn("source_name", F.lit(SOURCE_NAME))
            .withColumn(
                "source_file",
                F.substring_index(F.input_file_name(), "/", -1),
            )
            .withColumn(
                "ingested_at",
                F.to_timestamp(
                    F.lit(ingested_at_text),
                    "yyyy-MM-dd HH:mm:ss.SSSSSS",
                ),
            )
            .withColumn("ingestion_date", F.to_date("ingested_at"))
            .withColumn("batch_id", F.lit(batch_id))
        )

        validate_schema(
            schema_signature(bronze_df),
            EXPECTED_RAW_SCHEMA + EXPECTED_METADATA_SCHEMA,
            "Bronze trước khi ghi",
        )

        input_source_metrics = collect_source_metrics(bronze_df)
        input_source_counts = source_counts(input_source_metrics)
        input_source_fingerprints = source_fingerprints(input_source_metrics)
        input_rows = validate_source_counts(input_source_counts, "Raw")
        print(f"[BRONZE] input_rows={input_rows}", flush=True)

        print(f"[BRONZE] writing_staging={staging_dir}", flush=True)
        (
            bronze_df.write.mode("errorifexists")
            .option("compression", "snappy")
            .parquet(staging_dir.as_uri())
        )

        output_df = spark.read.parquet(staging_dir.as_uri())
        validate_schema(
            schema_signature(output_df),
            EXPECTED_RAW_SCHEMA + EXPECTED_METADATA_SCHEMA,
            "Bronze read-back",
        )

        output_source_metrics, stored_ingested_at = validate_output_metadata(
            output_df,
            batch_id,
            ingestion_date,
            expected_ingested_at_micros,
        )
        output_source_counts = source_counts(output_source_metrics)
        output_source_fingerprints = source_fingerprints(output_source_metrics)
        output_rows = validate_source_counts(output_source_counts, "Bronze")

        if output_source_fingerprints != input_source_fingerprints:
            raise RuntimeError(
                "Business data fingerprint changed between Raw and Bronze. "
                f"Input={input_source_fingerprints}, "
                f"output={output_source_fingerprints}"
            )

        success_marker = staging_dir / "_SUCCESS"
        part_files = sorted(staging_dir.glob("part-*.parquet"))
        if not success_marker.is_file():
            raise RuntimeError("Bronze output thiếu _SUCCESS")
        if not part_files:
            raise RuntimeError("Bronze output không có part Parquet")

        parquet_codecs = validate_parquet_compression(part_files)
        output_size_bytes = sum(path.stat().st_size for path in part_files)
        output_column_count = len(output_df.columns)

        stored_ingested_at = UTC_EPOCH + timedelta(
            microseconds=stored_ingested_at
        )

        # Release Spark file handles before promoting the validated directory.
        spark.stop()
        spark = None
        promote_staging(staging_dir, batch_id)

        final_part_files = sorted(BRONZE_DIR.glob("part-*.parquet"))
        if not (BRONZE_DIR / "_SUCCESS").is_file():
            raise RuntimeError("Promoted Bronze output thiếu _SUCCESS")
        if len(final_part_files) != len(part_files):
            raise RuntimeError(
                "Part-file count changed during staging promotion. "
                f"Before={len(part_files)}, after={len(final_part_files)}"
            )
        summary.update(
            {
                "status": "PASS",
                "spark_version": spark_version,
                "spark_master": spark_master,
                "driver_max_heap_bytes": driver_max_heap_bytes,
                "parquet_vectored_io": parquet_vectored_io,
                "input_file_count": len(raw_files),
                "input_rows": input_rows,
                "output_rows": output_rows,
                "business_column_count": len(EXPECTED_RAW_SCHEMA),
                "metadata_column_count": len(EXPECTED_METADATA_SCHEMA),
                "output_column_count": output_column_count,
                "input_rows_by_source_file": input_source_counts,
                "output_rows_by_source_file": output_source_counts,
                "input_business_fingerprints_by_source_file": (
                    input_source_fingerprints
                ),
                "output_business_fingerprints_by_source_file": (
                    output_source_fingerprints
                ),
                "stored_ingested_at_utc": stored_ingested_at.isoformat(),
                "ingestion_date": ingestion_date.isoformat(),
                "part_file_count": len(final_part_files),
                "output_size_bytes": output_size_bytes,
                "parquet_codecs": parquet_codecs,
                "validations": {
                    "disk_space_preflight": "PASS",
                    "runtime_memory_config": "PASS",
                    "raw_file_list": "PASS",
                    "raw_schema": "PASS",
                    "raw_row_count": "PASS",
                    "bronze_schema": "PASS",
                    "bronze_row_count": "PASS",
                    "per_source_row_count": "PASS",
                    "metadata_completeness": "PASS",
                    "metadata_batch_consistency": "PASS",
                    "business_fingerprint": "PASS",
                    "read_back": "PASS",
                    "success_marker": "PASS",
                    "parquet_compression": "PASS",
                    "staging_promotion": "PASS",
                },
            }
        )

        print(f"[BRONZE] output_rows={output_rows}", flush=True)
        print(f"[BRONZE] output_columns={output_column_count}", flush=True)
        print(f"[BRONZE] part_files={len(final_part_files)}", flush=True)
        print("[BRONZE] status=PASS", flush=True)
    except Exception as exc:
        summary["status"] = "FAIL"
        summary["error_type"] = type(exc).__name__
        summary["error_message"] = str(exc)
        print(f"[BRONZE] status=FAIL error={exc}", file=sys.stderr, flush=True)
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as stop_error:
                summary["spark_stop_warning"] = str(stop_error)

        if staging_dir.exists():
            try:
                remove_generated_directory(staging_dir)
            except Exception as cleanup_error:
                summary["staging_cleanup_warning"] = str(cleanup_error)

        completed_at = datetime.now(timezone.utc)
        summary["completed_at_utc"] = completed_at.isoformat()
        summary["duration_seconds"] = round(
            time.perf_counter() - started_clock,
            3,
        )
        try:
            run_summary_path = write_summary(summary, batch_id)
            print(
                f"[BRONZE] summary={run_summary_path.relative_to(PROJECT_ROOT)}",
                flush=True,
            )
        except Exception as summary_error:
            print(
                f"[BRONZE] failed_to_write_summary={summary_error}",
                file=sys.stderr,
                flush=True,
            )
            if summary["status"] == "PASS":
                raise


if __name__ == "__main__":
    main()
