import os
import shutil
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Keep a possible Windows SUBST drive instead of resolving it back to the
# Unicode project path, which Hadoop's local filesystem cannot open reliably.
PROJECT_ROOT = Path(__file__).absolute().parents[2]
RAW_SAMPLE_PATH = (
    PROJECT_ROOT / "data" / "raw" / "real_estate" / "shard_0000.parquet"
)
SMOKE_OUTPUT_PATH = PROJECT_ROOT / "data" / "bronze" / "_smoke_test"

EXPECTED_COLUMNS = [
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

BARE_LOCAL_FS_CLASS = "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"


def main():
    # Spark's Windows batch launcher does not quote PYSPARK_PYTHON correctly.
    # Put the active environment first on PATH and pass an ASCII executable name.
    python_dir = str(Path(sys.executable).parent)
    os.environ["PATH"] = python_dir + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = "python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python.exe"
    os.environ["SPARK_HOME"] = str(Path(pyspark.__file__).parent)

    if not RAW_SAMPLE_PATH.is_file():
        raise FileNotFoundError(f"Không tìm thấy raw sample: {RAW_SAMPLE_PATH}")

    if SMOKE_OUTPUT_PATH.exists():
        shutil.rmtree(SMOKE_OUTPUT_PATH)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("BronzeSmokeTest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.file.impl", BARE_LOCAL_FS_CLASS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        worker_versions = (
            spark.sparkContext.parallelize([1, 2], 2)
            .map(lambda value: (sys.version_info[:2], value))
            .collect()
        )

        source_df = spark.read.parquet(RAW_SAMPLE_PATH.as_uri())
        row_count = source_df.count()
        source_uri = source_df.select(
            F.input_file_name().alias("source_uri")
        ).first()["source_uri"]

        if row_count != 350_075:
            raise RuntimeError(
                f"Sai row count shard_0000: expected=350075, actual={row_count}"
            )
        if source_df.columns != EXPECTED_COLUMNS:
            raise RuntimeError("Schema/cột của shard_0000 không đúng specification")
        source_file = source_uri.rsplit("/", 1)[-1]
        if source_file != RAW_SAMPLE_PATH.name:
            raise RuntimeError(
                "Spark trả về sai source file: "
                f"expected={RAW_SAMPLE_PATH.name}, actual={source_file}"
            )

        source_df.limit(1_000).write.mode("overwrite").option(
            "compression", "snappy"
        ).parquet(SMOKE_OUTPUT_PATH.as_uri())
        smoke_output_rows = spark.read.parquet(
            SMOKE_OUTPUT_PATH.as_uri()
        ).count()
        if smoke_output_rows != 1_000:
            raise RuntimeError(
                "Sai row count khi ghi/đọc lại smoke output: "
                f"expected=1000, actual={smoke_output_rows}"
            )

        print("spark_version:", spark.version)
        print("spark_master:", spark.sparkContext.master)
        print(
            "java_version:",
            spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
        )
        print("python_workers:", worker_versions)
        print("sample_rows:", row_count)
        print("sample_columns:", len(source_df.columns))
        print("source_uri:", source_uri)
        print("smoke_output_rows:", smoke_output_rows)
        print("SMOKE_TEST: PASS")
    finally:
        spark.stop()
        if SMOKE_OUTPUT_PATH.exists():
            shutil.rmtree(SMOKE_OUTPUT_PATH)


if __name__ == "__main__":
    main()
