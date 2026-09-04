from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).absolute().parents[2]
DATA_PATH = (
    PROJECT_ROOT / "data" / "raw" / "real_estate" / "shard_*.parquet"
).as_posix()
OUTPUT_DIR = PROJECT_ROOT / "docs" / "profiling"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    try:
        provinces = con.execute(
            f"""
            SELECT
                province_name,
                COUNT(*) AS records
            FROM read_parquet('{DATA_PATH}')
            WHERE province_name IS NOT NULL
            GROUP BY province_name
            ORDER BY records DESC, province_name
            """
        ).df()

        districts = con.execute(
            f"""
            SELECT DISTINCT
                province_name,
                district_name
            FROM read_parquet('{DATA_PATH}')
            WHERE district_name IS NOT NULL
            ORDER BY province_name, district_name
            """
        ).df()

        wards = con.execute(
            f"""
            SELECT DISTINCT
                province_name,
                district_name,
                ward_name
            FROM read_parquet('{DATA_PATH}')
            WHERE ward_name IS NOT NULL
            ORDER BY province_name, district_name, ward_name
            """
        ).df()

        provinces.to_csv(
            OUTPUT_DIR / "provinces.csv",
            index=False,
        )
        districts.to_csv(
            OUTPUT_DIR / "district_distinct.csv",
            index=False,
        )
        wards.to_csv(
            OUTPUT_DIR / "ward_distinct.csv",
            index=False,
        )

        print("province:", len(provinces))
        print("district:", len(districts))
        print("ward:", len(wards))
        print("Output:", OUTPUT_DIR)
        print("Done")
    finally:
        con.close()


if __name__ == "__main__":
    main()
