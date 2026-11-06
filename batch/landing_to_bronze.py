"""Download original Olympic CSV files and persist both bronze tables."""
import argparse
import csv
import requests
from pyspark.sql.types import LongType, StringType, StructField, StructType
from project_config import DATA_ROOT
from batch.common import TABLES, create_spark, save_parquet, save_metrics


def download(table, base_url, refresh=False):
    directory = DATA_ROOT / "landing"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{table}.csv"
    if refresh or not target.exists():
        url = base_url.rstrip("/") + f"/{table}.csv"
        print(f"Downloading original CSV: {url}", flush=True)
        temporary = target.with_suffix(".csv.part")
        with requests.get(url, stream=True, timeout=(15, 120)) as response:
            response.raise_for_status()
            with temporary.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    output.write(chunk)
        temporary.replace(target)
    with target.open(encoding="utf-8-sig", newline="") as source:
        header = next(csv.reader(source))
    if "athlete_id" not in header or len(header) != len(set(header)):
        raise ValueError(f"Unexpected CSV header: {table}")
    return target, header


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--base-url", default="https://ftp.goit.study/neoversity/")
    args = parser.parse_args()
    spark = create_spark("olympic-landing-to-bronze")
    metrics = {}
    try:
        for table in TABLES:
            # Зберегти оригінальний CSV і прочитати його з явними типами та правилами лапок.
            path, header = download(table, args.base_url, args.refresh)
            schema = StructType([StructField(name, LongType() if name == "athlete_id" else StringType()) for name in header])
            df = spark.read.options(header=True, multiLine=True, quote='"', escape='"',
                mode="FAILFAST", enforceSchema=False, encoding="UTF-8").schema(schema).csv(str(path)).persist()
            try:
                metrics[table] = {"rows": save_parquet(df, "bronze", table), "schema": df.schema.simpleString()}
            finally:
                df.unpersist()
        save_metrics("bronze", metrics)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
