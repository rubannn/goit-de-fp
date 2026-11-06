"""Shared paths, text cleaning and Parquet output checks."""
import json
import os
from pathlib import Path
import re
import unicodedata
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
from project_config import DATA_ROOT

TABLES = ("athlete_bio", "athlete_event_results")
ARTIFACTS = Path("/opt/airflow/project/artifacts/batch")


def create_spark(name):
    spark = SparkSession.builder.appName(name).config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_text(value):
    if value is None:
        return None
    normalized = unicodedata.normalize("NFKC", value)
    cleaned = re.sub(r"[^\w\s,.\"'()-]", "", normalized)
    return re.sub(r"\s+", " ", cleaned).strip() or None


def clean_strings(df):
    # Очистити кожну текстову колонку, зберігаючи типи числових полів.
    clean = F.udf(clean_text, StringType())
    return df.select(*(clean(F.col(field.name)).alias(field.name)
        if isinstance(field.dataType, StringType) else F.col(field.name) for field in df.schema.fields))


def save_parquet(df, layer, table):
    path = DATA_ROOT / layer / table
    count = df.count()
    df.write.mode("overwrite").parquet(str(path))
    saved = df.sparkSession.read.parquet(str(path))
    if saved.count() != count or saved.schema.simpleString() != df.schema.simpleString():
        raise AssertionError(f"Parquet round-trip mismatch: {path}")
    print(f"FINAL DATAFRAME {layer}/{table} | rows={count} | path={path}", flush=True)
    saved.show(6, truncate=10 if layer != "gold" else False)
    return count


def save_metrics(stage, values):
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    record = {"data_root": str(DATA_ROOT), "airflow_run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID"), **values}
    (ARTIFACTS / f"{stage}.json").write_text(json.dumps(record, indent=2, default=str))
