"""Join cleaned athletes and results; calculate final analytical groups."""
from pyspark.sql import functions as F
from project_config import DATA_ROOT
from batch.common import create_spark, save_parquet, save_metrics

GROUPS = ["sport", "medal", "sex", "country_noc"]


def prepare_bio(df):
    bio = df.selectExpr("try_cast(athlete_id AS BIGINT) AS athlete_id", "sex", "country_noc",
                        "try_cast(height AS DOUBLE) AS height", "try_cast(weight AS DOUBLE) AS weight")
    valid = F.col("athlete_id").isNotNull()
    for field in ("height", "weight"):
        valid = valid & F.col(field).isNotNull() & ~F.isnan(field) & (F.abs(F.col(field)) < float("inf"))
    return bio.filter(valid)


def build_gold(bio, events):
    # Об'єднати таблиці за ключем; країну взяти з біографії, відсутню медаль зберегти окремо.
    events = events.withColumn("athlete_id", F.expr("try_cast(athlete_id AS BIGINT)"))
    joined = events.alias("e").join(bio.alias("b"), "athlete_id", "inner").select(
        F.col("e.sport").alias("sport"), F.coalesce(F.col("e.medal"), F.lit("No medal")).alias("medal"),
        F.col("b.sex").alias("sex"), F.col("b.country_noc").alias("country_noc"),
        F.col("b.height"), F.col("b.weight")).filter(F.col("sport").isNotNull())
    # Розрахувати середні за чотирма ознаками та додати UTC-час виконання.
    return joined.groupBy(*GROUPS).agg(F.avg("height").alias("avg_height"),
        F.avg("weight").alias("avg_weight"), F.count("*").alias("event_count")).withColumn("timestamp", F.current_timestamp())


def main():
    spark = create_spark("olympic-silver-to-gold")
    bio = None
    gold = None
    try:
        raw = spark.read.parquet(str(DATA_ROOT / "silver/athlete_bio"))
        events = spark.read.parquet(str(DATA_ROOT / "silver/athlete_event_results"))
        bio = prepare_bio(raw).persist()
        if bio.groupBy("athlete_id").count().filter("count > 1").limit(1).count():
            raise ValueError("Duplicate athlete_id in cleaned bio")
        gold = build_gold(bio, events).persist()
        count = save_parquet(gold, "gold", "avg_stats")
        save_metrics("gold", {"groups": count, "valid_bio_rows": bio.count(),
            "joined_events": gold.agg(F.sum("event_count")).first()[0]})
    finally:
        if gold is not None:
            gold.unpersist()
        if bio is not None:
            bio.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
