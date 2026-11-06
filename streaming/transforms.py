"""Pure DataFrame transformations shared by the streaming job and its checks."""
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

GROUPS = ["sport", "medal", "sex", "country_noc"]
EVENT_SCHEMA = StructType([
    StructField(name, LongType() if name == "athlete_id" else StringType(), True)
    for name in ("edition", "edition_id", "country_noc", "sport", "event", "result_id",
                 "athlete", "athlete_id", "pos", "medal", "isTeamSport", "_corrupt_record")
])


def clean_bio(raw):
    # Залишити атлетів із числовими, скінченними значеннями зросту та ваги.
    for name in ("height", "weight"):
        raw = raw.withColumn(name, F.regexp_replace(F.col(name), r"(?U)^\s+|\s+$", ""))
    numeric = raw.selectExpr("try_cast(athlete_id AS BIGINT) AS athlete_id",
        "try_cast(trim(height) AS DOUBLE) AS height", "try_cast(trim(weight) AS DOUBLE) AS weight",
        "sex", "country_noc")
    valid = F.col("athlete_id").isNotNull()
    for name in ("height", "weight"):
        valid = valid & F.col(name).isNotNull() & ~F.isnan(name) & (F.abs(F.col(name)) < float("inf"))
    return numeric.filter(valid)


def parse_events(raw):
    # Розгорнути JSON-повідомлення в окремі колонки за явною схемою.
    return raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}).alias("event")).select("event.*")


def valid_events(parsed):
    # Відкинути пошкоджені повідомлення та записи без ключа чи виду спорту.
    return parsed.filter(F.col("_corrupt_record").isNull() & F.col("athlete_id").isNotNull()
        & F.col("sport").isNotNull() & (F.trim("sport") != ""))


def enrich_events(events, bio):
    # Об'єднати виступи з біографіями; країну взяти з даних атлета.
    return events.alias("e").join(bio.alias("b"), "athlete_id", "inner").select(
        F.col("e.sport").alias("sport"),
        F.when(F.col("e.medal").isNull() | (F.trim("e.medal") == ""), "No medal")
            .otherwise(F.trim("e.medal")).alias("medal"),
        F.col("b.sex").alias("sex"), F.col("b.country_noc").alias("country_noc"),
        F.col("b.height"), F.col("b.weight"))


def aggregate_events(enriched):
    # Обчислити накопичувальні середні для спорту, медалі, статі та країни.
    return enriched.groupBy(*GROUPS).agg(F.avg("height").alias("avg_height"),
        F.avg("weight").alias("avg_weight"), F.count("*").alias("event_count"))
