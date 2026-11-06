"""Clean every text column, remove full-row duplicates and persist silver tables."""
from project_config import DATA_ROOT
from batch.common import TABLES, clean_strings, create_spark, save_parquet, save_metrics


def main():
    spark = create_spark("olympic-bronze-to-silver")
    metrics = {}
    try:
        for table in TABLES:
            raw = spark.read.parquet(str(DATA_ROOT / "bronze" / table))
            # Прибрати повні дублікати після очищення тексту.
            silver = clean_strings(raw).dropDuplicates().persist()
            try:
                before = raw.count()
                after = save_parquet(silver, "silver", table)
                metrics[table] = {"before": before, "after": after, "duplicates_removed": before - after}
            finally:
                silver.unpersist()
        save_metrics("silver", metrics)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
