"""Verify all Data Lake layers against an independent CSV calculation."""
from collections import defaultdict
import csv
import hashlib
import json
import math
import unicodedata
from pyspark.sql.types import LongType, StringType
from project_config import DATA_ROOT
from batch.common import ARTIFACTS, TABLES, clean_strings, clean_text, create_spark


def reference_text(value):
    if value is None:
        return None
    value = unicodedata.normalize("NFKC", value)
    allowed = set("_,.\"'()-")
    value = "".join(c for c in value if c.isalnum() or c.isspace() or c in allowed)
    return " ".join(value.split()) or None


def check_fixtures(spark):
    assert clean_text(None) is None
    assert clean_text("  \u00a0Anna\t  Smith! ") == "Anna Smith"
    assert clean_text("85-87") == "85-87"
    fixture = spark.createDataFrame([(1, " \u00a0\uff21nna ", "85-87", None),
        (1, "Anna", "85-87", None), (2, "Ivan!", "72.5", "note")],
        "athlete_id long, name string, weight string, notes string")
    cleaned = clean_strings(fixture).dropDuplicates()
    rows = {r.athlete_id: r for r in cleaned.collect()}
    assert len(rows) == 2 and rows[1].name == "Anna" and rows[1].notes is None
    assert rows[1].weight == "85-87" and rows[2].name == "Ivan"
    assert isinstance(cleaned.schema["athlete_id"].dataType, LongType)
    print("PASS text UDF: Unicode/whitespace, punctuation, NULL, numeric types, ranges and deduplication", flush=True)


def main():
    spark = create_spark("olympic-batch-verification")
    try:
        check_fixtures(spark)
        bio = {}
        expected_groups = defaultdict(lambda: [0.0, 0.0, 0])
        statistics = {}
        for table in TABLES:
            bronze = spark.read.parquet(str(DATA_ROOT / "bronze" / table))
            silver = spark.read.parquet(str(DATA_ROOT / "silver" / table))
            with (DATA_ROOT / "landing" / f"{table}.csv").open(encoding="utf-8-sig", newline="") as source:
                reader = csv.DictReader(source)
                columns = reader.fieldnames
                count = 0
                expected = set()
                for row in reader:
                    count += 1
                    expected.add(tuple(int(row[c]) if c == "athlete_id" and row[c] else
                        None if c == "athlete_id" else reference_text(row[c]) for c in columns))
            assert bronze.columns == columns
            assert isinstance(bronze.schema["athlete_id"].dataType, LongType)
            assert all(isinstance(field.dataType, StringType) for field in bronze.schema.fields if field.name != "athlete_id")
            assert bronze.count() == count
            actual = {tuple(row[c] for c in columns) for row in silver.toLocalIterator()}
            silver_count = silver.count()
            assert silver_count == len(actual) == len(expected), f"Duplicate or missing rows: {table}"
            assert actual == expected, f"Silver differs from independent cleaning: {table}"
            statistics[table] = {"bronze_rows": count, "silver_rows": silver_count, "duplicates_removed": count - silver_count}
            del actual
            for values in expected:
                row = dict(zip(columns, values))
                if table == "athlete_bio":
                    try:
                        height, weight = float(row["height"]), float(row["weight"])
                    except (ValueError, TypeError):
                        continue
                    if not (math.isfinite(height) and math.isfinite(weight)):
                        continue
                    assert row["athlete_id"] not in bio, "Conflicting athlete bio keys"
                    bio[row["athlete_id"]] = (height, weight, row["sex"], row["country_noc"])
                else:
                    athlete = bio.get(row["athlete_id"])
                    if athlete is None or row["sport"] is None:
                        continue
                    height, weight, sex, country = athlete
                    key = (row["sport"], row["medal"] or "No medal", sex, country)
                    group = expected_groups[key]
                    group[0] += height
                    group[1] += weight
                    group[2] += 1
            print(f"PASS {table}: bronze={count}; silver={silver_count}; full-row values match independent CSV calculation", flush=True)
        gold = spark.read.parquet(str(DATA_ROOT / "gold/avg_stats"))
        actual_gold = {(r.sport, r.medal, r.sex, r.country_noc): r for r in gold.collect()}
        assert gold.count() == len(actual_gold) == len(expected_groups)
        assert set(actual_gold) == set(expected_groups)
        timestamps = set()
        canonical = []
        for key, (height_sum, weight_sum, count) in expected_groups.items():
            row = actual_gold[key]
            assert row.event_count == count, (key, "event_count")
            assert math.isclose(row.avg_height, height_sum / count, rel_tol=1e-10, abs_tol=1e-8), (key, "avg_height")
            assert math.isclose(row.avg_weight, weight_sum / count, rel_tol=1e-10, abs_tol=1e-8), (key, "avg_weight")
            assert row.timestamp is not None
            timestamps.add(row.timestamp)
            canonical.append([*key, round(row.avg_height, 6), round(row.avg_weight, 6), row.event_count])
        assert len(timestamps) == 1, "Gold must contain one calculation timestamp"
        canonical.sort(key=lambda row: json.dumps(row, ensure_ascii=False))
        fingerprint = hashlib.sha256(json.dumps(canonical, ensure_ascii=False).encode()).hexdigest()
        summary = {"status": "PASS", "data_root": str(DATA_ROOT), "tables": statistics,
            "valid_bio_rows": len(bio), "joined_events": sum(v[2] for v in expected_groups.values()),
            "groups": len(actual_gold), "no_medal_groups": sum(k[1] == "No medal" for k in actual_gold),
            "timestamp_utc": next(iter(timestamps)).isoformat() + "Z", "content_sha256": fingerprint}
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        name = "verification_local.json" if DATA_ROOT.name == "_local_batch" else "verification.json"
        (ARTIFACTS / name).write_text(json.dumps(summary, indent=2))
        print("VERIFIED BATCH " + json.dumps(summary), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
