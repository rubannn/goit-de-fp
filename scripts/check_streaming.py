"""Meaningful edge-case and retry checks; uses a dedicated scratch table and check topic."""
import json
import math
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import patch
from pyspark.sql import SparkSession
from project_config import mysql_connection
from streaming.transforms import clean_bio, parse_events, valid_events, enrich_events, aggregate_events
from streaming.streaming_pipeline import make_sink


def main():
    spark = SparkSession.builder.appName("olympic-streaming-checks").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    table = "_fp_test_" + uuid.uuid4().hex[:16]
    os.environ["OUTPUT_MYSQL_TABLE"] = table
    os.environ["KAFKA_OUTPUT_TOPIC"] = "environment_check"
    try:
        bio = spark.createDataFrame([
            ("1", "180", "80", "Female", "UKR"), ("2", "bad", "80", "Female", "UKR"),
            ("3", None, "80", "Female", "UKR"), ("4", "180", "NaN", "Female", "UKR"),
            ("5", "inf", "80", "Female", "UKR"), ("6", "", "80", "Female", "UKR"),
            ("7", "180", "70-75", "Female", "UKR"), ("8", "170", "65.5", "Female", "UKR"),
            ("9", "173", "56\u00a0", "Male", "HUN"), ("10", "177\u202f", "72", "Male", "HUN")
        ], "athlete_id string, height string, weight string, sex string, country_noc string")
        cleaned = clean_bio(bio)
        assert {r.athlete_id for r in cleaned.collect()} == {1, 8, 9, 10}
        payloads = [json.dumps({"athlete_id": athlete, "sport": "Running", "medal": medal,
                               "country_noc": "USA"}) for athlete, medal in
                    [(1, "Gold"), (1, "Gold"), (8, "Gold"), (8, None), (999, "Gold")]]
        payloads += ['{"', '{"sport":"Running"}', '{"athlete_id":"bad","sport":"Running"}',
                     '{"athlete_id":1,"sport":""}']
        raw = spark.createDataFrame([(p,) for p in payloads], "value string")
        parsed = parse_events(raw)
        assert valid_events(parsed).count() == 5
        enriched = enrich_events(valid_events(parsed), cleaned)
        assert enriched.count() == 4
        assert {r.country_noc for r in enriched.collect()} == {"UKR"}
        results = aggregate_events(enriched)
        groups = {r.medal: r for r in results.collect()}
        assert set(groups) == {"Gold", "No medal"}
        assert groups["Gold"].event_count == 3
        assert math.isclose(groups["Gold"].avg_height, 530 / 3)
        assert math.isclose(groups["Gold"].avg_weight, 225.5 / 3)
        print("PASS cleaning, invalid JSON/keys, unmatched keys, bio country, no medal and averages", flush=True)
        sink = make_sink(uuid.uuid4().hex)
        fixed_time = datetime.now(timezone.utc).replace(microsecond=123456)
        with patch("streaming.streaming_pipeline.kafka_options", side_effect=RuntimeError("injected Kafka failure")), \
             patch("streaming.streaming_pipeline.datetime") as clock:
            clock.now.return_value = fixed_time
            try:
                sink(results, 0)
            except RuntimeError as error:
                assert str(error) == "injected Kafka failure"
            else:
                raise AssertionError("Injected sink failure was swallowed")
        with mysql_connection("OUTPUT") as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM `{table}` ORDER BY group_key")
                before = cursor.fetchall()
                assert len(before) == 2
                assert all(row["timestamp"] == fixed_time.replace(tzinfo=None) for row in before), "Lost timestamp precision"
        sink(results, 0)
        with mysql_connection("OUTPUT") as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM `{table}` ORDER BY group_key")
                assert before == cursor.fetchall(), "Retry changed rows or timestamps"
        sink(results.limit(0), 1)
        print("PASS failure after MySQL commit, retry without MySQL duplicates, microsecond timestamps and empty batch", flush=True)
    finally:
        with mysql_connection("OUTPUT") as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
        spark.stop()


if __name__ == "__main__":
    main()
