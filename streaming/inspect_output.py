"""Print real sink results and optionally verify every group against independent Python calculations."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from pyspark.sql import SparkSession
from project_config import CHECKPOINT_ROOT, mysql_connection, kafka_options, sql_identifier

ARTIFACTS = Path("/opt/airflow/project/artifacts/streaming")


def json_value(value):
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds") + "Z"
    raise TypeError(type(value).__name__)


def print_table(rows):
    columns = ["sport", "medal", "sex", "country_noc", "avg_height", "avg_weight", "event_count", "timestamp"]
    values = [[f"{row[c]:.6f}" if isinstance(row[c], float) else str(row[c]) for c in columns] for row in rows]
    widths = [max(len(c), *(len(row[i]) for row in values)) for i, c in enumerate(columns)]
    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    def line(items):
        return "| " + " | ".join(v.ljust(w) for v, w in zip(items, widths)) + " |"
    print(border)
    print(line(columns))
    print(border)
    for row in values:
        print(line(row))
    print(border, flush=True)


def independent_aggregates():
    """Read MySQL directly: deliberately use neither Spark nor its transformation helpers."""
    bio = {}
    with mysql_connection("SOURCE") as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT athlete_id, height, weight, sex, country_noc FROM athlete_bio")
            total_bio = 0
            for row in cursor:
                total_bio += 1
                try:
                    height, weight = float(row["height"]), float(row["weight"])
                except (TypeError, ValueError):
                    continue
                if not (math.isfinite(height) and math.isfinite(weight)):
                    continue
                athlete_id = row["athlete_id"]
                if athlete_id in bio:
                    raise AssertionError("Duplicate clean bio key")
                bio[athlete_id] = (height, weight, row["sex"], row["country_noc"])
            cursor.execute("SELECT athlete_id, sport, medal FROM athlete_event_results")
            groups = defaultdict(lambda: [0.0, 0.0, 0])
            total_events = joined = 0
            for row in cursor:
                total_events += 1
                athlete = bio.get(row["athlete_id"])
                if athlete is None or not row["sport"] or not row["sport"].strip():
                    continue
                height, weight, sex, country = athlete
                medal = (row["medal"] or "").strip() or "No medal"
                key = (row["sport"], medal, sex, country)
                values = groups[key]
                values[0] += height
                values[1] += weight
                values[2] += 1
                joined += 1
    return groups, {"bio_source_rows": total_bio, "bio_valid_rows": len(bio),
                    "source_events": total_events, "joined_events": joined}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--mysql-only", action="store_true", help="Print MySQL results without starting Spark")
    parser.add_argument("--kafka-only", action="store_true", help="Pretty-print two actual Kafka messages")
    args = parser.parse_args()
    if args.mysql_only and (args.kafka_only or args.verify):
        parser.error("--mysql-only cannot be combined with --kafka-only or --verify")
    stream_id = json.loads((CHECKPOINT_ROOT / "olympic_aggregates/identity.json").read_text())["stream_id"]
    table = sql_identifier(os.environ["OUTPUT_MYSQL_TABLE"])
    with mysql_connection("OUTPUT") as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM `{table}` WHERE stream_id=%s ORDER BY batch_id, group_key", (stream_id,))
            history = list(cursor.fetchall())
    if not history:
        raise ValueError("No streaming results: run make stream first")
    latest = {row["group_key"]: row for row in history}
    ordered = sorted(latest.values(), key=lambda r: (r["sport"], r["sex"] or "", r["country_noc"] or "", r["medal"]))
    sample = []
    for medal in ("No medal", "Gold", "Silver", "Bronze"):
        matching = next((r for r in ordered if r["medal"] == medal), None)
        if matching is not None:
            sample.append(matching)
    for row in ordered:
        if len(sample) == 6:
            break
        if row not in sample:
            sample.append(row)
    captured = datetime.now(timezone.utc).isoformat()
    if not args.kafka_only:
        print(f"MYSQL {os.environ['OUTPUT_MYSQL_DATABASE']}.{table}")
        print(f"stream_id={stream_id} | captured={captured}")
        print(f"SELECT * FROM `{table}` WHERE stream_id='{stream_id}' ORDER BY batch_id, group_key;")
        print(f"History rows: {len(history)}; latest groups: {len(latest)}; sample of latest groups:")
        print_table(sample)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "mysql_results.json").write_text(json.dumps({"captured_at": captured,
        "table": f"{os.environ['OUTPUT_MYSQL_DATABASE']}.{table}", "history_rows": len(history),
        "latest_groups": len(latest), "sample": sample}, ensure_ascii=False, indent=2, default=json_value), encoding="utf-8")
    if args.mysql_only:
        return
    spark = SparkSession.builder.appName("olympic-inspect-output").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        topic = os.environ["KAFKA_OUTPUT_TOPIC"]
        messages = spark.read.format("kafka").options(**kafka_options(), subscribe=topic,
            startingOffsets="earliest", endingOffsets="latest").load().selectExpr(
                "CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value", "partition", "offset")
        records = []
        for row in messages.toLocalIterator():
            payload = json.loads(row.value)
            if payload.get("stream_id") == stream_id:
                records.append({"key": row.key, "value": payload, "partition": row.partition, "offset": row.offset})
        by_key = {}
        for record in records:
            key = record["key"]
            if key in by_key and by_key[key]["value"] != record["value"]:
                raise AssertionError(f"Conflicting payload for Kafka key {key}")
            by_key[key] = record
        sample_keys = [f"{stream_id}:{r['batch_id']}:{r['group_key']}" for r in sample]
        kafka_sample = [by_key[key] for key in sample_keys]
        print(f"KAFKA topic={topic} | captured={captured} | messages={len(records)} | distinct_keys={len(by_key)}")
        for record in (kafka_sample[:2] if args.kafka_only else kafka_sample):
            print(json.dumps(record, ensure_ascii=False, indent=2 if args.kafka_only else None), flush=True)
        (ARTIFACTS / "kafka_output.json").write_text(json.dumps({"captured_at": captured, "topic": topic,
            "messages": len(records), "distinct_keys": len(by_key), "sample": kafka_sample},
            ensure_ascii=False, indent=2), encoding="utf-8")
        if args.verify:
            expected, counts = independent_aggregates()
            actual = {(r["sport"], r["medal"], r["sex"], r["country_noc"]): r for r in latest.values()}
            assert set(expected) == set(actual), "Group keys differ from independent source calculation"
            for key, (height_sum, weight_sum, count) in expected.items():
                row = actual[key]
                assert row["event_count"] == count, (key, "event count")
                assert math.isclose(row["avg_height"], height_sum / count, rel_tol=1e-10, abs_tol=1e-8), key
                assert math.isclose(row["avg_weight"], weight_sum / count, rel_tol=1e-10, abs_tol=1e-8), key
            expected_keys = {f"{stream_id}:{r['batch_id']}:{r['group_key']}" for r in history}
            assert expected_keys == set(by_key), "Kafka and MySQL histories differ"
            for row in history:
                payload = by_key[f"{stream_id}:{row['batch_id']}:{row['group_key']}"]["value"]
                for field in ("sport", "medal", "sex", "country_noc", "event_count", "stream_id", "batch_id", "group_key"):
                    assert payload[field] == row[field], (field, row["group_key"])
                for field in ("avg_height", "avg_weight"):
                    assert math.isclose(payload[field], row[field], rel_tol=1e-12), field
                assert datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00")).replace(tzinfo=None) == row["timestamp"]
            input_rows = spark.read.format("kafka").options(**kafka_options(),
                subscribe=os.environ["KAFKA_INPUT_TOPIC"], startingOffsets="earliest", endingOffsets="latest").load().count()
            assert input_rows == counts["source_events"], "Input topic contains missing/replayed source events"
            summary = {"verified_at": captured, "stream_id": stream_id, **counts,
                "input_kafka_rows": input_rows, "groups": len(expected), "history_rows": len(history),
                "kafka_messages": len(records), "no_medal_groups": sum(k[1] == "No medal" for k in expected),
                "batches": sorted({r["batch_id"] for r in history}), "status": "PASS"}
            (ARTIFACTS / "verification.json").write_text(json.dumps(summary, indent=2))
            print("VERIFIED " + json.dumps(summary), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
