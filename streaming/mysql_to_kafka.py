"""Publish one source snapshot to Kafka. An explicit flag is required to replay it."""
import argparse
import fcntl
import hashlib
import json
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from project_config import CHECKPOINT_ROOT, kafka_options, mysql_options


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Replay source (adds duplicate Kafka events)")
    args = parser.parse_args()
    topic = os.environ["KAFKA_INPUT_TOPIC"]
    identity = hashlib.sha256((os.environ["KAFKA_BOOTSTRAP_SERVERS"] + topic
        + mysql_options("SOURCE")["url"]).encode()).hexdigest()[:16]
    CHECKPOINT_ROOT.mkdir(parents=True, exist_ok=True)
    marker = CHECKPOINT_ROOT / f"producer_{identity}.json"
    with marker.with_suffix(".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if marker.exists() and not args.force:
            print(f"Source already published: {marker.read_text()}", flush=True)
            return
        spark = SparkSession.builder.appName("olympic-mysql-to-kafka").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        try:
            # Прочитати результати змагань із MySQL та опублікувати їх як JSON у Kafka.
            source = spark.read.format("jdbc").options(**mysql_options("SOURCE"),
                dbtable="athlete_event_results").load().persist()
            count = source.count()
            if not count:
                raise ValueError("athlete_event_results is empty")
            payload = F.to_json(F.struct(*source.columns), {"ignoreNullFields": "false"})
            source.select(F.sha2(payload, 256).alias("key"), payload.alias("value")).write.format(
                "kafka").options(**kafka_options(), topic=topic).save()
            summary = {"topic": topic, "rows": count, "published_at": datetime.now(timezone.utc).isoformat()}
            temporary = marker.with_suffix(".tmp")
            temporary.write_text(json.dumps(summary, indent=2))
            temporary.replace(marker)
            print("PUBLISHED " + json.dumps(summary), flush=True)
            source.unpersist()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
