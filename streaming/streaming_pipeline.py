"""Cumulative Olympic aggregates: Kafka + static MySQL -> MySQL history and Kafka."""
import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import uuid
from pyspark.sql import SparkSession, functions as F
from project_config import CHECKPOINT_ROOT, mysql_options, mysql_connection, kafka_options, sql_identifier
from streaming.transforms import GROUPS, clean_bio, parse_events, valid_events, enrich_events, aggregate_events


def ensure_table():
    table = sql_identifier(os.environ["OUTPUT_MYSQL_TABLE"])
    with mysql_connection("OUTPUT") as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS `{table}` (
                stream_id CHAR(32) NOT NULL, batch_id BIGINT NOT NULL, group_key CHAR(64) NOT NULL,
                sport VARCHAR(128), medal VARCHAR(32), sex VARCHAR(16), country_noc VARCHAR(16),
                avg_height DOUBLE NOT NULL, avg_weight DOUBLE NOT NULL, event_count BIGINT NOT NULL,
                `timestamp` DATETIME(6) NOT NULL,
                PRIMARY KEY (stream_id, batch_id, group_key)
            )""")
        connection.commit()
    return table


def make_sink(stream_id):
    table = ensure_table()

    def write_batch(batch_df, batch_id):
        batch_df.persist()
        try:
            count = batch_df.count()  # Consume the entire stateful batch, including an empty batch.
            if not count:
                print(f"BATCH {batch_id}: no changed groups", flush=True)
                return
            with mysql_connection("OUTPUT") as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT MIN(`timestamp`) AS ts FROM `{table}` WHERE stream_id=%s AND batch_id=%s",
                                   (stream_id, batch_id))
                    timestamp = cursor.fetchone()["ts"] or datetime.now(timezone.utc).replace(tzinfo=None)
            # Додати однаковий час розрахунку для запису в обидва приймачі.
            result = batch_df.withColumn("timestamp", F.lit(timestamp)).withColumn(
                "group_key", F.sha2(F.to_json(F.struct(*GROUPS), {"ignoreNullFields": "false"}), 256)
                ).withColumn("stream_id", F.lit(stream_id)).withColumn("batch_id", F.lit(batch_id)).persist()
            staging = f"_fp_stage_{stream_id}_{batch_id}"
            try:
                # Записати агрегати через JDBC і зберегти історію MySQL без повторів мікробатча.
                with mysql_connection("OUTPUT") as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"DROP TABLE IF EXISTS `{staging}`")
                        cursor.execute(f"CREATE TABLE `{staging}` LIKE `{table}`")
                result.write.format("jdbc").options(**mysql_options("OUTPUT"), dbtable=staging).mode("append").save()
                columns = result.columns
                names = ",".join(f"`{sql_identifier(name)}`" for name in columns)
                with mysql_connection("OUTPUT") as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"INSERT INTO `{table}` ({names}) SELECT {names} FROM `{staging}` "
                            "ON DUPLICATE KEY UPDATE avg_height=VALUES(avg_height), avg_weight=VALUES(avg_weight), "
                            "event_count=VALUES(event_count), `timestamp`=VALUES(`timestamp`)")
                    connection.commit()
                # Надіслати ті самі агрегати у вихідний Kafka-топік.
                key = F.concat_ws(":", "stream_id", F.col("batch_id").cast("string"), "group_key")
                result.select(key.alias("key"), F.to_json(F.struct(*result.columns),
                    {"ignoreNullFields": "false", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"})
                    .alias("value")).write.format("kafka").options(**kafka_options(),
                        topic=os.environ["KAFKA_OUTPUT_TOPIC"]).save()
                print(f"BATCH {batch_id}: {count} updated groups -> MySQL and Kafka at {timestamp.isoformat()}Z", flush=True)
                result.orderBy(*GROUPS).show(5, truncate=False)
            finally:
                result.unpersist()
                with mysql_connection("OUTPUT") as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"DROP TABLE IF EXISTS `{staging}`")
        finally:
            batch_df.unpersist()

    return write_batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--available-now", action="store_true", help="Drain available Kafka data, then exit")
    args = parser.parse_args()
    checkpoint = CHECKPOINT_ROOT / "olympic_aggregates"
    checkpoint.mkdir(parents=True, exist_ok=True)
    with (checkpoint / "pipeline.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        identity_path = checkpoint / "identity.json"
        config = {"source": mysql_options("SOURCE")["url"], "source_user": mysql_options("SOURCE")["user"],
                  "bootstrap": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "input": os.environ["KAFKA_INPUT_TOPIC"],
                  "output": os.environ["KAFKA_OUTPUT_TOPIC"], "output_db": mysql_options("OUTPUT")["url"],
                  "output_table": os.environ["OUTPUT_MYSQL_TABLE"], "version": 3}
        if identity_path.exists():
            identity = json.loads(identity_path.read_text())
            if identity["config"] != config:
                raise ValueError("Checkpoint configuration changed; use a new CHECKPOINT_ROOT")
        else:
            identity = {"stream_id": uuid.uuid4().hex, "config": config}
            identity_path.write_text(json.dumps(identity, indent=2))
        spark = SparkSession.builder.appName("olympic-streaming-aggregates").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        bio = None
        try:
            # Зчитати фізичні показники атлетів із MySQL через Spark JDBC.
            raw_bio = spark.read.format("jdbc").options(**mysql_options("SOURCE"), dbtable="athlete_bio").load()
            raw_bio.printSchema()
            bio = clean_bio(raw_bio).persist()
            print(f"BIO rows: before={raw_bio.count()}, valid={bio.count()}", flush=True)
            if bio.groupBy("athlete_id").count().filter("count > 1").limit(1).count():
                raise ValueError("Duplicate athlete_id in cleaned bio; resolve before joining")
            # Прочитати потік результатів із Kafka та розібрати повідомлення.
            raw = spark.readStream.format("kafka").options(**kafka_options(),
                subscribe=os.environ["KAFKA_INPUT_TOPIC"], startingOffsets="earliest",
                failOnDataLoss="true", maxOffsetsPerTrigger="100000").load()
            events = valid_events(parse_events(raw))
            aggregates = aggregate_events(enrich_events(events, bio))
            # Обробляти оновлення накопичувальних груп через foreachBatch із checkpoint.
            writer = aggregates.writeStream.outputMode("update").option("checkpointLocation", str(checkpoint)
                ).foreachBatch(make_sink(identity["stream_id"]))
            writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
            query = writer.start()
            query.awaitTermination()
            progress = query.recentProgress
            artifact_dir = Path("/opt/airflow/project/artifacts/streaming")
            artifact_dir.mkdir(parents=True, exist_ok=True)
            record = {"stream_id": identity["stream_id"], "progress": progress}
            (artifact_dir / "last_run.json").write_text(json.dumps(record, indent=2, default=str))
            print("STREAM COMPLETE " + json.dumps({"stream_id": identity["stream_id"],
                "input_rows_this_run": sum(p["numInputRows"] for p in progress)}), flush=True)
        finally:
            if bio is not None:
                bio.unpersist()
            spark.stop()


if __name__ == "__main__":
    main()
