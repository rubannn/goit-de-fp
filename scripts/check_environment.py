"""Real Spark/JDBC/Kafka/storage environment smoke check."""
import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from project_config import DATA_ROOT, CHECKPOINT_ROOT, mysql_options, kafka_options


def main():
    spark = SparkSession.builder.appName("goit-fp-environment-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    token = uuid.uuid4().hex
    try:
        assert spark.range(10).count() == 10
        # Also verifies that Python workers can execute user functions.
        assert spark.sparkContext.parallelize([1, 2]).map(lambda x: x + 1).collect() == [2, 3]
        print(f"PASS Spark {spark.version}: JVM and Python workers")
        for table in ("athlete_bio", "athlete_event_results"):
            df = spark.read.format("jdbc").options(**mysql_options("SOURCE"), dbtable=table).load()
            count = df.count()
            assert count > 0, f"Empty source: {table}"
            print(f"PASS JDBC {table}: {count} rows")
        output = mysql_options("OUTPUT")
        probe_table = "_environment_probe_" + token
        spark.createDataFrame([(token,)], ["value"]).write.format("jdbc").options(
            **output, dbtable=probe_table).mode("error").save()
        back = spark.read.format("jdbc").options(**output, dbtable=probe_table).load()
        assert back.first().value == token
        # Remove only this check's uniquely named scratch table.
        jdbc = spark._jvm.java.sql.DriverManager.getConnection(output["url"], output["user"], output["password"])
        try:
            statement = jdbc.createStatement()
            try:
                statement.executeUpdate(f"DROP TABLE `{probe_table}`")
            finally:
                statement.close()
        finally:
            jdbc.close()
        print("PASS output MySQL: write/read/drop scratch table")
        topic = "environment_check"
        spark.createDataFrame([(token,)], ["value"]).write.format("kafka").options(
            **kafka_options(), topic=topic).save()
        messages = spark.read.format("kafka").options(**kafka_options(), subscribe=topic,
            startingOffsets="earliest", endingOffsets="latest").load()
        assert messages.selectExpr("CAST(value AS STRING) AS value").filter(
            col("value") == token).count() == 1
        print("PASS Kafka: message written and read back")
        for folder in ("landing", "bronze", "silver", "gold"):
            path = DATA_ROOT / folder / (".probe_" + token)
            path.write_text("ok", encoding="utf-8")
            path.unlink()
        checkpoint_probe = CHECKPOINT_ROOT / (".probe_" + token)
        checkpoint_probe.write_text("ok", encoding="utf-8")
        checkpoint_probe.unlink()
        assert SparkSubmitHook(conn_id="spark-default")._resolve_connection()["master"] == "local[2]"
        print("PASS shared data/checkpoint paths and Airflow spark-default connection")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
