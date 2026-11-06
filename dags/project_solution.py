"""Sequential Olympic bronze, silver and gold Spark jobs."""
from datetime import timedelta
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

PROJECT = Path("/opt/airflow/project")

with DAG(dag_id="olympic_batch", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
         schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
         dagrun_timeout=timedelta(minutes=60), tags=["final-project", "batch", "spark"]) as dag:
    # Послідовно виконати три перетворення спільного Data Lake.
    bronze = SparkSubmitOperator(task_id="landing_to_bronze", conn_id="spark-default",
        application=str(PROJECT / "batch/landing_to_bronze.py"),
        application_args=["--refresh", "--base-url", "{{ dag_run.conf.get('source_url', 'https://ftp.goit.study/neoversity/') }}"],
        name="landing-to-bronze", verbose=True, retries=0, execution_timeout=timedelta(minutes=20))
    silver = SparkSubmitOperator(task_id="bronze_to_silver", conn_id="spark-default",
        application=str(PROJECT / "batch/bronze_to_silver.py"), name="bronze-to-silver",
        verbose=True, retries=0, execution_timeout=timedelta(minutes=20))
    gold = SparkSubmitOperator(task_id="silver_to_gold", conn_id="spark-default",
        application=str(PROJECT / "batch/silver_to_gold.py"), name="silver-to-gold",
        verbose=True, retries=0, execution_timeout=timedelta(minutes=20))
    bronze >> silver >> gold
