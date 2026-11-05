"""Eight-task DAG for the local Olympic medals assignment."""

import logging
import random
import time

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

MEDALS = ("Bronze", "Silver", "Gold")
CONN_ID = "goit_mysql_db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS olympic_dataset.medal_counts (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    medal_type VARCHAR(6) NOT NULL,
    `count` BIGINT UNSIGNED NOT NULL,
    created_at DATETIME(6) NOT NULL
);
"""

COUNT_MEDAL_SQL = """
INSERT INTO olympic_dataset.medal_counts (medal_type, `count`, created_at)
SELECT %(medal)s, COUNT(*), UTC_TIMESTAMP(6)
FROM olympic_dataset.athlete_event_results
WHERE medal = %(medal)s;
"""

FRESHNESS_SQL = """
SELECT COALESCE(
    MAX(created_at) BETWEEN UTC_TIMESTAMP(6) - INTERVAL 30 SECOND
                        AND UTC_TIMESTAMP(6),
    0
)
FROM olympic_dataset.medal_counts;
"""


def choose_medal():
    medal = random.choice(MEDALS)
    logging.info("Selected medal: %s", medal)
    return medal


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    if medal not in MEDALS:
        raise ValueError(f"Unexpected medal: {medal!r}")
    return f"calc_{medal}"


def wait_for_check(params):
    seconds = params["delay_seconds"]
    if isinstance(seconds, bool) or not isinstance(seconds, int) or not 0 <= seconds <= 300:
        raise ValueError("delay_seconds must be an integer between 0 and 300")
    logging.info("Waiting %s seconds before checking freshness", seconds)
    time.sleep(seconds)


with DAG(
    dag_id="olympic_medals",
    description="Random medal count, branching and a 30-second freshness check",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "goit", "retries": 0},
    params={"delay_seconds": Param(5, type="integer", minimum=0, maximum=300)},
    tags=["goit", "hw07", "mysql"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table", conn_id=CONN_ID, sql=CREATE_TABLE_SQL,
    )
    pick_medal = PythonOperator(task_id="pick_medal", python_callable=choose_medal)
    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task", python_callable=choose_branch,
    )
    calculations = [
        SQLExecuteQueryOperator(
            task_id=f"calc_{medal}",
            conn_id=CONN_ID,
            sql=COUNT_MEDAL_SQL,
            parameters={"medal": medal},
            autocommit=True,
        )
        for medal in MEDALS
    ]
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=wait_for_check,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONN_ID,
        sql=FRESHNESS_SQL,
        poke_interval=2,
        timeout=10,
        mode="poke",
        soft_fail=False,
        retries=0,
    )

    create_table >> pick_medal >> pick_medal_task >> calculations
    calculations >> generate_delay >> check_for_correctness
