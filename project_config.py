"""Shared container settings. Secrets are read only from environment variables."""
import os
import re
from pathlib import Path


def mysql_options(prefix):
    return {"url": f"jdbc:mysql://{os.environ[prefix + '_MYSQL_HOST']}:"
                   f"{os.environ[prefix + '_MYSQL_PORT']}/{os.environ[prefix + '_MYSQL_DATABASE']}"
                   "?connectionTimeZone=UTC",
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": os.environ[prefix + "_MYSQL_USER"],
            "password": os.environ[prefix + "_MYSQL_PASSWORD"]}


def kafka_options():
    options = {"kafka.bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
               "kafka.security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")}
    for name in ("SASL_MECHANISM", "SASL_JAAS_CONFIG"):
        if os.getenv("KAFKA_" + name):
            options["kafka." + name.lower().replace("_", ".")] = os.environ["KAFKA_" + name]
    return options


DATA_ROOT = Path(os.getenv("DATA_ROOT", "/opt/airflow/project/data"))
CHECKPOINT_ROOT = Path(os.getenv("CHECKPOINT_ROOT", "/opt/airflow/project/checkpoints"))


def sql_identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def mysql_connection(prefix):
    import pymysql
    return pymysql.connect(host=os.environ[prefix + "_MYSQL_HOST"],
        port=int(os.environ[prefix + "_MYSQL_PORT"]),
        user=os.environ[prefix + "_MYSQL_USER"], password=os.environ[prefix + "_MYSQL_PASSWORD"],
        database=os.environ[prefix + "_MYSQL_DATABASE"], charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor, init_command="SET time_zone = '+00:00'")
