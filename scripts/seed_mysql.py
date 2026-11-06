"""Seed LOCAL MySQL from original CSV. Never writes to remote source settings."""
import csv
import itertools
import os
from pathlib import Path
import re
import pymysql


def main():
    connection = pymysql.connect(host="mysql", user=os.environ["MYSQL_USER"],
                                 password=os.environ["MYSQL_PASSWORD"],
                                 database="olympic_dataset", charset="utf8mb4")
    try:
        with connection.cursor() as cursor:
            for table in ("athlete_bio", "athlete_event_results"):
                path = Path(os.environ["DATA_ROOT"]) / "landing" / f"{table}.csv"
                with path.open(encoding="utf-8-sig", newline="") as source:
                    reader = csv.reader(source)
                    columns = next(reader)
                    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col) for col in columns):
                        raise ValueError("Unexpected CSV column name")
                    definitions = ",".join(f"`{col}` " + ("BIGINT" if col == "athlete_id" else "TEXT")
                                           for col in columns)
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table}` ({definitions}, INDEX (`athlete_id`))")
                    cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                    if cursor.fetchone()[0]:
                        print(f"{table}: existing nonempty table preserved")
                        continue
                    names = ",".join(f"`{col}`" for col in columns)
                    statement = f"INSERT INTO `{table}` ({names}) VALUES ({','.join(['%s'] * len(columns))})"
                    count = 0
                    while rows := list(itertools.islice(reader, 2000)):
                        cursor.executemany(statement, [[v if v else None for v in row] for row in rows])
                        count += len(rows)
                    connection.commit()
                    print(f"{table}: imported {count} rows")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
