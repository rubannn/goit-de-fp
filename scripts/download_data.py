"""Download original course CSV files without transforming them."""
import csv
from pathlib import Path
from urllib.request import urlopen
import shutil

ROOT = Path(__file__).resolve().parents[1] / "data" / "landing"
TABLES = ("athlete_bio", "athlete_event_results")


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        target = ROOT / f"{table}.csv"
        if not target.exists():
            partial = target.with_suffix(".csv.part")
            with urlopen(f"https://ftp.goit.study/neoversity/{table}.csv", timeout=120) as response:
                with partial.open("wb") as output:
                    shutil.copyfileobj(response, output)
            partial.replace(target)
        with target.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if "athlete_id" not in (reader.fieldnames or []):
                raise ValueError(f"Invalid CSV header: {target.name}")
            count = sum(1 for _ in reader)
            if not count:
                raise ValueError(f"Empty dataset: {target.name}")
        print(f"{table}: {count} rows; original CSV saved in {target}")


if __name__ == "__main__":
    main()
