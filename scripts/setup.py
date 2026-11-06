"""Create local folders and private settings, preserving an existing .env."""
from pathlib import Path
import secrets

ROOT = Path(__file__).resolve().parents[1]


def main():
    for folder in ("streaming", "batch", "img/streaming", "img/batch",
                   "data/landing", "data/bronze", "data/silver", "data/gold", "checkpoints"):
        (ROOT / folder).mkdir(parents=True, exist_ok=True)
    target = ROOT / ".env"
    if target.exists():
        print("Existing .env preserved; compare its keys with .env.example.")
        return
    password = secrets.token_hex(18)
    values = {"MYSQL_ROOT_PASSWORD": secrets.token_hex(18), "MYSQL_PASSWORD": password,
              "SOURCE_MYSQL_PASSWORD": password, "OUTPUT_MYSQL_PASSWORD": password}
    lines = (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    target.write_text("\n".join(
        f"{line.split('=', 1)[0]}={values[line.split('=', 1)[0]]}"
        if "=" in line and line.split("=", 1)[0] in values else line
        for line in lines) + "\n", encoding="utf-8")
    print("Created .env with random local passwords (values are not printed).")


if __name__ == "__main__":
    main()
