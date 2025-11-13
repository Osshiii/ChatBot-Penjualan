import pandas as pd
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "penjualan_clean.csv"
DB_PATH = BASE_DIR / "penjualan.db"
TABLE_NAME = "penjualan"


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Tidak menemukan CSV: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    finally:
        conn.close()

if __name__ == "__main__":
    main()