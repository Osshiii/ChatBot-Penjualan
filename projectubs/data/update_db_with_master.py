# projectubs/data/update_db_with_master.py
import sqlite3
from pathlib import Path
import csv

DB_PATH = Path(__file__).resolve().parent / "penjualan.db"
MASTER_CSV = Path(__file__).resolve().parent / "product_master.csv"

if not DB_PATH.exists():
    raise SystemExit("DB not found: " + str(DB_PATH))
if not MASTER_CSV.exists():
    raise SystemExit("product_master.csv not found: " + str(MASTER_CSV))

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# add columns if not exists (SQLite workaround: check PRAGMA)
def ensure_column(col_name):
    cur.execute("PRAGMA table_info(penjualan);")
    cols = [r[1] for r in cur.fetchall()]
    if col_name not in cols:
        cur.execute(f"ALTER TABLE penjualan ADD COLUMN {col_name} TEXT;")
        print("Added column:", col_name)

ensure_column("MAIN_PROD")
ensure_column("KADAR")
ensure_column("NAMA")

# read master file into dict
master = {}
with open(MASTER_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        master[r["KODE_BARANG"]] = r

# update DB rows
updated = 0
for kode, info in master.items():
    main = info.get("MAIN_PROD") or ""
    kadar = info.get("KADAR") or ""
    nama = info.get("NAMA") or ""
    cur.execute("""
        UPDATE penjualan
        SET MAIN_PROD = :main, KADAR = :kadar, NAMA = :nama
        WHERE KODE_BARANG = :kode
    """, {"main": main, "kadar": kadar, "nama": nama, "kode": kode})
    updated += cur.rowcount

conn.commit()
print("Update done. Rows affected (sum of rowcounts per kode):", updated)
conn.close()
