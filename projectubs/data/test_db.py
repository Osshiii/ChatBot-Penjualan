import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "penjualan.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

cur = conn.execute("SELECT * FROM penjualan LIMIT 5;")
rows = cur.fetchall()
conn.close()

for r in rows:
    print(dict(r))