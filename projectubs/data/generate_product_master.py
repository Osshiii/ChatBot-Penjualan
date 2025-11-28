# generate_product_master.py
import pandas as pd
from pathlib import Path

# Path ke CSV hasil cleaning (yang kalian buat dengan cleaning.py)
csv_candidates = [
    Path("penjualan_clean.csv"),
    Path("penjualan_helped.csv"),
    Path("sheet1.csv"),
    Path("sheet2.csv"),
]

# Jika ingin pakai Excel, GANTI path sesuai lokasi asli di laptop kamu:
excel_fallback = None
out_path = Path("product_master.csv")

df = None
for p in csv_candidates:
    if p.exists():
        df = pd.read_csv(p)
        print("Loaded CSV:", p)
        break

if df is None:
    if excel_fallback.exists():
        df = pd.read_excel(excel_fallback)
        print("Loaded Excel fallback:", excel_fallback)
    else:
        raise FileNotFoundError("Tidak menemukan penjualan_clean.csv atau MAGANG (2).xlsx. Pastikan file ada.")

# Normalize column names
df.columns = [str(c).strip().upper() for c in df.columns]

if "KODE_BARANG" not in df.columns:
    raise RuntimeError("Kolom KODE_BARANG tidak ditemukan di dataset. Kolom yang ada: " + ", ".join(df.columns))

# Aggregate info
group = df.groupby("KODE_BARANG").agg(
    SAMPLE_COUNT = ("KODE_BARANG", "size"),
    AVG_BERAT_SATUAN = ("BERAT_SATUAN", lambda s: float(s.dropna().mean()) if "BERAT_SATUAN" in df.columns else None)
).reset_index()

# Add placeholder columns for manual fill
group["MAIN_PROD"] = ""   # fill with cincin/kalung/gelang/anting
group["KADAR"] = ""       # fill with e.g. 22K, 24K
group["NAMA"] = ""        # optional friendly name

out = group[["KODE_BARANG","MAIN_PROD","KADAR","NAMA","SAMPLE_COUNT","AVG_BERAT_SATUAN"]]
out.to_csv(out_path, index=False, encoding="utf-8")
print("product_master.csv generated at:", out_path)
