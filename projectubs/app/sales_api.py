# projectubs/app/sales_api.py
from fastapi import APIRouter, HTTPException
from typing import Optional
import sqlite3
from pathlib import Path
import csv

router = APIRouter()
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "penjualan.db"
MASTER_PATH = Path(__file__).resolve().parent.parent / "data" / "product_master.csv"

# load product_master into memory (PRODUCT_MAP)
PRODUCT_MAP = {}
if MASTER_PATH.exists():
    with open(MASTER_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            kode = r.get("KODE_BARANG")
            if kode:
                PRODUCT_MAP[kode] = {
                    "main_prod": (r.get("MAIN_PROD") or "").strip() or None,
                    "kadar": (r.get("KADAR") or "").strip() or None,
                    "nama": (r.get("NAMA") or "").strip() or None,
                    "sample_count": r.get("SAMPLE_COUNT"),
                    "avg_berat_satuan": r.get("AVG_BERAT_SATUAN")
                }

def row_to_dict(row):
    return dict(row)

def build_where(params):
    wheres = ["1=1"]
    binds = {}
    if params.get("kode_barang"):
        wheres.append("KODE_BARANG = :kode_barang"); binds["kode_barang"] = params["kode_barang"]
    if params.get("lokasi"):
        wheres.append("LOKASI = :lokasi"); binds["lokasi"] = params["lokasi"]
    if params.get("bulan"):
        wheres.append("BULAN = :bulan"); binds["bulan"] = params["bulan"]
    if params.get("tahun"):
        wheres.append("TAHUN = :tahun"); binds["tahun"] = params["tahun"]
    if params.get("min_berat") is not None:
        wheres.append("BERAT_SATUAN >= :min_berat"); binds["min_berat"] = params["min_berat"]
    if params.get("max_berat") is not None:
        wheres.append("BERAT_SATUAN <= :max_berat"); binds["max_berat"] = params["max_berat"]
    return " AND ".join(wheres), binds

# Limit safety
MAX_LIMIT = 2000

@router.get("/sales")
def get_sales(
    kode_barang: Optional[str] = None,
    lokasi: Optional[str] = None,
    bulan: Optional[int] = None,
    tahun: Optional[int] = None,
    min_berat: Optional[float] = None,
    max_berat: Optional[float] = None,
    limit: int = 100,
    offset: int = 0
):
    if limit < 0 or offset < 0:
        raise HTTPException(status_code=400, detail="limit and offset must be >= 0")
    limit = min(limit, MAX_LIMIT)

    params = {
        "kode_barang": kode_barang,
        "lokasi": lokasi,
        "bulan": bulan,
        "tahun": tahun,
        "min_berat": min_berat,
        "max_berat": max_berat
    }

    where_clause, binds = build_where(params)
    sql = f"SELECT * FROM penjualan WHERE {where_clause} ORDER BY TANGGAL DESC LIMIT :limit OFFSET :offset"
    binds["limit"] = limit
    binds["offset"] = offset

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql, binds)
        rows = [row_to_dict(r) for r in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    # enrich result with product master info
    for r in rows:
        prod = PRODUCT_MAP.get(r.get("KODE_BARANG"))
        r["product_info"] = prod

    return {"count": len(rows), "data": rows}

@router.get("/sales/summary")
def sales_summary(by: str = "product", top: int = 10, tahun: Optional[int] = None, bulan: Optional[int] = None):
    """
    Summary endpoint:
    - by=product -> top KODE_BARANG by BERAT_TOTAL (total gram)
    - by=lokasi  -> top LOKASI by BERAT_TOTAL
    - by=bulan   -> aggregate by month (TAHUN-BULAN)
    """
    top = int(top)
    if top <= 0 or top > 1000:
        top = 10

    where_parts = []
    binds = {}
    if tahun:
        where_parts.append("TAHUN = :tahun"); binds["tahun"] = tahun
    if bulan:
        where_parts.append("BULAN = :bulan"); binds["bulan"] = bulan
    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if by == "product":
        sql = f"""
            SELECT KODE_BARANG, SUM(BERAT_TOTAL) AS total_gram, COUNT(*) AS tx_count
            FROM penjualan {where_clause}
            GROUP BY KODE_BARANG
            ORDER BY total_gram DESC
            LIMIT :top
        """
        binds["top"] = top
        cur.execute(sql, binds)
        res = [dict(r) for r in cur.fetchall()]
        # enrich names
        for r in res:
            r["product_info"] = PRODUCT_MAP.get(r.get("KODE_BARANG"))
    elif by == "lokasi":
        sql = f"""
            SELECT LOKASI, SUM(BERAT_TOTAL) AS total_gram, COUNT(*) AS tx_count
            FROM penjualan {where_clause}
            GROUP BY LOKASI
            ORDER BY total_gram DESC
            LIMIT :top
        """
        binds["top"] = top
        cur.execute(sql, binds)
        res = [dict(r) for r in cur.fetchall()]
    elif by == "bulan":
        sql = f"""
            SELECT TAHUN, BULAN, SUM(BERAT_TOTAL) AS total_gram, COUNT(*) AS tx_count
            FROM penjualan {where_clause}
            GROUP BY TAHUN, BULAN
            ORDER BY TAHUN DESC, BULAN DESC
            LIMIT :top
        """
        binds["top"] = top
        cur.execute(sql, binds)
        res = [dict(r) for r in cur.fetchall()]
    else:
        conn.close()
        raise HTTPException(status_code=400, detail="unknown 'by' param, choose product|lokasi|bulan")

    conn.close()
    return {"by": by, "top": res}
