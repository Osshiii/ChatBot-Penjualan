# -*- coding: utf-8 -*-
"""
FastAPI Sales Data Endpoints
Provides REST API for querying jewelry sales data
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import sqlite3
from pathlib import Path

router = APIRouter(prefix="/api", tags=["sales"])
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "penjualan.db"

# Safety limit
MAX_LIMIT = 2000

def row_to_dict(row):
    """Convert sqlite3.Row to dict"""
    return dict(row)

def build_where(params):
    """Build WHERE clause and bindings from parameters"""
    wheres = ["1=1"]
    binds = {}
    
    if params.get("kode_barang"):
        wheres.append("KODE_BARANG = ?")
        binds["kode_barang"] = params["kode_barang"]
    if params.get("lokasi"):
        wheres.append("LOKASI = ?")
        binds["lokasi"] = params["lokasi"]
    if params.get("bulan"):
        wheres.append("BULAN = ?")
        binds["bulan"] = int(params["bulan"])
    if params.get("tahun"):
        wheres.append("TAHUN = ?")
        binds["tahun"] = int(params["tahun"])
    if params.get("klasifikasi_barang"):
        wheres.append("KLASIFIKASI_BARANG = ?")
        binds["klasifikasi_barang"] = params["klasifikasi_barang"]
    if params.get("warna_barang"):
        wheres.append("WARNA_BARANG = ?")
        binds["warna_barang"] = params["warna_barang"]
    if params.get("ukuran_barang"):
        wheres.append("UKURAN_BARANG = ?")
        binds["ukuran_barang"] = params["ukuran_barang"]
    if params.get("channel"):
        wheres.append("CHANNEL = ?")
        binds["channel"] = int(params["channel"])
    if params.get("min_berat") is not None:
        wheres.append("BERAT_SATUAN >= ?")
        binds["min_berat"] = float(params["min_berat"])
    if params.get("max_berat") is not None:
        wheres.append("BERAT_SATUAN <= ?")
        binds["max_berat"] = float(params["max_berat"])
    
    return " AND ".join(wheres), binds


@router.get("/sales")
def get_sales(
    kode_barang: Optional[str] = Query(None, description="Product code (e.g., MP000197)"),
    lokasi: Optional[str] = Query(None, description="Location code (e.g., LO000048)"),
    bulan: Optional[int] = Query(None, ge=1, le=12, description="Month (1-12)"),
    tahun: Optional[int] = Query(None, description="Year"),
    klasifikasi_barang: Optional[str] = Query(None, description="Classification code (e.g., KD000016)"),
    warna_barang: Optional[str] = Query(None, description="Color code (e.g., PL000037)"),
    ukuran_barang: Optional[str] = Query(None, description="Size code (e.g., SZ000012)"),
    channel: Optional[int] = Query(None, description="Channel number"),
    min_berat: Optional[float] = Query(None, description="Minimum weight"),
    max_berat: Optional[float] = Query(None, description="Maximum weight"),
    limit: int = Query(100, ge=1, le=MAX_LIMIT, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Record offset"),
):
    """
    Query sales records with optional filters.
    
    All filters are optional and use AND logic.
    Use kode ID format for all code parameters.
    """
    
    params = {
        "kode_barang": kode_barang,
        "lokasi": lokasi,
        "bulan": bulan,
        "tahun": tahun,
        "klasifikasi_barang": klasifikasi_barang,
        "warna_barang": warna_barang,
        "ukuran_barang": ukuran_barang,
        "channel": channel,
        "min_berat": min_berat,
        "max_berat": max_berat,
    }
    
    where_clause, binds = build_where(params)
    
    # Build bind list maintaining order
    bind_list = [
        binds.get("kode_barang"),
        binds.get("lokasi"),
        binds.get("bulan"),
        binds.get("tahun"),
        binds.get("klasifikasi_barang"),
        binds.get("warna_barang"),
        binds.get("ukuran_barang"),
        binds.get("channel"),
        binds.get("min_berat"),
        binds.get("max_berat"),
    ]
    bind_list = [v for v in bind_list if v is not None]
    bind_list.extend([limit, offset])
    
    sql = f"""
        SELECT * FROM penjualan
        WHERE {where_clause}
        ORDER BY TANGGAL DESC, KODE_BARANG
        LIMIT ? OFFSET ?
    """
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, bind_list)
        rows = [row_to_dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    return {
        "status": "success",
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "data": rows,
    }


@router.get("/sales/summary")
def sales_summary(
    by: str = Query("product", description="Group by: product|location|month|year|channel|classification|color|size"),
    kode_barang: Optional[str] = None,
    lokasi: Optional[str] = None,
    bulan: Optional[int] = None,
    tahun: Optional[int] = None,
    min_berat: Optional[float] = None,
    max_berat: Optional[float] = None,
    top: int = Query(20, ge=1, le=100, description="Number of top results"),
):
    """
    Get aggregated summary statistics grouped by specified dimension.
    
    Valid 'by' values:
    - product: KODE_BARANG
    - location: LOKASI
    - month: BULAN
    - year: TAHUN
    - channel: CHANNEL
    - classification: KLASIFIKASI_BARANG
    - color: WARNA_BARANG
    - size: UKURAN_BARANG
    """
    
    # Map friendly names to column names
    group_map = {
        "product": "KODE_BARANG",
        "location": "LOKASI",
        "month": "BULAN",
        "year": "TAHUN",
        "channel": "CHANNEL",
        "classification": "KLASIFIKASI_BARANG",
        "color": "WARNA_BARANG",
        "size": "UKURAN_BARANG",
    }
    
    group_column = group_map.get(by.lower(), "KODE_BARANG")
    
    # Build filters
    params = {
        "kode_barang": kode_barang,
        "lokasi": lokasi,
        "bulan": bulan,
        "tahun": tahun,
        "min_berat": min_berat,
        "max_berat": max_berat,
    }
    
    where_clause, binds = build_where(params)
    
    # Build bind list
    bind_list = [
        binds.get("kode_barang"),
        binds.get("lokasi"),
        binds.get("bulan"),
        binds.get("tahun"),
        binds.get("min_berat"),
        binds.get("max_berat"),
    ]
    bind_list = [v for v in bind_list if v is not None]
    bind_list.append(top)
    
    sql = f"""
        SELECT
            {group_column} as kategori,
            COUNT(*) as count_records,
            SUM(JUMLAH) as total_jumlah,
            SUM(BERAT_TOTAL) as total_berat,
            AVG(BERAT_SATUAN) as avg_berat,
            MIN(BERAT_SATUAN) as min_berat,
            MAX(BERAT_SATUAN) as max_berat
        FROM penjualan
        WHERE {where_clause}
        GROUP BY {group_column}
        ORDER BY total_jumlah DESC
        LIMIT ?
    """
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, bind_list)
        rows = [row_to_dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    return {
        "status": "success",
        "group_by": by,
        "group_column": group_column,
        "count": len(rows),
        "data": rows,
    }


@router.get("/sales/codes")
def get_codes(code_type: str = Query(..., description="Type: kode_barang|lokasi|klasifikasi|warna|ukuran|channel")):
    """
    Get unique code values for filtering.
    
    Useful for suggesting filter values to users.
    """
    
    column_map = {
        "kode_barang": "KODE_BARANG",
        "lokasi": "LOKASI",
        "klasifikasi": "KLASIFIKASI_BARANG",
        "klasifikasi_barang": "KLASIFIKASI_BARANG",
        "warna": "WARNA_BARANG",
        "warna_barang": "WARNA_BARANG",
        "ukuran": "UKURAN_BARANG",
        "ukuran_barang": "UKURAN_BARANG",
        "channel": "CHANNEL",
    }
    
    column = column_map.get(code_type.lower())
    if not column:
        raise HTTPException(status_code=400, detail=f"Unknown code_type: {code_type}")
    
    sql = f"SELECT DISTINCT {column} as code FROM penjualan ORDER BY {column}"
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.execute(sql)
        codes = [row[0] for row in cur.fetchall()]
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    return {
        "status": "success",
        "code_type": code_type,
        "column": column,
        "count": len(codes),
        "codes": codes,
    }


@router.get("/sales/stats")
def get_stats():
    """
    Get overall database statistics.
    """
    
    sql = """
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT KODE_BARANG) as unique_products,
            COUNT(DISTINCT LOKASI) as unique_locations,
            COUNT(DISTINCT CHANNEL) as unique_channels,
            MIN(TANGGAL) as first_date,
            MAX(TANGGAL) as last_date,
            MIN(BERAT_SATUAN) as min_weight,
            MAX(BERAT_SATUAN) as max_weight,
            AVG(BERAT_SATUAN) as avg_weight,
            SUM(JUMLAH) as total_quantity,
            SUM(BERAT_TOTAL) as total_weight
        FROM penjualan
    """
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql)
        result = dict(cur.fetchone())
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    return {
        "status": "success",
        "stats": result,
    }
