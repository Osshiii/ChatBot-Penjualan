# -*- coding: utf-8 -*-
"""
Chatbot Module for Jewelry Sales AI Assistant
- Intent classification: GENERAL (chit-chat), SUMMARY (aggregates), DETAIL (rows), SUGGESTION (advice)
- GENERAL: No database query, LLM-only natural response
- SUMMARY/SUGGESTION: Database query first, then LLM insight/advice
- Supports: help, exploratory, detail (rows), summary (aggregate), count, suggestion
- Fix: relative time ("bulan ini", "bulan lalu", "tahun ini") based on latest data in DB
- Optional: LLM rewrite (USE_LLM=1) for detail/summary/suggestion responses
- NEW: answer_mode -> ringkasan / insight / saran (output dipisah, tidak digabung)
"""

from __future__ import annotations

import os
import sqlite3
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.nlp_parser import NLPParser, QueryType

try:
    from app.llm_answer import generate_llm_answer
    from app.llm_client import LLMUnavailable
except Exception:
    generate_llm_answer = None
    LLMUnavailable = Exception

logger = logging.getLogger(__name__)


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def format_number(value: float, decimals: int = 0, thousands_sep: bool = True) -> str:
    """
    Format number with proper Indonesian/European locale separators.
    - Thousands: dot (1.234)
    - Decimal: comma (1,5)
    - Max 2 decimal places for readability
    """
    if value is None:
        return "0"
    
    try:
        val = float(value)
        
        # Limit decimals to max 2 for better readability
        if decimals > 2:
            decimals = 2
        
        if decimals == 0:
            # No decimals - just format with thousands separator
            if thousands_sep:
                int_val = int(round(val))
                str_val = str(int_val)
                parts = []
                for i, digit in enumerate(reversed(str_val)):
                    if i > 0 and i % 3 == 0:
                        parts.append('.')
                    parts.append(digit)
                return ''.join(reversed(parts))
            else:
                return str(int(round(val)))
        else:
            # With decimals (max 2)
            if thousands_sep:
                formatted = f"{val:.{decimals}f}"
                int_part, dec_part = formatted.split('.')
                
                # Add dots to integer part for thousands
                parts = []
                for i, digit in enumerate(reversed(int_part)):
                    if i > 0 and i % 3 == 0:
                        parts.append('.')
                    parts.append(digit)
                int_formatted = ''.join(reversed(parts))
                
                # Return with comma as decimal separator
                return f"{int_formatted},{dec_part}"
            else:
                return f"{val:.{decimals}f}".replace('.', ',')
    
    except (ValueError, TypeError):
        return str(value)


def format_percentage(value: float, decimals: int = 1) -> str:
    """Format percentage with max 1 decimal place."""
    try:
        val = float(value)
        # Max 1 decimal for percentages
        if decimals > 1:
            decimals = 1
        
        # Format with specified decimals
        formatted = f"{val:.{decimals}f}"
        
        # Replace dot with comma for decimal separator
        formatted = formatted.replace('.', ',')
        
        return f"{formatted}%"
    except (ValueError, TypeError):
        return f"{value}%"

# ============================================================
# Database Layer
# ============================================================
class SalesDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def execute_query(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def get_latest_date(self) -> Optional[str]:
        sql = "SELECT MAX(TANGGAL) as max_tanggal FROM penjualan"
        res = self.execute_query(sql)
        if not res:
            return None
        return res[0].get("max_tanggal")

    def get_min_date(self) -> Optional[str]:
        sql = "SELECT MIN(TANGGAL) as min_tanggal FROM penjualan"
        res = self.execute_query(sql)
        if not res:
            return None
        return res[0].get("min_tanggal")

    def get_latest_month_year(self) -> Tuple[int, int]:
        max_tgl = self.get_latest_date()
        if not max_tgl:
            now = datetime.now()
            return now.month, now.year

        s = str(max_tgl).strip().split(" ")[0]
        try:
            dt = datetime.fromisoformat(s)
            return dt.month, dt.year
        except Exception:
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.month, dt.year
                except Exception:
                    pass

        now = datetime.now()
        return now.month, now.year

    def get_db_date_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        min_tgl = self.get_min_date()
        max_tgl = self.get_latest_date()

        def parse_date(s: str) -> Optional[datetime]:
            if not s:
                return None
            s = str(s).strip().split(" ")[0]
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(s, fmt)
                except Exception:
                    pass
            return None

        return parse_date(min_tgl), parse_date(max_tgl)


# ============================================================
# Query Builder
# ============================================================
class SalesQueryBuilder:
    @staticmethod
    def build_where_clause(filters: Dict[str, Any]) -> Tuple[str, List[Any]]:
        where_parts = ["1=1"]
        params: List[Any] = []

        if filters.get("kode_barang"):
            where_parts.append("KODE_BARANG = ?")
            params.append(filters["kode_barang"])

        if filters.get("lokasi"):
            where_parts.append("LOKASI = ?")
            params.append(filters["lokasi"])

        if filters.get("klasifikasi_barang"):
            where_parts.append("KLASIFIKASI_BARANG = ?")
            params.append(filters["klasifikasi_barang"])

        if filters.get("warna_barang"):
            where_parts.append("WARNA_BARANG = ?")
            params.append(filters["warna_barang"])

        if filters.get("ukuran_barang"):
            where_parts.append("UKURAN_BARANG = ?")
            params.append(filters["ukuran_barang"])

        if filters.get("channel") is not None:
            where_parts.append("CHANNEL = ?")
            params.append(filters["channel"])

        if filters.get("bulan") is not None:
            where_parts.append("BULAN = ?")
            params.append(filters["bulan"])

        if filters.get("tahun") is not None:
            where_parts.append("TAHUN = ?")
            params.append(filters["tahun"])

        if filters.get("date_from"):
            where_parts.append("TANGGAL >= ?")
            params.append(filters["date_from"])

        if filters.get("date_to"):
            where_parts.append("TANGGAL <= ?")
            params.append(filters["date_to"])

        if filters.get("min_berat") is not None:
            where_parts.append("BERAT_SATUAN >= ?")
            params.append(filters["min_berat"])

        if filters.get("max_berat") is not None:
            where_parts.append("BERAT_SATUAN <= ?")
            params.append(filters["max_berat"])

        if filters.get("min_jumlah") is not None:
            where_parts.append("JUMLAH >= ?")
            params.append(filters["min_jumlah"])

        if filters.get("max_jumlah") is not None:
            where_parts.append("JUMLAH <= ?")
            params.append(filters["max_jumlah"])

        return " AND ".join(where_parts), params

    @staticmethod
    def build_count_query(filters: Dict[str, Any]) -> Tuple[str, List[Any]]:
        where_clause, params = SalesQueryBuilder.build_where_clause(filters)
        sql = f"SELECT COUNT(*) as total FROM penjualan WHERE {where_clause}"
        return sql, params

    @staticmethod
    def build_detail_query(filters: Dict[str, Any], limit: int = 5, offset: int = 0) -> Tuple[str, List[Any]]:
        where_clause, params = SalesQueryBuilder.build_where_clause(filters)
        sql = f"""
            SELECT * FROM penjualan
            WHERE {where_clause}
            ORDER BY TANGGAL DESC, KODE_BARANG
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        return sql, params

    @staticmethod
    def build_summary_query(filters: Dict[str, Any], group_by: str = "KODE_BARANG") -> Tuple[str, List[Any]]:
        where_clause, params = SalesQueryBuilder.build_where_clause(filters)

        valid_group_by = [
            "KODE_BARANG", "LOKASI", "BULAN", "TAHUN", "CHANNEL",
            "KLASIFIKASI_BARANG", "WARNA_BARANG", "UKURAN_BARANG",
        ]
        if group_by not in valid_group_by:
            group_by = "KODE_BARANG"

        sql = f"""
            SELECT
                {group_by} as kategori,
                COUNT(*) as count_records,
                SUM(JUMLAH) as total_jumlah,
                SUM(BERAT_TOTAL) as total_berat,
                AVG(BERAT_SATUAN) as avg_berat,
                MIN(BERAT_SATUAN) as min_berat,
                MAX(BERAT_SATUAN) as max_berat
            FROM penjualan
            WHERE {where_clause}
            GROUP BY {group_by}
            ORDER BY total_jumlah DESC
            LIMIT 20
        """
        return sql, params


# ============================================================
# Bot
# ============================================================
class JewelrySalesBot:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.parser = NLPParser()
        self.db = SalesDatabase(db_path)
        self.conversation_history: List[Dict[str, Any]] = []
        self.debug = env_bool("DEBUG", False)

    def process_message(self, user_message: str, limit: int = 10, offset: int = 0) -> Dict[str, Any]:
        """
        Process user message with intent-based routing:
        
        FLOW:
        1. Parse intent (GENERAL, SUMMARY/INSIGHT, SUGGESTION, DETAIL, etc.)
        2. Route based on intent:
           - GENERAL (greeting/chit-chat): LLM only, NO database query
           - SUMMARY/INSIGHT: Query database → aggregate → LLM insight
           - SUGGESTION: Query database → analyze → LLM business advice
           - DETAIL: Query database → return rows with pagination
        3. Optional LLM rewrite for better formatting
        """
        self.conversation_history.append({
            "timestamp": datetime.now().isoformat(),
            "role": "user",
            "message": user_message,
        })

        parsed_query = self.parser.parse(user_message)
        
        if parsed_query.get("error"):
            return {
                "query_type": "error",
                "message": f"❌ Validasi input gagal: {parsed_query['error']}",
                "data": [],
                "confidence": 0.0,
            }

        # Resolve relative time based on DB
        self._resolve_relative_time(parsed_query, user_message)

        if self.debug:
            print(f"[DEBUG] User: {user_message}")
            print(f"[DEBUG] Type: {parsed_query['query_type']}")
            print(f"[DEBUG] AnswerMode: {parsed_query.get('answer_mode')}")
            print(f"[DEBUG] Confidence: {parsed_query.get('confidence', 0):.2f}")

        qt = parsed_query["query_type"]

        # === GENERAL: No database query, LLM answer only ===
        if qt == QueryType.GENERAL:
            response = self._handle_general_query(user_message)
        # === DATA-DRIVEN HANDLERS ===
        elif qt == QueryType.HELP:
            response = self._handle_help_query()
        elif qt == QueryType.SUGGESTION:
            # Inject pagination for suggestion context
            if "filters" not in parsed_query:
                parsed_query["filters"] = {}
            parsed_query["filters"]["limit"] = limit
            parsed_query["filters"]["offset"] = offset
            response = self._handle_suggestion_query(parsed_query)
        elif qt == QueryType.LATEST_TRANSACTION:
            response = self._handle_latest_transaction_query(parsed_query)
        elif qt == QueryType.EXACT_QUERY:
            response = self._handle_exact_query(parsed_query)
        elif qt == QueryType.COMPARATIVE:
            response = self._handle_comparative_query(parsed_query)
        elif qt == QueryType.DETAIL:
            # Inject pagination params directly into filters (bypass NLP parsing)
            if "filters" not in parsed_query:
                parsed_query["filters"] = {}
            parsed_query["filters"]["limit"] = limit
            parsed_query["filters"]["offset"] = offset
            response = self._handle_detail_query(parsed_query)
            # Force show_data if query started with "tampilkan data"
            if parsed_query.get("force_show_data"):
                response["show_data"] = True
        elif qt == QueryType.COUNT:
            response = self._handle_count_query(parsed_query)
        elif qt == QueryType.SUMMARY:
            response = self._handle_summary_query(parsed_query)
        elif qt == QueryType.EXPLORATORY:
            response = self._handle_exploratory_query(parsed_query)
        else:
            response = self._handle_unknown_query(parsed_query)

        # NEW: potong message sesuai answer_mode (non-LLM)
        response = self._apply_answer_mode_non_llm(parsed_query, response)

        # LLM rewrite (detail/summary) -> output juga harus sesuai answer_mode
        response = self._maybe_rewrite_with_llm(user_message, parsed_query, response)

        self.conversation_history.append({
            "timestamp": datetime.now().isoformat(),
            "role": "assistant",
            "message": response.get("message", ""),
        })

        return response

    # -------------------------
    # Relative time resolver
    # -------------------------
    def _resolve_relative_time(self, parsed_query: Dict[str, Any], user_message: str) -> None:
        norm = (parsed_query.get("normalized") or "").lower()
        filters = parsed_query.get("filters", {}) or {}

        def prev_month(month: int, year: int) -> Tuple[int, int]:
            return (12, year - 1) if month == 1 else (month - 1, year)

        has_current_bulan = "bulan ini" in norm or "bulan sekarang" in norm
        has_current_tahun = "tahun ini" in norm

        db_min_date, db_max_date = self.db.get_db_date_range()
        now = datetime.now()

        if has_current_bulan:
            m, y = self.db.get_latest_month_year()
            filters["bulan"] = m
            filters["tahun"] = y

            if db_min_date and db_max_date:
                if (now.year > db_max_date.year) or (now.year == db_max_date.year and now.month > db_max_date.month):
                    parsed_query["_relative_time_warning"] = (
                        f"Data tersedia {db_min_date.strftime('%b %Y')} - {db_max_date.strftime('%b %Y')}. "
                        f"Fallback ke bulan terbaru: {m}/{y}"
                    )

        if "bulan lalu" in norm:
            m, y = self.db.get_latest_month_year()
            m2, y2 = prev_month(m, y)
            filters["bulan"] = m2
            filters["tahun"] = y2

        if has_current_tahun:
            _, y = self.db.get_latest_month_year()
            filters["tahun"] = y
            if db_min_date and db_max_date:
                if now.year > db_max_date.year:
                    parsed_query["_relative_time_warning"] = (
                        f"Data tersedia {db_min_date.year} - {db_max_date.year}. "
                        f"Fallback ke tahun terbaru: {y}"
                    )

        parsed_query["filters"] = filters

    # -------------------------
    # NEW: Apply answer_mode without LLM
    # -------------------------
    def _apply_answer_mode_non_llm(self, parsed_query: Dict[str, Any], response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Kalau USE_LLM=0, kita tetap harus pisahin output.
        Kalau query_type bukan summary/detail, biarin.
        """
        answer_mode = (parsed_query.get("answer_mode") or "auto").lower()
        qtype = (response.get("query_type") or "").lower()

        # Kalau bukan summary/detail, ga ada yang dipotong.
        if qtype not in ("summary", "detail"):
            return response

        # Kalau auto, biarin message asli dari handler.
        if answer_mode == "auto":
            return response

        # SUMMARY: bikin message sesuai mode dari data agregat
        if qtype == "summary":
            data = response.get("data", []) or []
            group_by = response.get("group_by") or parsed_query.get("group_by") or "KODE_BARANG"
            filters = response.get("filters", parsed_query.get("filters", {})) or {}

            total_transaksi = sum(int(r.get("count_records", 0) or 0) for r in data) if data else 0
            total_unit = sum(float(r.get("total_jumlah", 0) or 0) for r in data) if data else 0.0
            total_berat = sum(float(r.get("total_berat", 0) or 0) for r in data) if data else 0.0

            # weighted avg
            if total_transaksi > 0:
                avg_berat = (
                    sum((float(r.get("avg_berat", 0) or 0) * int(r.get("count_records", 0) or 0)) for r in data)
                    / total_transaksi
                )
            else:
                avg_berat = 0.0

            if answer_mode == "ringkasan":
                response["message"] = (
                    f"📊 Ringkasan penjualan berdasarkan {group_by}\n"
                    f"Total transaksi: {total_transaksi:,}\n"
                    f"Total unit: {total_unit:,.0f}\n"
                    f"Total berat: {total_berat:,.2f} g\n"
                    f"Rata-rata berat: {avg_berat:.2f} g/item"
                )
                return response

            if answer_mode == "insight":
                top = data[:3]
                lines = [f"🔎 Insight berdasarkan {group_by}:"]
                if not top:
                    lines.append("- Tidak ada data untuk dianalisis.")
                else:
                    # 1) top transaksi
                    top1 = top[0]
                    lines.append(
                        f"- Kategori teratas: {top1.get('kategori')} "
                        f"({int(top1.get('count_records', 0) or 0):,} transaksi, "
                        f"total {float(top1.get('total_berat', 0) or 0):,.2f} g)."
                    )

                    # 2) avg tertinggi dari top 20
                    best_avg = max(data, key=lambda r: float(r.get("avg_berat", 0) or 0)) if data else None
                    if best_avg:
                        lines.append(
                            f"- Rata-rata berat tertinggi: {best_avg.get('kategori')} "
                            f"({float(best_avg.get('avg_berat', 0) or 0):.2f} g/item)."
                        )

                    # 3) kontribusi 3 besar (berdasarkan total_jumlah)
                    total_qty_all = sum(float(r.get("total_jumlah", 0) or 0) for r in data) if data else 0.0
                    top3_qty = sum(float(r.get("total_jumlah", 0) or 0) for r in top) if top else 0.0
                    if total_qty_all > 0:
                        share = (top3_qty / total_qty_all) * 100.0
                        lines.append(f"- 3 kategori teratas menyumbang sekitar {share:.1f}% dari total unit.")
                response["message"] = "\n".join(lines)
                return response

            if answer_mode == "saran":
                desc = self._describe_filters(filters)
                lines = ["✅ Saran lanjutan:"]
                lines.append(f"- Kalau datanya masih terlalu umum {desc}, coba batasi bulan/tahun atau lokasi.")
                lines.append(f"- Lihat detail kategori teratas di {group_by} untuk cek transaksi per baris (detail query).")
                lines.append("- Bandingkan per channel/lokasi untuk melihat sumber kontribusi terbesar.")
                response["message"] = "\n".join(lines)
                return response

            return response

        # DETAIL: untuk sekarang simple (pisahin output detail jadi ringkasan/insight/saran juga bisa,
        # tapi kamu fokusnya summary per produk; jadi cukup biarin).
        return response

    # ============================================================
    # Latest Transaction Handler
    # ============================================================
    def _handle_latest_transaction_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle \"transaksi terbaru\" queries.
        Steps:
        1. Get max(TANGGAL) from database
        2. Query all transactions with that date
        3. Calculate summary
        4. Return with summary
        """
        try:
            # Step 1: Get latest date
            sql_max = "SELECT MAX(TANGGAL) as max_tanggal FROM penjualan"
            max_result = self.db.execute_query(sql_max)
            latest_date = max_result[0].get("max_tanggal") if max_result else None

            if not latest_date:
                return {
                    "query_type": "latest_transaction",
                    "message": "❌ Data transaksi tidak ditemukan.",
                    "data": [],
                    "confidence": 0.0,
                }

            # Step 2: Query all transactions on that date
            sql_detail = f"SELECT * FROM penjualan WHERE TANGGAL = ? ORDER BY KODE_BARANG"
            results = self.db.execute_query(sql_detail, (latest_date,))

            if not results:
                return {
                    "query_type": "latest_transaction",
                    "message": f"❌ Tidak ada transaksi pada tanggal {latest_date}.",
                    "data": [],
                    "confidence": 0.0,
                }

            # Step 3: Calculate summary
            total_count = len(results)
            total_qty = sum(float(r.get("JUMLAH", 0) or 0) for r in results)
            total_berat = sum(float(r.get("BERAT_TOTAL", 0) or 0) for r in results)
            unique_barang = len(set(r.get("KODE_BARANG") for r in results if r.get("KODE_BARANG")))
            unique_lokasi = len(set(r.get("LOKASI") for r in results if r.get("LOKASI")))

            # Step 4: Build response message
            summary_msg = (
                f"📅 Transaksi Terbaru: {latest_date}\n"
                f"Total transaksi: {total_count}\n"
                f"Total unit: {total_qty:,.0f}\n"
                f"Total berat: {total_berat:,.2f} g\n"
                f"Jumlah barang unik: {unique_barang}\n"
                f"Jumlah lokasi: {unique_lokasi}"
            )

            return {
                "query_type": "latest_transaction",
                "message": summary_msg,
                "data": results,
                "latest_date": latest_date,
                "count": total_count,
                "summary": {
                    "total_transaksi": total_count,
                    "total_qty": total_qty,
                    "total_berat": total_berat,
                    "unique_barang": unique_barang,
                    "unique_lokasi": unique_lokasi,
                },
                "confidence": 0.95,
            }

        except Exception as e:
            return {
                "query_type": "latest_transaction",
                "message": f"❌ Error: {str(e)}",
                "data": [],
                "confidence": 0.0,
                "error": str(e),
            }

    # ============================================================
    # Exact Query Handler (Specific Column/Date Requests)
    # ============================================================
    def _handle_exact_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle exact/specific queries like:
        - \"Barang apa yang terjual pada tanggal X?\"
        - \"Pada bulan dan tahun berapa kode MP002175 terjual?\"
        
        Returns only the requested information without analysis.
        """
        user_msg = parsed_query.get("original", "").lower()
        filters = (parsed_query.get("filters") or {}).copy()

        try:
            # Case 1: \"Barang apa yang terjual pada tanggal X?\"
            if "tanggal" in user_msg and ("barang apa" in user_msg or "produk apa" in user_msg):
                # Extract date if provided
                date_pattern = r"(\d{1,2})\s+([a-z]+)\s+(\d{4})"
                date_match = re.search(date_pattern, user_msg, re.I)
                
                if not date_match and "TANGGAL" not in filters:
                    return {
                        "query_type": "exact_query",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.5,
                    }

                # Use explicit date from query or filter
                where_clause = "1=1"
                params = []
                if "TANGGAL" in filters:
                    where_clause = "TANGGAL = ?"
                    params = [filters["TANGGAL"]]
                
                sql = f"SELECT DISTINCT KODE_BARANG FROM penjualan WHERE {where_clause} ORDER BY KODE_BARANG"
                results = self.db.execute_query(sql, tuple(params)) if params else self.db.execute_query(sql)

                if not results:
                    return {
                        "query_type": "exact_query",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }

                items = ", ".join(r.get("KODE_BARANG", "") for r in results if r.get("KODE_BARANG"))
                return {
                    "query_type": "exact_query",
                    "message": f"Barang yang terjual pada tanggal tersebut adalah: {items}",
                    "data": results,
                    "confidence": 0.9,
                }

            # Case 2: \"Pada bulan dan tahun berapa kode MP002175 terjual?\"
            if "kode" in user_msg and ("bulan" in user_msg and "tahun" in user_msg or "kapan" in user_msg):
                if "kode_barang" not in filters:
                    return {
                        "query_type": "exact_query",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.5,
                    }

                kode = filters["kode_barang"]
                sql = "SELECT DISTINCT BULAN, TAHUN FROM penjualan WHERE KODE_BARANG = ? ORDER BY TAHUN, BULAN"
                results = self.db.execute_query(sql, (kode,))

                if not results:
                    return {
                        "query_type": "exact_query",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }

                # Group by year
                by_year = {}
                for r in results:
                    tahun = r.get("TAHUN")
                    bulan = r.get("BULAN")
                    if tahun not in by_year:
                        by_year[tahun] = []
                    by_year[tahun].append(bulan)

                # Format response
                response_parts = []
                for tahun in sorted(by_year.keys()):
                    bulan_list = sorted(set(by_year[tahun]))
                    month_names = [
                        ["Januari", "Februari", "Maret", "April", "Mei", "Juni",
                         "Juli", "Agustus", "September", "Oktober", "November", "Desember"][b-1]
                        for b in bulan_list
                    ]
                    response_parts.append(f"bulan {', '.join(month_names)} tahun {tahun}")

                msg = f"Kode barang {kode} terjual pada {', dan '.join(response_parts)}."
                return {
                    "query_type": "exact_query",
                    "message": msg,
                    "data": results,
                    "confidence": 0.9,
                }

            # Default exact query: just return filtered data without analysis
            detail_sql, detail_params = SalesQueryBuilder.build_detail_query(filters, limit=50, offset=0)
            results = self.db.execute_query(detail_sql, tuple(detail_params))

            if not results:
                return {
                    "query_type": "exact_query",
                    "message": "Data tidak ditemukan.",
                    "data": [],
                    "confidence": 0.7,
                }

            return {
                "query_type": "exact_query",
                "message": f"Ditemukan {len(results)} data sesuai kriteria.",
                "data": results,
                "count": len(results),
                "confidence": 0.8,
                "show_data": True,
                "limit": 50,
                "offset": 0,
                "page": 1,
                "total_count": len(results)
            }

        except Exception as e:
            return {
                "query_type": "exact_query",
                "message": "Data tidak ditemukan.",
                "data": [],
                "confidence": 0.0,
                "error": str(e),
            }

    # ============================================================
    # Comparative/Aggregation Handler (Now Fully Implemented)
    # ============================================================
    def _handle_comparative_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle comparative/aggregation queries.
        
        Detects metric (SUM, COUNT, AVG, MAX, MIN, ranking)
        Detects dimension (location, product, date, month, year)
        Builds and executes aggregation queries
        Returns direct results without extra analysis
        
        Examples:
        - \"Lokasi mana yang memiliki penjualan terbanyak?\"
        - \"Barang apa yang memiliki rata-rata berat tertinggi?\"
        - \"Berapa total penjualan bulan Desember 2023?\"
        - \"Top 5 barang paling sering terjual\"
        """
        user_msg = parsed_query.get("original", "").lower()
        filters = (parsed_query.get("filters") or {}).copy()
        
        try:
            # Step 1: Detect metric type
            metric = self._detect_agg_metric(user_msg)
            if not metric:
                return {
                    "query_type": "comparative",
                    "message": "Data tidak ditemukan.",
                    "data": [],
                    "confidence": 0.3,
                }
            
            # Step 2: Detect dimension
            dimension = self._detect_agg_dimension(user_msg)
            
            # Step 3: Detect limit for "top N" queries
            top_n = self._extract_top_n(user_msg)
            
            # Step 4: Build and execute query
            if metric["type"] == "total":
                return self._query_total_sales(metric, dimension, filters, user_msg)
            elif metric["type"] == "count":
                return self._query_count_transactions(metric, dimension, filters, top_n, user_msg)
            elif metric["type"] == "average":
                return self._query_average_value(metric, dimension, filters, user_msg)
            elif metric["type"] == "ranking":
                return self._query_ranking(metric, dimension, filters, top_n, user_msg)
            elif metric["type"] == "daily_average":
                return self._query_daily_average(filters, user_msg)
            else:
                return {
                    "query_type": "comparative",
                    "message": "Data tidak ditemukan.",
                    "data": [],
                    "confidence": 0.3,
                }
        
        except Exception as e:
            return {
                "query_type": "comparative",
                "message": "Data tidak ditemukan.",
                "data": [],
                "error": str(e),
                "confidence": 0.0,
            }

    def _detect_agg_metric(self, text: str) -> Optional[Dict[str, Any]]:
        """Detect aggregation metric type from query text."""
        t = text.lower()
        
        # Detect "total" / "sum"
        if re.search(r"\b(total|berapa.*total|jumlah keseluruhan)\b", t):
            return {"type": "total", "func": "SUM"}
        
        # Detect "count" / "how many"
        if re.search(r"\b(berapa|banyak|frekuensi|sering)\b", t) and not re.search(r"\b(rata|average|avg)\b", t):
            return {"type": "count", "func": "COUNT"}
        
        # Detect "average" / "rata-rata"
        if re.search(r"\b(rata|rata-rata|average|avg)\b", t):
            return {"type": "average", "func": "AVG"}
        
        # Detect "daily average"
        if re.search(r"\b(per hari|harian|daily)\b", t) and re.search(r"\b(rata|average|avg)\b", t):
            return {"type": "daily_average", "func": "AVG"}
        
        # Detect "ranking" / "top N"
        if re.search(r"\b(top|tertinggi|terendah|terbanyak|paling|ranking|urutan|terbesar)\b", t):
            return {"type": "ranking", "func": "DESC"}
        
        return None

    def _detect_agg_dimension(self, text: str) -> Optional[str]:
        """Detect the dimension (what to group by)."""
        t = text.lower()
        
        if re.search(r"\b(lokasi|location|toko|store|outlet)\b", t):
            return "LOKASI"
        if re.search(r"\b(barang|produk|product|item|kode barang|kode produk)\b", t):
            return "KODE_BARANG"
        if re.search(r"\b(channel|chann?el|chennel|chenel|chanel)\b", t):
            return "CHANNEL"
        if re.search(r"\b(bulan|month)\b", t):
            return "BULAN"
        if re.search(r"\b(tahun|year)\b", t):
            return "TAHUN"
        if re.search(r"\b(warna|color|warna barang)\b", t):
            return "WARNA_BARANG"
        if re.search(r"\b(ukuran|size|ukuran barang)\b", t):
            return "UKURAN_BARANG"
        if re.search(r"\b(klasifikasi|classification)\b", t):
            return "KLASIFIKASI_BARANG"
        if re.search(r"\b(tanggal|date|per hari|harian)\b", t):
            return "TANGGAL"
        
        return None

    def _extract_top_n(self, text: str) -> int:
        """Extract top N value from query like 'top 5 barang'."""
        m = re.search(r"\b(top|top\s*)?(\d+)\b", text, re.I)
        if m:
            try:
                return int(m.group(2))
            except Exception:
                pass
        return 1

    def _query_total_sales(self, metric: Dict[str, Any], dimension: Optional[str], filters: Dict[str, Any], user_msg: str) -> Dict[str, Any]:
        """Query total sales (SUM) with optional grouping."""
        try:
            where_clause, params = SalesQueryBuilder.build_where_clause(filters)
            
            # Special case: if user specifies specific bulan+tahun, just sum that month (not ranking)
            if dimension == "BULAN" and "bulan" in filters and "tahun" in filters:
                # Query for total of the specific month requested (no ranking)
                sql = f"SELECT SUM(JUMLAH) as total_qty, SUM(BERAT_TOTAL) as total_berat, COUNT(*) as transaction_count FROM penjualan WHERE {where_clause}"
                results = self.db.execute_query(sql, tuple(params))
                
                if not results or not results[0].get("total_qty"):
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }
                
                total_qty = results[0].get("total_qty", 0)
                bulan_num = filters.get("bulan")
                tahun_num = filters.get("tahun")
                bulan_name = self._month_number_to_name(int(bulan_num)) if bulan_num else ""
                
                msg = f"Total penjualan bulan {bulan_name} {tahun_num} adalah: {total_qty:,.0f}."
                
                return {
                    "query_type": "comparative",
                    "message": msg,
                    "data": results,
                    "metric": metric["func"],
                    "confidence": 0.95,
                }
            
            if dimension:
                # Total per dimension
                sql = f"""
                    SELECT
                        {dimension} as kategori,
                        SUM(JUMLAH) as total_qty,
                        SUM(BERAT_TOTAL) as total_berat,
                        COUNT(*) as transaction_count
                    FROM penjualan
                    WHERE {where_clause}
                    GROUP BY {dimension}
                    ORDER BY total_qty DESC
                    LIMIT 1
                """
                results = self.db.execute_query(sql, tuple(params))
                
                if not results:
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }
                
                top_item = results[0]
                kategori = top_item.get("kategori")
                total_qty = top_item.get("total_qty", 0)
                
                # Handle None/empty entity
                if not kategori:
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.7,
                    }
                
                # Map month number to name if dimension is BULAN
                if dimension == "BULAN":
                    kategori = self._month_number_to_name(int(kategori))
                
                dimension_label = self._format_dimension_label(dimension)
                msg = f"{dimension_label} dengan penjualan terbanyak adalah: {kategori}."
                
                return {
                    "query_type": "comparative",
                    "message": msg,
                    "data": results,
                    "metric": metric["func"],
                    "dimension": dimension,
                    "confidence": 0.9,
                    "show_data": True,
                    "limit": 10,
                    "offset": 0,
                    "page": 1,
                    "count": len(results),
                    "total_count": len(results)
                }
            else:
                # Total overall
                sql = f"SELECT SUM(JUMLAH) as total_qty, SUM(BERAT_TOTAL) as total_berat FROM penjualan WHERE {where_clause}"
                results = self.db.execute_query(sql, tuple(params))
                
                if not results or not results[0].get("total_qty"):
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }
                
                total = results[0].get("total_qty", 0)
                msg = f"Total penjualan adalah: {total:,.0f}."
                
                return {
                    "query_type": "comparative",
                    "message": msg,
                    "data": results,
                    "metric": metric["func"],
                    "confidence": 0.9,
                    "show_data": True,
                    "limit": 10,
                    "offset": 0,
                    "page": 1,
                    "count": len(results),
                    "total_count": len(results)
                }
        
        except Exception as e:
            return {
                "query_type": "comparative",
                "message": "Data tidak ditemukan.",
                "data": [],
                "error": str(e),
            }

    def _query_count_transactions(self, metric: Dict[str, Any], dimension: Optional[str], filters: Dict[str, Any], top_n: int, user_msg: str) -> Dict[str, Any]:
        """Query transaction count with optional grouping."""
        try:
            where_clause, params = SalesQueryBuilder.build_where_clause(filters)
            
            if dimension:
                # Count per dimension - ranking style
                sql = f"""
                    SELECT
                        {dimension} as kategori,
                        COUNT(*) as transaction_count,
                        SUM(JUMLAH) as total_qty
                    FROM penjualan
                    WHERE {where_clause}
                    GROUP BY {dimension}
                    ORDER BY transaction_count DESC
                    LIMIT ?
                """
                params_list = list(params) + [top_n]
                results = self.db.execute_query(sql, tuple(params_list))
                
                if not results:
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }
                
                if top_n > 1:
                    # Top N format
                    items = []
                    for r in results:
                        item = r.get("kategori")
                        if item:
                            # Map month number to name if dimension is BULAN
                            if dimension == "BULAN":
                                item = self._month_number_to_name(int(item))
                            items.append(str(item))
                    
                    if not items:
                        return {
                            "query_type": "comparative",
                            "message": "Data tidak ditemukan.",
                            "data": [],
                            "confidence": 0.7,
                        }
                    
                    items_str = ", ".join(items)
                    dimension_label = self._format_dimension_label(dimension)
                    msg = f"Top {top_n} {dimension_label} paling sering terjual: {items_str}."
                else:
                    # Single top format
                    top_item = results[0]
                    kategori = top_item.get("kategori")
                    count = top_item.get("transaction_count", 0)
                    
                    # Handle None/empty entity
                    if not kategori:
                        return {
                            "query_type": "comparative",
                            "message": "Data tidak ditemukan.",
                            "data": [],
                            "confidence": 0.7,
                        }
                    
                    # Map month number to name if dimension is BULAN
                    if dimension == "BULAN":
                        kategori = self._month_number_to_name(int(kategori))
                    
                    dimension_label = self._format_dimension_label(dimension)
                    msg = f"{dimension_label} yang paling sering terjual adalah: {kategori} ({count} transaksi)."
                
                return {
                    "query_type": "comparative",
                    "message": msg,
                    "data": results,
                    "metric": "COUNT",
                    "dimension": dimension,
                    "top_n": top_n,
                    "confidence": 0.9,
                    "show_data": True,
                    "limit": 10,
                    "offset": 0,
                    "page": 1,
                    "count": len(results),
                    "total_count": len(results)
                }
            else:
                # Overall count
                sql = f"SELECT COUNT(*) as total_count FROM penjualan WHERE {where_clause}"
                results = self.db.execute_query(sql, tuple(params))
                
                if not results:
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }
                
                total = results[0].get("total_count", 0)
                msg = f"Total transaksi adalah: {total:,}."
                
                return {
                    "query_type": "comparative",
                    "message": msg,
                    "data": results,
                    "metric": "COUNT",
                    "confidence": 0.9,
                    "show_data": True,
                    "limit": 10,
                    "offset": 0,
                    "page": 1,
                    "count": len(results),
                    "total_count": len(results)
                }
        
        except Exception as e:
            return {
                "query_type": "comparative",
                "message": "Data tidak ditemukan.",
                "data": [],
                "error": str(e),
            }

    def _query_average_value(self, metric: Dict[str, Any], dimension: Optional[str], filters: Dict[str, Any], user_msg: str) -> Dict[str, Any]:
        """Query average values with optional grouping."""
        try:
            where_clause, params = SalesQueryBuilder.build_where_clause(filters)
            
            # Determine what to average
            avg_field = "BERAT_SATUAN"
            if re.search(r"\b(jumlah|qty|quantity)\b", user_msg):
                avg_field = "JUMLAH"
            
            if dimension:
                # Average per dimension
                sql = f"""
                    SELECT
                        {dimension} as kategori,
                        AVG({avg_field}) as avg_value,
                        COUNT(*) as transaction_count
                    FROM penjualan
                    WHERE {where_clause}
                    GROUP BY {dimension}
                    ORDER BY avg_value DESC
                    LIMIT 1
                """
                results = self.db.execute_query(sql, tuple(params))
                
                if not results:
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }
                
                top_item = results[0]
                kategori = top_item.get("kategori")
                avg_val = top_item.get("avg_value", 0)
                
                # Handle None/empty entity
                if not kategori:
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.7,
                    }
                
                # Map month number to name if dimension is BULAN
                if dimension == "BULAN":
                    kategori = self._month_number_to_name(int(kategori))
                
                unit = "g" if avg_field == "BERAT_SATUAN" else "unit"
                dimension_label = self._format_dimension_label(dimension)
                msg = f"{dimension_label} dengan rata-rata {avg_field.lower()} tertinggi adalah: {kategori} ({avg_val:.2f} {unit})."
                
                return {
                    "query_type": "comparative",
                    "message": msg,
                    "data": results,
                    "metric": "AVG",
                    "dimension": dimension,
                    "confidence": 0.9,
                }
            else:
                # Overall average
                sql = f"SELECT AVG({avg_field}) as avg_value FROM penjualan WHERE {where_clause}"
                results = self.db.execute_query(sql, tuple(params))
                
                if not results or results[0].get("avg_value") is None:
                    return {
                        "query_type": "comparative",
                        "message": "Data tidak ditemukan.",
                        "data": [],
                        "confidence": 0.8,
                    }
                
                avg_val = results[0].get("avg_value", 0)
                unit = "g" if avg_field == "BERAT_SATUAN" else "unit"
                msg = f"Rata-rata {avg_field.lower()} adalah: {avg_val:.2f} {unit}."
                
                return {
                    "query_type": "comparative",
                    "message": msg,
                    "data": results,
                    "metric": "AVG",
                    "confidence": 0.9,
                }
        
        except Exception as e:
            return {
                "query_type": "comparative",
                "message": "Data tidak ditemukan.",
                "data": [],
                "error": str(e),
            }

    def _query_ranking(self, metric: Dict[str, Any], dimension: Optional[str], filters: Dict[str, Any], top_n: int, user_msg: str) -> Dict[str, Any]:
        """Query ranking/top N results."""
        try:
            where_clause, params = SalesQueryBuilder.build_where_clause(filters)
            
            if not dimension:
                dimension = "KODE_BARANG"  # Default to product ranking
            
            # Determine what metric to rank by
            if re.search(r"\b(berat|weight)\b", user_msg):
                order_field = "AVG(BERAT_SATUAN)"
            elif re.search(r"\b(terbanyak|paling|tertinggi|terbesar|paling sering)\b", user_msg):
                order_field = "SUM(JUMLAH)"
            else:
                order_field = "COUNT(*)"
            
            sql = f"""
                SELECT
                    {dimension} as kategori,
                    COUNT(*) as transaction_count,
                    SUM(JUMLAH) as total_qty,
                    AVG(BERAT_SATUAN) as avg_berat
                FROM penjualan
                WHERE {where_clause}
                GROUP BY {dimension}
                ORDER BY {order_field} DESC
                LIMIT ?
            """
            params_list = list(params) + [top_n]
            results = self.db.execute_query(sql, tuple(params_list))
            
            if not results:
                return {
                    "query_type": "comparative",
                    "message": "Data tidak ditemukan.",
                    "data": [],
                    "confidence": 0.8,
                }
            
            items = []
            for r in results:
                item = r.get("kategori")
                if item:
                    # Map month number to name if dimension is BULAN
                    if dimension == "BULAN":
                        item = self._month_number_to_name(int(item))
                    items.append(str(item))
            
            if not items:
                return {
                    "query_type": "comparative",
                    "message": "Data tidak ditemukan.",
                    "data": [],
                    "confidence": 0.7,
                }
            
            items_str = ", ".join(items)
            dimension_label = self._format_dimension_label(dimension)
            
            msg = f"Top {top_n} {dimension_label}: {items_str}."
            
            return {
                "query_type": "comparative",
                "message": msg,
                "data": results,
                "metric": "RANKING",
                "dimension": dimension,
                "top_n": top_n,
                "confidence": 0.9,
            }
        
        except Exception as e:
            return {
                "query_type": "comparative",
                "message": "Data tidak ditemukan.",
                "data": [],
                "error": str(e),
            }

    def _query_daily_average(self, filters: Dict[str, Any], user_msg: str) -> Dict[str, Any]:
        """Query average transactions per day (no ranking/top date)."""
        try:
            where_clause, params = SalesQueryBuilder.build_where_clause(filters)
            
            # Count transactions per day, then calculate average
            sql = f"""
                SELECT
                    TANGGAL as tanggal,
                    COUNT(*) as daily_count
                FROM penjualan
                WHERE {where_clause}
                GROUP BY TANGGAL
            """
            daily_results = self.db.execute_query(sql, tuple(params))
            
            if not daily_results:
                return {
                    "query_type": "comparative",
                    "message": "Data tidak ditemukan.",
                    "data": [],
                    "confidence": 0.8,
                }
            
            counts = [float(r.get("daily_count", 0)) for r in daily_results if r.get("daily_count")]
            if not counts:
                return {
                    "query_type": "comparative",
                    "message": "Data tidak ditemukan.",
                    "data": [],
                    "confidence": 0.8,
                }
            
            avg_daily = sum(counts) / len(counts)
            # Format message to show only the average, no specific dates or rankings
            msg = f"Rata-rata jumlah transaksi per hari adalah: {avg_daily:.2f} transaksi/hari."
            
            return {
                "query_type": "comparative",
                "message": msg,
                "data": daily_results,
                "metric": "AVG_DAILY",
                "daily_average": avg_daily,
                "confidence": 0.9,
            }
        
        except Exception as e:
            return {
                "query_type": "comparative",
                "message": "Data tidak ditemukan.",
                "data": [],
                "error": str(e),
            }

    def _format_dimension_label(self, dimension: str) -> str:
        """Format dimension name to human-readable label."""
        labels = {
            "LOKASI": "Lokasi",
            "KODE_BARANG": "Barang",
            "CHANNEL": "Channel",
            "BULAN": "Bulan",
            "TAHUN": "Tahun",
            "WARNA_BARANG": "Warna",
            "UKURAN_BARANG": "Ukuran",
            "KLASIFIKASI_BARANG": "Klasifikasi",
            "TANGGAL": "Tanggal",
        }
        return labels.get(dimension, dimension)

    def _month_number_to_name(self, month_num: int) -> str:
        """Convert month number (1-12) to Indonesian month name."""
        months = [
            "Januari", "Februari", "Maret", "April", "Mei", "Juni",
            "Juli", "Agustus", "September", "Oktober", "November", "Desember"
        ]
        try:
            if 1 <= month_num <= 12:
                return months[month_num - 1]
        except Exception:
            pass
        return str(month_num)

    # ============================================================
    # LLM rewrite hook (NEW: pakai answer_mode, bukan gabungan)
    # ============================================================
    def _maybe_rewrite_with_llm(
        self,
        user_message: str,
        parsed_query: Dict[str, Any],
        response: Dict[str, Any],
    ) -> Dict[str, Any]:

        response["llm_used"] = False

        if not env_bool("USE_LLM", False):
            return response

        if generate_llm_answer is None:
            response["llm_error"] = "LLM module not available (import failed)."
            return response

        qtype = (response.get("query_type") or "").lower()
        # Skip LLM rewrite for GENERAL (already has LLM response) dan SUGGESTION (already has LLM response)
        if qtype not in ("detail", "summary"):
            return response

        # NEW: kalau answer_mode auto, jangan LLM (biar tidak keluar 3 blok)
        answer_mode = (parsed_query.get("answer_mode") or "auto").lower()
        if answer_mode == "auto":
            return response

        try:
            rewritten = generate_llm_answer(
                user_message=user_message,
                parsed_query=parsed_query,
                response=response,
                answer_mode=answer_mode,   # <-- NEW
            )

            if isinstance(rewritten, str) and rewritten.strip():
                response["message_llm"] = rewritten.strip()
                response["message"] = rewritten.strip()
                response["llm_used"] = True
            else:
                response["llm_error"] = "generate_llm_answer returned empty/None."
                response["llm_used"] = False

        except LLMUnavailable as e:
            response["llm_error"] = f"Ollama unavailable: {str(e)}"
            response["llm_used"] = False
            logger.exception("Ollama unavailable")

        except Exception as e:
            response["llm_error"] = f"LLM error: {str(e)}"
            response["llm_used"] = False
            logger.exception("LLM rewrite error")

        return response

    # ============================================================
    # Handlers
    # ============================================================
    def _handle_detail_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        filters = (parsed_query.get("filters") or {}).copy()
        confidence = parsed_query.get("confidence", 0.0)
        relative_warning = parsed_query.get("_relative_time_warning")

        limit = int(filters.pop("limit", 10) or 10)
        offset = int(filters.pop("offset", 0) or 0)
        
        # PENTING: Jangan batasi limit untuk CSV download
        # Hanya batasi kalau limit masih kecil (untuk display)
        if limit <= 1000:  # Display mode
            limit = max(1, min(limit, 50))
        # else: CSV download mode, no limit restriction
        
        offset = max(0, offset)

        try:
            count_sql, count_params = SalesQueryBuilder.build_count_query(filters)
            count_result = self.db.execute_query(count_sql, tuple(count_params))
            total_records = int(count_result[0].get("total", 0)) if count_result else 0

            detail_sql, detail_params = SalesQueryBuilder.build_detail_query(filters, limit=limit, offset=offset)
            results = self.db.execute_query(detail_sql, tuple(detail_params))

            if total_records > 0:
                filter_desc = self._describe_filters(filters)
                remaining = total_records - len(results)
                message = f"Ditemukan {total_records:,} data penjualan untuk {filter_desc}. Berikut {len(results)} data teratas."
                if remaining > 0:
                    message += f" (and {remaining:,} more…)"
            else:
                filter_desc = self._describe_filters(filters)
                message = f"❌ Tidak ada hasil{f' {filter_desc}' if filter_desc else ''}."

            if relative_warning:
                message = f"⚠️ {relative_warning}\n\n{message}"

            return {
                "query_type": "detail",
                "message": message,
                "data": results,
                "sql": detail_sql,
                "filters": filters,
                "confidence": confidence,
                "count": total_records,
                "displayed": len(results),
                "limit": limit,
                "offset": offset,
                "total_count": total_records,
                "next_offset": (offset + limit) if (offset + limit) < total_records else None,
                "prev_offset": max(0, offset - limit) if offset > 0 else None,
                "show_data": True,
                "page": (offset // limit) + 1 if limit > 0 else 1
            }

        except Exception as e:
            return {
                "query_type": "detail",
                "message": f"❌ Error: {str(e)}",
                "data": [],
                "sql": "",
                "filters": filters,
                "confidence": 0.0,
                "error": str(e),
            }
    
    def _handle_count_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        filters = (parsed_query.get("filters") or {}).copy()
        confidence = parsed_query.get("confidence", 0.0)

        filters.pop("limit", None)
        filters.pop("offset", None)

        try:
            count_sql, count_params = SalesQueryBuilder.build_count_query(filters)
            count_result = self.db.execute_query(count_sql, tuple(count_params))
            total = int(count_result[0].get("total", 0)) if count_result else 0

            filter_desc = self._describe_filters(filters)
            message = f"📊 Ditemukan {total:,} transaksi{f' {filter_desc}' if filter_desc else ''}."

            return {
                "query_type": "count",
                "message": message,
                "data": [],
                "sql": count_sql,
                "filters": filters,
                "confidence": confidence,
                "count": total,
            }

        except Exception as e:
            return {
                "query_type": "count",
                "message": f"❌ Error: {str(e)}",
                "data": [],
                "sql": "",
                "filters": filters,
                "confidence": 0.0,
                "error": str(e),
            }

    def _handle_summary_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        filters = parsed_query.get("filters") or {}
        group_by = parsed_query.get("group_by") or "KODE_BARANG"
        confidence = parsed_query.get("confidence", 0.0)

        try:
            sql, params = SalesQueryBuilder.build_summary_query(filters, group_by)
            results = self.db.execute_query(sql, tuple(params))

            if results:
                total_records = sum(int(r.get("count_records", 0) or 0) for r in results)
                total_qty = sum(float(r.get("total_jumlah", 0) or 0) for r in results)
                total_berat = sum(float(r.get("total_berat", 0) or 0) for r in results)

                avg_berat = (
                    sum((float(r.get("avg_berat", 0) or 0) * int(r.get("count_records", 0) or 0)) for r in results)
                    / total_records
                    if total_records else 0.0
                )

                message = (
                    f"📊 Ringkasan penjualan berdasarkan {group_by}\n"
                    f"Total transaksi: {total_records:,} | Total qty: {total_qty:,.0f} | Total berat: {total_berat:,.2f} g | Avg berat: {avg_berat:.2f} g/item"
                )
            else:
                message = "❌ Tidak ada data untuk ringkasan."

            return {
                "query_type": "summary",
                "message": message,
                "data": results,
                "sql": sql,
                "filters": filters,
                "confidence": confidence,
                "group_by": group_by,
                "count": len(results),
                "show_data": True,
                "limit": 10,
                "offset": 0,
                "page": 1,
                "total_count": len(results)
            }

        except Exception as e:
            return {
                "query_type": "summary",
                "message": f"❌ Error executing summary: {str(e)}",
                "data": [],
                "sql": "",
                "filters": filters,
                "confidence": 0.0,
                "error": str(e),
            }

    # ============================================================
    # Exploratory + Help + Unknown
    # ============================================================
    def _handle_exploratory_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        intent = parsed_query.get("exploratory_intent", {})
        ask_about = intent.get("ask_about", "data_overview")

        try:
            if ask_about == "year_range":
                return self._handle_year_range_query()
            if ask_about == "month_range":
                return self._handle_month_range_query()
            if ask_about == "available_codes":
                return self._handle_available_codes_query()
            if ask_about == "available_locations":
                return self._handle_available_locations_query()
            if ask_about == "available_channels":
                return self._handle_available_channels_query()
            return self._handle_data_overview_query()
        except Exception as e:
            return {
                "query_type": "exploratory",
                "message": f"❌ Error: {str(e)}",
                "data": [],
                "confidence": 0.0,
                "error": str(e),
            }

    def _handle_year_range_query(self) -> Dict[str, Any]:
        sql = "SELECT DISTINCT TAHUN FROM penjualan ORDER BY TAHUN"
        years = self.db.execute_query(sql)
        year_list = [str(r["TAHUN"]) for r in years if r.get("TAHUN") is not None]
        if year_list:
            return {
                "query_type": "exploratory",
                "message": f"📅 Tahun tersedia: {', '.join(year_list)} (rentang {year_list[0]} - {year_list[-1]})",
                "data": years,
                "confidence": 0.95,
            }
        return {"query_type": "exploratory", "message": "❌ Data tahun tidak ditemukan.", "data": [], "confidence": 0.7}

    def _handle_month_range_query(self) -> Dict[str, Any]:
        sql = "SELECT DISTINCT BULAN FROM penjualan ORDER BY BULAN"
        months = self.db.execute_query(sql)
        month_list = [int(r["BULAN"]) for r in months if r.get("BULAN") is not None]
        if month_list:
            return {
                "query_type": "exploratory",
                "message": f"📆 Bulan tersedia: {', '.join(map(str, month_list))}",
                "data": months,
                "confidence": 0.95,
            }
        return {"query_type": "exploratory", "message": "❌ Data bulan tidak ditemukan.", "data": [], "confidence": 0.7}

    def _handle_available_codes_query(self) -> Dict[str, Any]:
        sql = "SELECT DISTINCT KODE_BARANG FROM penjualan LIMIT 20"
        rows = self.db.execute_query(sql)
        codes = [r.get("KODE_BARANG") for r in rows if r.get("KODE_BARANG")]
        return {
            "query_type": "exploratory",
            "message": "🏷️ Contoh kode barang: " + (", ".join(codes) if codes else "Tidak ada"),
            "data": rows,
            "confidence": 0.95,
        }

    def _handle_available_locations_query(self) -> Dict[str, Any]:
        sql = "SELECT DISTINCT LOKASI FROM penjualan LIMIT 20"
        rows = self.db.execute_query(sql)
        locs = [r.get("LOKASI") for r in rows if r.get("LOKASI")]
        return {
            "query_type": "exploratory",
            "message": "📍 Contoh lokasi: " + (", ".join(locs) if locs else "Tidak ada"),
            "data": rows,
            "confidence": 0.95,
        }

    def _handle_available_channels_query(self) -> Dict[str, Any]:
        sql = "SELECT DISTINCT CHANNEL FROM penjualan ORDER BY CHANNEL"
        rows = self.db.execute_query(sql)
        channels = [str(r.get("CHANNEL")) for r in rows if r.get("CHANNEL") is not None]
        return {
            "query_type": "exploratory",
            "message": "📡 Channel tersedia: " + (", ".join(channels) if channels else "Tidak ada"),
            "data": rows,
            "confidence": 0.95,
        }

    def _handle_data_overview_query(self) -> Dict[str, Any]:
        sql = "SELECT COUNT(*) as total_records, MIN(TANGGAL) as min_date, MAX(TANGGAL) as max_date FROM penjualan"
        res = self.db.execute_query(sql)
        info = res[0] if res else {}
        return {
            "query_type": "exploratory",
            "message": f"📊 Total transaksi: {int(info.get('total_records', 0)):,} | Periode: {info.get('min_date')} - {info.get('max_date')}",
            "data": res,
            "confidence": 0.95,
        }

    def _handle_general_query(self, user_message: str) -> Dict[str, Any]:
        """
        Handle general chit-chat and greetings without database query.
        Use LLM to generate natural, contextual responses.
        """
        context = f"Anda adalah asisten chatbot penjualan perhiasan yang ramah dan profesional."
        
        llm_answer = None
        if generate_llm_answer:
            try:
                llm_answer = generate_llm_answer(
                    user_message=user_message,
                    context=context,
                    data_summary="",
                    answer_mode="natural"
                )
            except (LLMUnavailable, Exception):
                llm_answer = None
        
        if llm_answer:
            return {
                "query_type": "general",
                "message": llm_answer,
                "data": [],
                "confidence": 0.9,
            }
        
        # Fallback jika LLM tidak tersedia
        fallback_responses = {
            "halo": "Halo! 👋 Ada yang bisa saya bantu tentang penjualan perhiasan?",
            "hai": "Hai! 😊 Silakan tanya tentang data penjualan Anda.",
            "apa kabar": "Saya baik-baik saja! Bagaimana dengan Anda? Ada pertanyaan tentang penjualan?",
            "bisa apa": "Saya bisa membantu Anda dengan:\n- 📊 Ringkasan penjualan\n- 📈 Analisis per lokasi/produk\n- 💡 Saran strategi penjualan\n- 🔍 Pencarian data spesifik",
            "siapa kamu": "Saya adalah asisten AI untuk analisis penjualan perhiasan. Saya siap membantu Anda menganalisis data dan memberikan insight bisnis!",
        }
        
        normalized = (user_message or "").lower().strip()
        for key, resp in fallback_responses.items():
            if key in normalized:
                return {
                    "query_type": "general",
                    "message": resp,
                    "data": [],
                    "confidence": 0.85,
                }
        
        return {
            "query_type": "general",
            "message": "Terima kasih atas pesan Anda! 😊 Silakan tanya tentang data penjualan perhiasan Anda.",
            "data": [],
            "confidence": 0.75,
        }

    def _handle_suggestion_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle suggestion/recommendation requests with KPI-driven analysis.
        Generates context-specific recommendations based on multiple metrics.
        """
        filters = (parsed_query.get("filters") or {}).copy()
        confidence = parsed_query.get("confidence", 0.0)
        user_message = parsed_query.get("original", "")
        
        # Determine if user explicitly asked to display data
        # Check for keywords: "tampilkan", "lihat", "tunjukkan", "semua", "seluruh", "lengkap", "data"
        user_msg_lower = user_message.lower()
        show_all_data = any(word in user_msg_lower for word in [
            "tampilkan", "lihat", "tunjukkan", "tampil", 
            "semua", "seluruh", "lengkap", "all", 
            "data", "tabel", "table"
        ])
        
        # Extract pagination parameters
        display_limit = int(filters.pop("limit", 10) or 10)
        display_offset = int(filters.pop("offset", 0) or 0)
        
        # Determine query limit based on show_all_data
        # For KPI analysis, we need all data
        # For display, we show 10 per page (pagination handled by display_limit/display_offset)
        if show_all_data:
            # Query ALL data for complete KPI analysis
            query_limit = 9999999  # No practical limit
            query_offset = 0
        else:
            # Query only what's needed
            query_limit = min(display_limit, 100)
            query_offset = display_offset
        
        # Detect scope: product-specific, location-specific, or general
        has_kode_barang = "kode_barang" in filters
        has_lokasi = "lokasi" in filters
        scope = "product" if has_kode_barang else ("location" if has_lokasi else "general")
        
        # Check if data exists
        count_sql, count_params = SalesQueryBuilder.build_count_query(filters)
        count_result = self.db.execute_query(count_sql, tuple(count_params))
        total_records = int(count_result[0].get("total", 0)) if count_result else 0
        
        if total_records == 0:
            filter_desc = self._describe_filters(filters)
            return {
                "query_type": "suggestion",
                "message": f"❌ Tidak ada data {filter_desc} untuk memberikan saran.",
                "data": [],
                "confidence": confidence,
            }
        
        # Get data for KPI analysis and display
        detail_sql, detail_params = SalesQueryBuilder.build_detail_query(filters, limit=query_limit, offset=query_offset)
        results = self.db.execute_query(detail_sql, tuple(detail_params))
        
        # If show_all_data, extract paginated results for display (10 per page)
        # But keep all results for KPI analysis
        if show_all_data:
            # results contains ALL data, now paginate for display
            all_results_for_kpi = results.copy()
            display_data = results[display_offset:display_offset + display_limit]
        else:
            all_results_for_kpi = results
            display_data = results
        
        # Calculate KPI packet based on scope - use ALL data for accurate analysis
        kpi_packet = self._calculate_kpi_packet_for_suggestion(
            results=all_results_for_kpi,
            filters=filters,
            total_records=total_records,
            scope=scope
        )
                
        # Get LLM answer with KPI context
        llm_answer = None
        if generate_llm_answer:
            try:
                llm_answer = generate_llm_answer(
                    user_message=user_message,
                    parsed_query={"filters": filters, "confidence": confidence},
                    response={
                        "query_type": "suggestion",
                        "data": all_results_for_kpi,
                        "count": total_records,
                        "filters": filters,
                        "confidence": confidence,
                        "kpi_packet": kpi_packet,
                        "scope": scope,
                        "show_data": show_all_data
                    },
                    answer_mode="saran"
                )
            except (LLMUnavailable, Exception):
                llm_answer = None
        
        # Fallback message if LLM not available
        if not llm_answer:
            llm_answer = self._fallback_suggestion_message(kpi_packet, scope)
        
        # Return: data untuk display (sudah paginated if show_all_data), total_count untuk pagination UI
        return_data = display_data if show_all_data else []
        
        return {
            "query_type": "suggestion",
            "message": llm_answer,
            "data": return_data,
            "filters": filters,
            "confidence": confidence,
            "count": total_records,  # Total count for pagination
            "displayed": len(return_data),
            "scope": scope,
            "kpi_analysis": kpi_packet,
            "show_data": show_all_data
        }


    def _build_data_summary_for_llm(self, results: List[Dict[str, Any]], filters: Dict[str, Any], total_count: int) -> str:
        """Build a summary of data for LLM context."""
        if not results:
            return f"Tidak ada data untuk filter: {filters}"
        
        summary = f"Total records: {total_count}\n"
        summary += f"Menampilkan: {len(results)} data teratas\n"
        summary += f"Filters: {filters}\n\n"
        summary += "Sample data:\n"
        
        for i, row in enumerate(results[:5], 1):
            summary += f"{i}. {dict(row)}\n"
        
        return summary

    def _calculate_kpi_packet_for_suggestion(self, results: List[Dict[str, Any]], filters: Dict[str, Any], total_records: int, scope: str) -> Dict[str, Any]:
        """
        Calculate comprehensive KPI packet for suggestion generation.
        Includes transaction count, trend, contribution %, and channel analysis.
        """
        kpi_packet = {
            "scope": scope,
            "total_transactions": total_records,
            "period_coverage": self._detect_period_from_results(results),
            "transaction_count": total_records,
            "unit_total": 0,
            "weight_total_g": 0,
            "channels": {},
            "top_items": [],
            "trend_vs_previous": 0,  # percentage
            "contribution_pct": 0,
            "dominant_channel": None,
        }
        
        if not results:
            return kpi_packet
        
        # Calculate basic metrics
        unit_total = 0
        weight_total = 0
        channels_dict = {}
        
        for row in results:
            try:
                unit_total += float(row.get("JUMLAH", 0) or 0)
                weight_total += float(row.get("BERAT_TOTAL", 0) or 0)
                channel = row.get("CHANNEL", "Unknown")
                if channel not in channels_dict:
                    channels_dict[channel] = 0
                channels_dict[channel] += 1
            except (ValueError, TypeError):
                pass
        
        kpi_packet["unit_total"] = round(unit_total, 2)
        kpi_packet["weight_total_g"] = round(weight_total, 2)
        kpi_packet["channels"] = channels_dict
        
        # Determine dominant channel
        if channels_dict:
            dominant_channel = max(channels_dict.items(), key=lambda x: x[1])
            kpi_packet["dominant_channel"] = dominant_channel[0]
            kpi_packet["dominant_channel_count"] = dominant_channel[1]
            kpi_packet["dominant_channel_pct"] = round((dominant_channel[1] / total_records) * 100, 1)
        
        # Extract top items by frequency
        item_counts = {}
        location_counts = {}
        for row in results:
            kode = row.get("KODE_BARANG")
            lokasi = row.get("LOKASI")
            if kode:
                item_counts[kode] = item_counts.get(kode, 0) + 1
            if lokasi:
                location_counts[lokasi] = location_counts.get(lokasi, 0) + 1
        
        # Sort top items/locations
        top_items_list = sorted(item_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        top_locations_list = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        kpi_packet["top_items"] = [{"kode_barang": k, "count": v, "pct": round((v/total_records)*100, 1)} for k, v in top_items_list]
        kpi_packet["top_locations"] = [{"lokasi": k, "count": v, "pct": round((v/total_records)*100, 1)} for k, v in top_locations_list]
        
        # Calculate trend (simplified: compare first half vs second half of period)
        trend = self._calculate_trend_analysis(results)
        kpi_packet["trend_vs_previous"] = trend["change_pct"]
        kpi_packet["trend_direction"] = trend["direction"]
        kpi_packet["trend_growth"] = trend["is_growing"]
        
        return kpi_packet

    def _detect_period_from_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Detect date/period range from result set."""
        if not results:
            return {"start": None, "end": None, "days": 0}
        
        dates = []
        for row in results:
            tanggal = row.get("TANGGAL")
            if tanggal:
                dates.append(str(tanggal))
        
        if not dates:
            return {"start": None, "end": None, "days": 0}
        
        dates_sorted = sorted(dates)
        return {
            "start": dates_sorted[0],
            "end": dates_sorted[-1],
            "days": len(set(dates_sorted))
        }

    def _calculate_trend_analysis(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate trend by comparing first half vs second half of transactions.
        Returns change percentage and direction.
        """
        if len(results) < 2:
            return {"change_pct": 0, "direction": "stable", "is_growing": False}
        
        mid = len(results) // 2
        first_half = results[:mid]
        second_half = results[mid:]
        
        # Sum weights for comparison
        first_half_weight = sum(float(r.get("BERAT_TOTAL", 0) or 0) for r in first_half)
        second_half_weight = sum(float(r.get("BERAT_TOTAL", 0) or 0) for r in second_half)
        
        if first_half_weight == 0:
            return {"change_pct": 0, "direction": "new", "is_growing": True}
        
        change_pct = ((second_half_weight - first_half_weight) / first_half_weight) * 100
        
        direction = "growing" if change_pct > 5 else ("declining" if change_pct < -5 else "stable")
        is_growing = change_pct > 0
        
        return {
            "change_pct": round(change_pct, 1),
            "direction": direction,
            "is_growing": is_growing
        }

    def _fallback_suggestion_message(self, kpi_packet: Dict[str, Any], scope: str) -> str:
        """
        Generate fallback suggestion message with proper number formatting.
        """
        scope_label = {
            "product": "Produk",
            "location": "Lokasi",
            "general": "Penjualan"
        }.get(scope, "Data")
        
        lines = []
        lines.append("Saran Berdasarkan Analisis Data")
        lines.append("")
        
        # Format numbers with max 2 decimals
        total_transaksi = format_number(kpi_packet['total_transactions'], decimals=0)
        total_berat = format_number(kpi_packet['weight_total_g'], decimals=2)
        
        lines.append(f"Scope           : {scope_label}")
        lines.append(f"Total Transaksi : {total_transaksi}")
        lines.append(f"Total Berat     : {total_berat} g")
        
        if kpi_packet.get("dominant_channel"):
            channel_name = str(kpi_packet['dominant_channel'])
            channel_pct = format_percentage(kpi_packet.get('dominant_channel_pct', 0), decimals=1)
            lines.append(f"Channel Dominan : {channel_name} ({channel_pct})")
        
        lines.append("")
        lines.append("Analisis dan Rekomendasi:")
        lines.append("")
        
        if scope == "product":
            top_item = kpi_packet['top_items'][0] if kpi_packet['top_items'] else None
            if top_item:
                item_pct = format_percentage(top_item['pct'], decimals=1)
                lines.append(f"Berdasarkan data, produk {top_item['kode_barang']} memiliki kontribusi sebesar {item_pct} dari total transaksi yang terjadi. Produk ini menunjukkan peran penting dalam portofolio penjualan Anda.")
            
            trend_pct = format_percentage(abs(kpi_packet['trend_vs_previous']), decimals=1)
            if kpi_packet.get('trend_growth'):
                lines.append(f"Tren penjualan menunjukkan peningkatan sebesar {trend_pct} dibandingkan dengan periode sebelumnya. Hal ini merupakan sinyal positif yang menunjukkan akselerasi penjualan. Disarankan untuk mempertahankan momentum ini dengan menjaga konsistensi stok, memperkuat promosi, dan memastikan kepuasan pelanggan tetap tinggi.")
            else:
                lines.append(f"Tren penjualan mengalami penurunan sebesar {trend_pct} dibandingkan dengan periode sebelumnya. Kondisi ini memerlukan perhatian khusus untuk dapat memulihkan performa produk. Beberapa strategi yang dapat dipertimbangkan adalah revitalisasi produk melalui desain atau fitur baru, melakukan bundling dengan produk lainnya, menyesuaikan strategi pemasaran, atau melakukan analisis mendalam tentang preferensi pasar saat ini.")
            
            lines.append(f"Pastikan strategi pemasaran disesuaikan dengan kondisi pasar terkini dan feedback dari pelanggan untuk hasil yang optimal.")
        
        elif scope == "location":
            top_loc = kpi_packet['top_locations'][0] if kpi_packet['top_locations'] else None
            if top_loc:
                loc_pct = format_percentage(top_loc['pct'], decimals=1)
                lines.append(f"Lokasi {top_loc['lokasi']} menunjukkan performa terbaik dengan kontribusi sebesar {loc_pct} dari total transaksi. Wilayah ini dapat dijadikan sebagai fokus utama pengembangan bisnis dan ekspansi lebih lanjut.")
            
            channel_name = str(kpi_packet.get('dominant_channel', 'N/A'))
            channel_pct = format_percentage(kpi_packet.get('dominant_channel_pct', 0), decimals=1)
            lines.append(f"Channel {channel_name} menunjukkan dominansi dengan {channel_pct} dari total transaksi di region ini. Ini menunjukkan preferensi pelanggan yang kuat terhadap channel penjualan tersebut.")
            
            lines.append(f"Untuk meningkatkan performa, optimalisasikan mix produk lokal sesuai dengan preferensi channel yang dominan. Pertimbangkan juga untuk melakukan inisiatif promosi khusus yang disesuaikan dengan karakteristik unik setiap lokasi dan channel.")
        
        else:  # general scope
            description_items = []
            if kpi_packet['top_items']:
                top_3_items = kpi_packet['top_items'][:3]
                item_list = ", ".join([f"{item['kode_barang']} ({format_percentage(item['pct'], 1)})" for item in top_3_items])
                description_items.append(f"Produk unggulan saat ini adalah {item_list}, yang bersama-sama menunjukkan momentum penjualan terkuat")
            
            unit_total = format_number(kpi_packet['unit_total'], decimals=0)
            description_items.append(f"total volume penjualan mencapai {unit_total} unit")
            
            channel_name = str(kpi_packet.get('dominant_channel', 'channel'))
            description_items.append(f"channel {channel_name} menunjukkan dominansi dalam penjualan")
            
            combined_desc = ", ".join(description_items)
            lines.append(f"Secara keseluruhan, {combined_desc}. Untuk mempercepat pertumbuhan, manfaatkan potensi channel dan produk unggulan ini sebagai fokus utama strategi ekspansi penjualan ke depan.")
        
        return "\n".join(lines)
    
    def _handle_help_query(self) -> Dict[str, Any]:
        message = (
            "📚 Bantuan Query\n\n"
            "Contoh:\n"
            "- Tampilkan penjualan MP000197 bulan 4 tahun 2022\n"
            "- Penjualan lokasi LO000048\n"
            "- Berat 5 sampai 10\n"
            "- Ringkasan per lokasi\n\n"
            "Kode:\n"
            "- KODE_BARANG: MP000xxx\n"
            "- LOKASI: LO000xxx\n"
            "- KLASIFIKASI: KD000xxx\n"
            "- WARNA: PL000xxx\n"
            "- UKURAN: SZ000xxx\n"
        )
        return {"query_type": "help", "message": message}

    def _handle_unknown_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        conf = parsed_query.get("confidence", 0.0)
        return {
            "query_type": "unknown",
            "message": (
                f"❓ Query belum terbaca (confidence: {conf:.0%}).\n"
                f"Coba sebut kode (MP000xxx/LO000xxx) atau minta 'ringkasan per lokasi'."
            ),
            "confidence": conf,
        }

    def _describe_filters(self, filters: Dict[str, Any]) -> str:
        if not filters:
            return "(all data)"
        parts = []

        if "kode_barang" in filters:
            parts.append(f"kode {filters['kode_barang']}")
        if "lokasi" in filters:
            parts.append(f"lokasi {filters['lokasi']}")
        if "bulan" in filters:
            parts.append(f"bulan {filters['bulan']}")
        if "tahun" in filters:
            parts.append(f"tahun {filters['tahun']}")
        if "channel" in filters:
            parts.append(f"channel {filters['channel']}")
        if "min_berat" in filters and "max_berat" in filters:
            parts.append(f"berat {filters['min_berat']}-{filters['max_berat']}g")
        elif "min_berat" in filters:
            parts.append(f"berat >={filters['min_berat']}g")
        elif "max_berat" in filters:
            parts.append(f"berat <={filters['max_berat']}g")

        return "(" + ", ".join(parts) + ")" if parts else "(all data)"


def create_bot(db_path: str) -> JewelrySalesBot:
    return JewelrySalesBot(db_path)
