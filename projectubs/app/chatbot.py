# -*- coding: utf-8 -*-
"""
Chatbot Module for Jewelry Sales AI Assistant
- Query processing + SQL builder
- Optional LLM rewrite via Ollama (USE_LLM=1)
"""

import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Tuple

from app.nlp_parser import NLPParser, QueryType
from app.llm_answer import generate_llm_answer
from app.llm_client import LLMUnavailable


class SalesDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def execute_query(self, sql: str, params: tuple = ()) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()


class SalesQueryBuilder:
    @staticmethod
    def build_where_clause(filters: Dict[str, Any]) -> Tuple[str, List[Any]]:
        where = ["1=1"]
        params: List[Any] = []

        mapping = [
            ("kode_barang", "KODE_BARANG"),
            ("lokasi", "LOKASI"),
            ("bulan", "BULAN"),
            ("tahun", "TAHUN"),
            ("klasifikasi_barang", "KLASIFIKASI_BARANG"),
            ("warna_barang", "WARNA_BARANG"),
            ("ukuran_barang", "UKURAN_BARANG"),
            ("channel", "CHANNEL"),
        ]

        for key, col in mapping:
            if key in filters:
                where.append(f"{col} = ?")
                params.append(filters[key])

        if "min_berat" in filters:
            where.append("BERAT_SATUAN >= ?")
            params.append(filters["min_berat"])

        if "max_berat" in filters:
            where.append("BERAT_SATUAN <= ?")
            params.append(filters["max_berat"])

        return " AND ".join(where), params

    @staticmethod
    def build_detail_query(filters: Dict[str, Any], limit: int, offset: int) -> Tuple[str, tuple]:
        where_clause, params = SalesQueryBuilder.build_where_clause(filters)
        sql = f"""
            SELECT * FROM penjualan
            WHERE {where_clause}
            ORDER BY TANGGAL DESC, KODE_BARANG
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        return sql, tuple(params)

    @staticmethod
    def build_summary_query(filters: Dict[str, Any], group_by: str) -> Tuple[str, tuple]:
        where_clause, params = SalesQueryBuilder.build_where_clause(filters)

        valid = {
            "KODE_BARANG", "LOKASI", "BULAN", "TAHUN", "CHANNEL",
            "KLASIFIKASI_BARANG", "WARNA_BARANG", "UKURAN_BARANG"
        }
        if group_by not in valid:
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
        return sql, tuple(params)


class JewelrySalesBot:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.parser = NLPParser()
        self.db = SalesDatabase(db_path)
        self.conversation_history: List[Dict[str, Any]] = []

    def process_message(self, user_message: str) -> Dict[str, Any]:
        self.conversation_history.append({
            "timestamp": datetime.now().isoformat(),
            "role": "user",
            "message": user_message
        })

        parsed = self.parser.parse(user_message)
        qtype = parsed["query_type"]

        if qtype == QueryType.HELP:
            response = self._handle_help_query()
        elif qtype == QueryType.EXPLORATORY:
            response = self._handle_exploratory_query(parsed)
        elif qtype == QueryType.SUMMARY:
            response = self._handle_summary_query(parsed)
        elif qtype == QueryType.DETAIL:
            response = self._handle_detail_query(parsed)
        elif qtype == QueryType.COUNT:
            response = self._handle_count_query(parsed)
        else:
            response = self._handle_unknown_query(parsed)

        # Optional LLM rewrite (detail + summary)
        try:
            if response.get("query_type") in ("detail", "summary") and os.getenv("USE_LLM", "0") == "1":
                response["message_llm"] = generate_llm_answer(user_message, parsed, response)
                if response["message_llm"]:
                    response["message"] = response["message_llm"]
                    response["llm_used"] = True
                else:
                    response["llm_used"] = False
            else:
                response["llm_used"] = False
        except LLMUnavailable as e:
            response["llm_used"] = False
            response["llm_error"] = f"Ollama unavailable: {str(e)}"
        except Exception as e:
            response["llm_used"] = False
            response["llm_error"] = f"LLM error: {str(e)}"

        self.conversation_history.append({
            "timestamp": datetime.now().isoformat(),
            "role": "assistant",
            "message": response.get("message", "")
        })

        return response

    # -------------------- Exploratory handlers --------------------
    def _handle_exploratory_query(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        intent = parsed.get("exploratory_intent", {}) or {}
        ask = intent.get("ask_about", "data_overview")

        try:
            if ask == "year_range":
                sql = "SELECT DISTINCT TAHUN FROM penjualan ORDER BY TAHUN"
                rows = self.db.execute_query(sql)
                years = [str(r["TAHUN"]) for r in rows]
                msg = f"📅 Tahun tersedia: {', '.join(years)}" if years else "❌ Data tahun tidak ditemukan."
                return {"query_type": "exploratory", "message": msg, "data": rows, "confidence": 0.9}

            if ask == "month_range":
                sql = "SELECT DISTINCT BULAN FROM penjualan ORDER BY BULAN"
                rows = self.db.execute_query(sql)
                months = [str(r["BULAN"]) for r in rows]
                msg = f"📆 Bulan tersedia: {', '.join(months)}" if months else "❌ Data bulan tidak ditemukan."
                return {"query_type": "exploratory", "message": msg, "data": rows, "confidence": 0.9}

            if ask == "available_codes":
                sql = "SELECT DISTINCT KODE_BARANG FROM penjualan LIMIT 20"
                rows = self.db.execute_query(sql)
                msg = "🏷️ Contoh kode barang: " + ", ".join([r["KODE_BARANG"] for r in rows]) if rows else "❌ Tidak ada kode barang."
                return {"query_type": "exploratory", "message": msg, "data": rows, "confidence": 0.9}

            if ask == "available_locations":
                sql = "SELECT DISTINCT LOKASI FROM penjualan LIMIT 20"
                rows = self.db.execute_query(sql)
                msg = "📍 Contoh lokasi: " + ", ".join([r["LOKASI"] for r in rows]) if rows else "❌ Tidak ada lokasi."
                return {"query_type": "exploratory", "message": msg, "data": rows, "confidence": 0.9}

            if ask == "available_channels":
                sql = "SELECT DISTINCT CHANNEL FROM penjualan ORDER BY CHANNEL"
                rows = self.db.execute_query(sql)
                msg = "📡 Channel tersedia: " + ", ".join([str(r["CHANNEL"]) for r in rows]) if rows else "❌ Tidak ada channel."
                return {"query_type": "exploratory", "message": msg, "data": rows, "confidence": 0.9}

            # default overview
            sql = "SELECT COUNT(*) as total_records FROM penjualan"
            total = self.db.execute_query(sql)
            total_records = total[0]["total_records"] if total else 0
            msg = f"📊 Ringkasan data: total transaksi {total_records:,}"
            return {"query_type": "exploratory", "message": msg, "data": total, "confidence": 0.85}

        except Exception as e:
            return {"query_type": "exploratory", "message": f"❌ Error: {str(e)}", "data": [], "confidence": 0.0}

    # -------------------- Detail / Summary / Count --------------------
    def _handle_detail_query(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        filters = (parsed.get("filters") or {}).copy()
        confidence = parsed.get("confidence", 0.0)

        limit = int(filters.pop("limit", 100))
        offset = int(filters.pop("offset", 0))
        limit = min(limit, 2000)

        sql, params = SalesQueryBuilder.build_detail_query(filters, limit, offset)
        rows = self.db.execute_query(sql, params)

        if rows:
            msg = (
                f"✅ Ditemukan {len(rows)} transaksi.\n"
                f"📌 Filter: {filters if filters else '(tanpa filter)'}"
            )
        else:
            msg = (
                f"❌ Tidak ada hasil.\n"
                f"📌 Filter: {filters if filters else '(tanpa filter)'}"
            )

        return {
            "query_type": "detail",
            "message": msg,
            "data": rows,
            "filters": filters,
            "confidence": confidence,
            "count": len(rows),
            "sql": sql,
        }

    def _handle_summary_query(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        filters = parsed.get("filters", {}) or {}
        confidence = parsed.get("confidence", 0.0)
        group_by = parsed.get("group_by") or "KODE_BARANG"

        sql, params = SalesQueryBuilder.build_summary_query(filters, group_by)
        rows = self.db.execute_query(sql, params)

        msg = "📊 Ringkasan penjualan siap." if rows else "❌ Tidak ada data untuk ringkasan."
        return {
            "query_type": "summary",
            "message": msg,
            "data": rows,
            "filters": filters,
            "confidence": confidence,
            "group_by": group_by,
            "count": len(rows),
            "sql": sql,
        }

    def _handle_count_query(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        filters = parsed.get("filters", {}) or {}
        where, params = SalesQueryBuilder.build_where_clause(filters)
        sql = f"SELECT COUNT(*) as total FROM penjualan WHERE {where}"
        rows = self.db.execute_query(sql, tuple(params))
        total = rows[0]["total"] if rows else 0
        msg = f"🔢 Total transaksi: {total:,}"
        return {"query_type": "count", "message": msg, "data": rows, "filters": filters, "confidence": parsed.get("confidence", 0.0)}

    # -------------------- Help / Unknown --------------------
    def _handle_help_query(self) -> Dict[str, Any]:
        return {
            "query_type": "help",
            "message": (
                "📚 BANTUAN\n\n"
                "Contoh:\n"
                "- Tampilkan penjualan MP000197 bulan 4 tahun 2022\n"
                "- Ringkasan penjualan per lokasi\n"
                "- Berat 5 sampai 10\n"
                "- Ada berapa channel?\n"
            ),
        }

    def _handle_unknown_query(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        conf = parsed.get("confidence", 0.0)
        return {
            "query_type": "unknown",
            "message": (
                f"❓ Query belum kebaca (confidence {conf:.0%}).\n"
                "Coba sebut kode (MP000xxx/LO000xxx) atau minta 'ringkasan per lokasi'."
            ),
            "confidence": conf,
        }


def create_bot(db_path: str) -> JewelrySalesBot:
    return JewelrySalesBot(db_path)
