# -*- coding: utf-8 -*-
"""
Chatbot Module for Jewelry Sales AI Assistant
- Conversation logic + query processing + DB access
- Supports: help, exploratory, detail (rows), summary (aggregate), count
- Fix: relative time ("bulan ini", "bulan lalu", "tahun ini") based on latest data in DB
- Optional: LLM rewrite (USE_LLM=1) for detail/summary responses
- NEW: answer_mode -> ringkasan / insight / saran (output dipisah, tidak digabung)
"""

from __future__ import annotations

import os
import sqlite3
import logging
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

    def process_message(self, user_message: str) -> Dict[str, Any]:
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
            print(f"[DEBUG] Filters: {parsed_query.get('filters', {})}")
            print(f"[DEBUG] Group: {parsed_query.get('group_by')}")
            print(f"[DEBUG] Confidence: {parsed_query.get('confidence', 0):.2f}")

        qt = parsed_query["query_type"]

        if qt == QueryType.HELP:
            response = self._handle_help_query()
        elif qt == QueryType.DETAIL:
            response = self._handle_detail_query(parsed_query)
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

    # -------------------------
    # LLM rewrite hook (NEW: pakai answer_mode, bukan gabungan)
    # -------------------------
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
        limit = max(1, min(limit, 50))
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
                "total_records": total_records,
                "next_offset": (offset + limit) if (offset + limit) < total_records else None,
                "prev_offset": max(0, offset - limit) if offset > 0 else None
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
