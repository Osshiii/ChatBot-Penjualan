# -*- coding: utf-8 -*-
"""
NLP Parser for Jewelry Sales Chatbot (Indonesian)
- Detect intent (help / exploratory / detail / summary / count)
- Extract filters: kode_barang, lokasi, klasifikasi_barang, warna_barang, ukuran_barang,
  channel, bulan, tahun, min/max berat
- Support relative time: "bulan ini", "bulan lalu", "tahun ini"
- NEW: answer_mode (ringkasan / insight / saran / auto)
"""

from __future__ import annotations

import re
from enum import Enum
from datetime import datetime
from typing import Any, Dict, Optional, Tuple


class QueryType(str, Enum):
    HELP = "help"
    GENERAL = "general"
    EXPLORATORY = "exploratory"
    SUMMARY = "summary"
    DETAIL = "detail"
    COUNT = "count"
    EXACT_QUERY = "exact_query"
    COMPARATIVE = "comparative"
    LATEST_TRANSACTION = "latest_transaction"
    SUGGESTION = "suggestion"
    UNKNOWN = "unknown"

    # backward compat alias (kalau ada code lama)
    FILTER = "detail"


class AnswerMode(str, Enum):
    AUTO = "auto"
    RINGKASAN = "ringkasan"
    INSIGHT = "insight"
    SARAN = "saran"


_MONTH_MAP = {
    "januari": 1, "jan": 1,
    "februari": 2, "feb": 2,
    "maret": 3, "mar": 3,
    "april": 4, "apr": 4,
    "mei": 5,
    "juni": 6, "jun": 6,
    "juli": 7, "jul": 7,
    "agustus": 8, "agu": 8, "ags": 8,
    "september": 9, "sep": 9,
    "oktober": 10, "okt": 10,
    "november": 11, "nov": 11,
    "desember": 12, "des": 12,
}

HELP_KEYWORDS = {"bantuan", "help", "cara", "contoh", "panduan", "petunjuk"}
COUNT_HINTS = {"baris", "row", "rows", "transaksi", "record", "data", "jumlah transaksi"}

# General greeting/chit-chat keywords (no database needed)
GENERAL_KEYWORDS = {
    "halo", "hai", "hello", "hii", "hi", "pagi", "siang", "sore", "malam",
    "apa kabar", "gimana kabar", "siapa nama kamu", "siapa kamu", "apa nama mu", 
    "nama kamu siapa", "kamu siapa", "siapa nih", "kamu itu apa",
    "bisa apa", "apa saja yang bisa", "apa yang bisa", "kemampuan mu",
    "terima kasih", "thanks", "thank you", "makasih", "good day", "selamat",
    "sampai jumpa", "bye", "dada", "see you", "assalamualaikum", "wassalam",
    "apa yang sedang", "lagi apa", "sedang apa", "ok", "okay", "yes", "yap",
    "oke", "okie", "siap", "bagus", "good", "baik", "mantap", "sip"
}

# Aggregation & Comparative keywords (future-ready)
AGGREGATION_KEYWORDS = {"terbanyak", "paling", "terbesar", "tertinggi", "terendah", "rata-rata", "total", "sum", "count", "average", "avg", "top", "ranking", "urutan", "berapa total", "berapa banyak", "mana yang"}
COMPARATIVE_KEYWORDS = {"perbandingan", "bandingkan", "dibanding", "dibandingkan", "mana lebih", "perbedaan", "difference", "compare"}

# Suggestion keywords (saran bisnis)
SUGGESTION_KEYWORDS = {"saran", "rekomendasi", "masukan", "usul", "suggestion", "recommend", "tips", "strategi", "cara terbaik", "yang sebaiknya", "bagaimana cara", "gimana cara"}

# Latest transaction detection
LATEST_KEYWORDS = {"terbaru", "latest", "terkini", "paling baru", "terakhir"}

# Exact query keywords (specific column/date requests)
EXACT_QUERY_KEYWORDS = {"bulan apa", "tahun berapa", "tanggal berapa", "kapan", "pada tanggal", "pada bulan", "barang apa", "produk apa", "apa yang", "daftar", "sebutkan"}

EXPLORATORY_RULES = [
    (r"(kode barang|kode produk|produk)\s*(apa saja|yang ada|tersedia|daftar|list)", "available_codes"),
    (r"(lokasi|store|toko)\s*(apa saja|yang ada|tersedia|daftar|list)", "available_locations"),
    (r"(channel|chann?el|chennel|chenel|chanel)\s*(apa saja|yang ada|tersedia|daftar|list)", "available_channels"),
    (r"(ada\s+)?berapa\s+(channel|chann?el|chennel|chenel|chanel)\b", "available_channels"),
    (r"(tahun)\s*(apa saja|yang ada|tersedia|rentang|range)", "year_range"),
    (r"(bulan)\s*(apa saja|yang ada|tersedia|rentang|range)", "month_range"),
    (r"(data apa saja|overview|gambaran data|ringkasan data)", "data_overview"),
]


def extract_api_params(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Backward-compat helper."""
    return parsed.get("filters", {}) or {}


def _normalize_text(text: str) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"\bchennel\b", "channel", t)
    t = re.sub(r"\bchenel\b", "channel", t)
    t = re.sub(r"\bchanel\b", "channel", t)
    t = re.sub(r"\bchanell\b", "channel", t)
    t = re.sub(r"\s+", " ", t)
    return t


def _has_any_code(text: str) -> bool:
    return re.search(r"\b[a-z]{2}\d{6}\b", text, flags=re.I) is not None


def _parse_float(num_str: str) -> Optional[float]:
    if num_str is None:
        return None
    s = num_str.strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _month_from_text(text: str) -> Optional[int]:
    m = re.search(r"\bbulan(?:\s+ke)?\s*(\d{1,2})\b", text, flags=re.I)
    if m:
        try:
            v = int(m.group(1))
            if 1 <= v <= 12:
                return v
        except Exception:
            pass

    for name, num in _MONTH_MAP.items():
        if re.search(rf"\b{name}\b", text, flags=re.I):
            return num
    return None


def _year_from_text(text: str) -> Optional[int]:
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _extract_channel(text: str) -> Optional[int]:
    m = re.search(r"\bchannel\s*(\d{1,3})\b", text, flags=re.I)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _extract_codes(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    patterns = {
        "kode_barang": r"\bMP\d{6}\b",
        "lokasi": r"\bLO\d{6}\b",
        "klasifikasi_barang": r"\bKD\d{6}\b",
        "warna_barang": r"\bPL\d{6}\b",
        "ukuran_barang": r"\bSZ\d{6}\b",
    }
    for key, pat in patterns.items():
        m = re.search(pat, text, flags=re.I)
        if m:
            out[key] = m.group(0).upper()
    return out


def _extract_weight_range(text: str) -> Tuple[Optional[float], Optional[float]]:
    t = text.lower()

    m = re.search(
        r"\bberat(?:\s+satuan)?\s*([0-9]+(?:[.,][0-9]+)?)\s*(?:-|sampai|sd|s/d|hingga|to)\s*([0-9]+(?:[.,][0-9]+)?)",
        t
    )
    if m:
        return _parse_float(m.group(1)), _parse_float(m.group(2))

    m = re.search(
        r"\bberat(?:\s+satuan)?\s*(?:di\s+atas|lebih dari|>=|>)\s*([0-9]+(?:[.,][0-9]+)?)",
        t
    )
    if m:
        return _parse_float(m.group(1)), None

    m = re.search(
        r"\bberat(?:\s+satuan)?\s*(?:di\s+bawah|kurang dari|<=|<)\s*([0-9]+(?:[.,][0-9]+)?)",
        t
    )
    if m:
        return None, _parse_float(m.group(1))

    m = re.search(r"\bberat(?:\s+satuan)?\s*([0-9]+(?:[.,][0-9]+)?)", t)
    if m:
        v = _parse_float(m.group(1))
        return v, v

    return None, None


def _apply_relative_time(text: str) -> Tuple[Optional[int], Optional[int]]:
    now = datetime.now()
    bulan = None
    tahun = None

    if re.search(r"\btahun\s+ini\b", text):
        tahun = now.year

    if re.search(r"\bbulan\s+(ini|sekarang)\b", text):
        bulan = now.month
        tahun = tahun or now.year

    if re.search(r"\bbulan\s+lalu\b", text):
        if now.month == 1:
            bulan = 12
            tahun = (tahun or now.year) - 1
        else:
            bulan = now.month - 1
            tahun = tahun or now.year

    return bulan, tahun


def _extract_limit(text: str) -> Optional[int]:
    m = re.search(r"(\d{1,3})\s*(?:baris|data|row|rows|transaksi|record|penjualan)", text, flags=re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return None


def _extract_group_by(text: str) -> Optional[str]:
    t = text.lower()

    if re.search(r"\bringkasan\s+lokasi\b", t):
        return "LOKASI"
    if re.search(r"\bringkasan\s+(produk|kode barang|kode produk)\b", t):
        return "KODE_BARANG"

    if re.search(r"\b(per|berdasarkan)\s+lokasi\b", t):
        return "LOKASI"
    if re.search(r"\b(per|berdasarkan)\s+(produk|kode barang|kode produk)\b", t):
        return "KODE_BARANG"
    if re.search(r"\b(per|berdasarkan)\s+bulan\b", t):
        return "BULAN"
    if re.search(r"\b(per|berdasarkan)\s+tahun\b", t):
        return "TAHUN"
    if re.search(r"\b(per|berdasarkan)\s+channel\b", t):
        return "CHANNEL"
    if re.search(r"\b(per|berdasarkan)\s+warna\b", t):
        return "WARNA_BARANG"
    if re.search(r"\b(per|berdasarkan)\s+ukuran\b", t):
        return "UKURAN_BARANG"
    if re.search(r"\b(per|berdasarkan)\s+klasifikasi\b", t):
        return "KLASIFIKASI_BARANG"

    return None


def _detect_answer_mode(text: str) -> AnswerMode:
    """
    Pilih output yang diminta user:
    - ringkasan: hanya ringkasan
    - insight: hanya insight/analisis
    - saran: hanya saran/rekomendasi
    - auto: default
    """
    t = text.lower()

    # prioritas: saran > insight > ringkasan (biar "saran ringkasan" tetap saran)
    if re.search(r"\b(saran|rekomendasi|anjuran|next step|langkah selanjutnya|apa yang harus)\b", t):
        return AnswerMode.SARAN

    if re.search(r"\b(insight|analisis|analysis|tren|trend|kenapa|mengapa|sebab|paling|tertinggi|terendah|bandingkan|perbandingan)\b", t):
        return AnswerMode.INSIGHT

    if re.search(r"\b(ringkasan|summary|rekap|kesimpulan|resume)\b", t):
        return AnswerMode.RINGKASAN

    return AnswerMode.AUTO


class NLPParser:
    def parse(self, user_message: str) -> Dict[str, Any]:
        raw = user_message or ""
        text = _normalize_text(raw)

        filters: Dict[str, Any] = {}
        filters.update(_extract_codes(text))

        ch = _extract_channel(text)
        if ch is not None:
            filters["channel"] = ch

        mo = _month_from_text(text)
        yr = _year_from_text(text)

        rel_mo, rel_yr = _apply_relative_time(text)

        bulan = mo if mo is not None else rel_mo
        tahun = yr if yr is not None else rel_yr

        if bulan is not None:
            filters["bulan"] = bulan
        if tahun is not None:
            filters["tahun"] = tahun

        wmin, wmax = _extract_weight_range(text)
        if wmin is not None:
            filters["min_berat"] = wmin
        if wmax is not None:
            filters["max_berat"] = wmax

        limit = _extract_limit(text)
        if limit is not None:
            filters["limit"] = limit

        exploratory_intent = self._detect_exploratory_intent(text)
        query_type = self._detect_query_type(text, filters, exploratory_intent)
        group_by = _extract_group_by(text)
        confidence = self._estimate_confidence(query_type, filters, text)

        answer_mode = _detect_answer_mode(text)

        return {
            "query_type": query_type,
            "answer_mode": answer_mode.value,  # string biar gampang dipakai di luar
            "filters": filters,
            "group_by": group_by,
            "exploratory_intent": exploratory_intent,
            "confidence": confidence,
            "original": raw,
            "normalized": text,
        }

    def _detect_exploratory_intent(self, text: str) -> Dict[str, Any]:
        for pat, ask_about in EXPLORATORY_RULES:
            if re.search(pat, text, flags=re.I):
                return {"ask_about": ask_about}
        return {}

    def _detect_query_type(self, text: str, filters: Dict[str, Any], exploratory_intent: Dict[str, Any]) -> QueryType:
        # === CHECK DATA-INTENT FIRST (higher priority) ===
        if any(k in text for k in HELP_KEYWORDS):
            return QueryType.HELP

        # === CHECK SUGGESTION (data-driven advice) ===
        if any(k in text for k in SUGGESTION_KEYWORDS):
            return QueryType.SUGGESTION

        # Check for latest transaction (\"transaksi terbaru\")
        if any(k in text for k in LATEST_KEYWORDS) and "transaksi" in text:
            return QueryType.LATEST_TRANSACTION

        # Check for comparative/aggregation queries (future-ready, no calculation)
        has_agg = any(k in text for k in AGGREGATION_KEYWORDS)
        has_comp = any(k in text for k in COMPARATIVE_KEYWORDS)
        if has_agg or has_comp:
            return QueryType.COMPARATIVE

        # Check for exact queries (specific column/date requests)
        has_exact = any(k in text for k in EXACT_QUERY_KEYWORDS)
        if has_exact:
            return QueryType.EXACT_QUERY

        if any(k in text for k in ("ringkasan", "summary", "rekap", "agregat")):
            return QueryType.SUMMARY

        if "berapa" in text and any(h in text for h in COUNT_HINTS):
            return QueryType.COUNT

        if filters or _has_any_code(text) or any(k in text for k in ("tampilkan", "lihat", "cari", "show", "display")):
            return QueryType.DETAIL

        if "per " in text or "berdasarkan" in text:
            return QueryType.SUMMARY

        ask_about = (exploratory_intent or {}).get("ask_about")
        if ask_about:
            return QueryType.EXPLORATORY

        # === CHECK GENERAL LAST (only if no data-intent detected) ===
        if any(k in text for k in GENERAL_KEYWORDS):
            return QueryType.GENERAL

        return QueryType.UNKNOWN

    def _estimate_confidence(self, qtype: QueryType, filters: Dict[str, Any], text: str) -> float:
        if qtype == QueryType.HELP:
            return 0.95
        if qtype == QueryType.EXPLORATORY:
            return 0.85
        if qtype == QueryType.SUMMARY:
            return 0.85 if ("ringkasan" in text or "summary" in text) else 0.7
        if qtype == QueryType.COUNT:
            return 0.8
        if qtype == QueryType.DETAIL:
            base = 0.65
            bonus = min(0.35, 0.08 * len(filters))
            return round(base + bonus, 3)
        return 0.35
