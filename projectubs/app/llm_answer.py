# -*- coding: utf-8 -*-
"""
LLM Answer Rewriter (Ollama / Llama3)
- Rewrite hasil query DB jadi jawaban natural Bahasa Indonesia
- Aman: hanya pakai fakta dari hasil DB (bukan asumsi)
- Anti "debug leak": tidak menyebut JSON / prompt / SQL
"""

import os
import json
from typing import Any, Dict, List, Optional, Tuple

from app.llm_client import OllamaClient, LLMUnavailable


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str, indent=2)


def _pick(row: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    return {k: row.get(k) for k in keys if k in row}


def _sum_num(rows: List[Dict[str, Any]], key: str) -> float:
    total = 0.0
    for r in rows:
        v = r.get(key)
        if v is None:
            continue
        try:
            total += float(v)
        except Exception:
            pass
    return total


def _avg_num(rows: List[Dict[str, Any]], key: str) -> float:
    vals = []
    for r in rows:
        v = r.get(key)
        if v is None:
            continue
        try:
            vals.append(float(v))
        except Exception:
            pass
    return (sum(vals) / len(vals)) if vals else 0.0


def _minmax_str(rows: List[Dict[str, Any]], key: str) -> Tuple[Optional[str], Optional[str]]:
    vals = [str(r.get(key)) for r in rows if r.get(key)]
    if not vals:
        return None, None
    return min(vals), max(vals)


SYSTEM_PROMPT = """
Berperan sebagai asisten analis data penjualan perhiasan.

Aturan ketat:
1) Jawaban WAJIB hanya berdasarkan fakta pada data yang diberikan.
2) Jangan menyebut JSON/prompt/system/SQL.
3) Jangan mengarang angka/kode/periode.
4) Satuan berat selalu gram (g).
5) Output wajib pakai newline dan bullet '-'.

Format output WAJIB:

Ringkasan: <1–2 kalimat>

Insight:
- <insight 1>
- <insight 2>
- <insight 3>

Saran lanjutan:
- <saran 1>
- <saran 2>
""".strip()


def generate_llm_answer(user_message: str, parsed_query: Dict[str, Any], response: Dict[str, Any]) -> str:
    if not _env_bool("USE_LLM", "0"):
        return response.get("message", "")

    temp = _env_float("LLM_TEMPERATURE", 0.2)
    top_p = _env_float("LLM_TOP_P", 0.9)
    max_tokens = _env_int("LLM_MAX_TOKENS", 350)
    sample_n = _env_int("LLM_SAMPLE_ROWS", 8)

    query_type = (response.get("query_type") or "").lower()
    filters = response.get("filters", parsed_query.get("filters", {})) or {}
    confidence = float(parsed_query.get("confidence", response.get("confidence", 0.0)) or 0.0)

    data = response.get("data", [])
    if not isinstance(data, list):
        data = []

    facts: Dict[str, Any] = {
        "pertanyaan_user": user_message,
        "query_type": query_type,
        "filters": filters,
        "confidence": round(confidence, 3),
        "count": response.get("count", len(data)),
    }

    # DETAIL (alias FILTER)
    if query_type in ("detail", "filter"):
        cols = ["TANGGAL", "KODE_BARANG", "LOKASI", "CHANNEL", "BULAN", "TAHUN", "BERAT_SATUAN", "JUMLAH", "BERAT_TOTAL"]
        sample_rows = [_pick(r, cols) for r in data[: max(0, sample_n)]]

        tmin, tmax = _minmax_str(data, "TANGGAL")
        facts["ringkasan_data"] = {
            "periode_min": tmin,
            "periode_max": tmax,
            "total_jumlah": _sum_num(data, "JUMLAH"),
            "total_berat": _sum_num(data, "BERAT_TOTAL"),
            "avg_berat_satuan": round(_avg_num(data, "BERAT_SATUAN"), 4),
        }
        facts["contoh_baris"] = sample_rows

    # SUMMARY
    elif query_type == "summary":
        cols = ["kategori", "count_records", "total_jumlah", "total_berat", "avg_berat", "min_berat", "max_berat"]
        sample_rows = [_pick(r, cols) for r in data[: max(0, sample_n)]]

        facts["ringkasan_data"] = {
            "total_transaksi": _sum_num(data, "count_records"),
            "total_jumlah": _sum_num(data, "total_jumlah"),
            "total_berat": _sum_num(data, "total_berat"),
        }
        facts["top_ringkasan"] = sample_rows

    else:
        # tipe lain biarin fallback message
        return response.get("message", "")

    prompt = (
        "Gunakan fakta berikut untuk menjawab.\n"
        "Jika count = 0, jelaskan tidak ada data dan sarankan filter lain.\n\n"
        f"FAKTA:\n{_safe_json(facts)}\n"
    )

    client = OllamaClient()
    text = client.generate(
        prompt=prompt,
        system=SYSTEM_PROMPT,
        temperature=temp,
        top_p=top_p,
        max_tokens=max_tokens,
    )

    return (text or response.get("message", "")).strip()
