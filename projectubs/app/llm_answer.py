# -*- coding: utf-8 -*-
"""
LLM Answer Rewriter (Ollama / Llama3)
- Rewrite hasil query DB jadi jawaban natural Bahasa Indonesia
- Aman: hanya pakai fakta dari hasil DB (bukan asumsi)
- Anti debug leak: tidak menyebut JSON / prompt / system / SQL
- Stats dihitung dulu di Python supaya LLM tidak "ngarang angka"
- NEW: answer_mode (ringkasan/insight/saran) -> output DIPISAH, tidak digabung
"""

import os
import json
import re
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


def _uniq(rows: List[Dict[str, Any]], key: str, limit: int = 30) -> List[Any]:
    out = []
    seen = set()
    for r in rows:
        v = r.get(key)
        if v is None or v == "":
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
        if len(out) >= limit:
            break
    return out


def _minmax_str(rows: List[Dict[str, Any]], key: str) -> Tuple[Optional[str], Optional[str]]:
    vals = [str(r.get(key)) for r in rows if r.get(key)]
    if not vals:
        return None, None
    return min(vals), max(vals)


def _system_prompt_for_mode(answer_mode: str) -> str:
    base = """
Berperan sebagai asisten analis data penjualan perhiasan.

Aturan ketat:
1) Jawaban WAJIB hanya berdasarkan fakta pada data yang diberikan.
2) Jangan menyebut JSON/prompt/system/SQL.
3) Jangan mengarang angka/kode/periode.
4) Jangan gunakan simbol mata uang (contoh: Rp, $, dll).
5) Satuan berat selalu gram (g). Jangan tulis kg.
6) Output wajib pakai newline dan bullet '-' (jangan paragraf panjang).
""".strip()

    mode = (answer_mode or "auto").lower()

    if mode == "ringkasan":
        return (base + """

Kamu HANYA boleh mengeluarkan bagian "Ringkasan" saja.
Dilarang menulis "Insight" atau "Saran".

Format WAJIB:
Ringkasan: <1–2 kalimat>
""").strip()

    if mode == "insight":
        return (base + """

Kamu HANYA boleh mengeluarkan bagian "Insight" saja.
Dilarang menulis "Ringkasan" atau "Saran".

Format WAJIB:
Insight:
- <insight 1>
- <insight 2>
- <insight 3>
""").strip()

    if mode == "saran":
        return (base + """

Kamu HANYA boleh mengeluarkan bagian "Saran lanjutan" saja.
Dilarang menulis "Ringkasan" atau "Insight".

Format WAJIB:
Saran lanjutan:
- <saran 1>
- <saran 2>
""").strip()

    # fallback (sebaiknya tidak dipakai)
    return (base + """

Jika mode tidak jelas, tulis jawaban singkat 2-4 bullet.
""").strip()


def _post_clean(text: str) -> str:
    if not text:
        return text
    text = text.replace("Rp", "").replace("rp", "")
    text = text.replace(" kg", " g").replace("KG", "g").replace("Kg", "g")
    return text.strip()


def _extract_only_mode_section(text: str, answer_mode: str) -> str:
    """
    Guardrail kalau model bandel dan tetap nulis 3 bagian.
    Kita potong sesuai mode.
    """
    t = (text or "").strip()
    mode = (answer_mode or "auto").lower()
    if not t:
        return t

    if mode == "ringkasan":
        # ambil baris yang dimulai "Ringkasan:"
        m = re.search(r"(Ringkasan:\s*.*)", t, flags=re.I)
        return (m.group(1).strip() if m else t)

    if mode == "insight":
        # ambil blok Insight sampai sebelum heading lain
        m = re.search(r"(Insight:\s*(?:\n-.*)+)", t, flags=re.I)
        return (m.group(1).strip() if m else t)

    if mode == "saran":
        m = re.search(r"(Saran lanjutan:\s*(?:\n-.*)+)", t, flags=re.I)
        return (m.group(1).strip() if m else t)

    return t


def generate_llm_answer(
    user_message: str,
    parsed_query: Dict[str, Any],
    response: Dict[str, Any],
    answer_mode: str = "auto",
) -> str:
    if not _env_bool("USE_LLM", "0"):
        return ""

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
        "tipe": query_type,
        "answer_mode": answer_mode,
        "filters": filters,
        "confidence": round(confidence, 3),
        "jumlah_baris": response.get("count", len(data)),
        "unit": {"berat": "gram", "jumlah": "unit", "transaksi": "baris"},
    }

    if query_type in ("detail", "filter"):
        cols = ["TANGGAL", "KODE_BARANG", "LOKASI", "CHANNEL", "BULAN", "TAHUN", "BERAT_SATUAN", "JUMLAH", "BERAT_TOTAL"]
        sample_rows = [_pick(r, cols) for r in data[: max(0, sample_n)]]

        tmin, tmax = _minmax_str(data, "TANGGAL")
        total_unit = _sum_num(data, "JUMLAH")
        total_berat_g = _sum_num(data, "BERAT_TOTAL")
        avg_berat_satuan_g = _avg_num(data, "BERAT_SATUAN")

        facts["ringkasan_data"] = {
            "periode_min": tmin,
            "periode_max": tmax,
            "unique_lokasi": _uniq(data, "LOKASI", 20),
            "unique_channel": _uniq(data, "CHANNEL", 20),
            "total_unit": round(total_unit, 3),
            "total_berat_gram": round(total_berat_g, 3),
            "rata2_berat_satuan_gram": round(avg_berat_satuan_g, 4),
        }
        facts["contoh_baris"] = sample_rows

    elif query_type == "summary":
        cols = ["kategori", "count_records", "total_jumlah", "total_berat", "avg_berat", "min_berat", "max_berat"]
        top_rows = [_pick(r, cols) for r in data[: max(0, sample_n)]]

        total_transaksi = _sum_num(data, "count_records")
        total_unit = _sum_num(data, "total_jumlah")
        total_berat_g = _sum_num(data, "total_berat")

        facts["ringkasan_data"] = {
            "total_transaksi": int(total_transaksi),
            "total_unit": round(total_unit, 3),
            "total_berat_gram": round(total_berat_g, 3),
        }
        facts["top_kategori"] = top_rows

    else:
        return response.get("message", "")

    prompt = (
        "Tulis jawaban berdasarkan FAKTA di bawah.\n"
        "Catatan: 'total_transaksi' adalah jumlah baris transaksi, bukan uang.\n"
        "Jika jumlah_baris = 0, jelaskan tidak ada data dan sarankan filter lain.\n"
        f"Mode jawaban: {answer_mode}\n\n"
        f"FAKTA:\n{_safe_json(facts)}\n"
    )

    system_prompt = _system_prompt_for_mode(answer_mode)

    client = OllamaClient()
    text = client.generate(
        prompt=prompt,
        system=system_prompt,
        temperature=temp,
        top_p=top_p,
        max_tokens=max_tokens,
    )

    cleaned = _post_clean(text or response.get("message", ""))
    cleaned = _extract_only_mode_section(cleaned, answer_mode)
    return cleaned
