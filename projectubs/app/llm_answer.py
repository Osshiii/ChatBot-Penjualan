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
7) JANGAN GUNAKAN BOLD (**text**), ITALIC, atau MARKDOWN. Gunakan teks polos saja.
8) Format angka:
   - Ribuan: pakai separator titik (1.000, 22.506)
   - Desimal: pakai koma dan MAKSIMAL 2 angka (102,5 atau 102,52)
   - Untuk angka besar (>100): TANPA desimal (1.234 bukan 1.234,00)
   - Untuk angka kecil (<1): maksimal 2 desimal (0,75)
   - Persentase: maksimal 1 desimal (15,3% bukan 15,345%)
9) Jangan tampilkan angka dengan banyak desimal seperti 0,0000123 atau 1.234,5678
""".strip()

    mode = (answer_mode or "auto").lower()

    if mode == "ringkasan":
        return (base + """

Kamu HANYA boleh mengeluarkan bagian "Ringkasan" saja.
Dilarang menulis "Insight" atau "Saran".

Format WAJIB:
Ringkasan: <1–2 kalimat>

PENTING - Format angka:
- Total transaksi: 1.234 (tanpa desimal)
- Total berat: 1.234,5 g (max 1 desimal)
- Rata-rata: 15,3 g (max 1 desimal)
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

PENTING - Format angka:
- Kontribusi: 25,5% (max 1 desimal)
- Jumlah transaksi: 1.234 (tanpa desimal)
- Berat: 102,5 g (max 1-2 desimal)
""").strip()

    if mode == "saran":
        return (base + """

Kamu HANYA boleh mengeluarkan bagian "Saran Bisnis" saja.
Dilarang menulis "Ringkasan" atau "Insight".

ATURAN SANGAT PENTING:
1) **JANGAN** gunakan angka, persentase, atau statistik APAPUN dalam saran!
2) **JANGAN** menyebut tren, peningkatan, penurunan, atau perubahan numerik
3) **JANGAN** tampilkan data raw, tabel, atau informasi kuantitatif
4) Fokus HANYA pada rekomendasi bisnis kualitatif yang bersifat strategis
5) Gunakan teks polos - jangan ada bold (**text**), italic, atau markdown

KONTEN YANG WAJIB DISERTAKAN:
Berikan rekomendasi strategis yang bersifat umum dan universal, seperti:
- Evaluasi produk/lokasi dalam konteks portfolio penjualan
- Saran untuk menyesuaikan strategi pemasaran
- Eksplorasi peluang bundling atau kerjasama strategis
- Analisis preferensi pelanggan dan feedback
- Pertimbangan repositioning atau revitalisasi
- Optimalisasi mix produk atau alokasi resource

CONTOH SARAN YANG BENAR:
- "Produk ini memerlukan evaluasi mendalam untuk memahami positioning-nya di pasar."
- "Pertimbangkan untuk menyesuaikan strategi pemasaran berdasarkan tren pasar terkini."
- "Eksplorasi peluang bundling dengan produk lain untuk meningkatkan daya tarik."

CONTOH SARAN YANG SALAH:
- "Produk ini berkontribusi 25,5% dari total transaksi..." (mengandung angka)
- "Tren penjualan meningkat sebesar 15,3%..." (mengandung persentase)
- "Total transaksi mencapai 1.234 unit..." (mengandung statistik)

TEMPLATE OUTPUT SARAN:
Saran Bisnis:
- <rekomendasi strategis tanpa angka atau persentase>
- <rekomendasi strategis tanpa angka atau persentase>
- <rekomendasi strategis tanpa angka atau persentase>
- <rekomendasi strategis tanpa angka atau persentase>
""").strip()

    return (base + """

Jika mode tidak jelas, tulis jawaban singkat 2-4 bullet dengan angka terformat dengan benar.
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

    elif query_type == "suggestion":
        # KPI-driven suggestion with detailed metrics
        kpi_packet = response.get("kpi_packet", {})
        scope = response.get("scope", "general")
        
        facts["kpi_analisis"] = {
            "scope": scope,
            "total_transaksi": kpi_packet.get("total_transactions", len(data)),
            "total_unit": kpi_packet.get("unit_total", 0),
            "total_berat_gram": kpi_packet.get("weight_total_g", 0),
            "channel_dominan": kpi_packet.get("dominant_channel"),
            "channel_dominan_pct": kpi_packet.get("dominant_channel_pct", 0),
            "tren_vs_sebelumnya_pct": kpi_packet.get("trend_vs_previous", 0),
            "tren_arah": kpi_packet.get("trend_direction", "stable"),
            "tren_tumbuh": kpi_packet.get("trend_growth", False),
            "top_items": kpi_packet.get("top_items", [])[:3],
            "top_lokasi": kpi_packet.get("top_locations", [])[:3],
            "periode_cakupan": kpi_packet.get("period_coverage", {})
        }
        
        # Include sample rows for reference ONLY if user didn't request data
        show_data = response.get("show_data", False)
        if not show_data:
            # Don't include raw data rows - LLM should focus on analysis only
            facts["catatan"] = "User hanya minta saran, bukan data. Jangan tampilkan tabel atau list data raw."
        else:
            cols = ["TANGGAL", "KODE_BARANG", "LOKASI", "CHANNEL", "JUMLAH", "BERAT_TOTAL"]
            sample_rows = [_pick(r, cols) for r in data[: max(0, sample_n)]]
            facts["contoh_baris_transaksi"] = sample_rows
        
        # NOTE: Continue to prompt generation below (don't return early)

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