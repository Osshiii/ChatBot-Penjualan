# -*- coding: utf-8 -*-
"""
LLM Answer Rewriter (Ollama / Llama3)
- Rewrite hasil query DB jadi jawaban natural Bahasa Indonesia
- Aman: hanya pakai fakta dari hasil DB (bukan asumsi)
- Anti "debug leak": tidak menyebut JSON / prompt / SQL
- Punya VALIDATOR + FALLBACK: kalau LLM ngaco/tidak mengikuti format -> pakai template Python
"""

import os
import json
from typing import Any, Dict, List, Optional, Tuple

from app.llm_client import OllamaClient, LLMUnavailable


# ---------------------------
# Helpers env
# ---------------------------
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


def _uniq(rows: List[Dict[str, Any]], key: str, limit: int = 50) -> List[Any]:
    out, seen = [], set()
    for r in rows:
        v = r.get(key)
        if v in (None, ""):
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


# ---------------------------
# Prompt (lebih keras)
# ---------------------------
SYSTEM_PROMPT = """
Berperan sebagai asisten analis data penjualan perhiasan.

ATURAN WAJIB:
1) Jawaban hanya berdasarkan fakta yang diberikan.
2) DILARANG menyebut JSON/prompt/system/SQL.
3) DILARANG mengarang angka/kode/periode.
4) Semua berat gunakan GRAM (g). Jangan tulis kg.
5) Output HARUS mengikuti format persis berikut (pakai newline dan bullet '-').

FORMAT:
Ringkasan: <1–2 kalimat>

Insight:
- <insight 1>
- <insight 2>
- <insight 3>

Saran lanjutan:
- <saran 1>
- <saran 2>
""".strip()


# ---------------------------
# Fallback formatter (kalau LLM ngaco)
# ---------------------------
def _fallback_answer(facts: Dict[str, Any]) -> str:
    qt = facts.get("query_type", "")
    count = int(facts.get("count") or 0)
    f = facts.get("filters") or {}
    unit = facts.get("unit") or {"berat": "gram", "jumlah": "unit"}

    # bikin deskripsi filter singkat
    parts = []
    if f.get("kode_barang"): parts.append(f"kode {f['kode_barang']}")
    if f.get("lokasi"): parts.append(f"lokasi {f['lokasi']}")
    if f.get("bulan"): parts.append(f"bulan {f['bulan']}")
    if f.get("tahun"): parts.append(f"tahun {f['tahun']}")
    if f.get("channel"): parts.append(f"channel {f['channel']}")
    filter_desc = (", ".join(parts)) if parts else "tanpa filter khusus"

    if count <= 0:
        return (
            f"Ringkasan: Tidak ada data yang cocok ({filter_desc}).\n\n"
            f"Insight:\n"
            f"- Jumlah hasil: 0\n"
            f"- Coba cek format kode (MP/LO/KD/PL/SZ) atau rentang waktu\n"
            f"- Gunakan filter lebih umum dulu\n\n"
            f"Saran lanjutan:\n"
            f"- Coba: \"data apa saja\" / \"tahun berapa\" / \"daftar lokasi\"\n"
            f"- Coba hapus salah satu filter (misalnya bulan/tahun)\n"
        )

    if qt == "filter":
        ring = facts.get("ringkasan_data") or {}
        tmin, tmax = ring.get("periode_min"), ring.get("periode_max")
        uniq_lok = ring.get("unique_lokasi") or []
        uniq_ch = ring.get("unique_channel") or []
        total_jumlah = ring.get("total_jumlah", 0.0)
        total_berat = ring.get("total_berat", 0.0)
        avg_berat = ring.get("avg_berat_satuan", 0.0)

        return (
            f"Ringkasan: Ditemukan {count} transaksi penjualan ({filter_desc})"
            f"{f' pada periode {tmin} s.d {tmax}' if (tmin and tmax) else ''}.\n\n"
            f"Insight:\n"
            f"- Total jumlah: {int(total_jumlah)} {unit['jumlah']}\n"
            f"- Total berat: {float(total_berat):.2f} {unit['berat']}\n"
            f"- Rata-rata berat satuan: {float(avg_berat):.2f} {unit['berat']}\n"
            f"- Lokasi unik: {len(uniq_lok)}\n"
            f"- Channel unik: {len(uniq_ch)}\n\n"
            f"Saran lanjutan:\n"
            f"- Tambahkan filter: \"bulan ...\" atau \"tahun ...\" atau \"lokasi ...\"\n"
            f"- Minta ringkasan: \"ringkasan penjualan per lokasi\" / \"per bulan\"\n"
        )

    if qt == "summary":
        ring = facts.get("ringkasan_data") or {}
        total_transaksi = int(ring.get("total_transaksi", 0))
        total_jumlah = ring.get("total_jumlah", 0.0)
        total_berat = ring.get("total_berat", 0.0)

        return (
            f"Ringkasan: Ringkasan penjualan ({filter_desc}) tersedia dalam bentuk agregasi.\n\n"
            f"Insight:\n"
            f"- Total transaksi (akumulasi kategori): {total_transaksi}\n"
            f"- Total jumlah: {int(total_jumlah)} {unit['jumlah']}\n"
            f"- Total berat: {float(total_berat):.2f} {unit['berat']}\n\n"
            f"Saran lanjutan:\n"
            f"- Ubah grouping: \"ringkasan per bulan\" / \"per lokasi\" / \"per channel\"\n"
            f"- Tambahkan filter kode/lokasi/tahun untuk fokus analisis\n"
        )

    # default
    return (
        f"Ringkasan: Berikut hasil berdasarkan data yang tersedia.\n\n"
        f"Insight:\n- Jumlah hasil: {count}\n- Filter: {filter_desc}\n- Silakan tambah filter untuk mempersempit\n\n"
        f"Saran lanjutan:\n- Tanyakan \"ringkasan\" untuk agregasi\n- Tanyakan \"data apa saja\" untuk eksplorasi\n"
    )


def _looks_valid_llm(text: str, count: int) -> bool:
    """Validasi output LLM: harus punya section + tidak kontradiksi count."""
    if not text:
        return False
    must_have = ["Ringkasan:", "Insight:", "Saran lanjutan:"]
    if any(x not in text for x in must_have):
        return False
    # minimal ada 2 bullet di Insight dan 1 bullet di saran
    if text.count("\n- ") < 3:
        return False
    # kalau count > 0 tapi LLM bilang tidak ada data -> invalid
    lowered = text.lower()
    if count > 0 and ("tidak ada data" in lowered or "tidak ditemukan" in lowered or "tidak ada transaksi" in lowered):
        return False
    return True


# ---------------------------
# Main
# ---------------------------
def generate_llm_answer(user_message: str, parsed_query: Dict[str, Any], response: Dict[str, Any]) -> str:
    """
    Rewrite response['message'] jadi jawaban natural pakai Ollama.
    Kalau Ollama error atau outputnya tidak valid -> fallback template Python.
    """
    if not _env_bool("USE_LLM", "0"):
        return response.get("message", "")

    temp = _env_float("LLM_TEMPERATURE", 0.2)
    top_p = _env_float("LLM_TOP_P", 0.9)
    max_tokens = _env_int("LLM_MAX_TOKENS", 350)
    sample_n = _env_int("LLM_SAMPLE_ROWS", 8)

    query_type = response.get("query_type", "")
    filters = response.get("filters", parsed_query.get("filters", {})) or {}
    confidence = float(parsed_query.get("confidence", response.get("confidence", 0.0)) or 0.0)

    data = response.get("data", [])
    if not isinstance(data, list):
        data = []

    count = int(response.get("count", len(data)) or 0)

    facts: Dict[str, Any] = {
        "pertanyaan_user": user_message,
        "query_type": query_type,
        "filters": filters,
        "confidence": round(confidence, 3),
        "count": count,
        "unit": {"berat": "gram", "jumlah": "unit"},
        "aturan": {
            "jika_count_0": "Wajib bilang tidak ada data",
            "jika_count_gt_0": "Dilarang bilang tidak ada data/ transaksi",
        },
    }

    if query_type == "filter":
        cols = ["TANGGAL", "KODE_BARANG", "LOKASI", "CHANNEL", "BULAN", "TAHUN", "BERAT_SATUAN", "JUMLAH", "BERAT_TOTAL"]
        sample_rows = [_pick(r, cols) for r in data[: max(0, sample_n)]]

        tmin, tmax = _minmax_str(data, "TANGGAL")
        facts["ringkasan_data"] = {
            "periode_min": tmin,
            "periode_max": tmax,
            "unique_lokasi": _uniq(data, "LOKASI", 50),
            "unique_channel": _uniq(data, "CHANNEL", 50),
            "total_jumlah": _sum_num(data, "JUMLAH"),
            "total_berat": _sum_num(data, "BERAT_TOTAL"),
            "avg_berat_satuan": round(_avg_num(data, "BERAT_SATUAN"), 4),
        }
        facts["contoh_baris"] = sample_rows

    elif query_type == "summary":
        cols = ["kategori", "count_records", "total_jumlah", "total_berat", "avg_berat", "min_berat", "max_berat"]
        sample_rows = [_pick(r, cols) for r in data[: max(0, sample_n)]]

        facts["ringkasan_data"] = {
            "total_transaksi": _sum_num(data, "count_records"),
            "total_jumlah": _sum_num(data, "total_jumlah"),
            "total_berat": _sum_num(data, "total_berat"),
        }
        facts["top_ringkasan"] = sample_rows

    # Prompt: paksa output sesuai template
    prompt = (
        "Gunakan FAKTA berikut untuk menulis jawaban.\n"
        "Output HARUS sesuai FORMAT yang diminta di SYSTEM.\n"
        "Kalau count > 0, jangan pernah bilang 'tidak ada data'.\n\n"
        f"FAKTA:\n{_safe_json(facts)}\n\n"
        "Tulis jawabannya sekarang."
    )

    # Call LLM
    try:
        client = OllamaClient()
        text = client.generate(
            prompt=prompt,
            system=SYSTEM_PROMPT,
            temperature=temp,
            top_p=top_p,
            max_tokens=max_tokens,
        )
        text = (text or "").strip()

        # validate, kalau gagal -> fallback
        if not _looks_valid_llm(text, count):
            return _fallback_answer(facts)

        return text

    except LLMUnavailable:
        return _fallback_answer(facts)
    except Exception:
        return _fallback_answer(facts)
