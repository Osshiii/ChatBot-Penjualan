#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test suggestion output formatting."""

import sys
from pathlib import Path

app_path = Path(__file__).parent / "app"
sys.path.insert(0, str(app_path.parent))

from app.chatbot import JewelrySalesBot, format_number, format_percentage

def test_number_formatting():
    """Test number and percentage formatting."""
    print("\n=== NUMBER FORMATTING TEST ===")
    
    # Test thousands separator
    assert format_number(22506, decimals=0, thousands_sep=True) == "22.506"
    assert format_number(1234.5, decimals=1, thousands_sep=True) == "1.234,5"
    assert format_number(102.0, decimals=1, thousands_sep=False) == "102,0"
    
    # Test percentage
    assert format_percentage(91.5, decimals=1) == "91,5%"
    assert format_percentage(0.0, decimals=1) == "0,0%"
    
    print("✓ Number formatting: 22506 → 22.506")
    print("✓ Decimal formatting: 1234.5 → 1.234,5")
    print("✓ Percentage formatting: 91.5 → 91,5%")


def test_fallback_suggestion_format():
    """Test fallback message format - no markdown, clean layout."""
    print("\n=== FALLBACK SUGGESTION FORMAT TEST ===")
    
    bot = JewelrySalesBot(db_path="data/sales.db")
    
    kpi_product = {
        "scope": "product",
        "total_transactions": 22506,
        "weight_total_g": 102.0,
        "unit_total": 500,
        "dominant_channel": "Tokopedia",
        "dominant_channel_pct": 65.3,
        "trend_vs_previous": -91.5,
        "trend_growth": False,
        "top_items": [
            {"kode_barang": "MP000197", "count": 10, "pct": 0.044},
        ],
        "top_locations": []
    }
    
    msg = bot._fallback_suggestion_message(kpi_product, "product")
    
    print("Output:")
    print(msg)
    print()
    
    # Verify format requirements
    assert "**" not in msg, "Should not contain markdown bold"
    assert "Scope           :" in msg, "Should have aligned format"
    assert "Total Transaksi" in msg, "Should have transaction count"
    assert "22.506" in msg, "Should format numbers with thousands separator"
    assert "102,0 g" in msg, "Should format decimals with comma"
    assert "Rekomendasi:" in msg, "Should have recommendation section"
    assert "MP000197" in msg, "Should mention specific product"
    assert "91,5%" in msg, "Should format percentage with comma"
    assert "-" in msg, "Should use bullet format"
    
    print("✓ No markdown formatting found")
    print("✓ Aligned key-value format present")
    print("✓ Numbers properly formatted (22.506, 102,0, 91,5%)")
    print("✓ Specific product mentioned with percentage")


def test_scope_aware_messages():
    """Test different scope messages."""
    print("\n=== SCOPE-AWARE MESSAGE TEST ===")
    
    bot = JewelrySalesBot(db_path="data/sales.db")
    
    # Product scope
    kpi_prod = {
        "scope": "product",
        "total_transactions": 1000,
        "weight_total_g": 500.5,
        "unit_total": 250,
        "dominant_channel": "Shopee",
        "dominant_channel_pct": 45.2,
        "trend_vs_previous": 15.3,
        "trend_growth": True,
        "top_items": [{"kode_barang": "MP000197", "count": 250, "pct": 25.0}],
        "top_locations": []
    }
    
    msg_prod = bot._fallback_suggestion_message(kpi_prod, "product")
    assert "MP000197" in msg_prod and "25,0%" in msg_prod
    assert "peningkatan" in msg_prod.lower() or "pertumbuhan" in msg_prod.lower()
    print("✓ Product scope: specific product with percentage and growth message")
    
    # Location scope
    kpi_loc = {
        "scope": "location",
        "total_transactions": 500,
        "weight_total_g": 250.0,
        "unit_total": 150,
        "dominant_channel": "Tokopedia",
        "dominant_channel_pct": 60.0,
        "trend_vs_previous": -10.0,
        "trend_growth": False,
        "top_items": [],
        "top_locations": [{"lokasi": "Jakarta", "count": 300, "pct": 60.0}]
    }
    
    msg_loc = bot._fallback_suggestion_message(kpi_loc, "location")
    assert "Jakarta" in msg_loc and "60,0%" in msg_loc
    assert "Tokopedia" in msg_loc
    print("✓ Location scope: specific location and channel dominance")
    
    # General scope
    kpi_gen = {
        "scope": "general",
        "total_transactions": 2000,
        "weight_total_g": 1000.0,
        "unit_total": 500,
        "dominant_channel": "Lazada",
        "dominant_channel_pct": 35.5,
        "trend_vs_previous": 5.2,
        "trend_growth": True,
        "top_items": [
            {"kode_barang": "MP000197", "count": 500, "pct": 25.0},
            {"kode_barang": "MP000198", "count": 400, "pct": 20.0},
        ],
        "top_locations": []
    }
    
    msg_gen = bot._fallback_suggestion_message(kpi_gen, "general")
    assert "MP000197" in msg_gen and "MP000198" in msg_gen
    assert "Lazada" in msg_gen
    print("✓ General scope: top products and channel")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("SUGGESTION OUTPUT FORMATTING TEST SUITE")
    print("="*60)
    
    try:
        test_number_formatting()
        test_fallback_suggestion_format()
        test_scope_aware_messages()
        
        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED - Format is clean and correct")
        print("="*60)
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
