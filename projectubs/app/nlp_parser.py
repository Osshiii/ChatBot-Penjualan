# -*- coding: utf-8 -*-
"""
Natural Language Parser for Jewelry Sales Queries (REFACTORED)
Implements comprehensive intent detection + filter extraction
"""

import re
from typing import Dict, List, Optional, Tuple
from enum import Enum
from datetime import datetime

class QueryType(Enum):
    """Query intent types"""
    DETAIL = "detail"           # Row-level data with filters
    COUNT = "count"             # Total count with filters
    SUMMARY = "summary"         # Aggregation with GROUP BY
    EXPLORATORY = "exploratory" # Distinct values, ranges
    HELP = "help"
    UNKNOWN = "unknown"
    
    # Backward compat
    FILTER = "detail"

class NLPParser:
    """
    Complete NLP parser with intent detection and filter extraction
    """
    
    # Month mapping
    MONTH_KEYWORDS = {
        "januari": 1, "februari": 2, "maret": 3, "april": 4,
        "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
        "september": 9, "oktober": 10, "november": 11, "desember": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "mei": 5,
        "jun": 6, "jul": 7, "aug": 8, "sep": 9, "okt": 10, "nov": 11, "des": 12,
    }
    
    # Intent keywords (priority order)
    HELP_KEYWORDS = ["bantuan", "help", "apa itu", "bagaimana", "cara"]
    
    COUNT_KEYWORDS = ["berapa", "jumlah", "total", "ada berapa"]
    
    SUMMARY_KEYWORDS = ["ringkasan", "summary", "per", "berdasarkan", "aggregat", "statistik"]
    
    EXPLORATORY_KEYWORDS = ["apa saja", "data apa", "dari tahun", "sampai tahun", "range", "ada berapa"]
    
    DETAIL_KEYWORDS = ["tampilkan", "lihat", "cari", "filter", "show", "find", "dengan"]
    
    # GROUP BY mapping (for summary queries)
    GROUP_BY_MAP = {
        "kode": "KODE_BARANG",
        "produk": "KODE_BARANG",
        "code": "KODE_BARANG",
        "lokasi": "LOKASI",
        "location": "LOKASI",
        "bulan": "BULAN",
        "month": "BULAN",
        "tahun": "TAHUN",
        "year": "TAHUN",
        "channel": "CHANNEL",
        "klasifikasi": "KLASIFIKASI_BARANG",
        "warna": "WARNA_BARANG",
        "color": "WARNA_BARANG",
        "ukuran": "UKURAN_BARANG",
        "size": "UKURAN_BARANG",
    }
    
    def __init__(self):
        """Initialize parser"""
        pass
    
    def parse(self, user_input: str) -> Dict:
        """
        Main parse method
        Returns: {query_type, filters, group_by, confidence, error}
        """
        input_lower = user_input.lower().strip()
        
        # Step 1: Detect intent
        query_type = self._detect_intent(input_lower)
        
        # Step 2: Extract all filters
        filters = self._extract_all_filters(input_lower)
        
        # Step 3: Validate filters
        is_valid, error_msg = self._validate_filters(filters)
        if not is_valid:
            return {
                "query_type": QueryType.UNKNOWN,
                "filters": {},
                "group_by": None,
                "confidence": 0.0,
                "error": error_msg,
                "original_input": user_input,
            }
        
        # Step 4: Extract GROUP BY (if summary)
        group_by = None
        if query_type == QueryType.SUMMARY:
            group_by = self._extract_group_by(input_lower)
        
        # Step 5: Calculate confidence
        confidence = self._calculate_confidence(query_type, filters, group_by)
        
        return {
            "query_type": query_type,
            "filters": filters,
            "group_by": group_by,
            "confidence": confidence,
            "error": None,
            "original_input": user_input,
        }
    
    # ==================== INTENT DETECTION ====================
    
    def _detect_intent(self, input_lower: str) -> QueryType:
        """
        Detect query intent with priority:
        HELP > COUNT > SUMMARY > DETAIL > EXPLORATORY > UNKNOWN
        """
        
        # HELP
        if any(kw in input_lower for kw in self.HELP_KEYWORDS):
            return QueryType.HELP
        
        # COUNT - "berapa" + filter context
        if any(kw in input_lower for kw in self.COUNT_KEYWORDS):
            if self._has_filter_indicators(input_lower):
                return QueryType.COUNT
        
        # SUMMARY - "ringkasan", "per", "berdasarkan"
        if any(kw in input_lower for kw in self.SUMMARY_KEYWORDS):
            return QueryType.SUMMARY
        
        # EXPLORATORY - "apa saja", "dari tahun", "range"
        if any(kw in input_lower for kw in self.EXPLORATORY_KEYWORDS):
            return QueryType.EXPLORATORY
        
        # DETAIL - "tampilkan", "cari", filter keyword + codes/numerics
        if any(kw in input_lower for kw in self.DETAIL_KEYWORDS):
            return QueryType.DETAIL
        
        # DETAIL - Implicit: has code patterns or numeric filters
        if self._has_code_patterns(input_lower) or self._has_numeric_filters(input_lower):
            return QueryType.DETAIL
        
        return QueryType.UNKNOWN
    
    def _has_filter_indicators(self, input_lower: str) -> bool:
        """Check if input has any filter indicators"""
        return (
            self._has_code_patterns(input_lower) or
            self._has_temporal_patterns(input_lower) or
            self._has_numeric_filters(input_lower)
        )
    
    def _has_code_patterns(self, input_lower: str) -> bool:
        """Check for code patterns (MP/LO/KD/PL/SZ + 6 digits)"""
        return bool(re.search(r'[a-z]{2}\d{6}', input_lower, re.IGNORECASE))
    
    def _has_temporal_patterns(self, input_lower: str) -> bool:
        """Check for year/month/date patterns"""
        return bool(re.search(r'(tahun|bulan|month|20\d{2}|\d{4}-\d{2}-\d{2})', input_lower))
    
    def _has_numeric_filters(self, input_lower: str) -> bool:
        """Check for numeric filters (berat, jumlah, channel)"""
        return bool(re.search(r'(berat|jumlah|quantity|channel)', input_lower))
    
    # ==================== FILTER EXTRACTION ====================
    
    def _extract_all_filters(self, input_lower: str) -> Dict:
        """
        Extract all filters from input
        Returns dict with: kode_barang, lokasi, klasifikasi_barang, warna_barang, 
                         ukuran_barang, bulan, tahun, date_from, date_to,
                         min_berat, max_berat, min_jumlah, max_jumlah, channel
        """
        filters = {}
        
        # Extract codes (case-insensitive, normalize uppercase)
        self._extract_codes(input_lower, filters)
        
        # Extract temporal filters
        self._extract_temporal(input_lower, filters)
        
        # Extract numeric filters
        self._extract_numeric(input_lower, filters)
        
        return filters
    
    def _extract_codes(self, input_lower: str, filters: Dict) -> None:
        """Extract code patterns"""
        code_patterns = [
            (r'(?i)\bmp(\d{6})\b', 'kode_barang'),
            (r'(?i)\blo(\d{6})\b', 'lokasi'),
            (r'(?i)\bkd(\d{6})\b', 'klasifikasi_barang'),
            (r'(?i)\bpl(\d{6})\b', 'warna_barang'),
            (r'(?i)\bsz(\d{6})\b', 'ukuran_barang'),
        ]
        
        for pattern, field in code_patterns:
            match = re.search(pattern, input_lower)
            if match:
                filters[field] = match.group(0).upper()  # Entire match, uppercase
    
    def _extract_temporal(self, input_lower: str, filters: Dict) -> None:
        """Extract date/month/year filters"""
        
        # Single date: YYYY-MM-DD
        date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', input_lower)
        if date_match:
            filters['date_from'] = date_match.group(0)
            # If only one date, also set as "to" for single-day queries
            filters['date_to'] = date_match.group(0)
        
        # Date range: "dari 2022-01-01 sampai 2022-12-31"
        range_match = re.search(
            r'(?:dari|from)?\s*(\d{4}-\d{2}-\d{2})\s+(?:sampai|hingga|s\.d|to)\s+(\d{4}-\d{2}-\d{2})',
            input_lower
        )
        if range_match:
            filters['date_from'] = range_match.group(1)
            filters['date_to'] = range_match.group(2)
        
        # Year: "tahun 2022"
        year_match = re.search(r'(?:tahun|year)\s+(20\d{2})', input_lower)
        if year_match:
            filters['tahun'] = int(year_match.group(1))
        
        # Month by number: "bulan 4"
        month_num_match = re.search(r'(?:bulan|month)\s+([1-9]|1[0-2])', input_lower)
        if month_num_match:
            filters['bulan'] = int(month_num_match.group(1))
        else:
            # Month by name
            for month_name, month_num in self.MONTH_KEYWORDS.items():
                if re.search(rf'\b{month_name}\b', input_lower):
                    filters['bulan'] = month_num
                    break
    
    def _extract_numeric(self, input_lower: str, filters: Dict) -> None:
        """Extract numeric range filters (berat, jumlah)"""
        
        # Pattern: "5-10" or "5 sampai 10"
        range_pattern = r'(\d+(?:\.\d+)?)\s*(?:-|sampai|hingga|to|s\.d)\s*(\d+(?:\.\d+)?)'
        
        # BERAT filters
        if 'berat' in input_lower:
            # Range: "berat 5-10"
            match = re.search(rf'berat\s+{range_pattern}', input_lower)
            if match:
                filters['min_berat'] = float(match.group(1))
                filters['max_berat'] = float(match.group(2))
            else:
                # Min: "berat minimal 5"
                min_match = re.search(r'berat\s+(?:minimal|min)\s+(\d+(?:\.\d+)?)', input_lower)
                if min_match:
                    filters['min_berat'] = float(min_match.group(1))
                
                # Max: "berat maksimal 10"
                max_match = re.search(r'berat\s+(?:maksimal|max)\s+(\d+(?:\.\d+)?)', input_lower)
                if max_match:
                    filters['max_berat'] = float(max_match.group(1))
        
        # JUMLAH filters
        if 'jumlah' in input_lower:
            match = re.search(rf'jumlah\s+{range_pattern}', input_lower)
            if match:
                filters['min_jumlah'] = float(match.group(1))
                filters['max_jumlah'] = float(match.group(2))
        
        # CHANNEL
        channel_match = re.search(r'channel\s+(\d+)', input_lower)
        if channel_match:
            filters['channel'] = int(channel_match.group(1))
    
    def _extract_group_by(self, input_lower: str) -> Optional[str]:
        """Extract GROUP BY field for summary queries"""
        for keyword, sql_field in self.GROUP_BY_MAP.items():
            if keyword in input_lower:
                return sql_field
        return "KODE_BARANG"  # Default
    
    # ==================== VALIDATION ====================
    
    def _validate_filters(self, filters: Dict) -> Tuple[bool, Optional[str]]:
        """Validate extracted filters"""
        
        # Bulan validation
        if 'bulan' in filters:
            if not (1 <= filters['bulan'] <= 12):
                return False, "Bulan harus 1-12"
        
        # Tahun validation
        if 'tahun' in filters:
            if not (2000 <= filters['tahun'] <= 2099):
                return False, "Tahun tidak valid (2000-2099)"
        
        # Date range validation
        if 'date_from' in filters and 'date_to' in filters:
            if filters['date_from'] > filters['date_to']:
                return False, "Tanggal awal tidak boleh lebih besar dari akhir"
        
        # Weight range validation
        if 'min_berat' in filters and 'max_berat' in filters:
            if filters['min_berat'] > filters['max_berat']:
                return False, "Berat minimal tidak boleh > maksimal"
        
        # Jumlah range validation
        if 'min_jumlah' in filters and 'max_jumlah' in filters:
            if filters['min_jumlah'] > filters['max_jumlah']:
                return False, "Jumlah minimal tidak boleh > maksimal"
        
        return True, None
    
    # ==================== CONFIDENCE ====================
    
    def _calculate_confidence(self, query_type: QueryType, filters: Dict, group_by: Optional[str]) -> float:
        """Calculate confidence score (0-1)"""
        confidence = 0.5
        
        # Intent clarity bonus
        if query_type in [QueryType.SUMMARY, QueryType.COUNT]:
            confidence += 0.25
        elif query_type == QueryType.DETAIL:
            confidence += 0.20
        elif query_type == QueryType.EXPLORATORY:
            confidence += 0.15
        elif query_type == QueryType.UNKNOWN:
            return 0.1
        
        # Filter count bonus
        num_filters = len(filters)
        if num_filters >= 3:
            confidence += 0.25
        elif num_filters == 2:
            confidence += 0.15
        elif num_filters == 1:
            confidence += 0.05
        
        # Code filter bonus (more specific)
        has_code = any(k in filters for k in ['kode_barang', 'lokasi'])
        if has_code:
            confidence += 0.10
        
        # Temporal filter bonus
        has_temporal = any(k in filters for k in ['tahun', 'bulan', 'date_from', 'date_to'])
        if has_temporal:
            confidence += 0.10
        
        return min(0.99, confidence)

# ==================== UTILITY FUNCTIONS ====================

def extract_api_params(parsed_query: Dict) -> Dict:
    """Convert parsed query to API parameters"""
    return parsed_query.get('filters', {})
