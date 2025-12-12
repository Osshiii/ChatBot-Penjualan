# -*- coding: utf-8 -*-
"""
Natural Language Parser for Jewelry Sales Queries
Parses user input and extracts filter parameters
Uses pattern matching and keyword extraction (no LLM required)
"""

import re
from typing import Dict, List, Optional, Tuple
from enum import Enum

class QueryType(Enum):
    """Types of queries the chatbot can handle"""
    FILTER = "filter"           # Get specific records with filters
    SUMMARY = "summary"         # Get aggregated summary
    EXPLORATORY = "exploratory" # Exploratory queries (what data, year range, etc)
    HELP = "help"               # Show help/info
    UNKNOWN = "unknown"

class NLPParser:
    """
    Natural Language Parser for sales queries.
    Converts natural language input to structured filter parameters.
    """
    
    # Keywords for different filter types
    MONTH_KEYWORDS = {
        "januari": 1, "februari": 2, "maret": 3, "april": 4,
        "mei": 5, "juni": 6, "juli": 7, "agustus": 8,
        "september": 9, "oktober": 10, "november": 11, "desember": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    
    SUMMARY_KEYWORDS = ["ringkasan", "summary", "total", "statistik", "aggregat", "overview", "per", "berdasarkan"]
    EXPLORATORY_KEYWORDS = ["apa", "ada", "data apa", "apa saja", "dari tahun", "range", "berapa", "kapan", "mana", "ada berapa", "tahun berapa"]
    COMPARISON_KEYWORDS = ["mana", "tertinggi", "terendah", "terbanyak", "tersedikit", "paling banyak", "highest", "lowest", "most", "least"]
    LOCATION_PREFIX = ["lokasi", "location", "di", "from", "dari"]
    PRODUCT_PREFIX = ["kode", "code", "product", "produk", "barang", "item"]
    FILTER_PREFIX = ["filter", "cari", "search", "tampilkan", "show", "find", "lihat", "dengan"]
    
    def __init__(self):
        """Initialize the NLP parser"""
        pass
    
    def parse(self, user_input: str) -> Dict:
        """
        Parse user input and extract query parameters.
        
        Args:
            user_input (str): Natural language query from user
            
        Returns:
            Dict: Structured query with:
                - query_type: QueryType enum
                - filters: Dict of filter parameters
                - summary_by: Optional summary grouping field
                - confidence: Float (0-1) indicating confidence
        """
        
        user_input_lower = user_input.lower().strip()
        
        # Determine query type
        query_type = self._detect_query_type(user_input_lower)
        
        # Extract filters based on query type
        filters = {}
        summary_by = None
        exploratory_intent = None
        
        if query_type == QueryType.SUMMARY:
            filters, summary_by = self._extract_summary_filters(user_input_lower)
        elif query_type == QueryType.FILTER:
            filters = self._extract_filters(user_input_lower)
        elif query_type == QueryType.EXPLORATORY:
            exploratory_intent = self._extract_exploratory_intent(user_input_lower)
            # Also try to extract any filters present
            filters = self._extract_filters(user_input_lower)
        elif query_type == QueryType.HELP:
            filters = {}
        
        # Calculate confidence
        confidence = self._calculate_confidence(user_input_lower, filters, query_type)
        
        return {
            "query_type": query_type,
            "filters": filters,
            "summary_by": summary_by,
            "exploratory_intent": exploratory_intent,
            "confidence": confidence,
            "original_input": user_input,
        }
    
    def _detect_query_type(self, user_input_lower: str) -> QueryType:
        """
        Detect the type of query from user input.
        
        Args:
            user_input_lower (str): Lowercase user input
            
        Returns:
            QueryType: Type of query detected
        """
        
        # Check for help queries (highest priority)
        if any(word in user_input_lower for word in ["bantuan", "help", "apa itu", "bagaimana", "cara"]):
            return QueryType.HELP
        
        # Check for summary/aggregation queries
        for keyword in self.SUMMARY_KEYWORDS:
            if keyword in user_input_lower:
                return QueryType.SUMMARY
        
        # Check for exploratory queries (e.g., "data apa saja?", "ada dari tahun berapa?")
        if any(word in user_input_lower for word in self.EXPLORATORY_KEYWORDS):
            if "tahun" in user_input_lower or "bulan" in user_input_lower or "kode" in user_input_lower:
                return QueryType.EXPLORATORY
            # Differentiate: if asking about existence/range of data
            if any(x in user_input_lower for x in ["apa saja", "ada apa", "apa ada", "dari tahun", "sampai tahun"]):
                return QueryType.EXPLORATORY
        
        # Check for comparison queries
        if any(word in user_input_lower for word in self.COMPARISON_KEYWORDS):
            return QueryType.SUMMARY
        
        # Check for filter queries with explicit keywords
        if any(word in user_input_lower for word in self.FILTER_PREFIX):
            return QueryType.FILTER
        
        # Check if it contains code-like patterns (any 2 uppercase letters + 6 digits)
        if re.search(r'[A-Z]{2}\d{6}', user_input_lower):
            return QueryType.FILTER
        
        # Check if it contains year/month patterns with filter context
        if re.search(r'\b(20\d{2}|tahun|bulan)\b', user_input_lower):
            if any(word in user_input_lower for word in ["tampilkan", "lihat", "cari", "dengan", "di"]):
                return QueryType.FILTER
            # Standalone year/month might be exploratory
            if any(x in user_input_lower for x in ["dari", "sampai", "berapa", "ada"]):
                return QueryType.EXPLORATORY
        
        # Check for weight/numeric filters
        if re.search(r'berat|jumlah|channel', user_input_lower):
            return QueryType.FILTER
        
        return QueryType.UNKNOWN
    
    def _extract_filters(self, user_input_lower: str) -> Dict:
        """
        Extract filter parameters from query.
        
        Args:
            user_input_lower (str): Lowercase user input
            
        Returns:
            Dict: Filter parameters
        """
        
        filters = {}
        
        # Code pattern mapping: (pattern, field_name)
        code_patterns = [
            (r'\bMP\d{6}\b', "kode_barang"),
            (r'\bLO\d{6}\b', "lokasi"),
            (r'\bKD\d{6}\b', "klasifikasi_barang"),
            (r'\bPL\d{6}\b', "warna_barang"),
            (r'\bSZ\d{6}\b', "ukuran_barang"),
        ]
        
        # Extract all code patterns dynamically (not hardcoded)
        for pattern, field_name in code_patterns:
            code_match = re.search(pattern, user_input_lower)
            if code_match:
                filters[field_name] = code_match.group().upper()
        
        # Extract TAHUN (year) - must have "tahun" prefix or be after specific keywords
        # Avoid matching year digits embedded in codes
        year_match = re.search(r'(?:tahun\s+|year\s+)(20\d{2})', user_input_lower)
        if year_match:
            filters["tahun"] = int(year_match.group(1))
        else:
            # Also check for standalone year if preceded by specific keywords
            year_match = re.search(r'\b(20\d{2})\b(?=\s*(?:bulan|month|q\d))', user_input_lower)
            if year_match:
                filters["tahun"] = int(year_match.group(1))
        
        # Extract BULAN (month) - prioritize month keyword + number
        month_num_match = re.search(r'(?:bulan|month)\s+([1-9]|1[0-2])', user_input_lower)
        if month_num_match:
            filters["bulan"] = int(month_num_match.group(1))
        else:
            # Month name match - check full month names first to avoid partial matches
            month_keywords_ordered = sorted(self.MONTH_KEYWORDS.keys(), key=len, reverse=True)
            for month_word in month_keywords_ordered:
                if re.search(rf'\b{re.escape(month_word)}\b', user_input_lower):
                    filters["bulan"] = self.MONTH_KEYWORDS[month_word]
                    break
        
        # Extract weight range (BERAT_SATUAN)
        # Pattern: "berat 5-10" or "berat min 5 max 10" or "berat > 5"
        weight_patterns = [
            (r'berat\s+(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)', 'range'),  # 5-10
            (r'berat\s+(?:antara\s+)?(\d+(?:\.\d+)?)\s+(?:sampai|hingga|to|s\.d)\s+(\d+(?:\.\d+)?)', 'range'),  # antara 5 sampai 10
            (r'(?:berat\s+)?(?:min|minimum)\s+(\d+(?:\.\d+)?)', 'min'),  # min 5
            (r'(?:berat\s+)?(?:max|maksimum)\s+(\d+(?:\.\d+)?)', 'max'),  # max 10
            (r'berat\s+([><=]+)\s*(\d+(?:\.\d+)?)', 'comparison'),  # berat > 5
        ]
        
        for pattern, pattern_type in weight_patterns:
            match = re.search(pattern, user_input_lower)
            if match:
                if pattern_type == 'range':
                    filters["min_berat"] = float(match.group(1))
                    filters["max_berat"] = float(match.group(2))
                elif pattern_type == 'min':
                    filters["min_berat"] = float(match.group(1))
                elif pattern_type == 'max':
                    filters["max_berat"] = float(match.group(1))
                elif pattern_type == 'comparison':
                    operator = match.group(1)
                    value = float(match.group(2))
                    if operator == '>':
                        filters["min_berat"] = value
                    elif operator == '<':
                        filters["max_berat"] = value
                    elif operator in ['>=', '>==']:
                        filters["min_berat"] = value
                    elif operator in ['<=', '<==']:
                        filters["max_berat"] = value
                break
        
        # Extract CHANNEL
        channel_match = re.search(r'channel\s+(\d+)', user_input_lower)
        if channel_match:
            filters["channel"] = int(channel_match.group(1))
        
        # Extract pagination parameters
        limit_match = re.search(r'(?:limit|tampilkan|show)\s+(\d+)', user_input_lower)
        if limit_match:
            filters["limit"] = int(limit_match.group(1))
        
        offset_match = re.search(r'offset\s+(\d+)', user_input_lower)
        if offset_match:
            filters["offset"] = int(offset_match.group(1))
        
        return filters
    
    def _extract_exploratory_intent(self, user_input_lower: str) -> Dict:
        """
        Extract intent from exploratory queries.
        
        Args:
            user_input_lower (str): Lowercase user input
            
        Returns:
            Dict: Exploratory intent with 'ask_about' key
        """
        intent = {"ask_about": None}
        
        if "tahun" in user_input_lower:
            intent["ask_about"] = "year_range"
        elif "bulan" in user_input_lower:
            intent["ask_about"] = "month_range"
        elif "kode" in user_input_lower or "product" in user_input_lower:
            intent["ask_about"] = "available_codes"
        elif "lokasi" in user_input_lower or "location" in user_input_lower:
            intent["ask_about"] = "available_locations"
        elif "channel" in user_input_lower:
            intent["ask_about"] = "available_channels"
        else:
            intent["ask_about"] = "data_overview"
        
        return intent
    
    def _extract_summary_filters(self, user_input_lower: str) -> Tuple[Dict, Optional[str]]:
        """
        Extract filters for summary queries.
        
        Args:
            user_input_lower (str): Lowercase user input
            
        Returns:
            Tuple: (filters dict, summary_by field)
        """
        
        filters, _ = self._extract_filters(user_input_lower), None
        
        # Determine what to group by
        summary_by = "product"  # default
        
        if "lokasi" in user_input_lower or "location" in user_input_lower:
            summary_by = "location"
        elif "bulan" in user_input_lower or "month" in user_input_lower:
            summary_by = "month"
        elif "tahun" in user_input_lower or "year" in user_input_lower:
            summary_by = "year"
        elif "channel" in user_input_lower:
            summary_by = "channel"
        elif "klasifikasi" in user_input_lower or "classification" in user_input_lower:
            summary_by = "classification"
        elif "warna" in user_input_lower or "color" in user_input_lower:
            summary_by = "color"
        elif "ukuran" in user_input_lower or "size" in user_input_lower:
            summary_by = "size"
        
        return filters, summary_by
    
    def _calculate_confidence(self, user_input_lower: str, filters: Dict, query_type: QueryType) -> float:
        """
        Calculate confidence score for the parsed query.
        
        Args:
            user_input_lower (str): Lowercase user input
            filters (Dict): Extracted filters
            query_type (QueryType): Detected query type
            
        Returns:
            float: Confidence score (0-1)
        """
        
        # Start with type-based confidence
        confidence = 0.5
        
        if query_type == QueryType.UNKNOWN:
            confidence = 0.2
        elif query_type == QueryType.HELP:
            confidence = 0.95
        elif query_type in [QueryType.FILTER, QueryType.SUMMARY, QueryType.EXPLORATORY]:
            confidence = 0.7
        
        # Boost for valid code patterns
        code_count = len(re.findall(r'[A-Z]{2}\d{6}', user_input_lower))
        confidence += min(0.2, code_count * 0.1)
        
        # Boost for time-based filters
        has_time = bool(re.search(r'\b(tahun|bulan|month|year)\b', user_input_lower))
        if has_time:
            confidence += 0.1
        
        # Boost for numeric filters
        has_numeric = bool(re.search(r'berat|jumlah|channel|\d+', user_input_lower))
        if has_numeric:
            confidence += 0.05
        
        # Boost for multiple clear filters
        num_filters = len(filters)
        if num_filters > 0:
            confidence += min(0.15, num_filters * 0.05)
        
        return min(0.99, max(0.1, confidence))
    
    def format_query_response(self, parsed_query: Dict) -> str:
        """
        Format parsed query into human-readable explanation.
        
        Args:
            parsed_query (Dict): Parsed query result
            
        Returns:
            str: Human-readable explanation
        """
        
        query_type = parsed_query["query_type"]
        filters = parsed_query["filters"]
        confidence = parsed_query["confidence"]
        
        lines = []
        lines.append(f"📊 Query Type: {query_type.value}")
        lines.append(f"🎯 Confidence: {confidence:.0%}")
        
        if filters:
            lines.append("\n🔍 Filters:")
            for key, value in filters.items():
                if key in ["min_berat", "max_berat"]:
                    lines.append(f"   • {key}: {value:.2f}")
                else:
                    lines.append(f"   • {key}: {value}")
        else:
            lines.append("\n🔍 No specific filters detected")
        
        if parsed_query.get("summary_by"):
            lines.append(f"\n📈 Summary by: {parsed_query['summary_by']}")
        
        return "\n".join(lines)

# Utility functions for integration with FastAPI

def parse_query(user_input: str) -> Dict:
    """
    Parse user query using NLPParser.
    
    Args:
        user_input (str): Natural language input from user
        
    Returns:
        Dict: Parsed query structure
    """
    parser = NLPParser()
    return parser.parse(user_input)

def extract_api_params(parsed_query: Dict) -> Dict:
    """
    Convert parsed query to API request parameters.
    
    Args:
        parsed_query (Dict): Output from parse_query()
        
    Returns:
        Dict: API parameters ready for /sales or /sales/summary endpoint
    """
    
    api_params = {}
    filters = parsed_query.get("filters", {})
    
    # Map parsed filters to API parameter names
    param_mapping = {
        "kode_barang": "kode_barang",
        "lokasi": "lokasi",
        "bulan": "bulan",
        "tahun": "tahun",
        "klasifikasi_barang": "klasifikasi_barang",
        "warna_barang": "warna_barang",
        "ukuran_barang": "ukuran_barang",
        "min_berat": "min_berat",
        "max_berat": "max_berat",
        "limit": "limit",
        "offset": "offset",
        "channel": "channel",
    }
    
    for parsed_key, api_key in param_mapping.items():
        if parsed_key in filters:
            api_params[api_key] = filters[parsed_key]
    
    return api_params
