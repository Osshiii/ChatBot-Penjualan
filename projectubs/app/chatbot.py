# -*- coding: utf-8 -*-
"""
Chatbot Module for Jewelry Sales AI Assistant
Handles conversation logic, query processing, and response generation
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import json
from datetime import datetime

from app.nlp_parser import NLPParser, QueryType, extract_api_params

class SalesDatabase:
    """Helper class for database queries"""
    
    def __init__(self, db_path: str):
        """Initialize database connection"""
        self.db_path = db_path
    
    def execute_query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Execute SELECT query and return results as list of dicts"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(sql, params)
            results = [dict(row) for row in cursor.fetchall()]
            return results
        finally:
            conn.close()
    
    def get_filter_values(self, column: str) -> List[str]:
        """Get unique values for a column (for suggestions)"""
        sql = f"SELECT DISTINCT {column} FROM penjualan ORDER BY {column}"
        results = self.execute_query(sql)
        return [r[column] for r in results if r[column]]

class SalesQueryBuilder:
    """Build SQL queries based on filters - Improved version"""
    
    @staticmethod
    def build_where_clause(filters: Dict) -> Tuple[str, list]:
        """
        Build dynamic WHERE clause with proper parameterization.
        Returns: (where_clause_str, params_list)
        """
        where_parts = ["1=1"]
        params = []
        
        # Code filters (exact match)
        if "kode_barang" in filters:
            where_parts.append("KODE_BARANG = ?")
            params.append(filters["kode_barang"])
        
        if "lokasi" in filters:
            where_parts.append("LOKASI = ?")
            params.append(filters["lokasi"])
        
        if "klasifikasi_barang" in filters:
            where_parts.append("KLASIFIKASI_BARANG = ?")
            params.append(filters["klasifikasi_barang"])
        
        if "warna_barang" in filters:
            where_parts.append("WARNA_BARANG = ?")
            params.append(filters["warna_barang"])
        
        if "ukuran_barang" in filters:
            where_parts.append("UKURAN_BARANG = ?")
            params.append(filters["ukuran_barang"])
        
        if "channel" in filters:
            where_parts.append("CHANNEL = ?")
            params.append(filters["channel"])
        
        # Time filters
        if "bulan" in filters:
            where_parts.append("BULAN = ?")
            params.append(filters["bulan"])
        
        if "tahun" in filters:
            where_parts.append("TAHUN = ?")
            params.append(filters["tahun"])
        
        # Date range filters
        if "date_from" in filters:
            where_parts.append("TANGGAL >= ?")
            params.append(filters["date_from"])
        
        if "date_to" in filters:
            where_parts.append("TANGGAL <= ?")
            params.append(filters["date_to"])
        
        # Numeric range filters
        if "min_berat" in filters:
            where_parts.append("BERAT_SATUAN >= ?")
            params.append(filters["min_berat"])
        
        if "max_berat" in filters:
            where_parts.append("BERAT_SATUAN <= ?")
            params.append(filters["max_berat"])
        
        if "min_jumlah" in filters:
            where_parts.append("JUMLAH >= ?")
            params.append(filters["min_jumlah"])
        
        if "max_jumlah" in filters:
            where_parts.append("JUMLAH <= ?")
            params.append(filters["max_jumlah"])
        
        where_clause = " AND ".join(where_parts)
        return where_clause, params
    
    @staticmethod
    def build_count_query(filters: Dict) -> Tuple[str, list]:
        """
        Build COUNT query (no LIMIT).
        Returns total matching records.
        """
        where_clause, params = SalesQueryBuilder.build_where_clause(filters)
        sql = f"SELECT COUNT(*) as total FROM penjualan WHERE {where_clause}"
        return sql, params
    
    @staticmethod
    def build_detail_query(filters: Dict, limit: int = 5, offset: int = 0) -> Tuple[str, list]:
        """
        Build detail query with LIMIT 5 (sample data).
        Returns: (sql, params)
        """
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
    def build_summary_query(filters: Dict, group_by: str = "KODE_BARANG") -> Tuple[str, list]:
        """
        Build aggregation query with GROUP BY.
        Returns: (sql, params)
        """
        where_clause, params = SalesQueryBuilder.build_where_clause(filters)
        
        # Validate group_by
        valid_group_by = [
            "KODE_BARANG", "LOKASI", "BULAN", "TAHUN", "CHANNEL",
            "KLASIFIKASI_BARANG", "WARNA_BARANG", "UKURAN_BARANG"
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
    
    @staticmethod
    def build_filter_query(filters: Dict) -> tuple:
        """
        Legacy method for backward compatibility.
        Build WHERE clause and parameters from filters.
        """
        where_clause, params = SalesQueryBuilder.build_where_clause(filters)
        return where_clause, tuple(params)
        
        return sql, params
    
    @staticmethod
    def build_count_query(filters: Dict) -> tuple:
        """Build COUNT query to get total matching records"""
        where_clause, params = SalesQueryBuilder.build_filter_query(filters)
        
        sql = f"SELECT COUNT(*) as total FROM penjualan WHERE {where_clause}"
        
        return sql, params

class JewelrySalesBot:
    """Main chatbot class"""
    
    def __init__(self, db_path: str):
        """Initialize chatbot with database connection"""
        self.db_path = db_path
        self.parser = NLPParser()
        self.db = SalesDatabase(db_path)
        self.conversation_history = []
    
    def process_message(self, user_message: str) -> Dict[str, Any]:
        """
        Process user message and generate response.
        
        Args:
            user_message (str): Natural language message from user
            
        Returns:
            Dict: Response structure containing:
                - query_type: Type of query
                - message: Human-readable response
                - data: Query results (if applicable)
                - sql: Generated SQL (for debugging)
                - filters: Extracted filters
                - confidence: Parse confidence
        """
        
        # Store in conversation history
        self.conversation_history.append({
            "timestamp": datetime.now().isoformat(),
            "role": "user",
            "message": user_message
        })
        
        # Parse the input
        parsed_query = self.parser.parse(user_message)
        
        # Check for parse errors
        if parsed_query.get("error"):
            return {
                "query_type": "error",
                "message": f"❌ Validasi input gagal: {parsed_query['error']}",
                "data": [],
                "confidence": 0.0,
            }
        
        # DEBUG: Log parsing result
        print(f"[DEBUG] User Message: {user_message}")
        print(f"[DEBUG] Intent: {parsed_query['query_type'].value}")
        print(f"[DEBUG] Filters: {parsed_query.get('filters', {})}")
        print(f"[DEBUG] Group By: {parsed_query.get('group_by')}")
        print(f"[DEBUG] Confidence: {parsed_query.get('confidence', 0):.2f}")
        
        # Handle different query types
        query_type = parsed_query["query_type"]
        
        if query_type == QueryType.HELP:
            response = self._handle_help_query()
        elif query_type == QueryType.DETAIL:
            response = self._handle_detail_query(parsed_query)
        elif query_type == QueryType.COUNT:
            response = self._handle_count_query(parsed_query)
        elif query_type == QueryType.SUMMARY:
            response = self._handle_summary_query(parsed_query)
        elif query_type == QueryType.EXPLORATORY:
            response = self._handle_exploratory_query(parsed_query)
        else:
            response = self._handle_unknown_query(parsed_query)
        
        # DEBUG: Log SQL if available
        if "sql" in response:
            print(f"[DEBUG] SQL: {response['sql']}")
        
        # Store bot response in history
        self.conversation_history.append({
            "timestamp": datetime.now().isoformat(),
            "role": "assistant",
            "message": response["message"]
        })
        
        return response
    
    def _handle_exploratory_query(self, parsed_query: Dict) -> Dict[str, Any]:
        """Handle exploratory queries (data overview, year range, etc)"""
        intent = parsed_query.get("exploratory_intent", {})
        ask_about = intent.get("ask_about", "data_overview")
        filters = parsed_query.get("filters", {})
        confidence = parsed_query.get("confidence", 0.7)
        
        try:
            if ask_about == "year_range":
                return self._handle_year_range_query()
            elif ask_about == "month_range":
                return self._handle_month_range_query()
            elif ask_about == "available_codes":
                return self._handle_available_codes_query()
            elif ask_about == "available_locations":
                return self._handle_available_locations_query()
            elif ask_about == "available_channels":
                return self._handle_available_channels_query()
            else:
                return self._handle_data_overview_query()
        except Exception as e:
            return {
                "query_type": "exploratory",
                "message": f"❌ Error gathering information: {str(e)}",
                "data": [],
                "confidence": 0.0,
                "error": str(e),
            }
    
    def _handle_year_range_query(self) -> Dict[str, Any]:
        """Answer questions about year range in data"""
        sql = "SELECT DISTINCT TAHUN FROM penjualan ORDER BY TAHUN"
        years = self.db.execute_query(sql)
        year_list = [str(r["TAHUN"]) for r in years]
        
        if year_list:
            years_str = ", ".join(year_list)
            message = (
                f"📅 Data Tahun Tersedia\n\n"
                f"Database kami memiliki data penjualan dari tahun:\n{years_str}\n\n"
                f"Rentang: {year_list[0]} - {year_list[-1]}\n\n"
                f"💡 Anda dapat melakukan filter berdasarkan tahun tertentu dengan menyebutkan tahunnya."
            )
        else:
            message = "❌ Data tahun tidak ditemukan dalam database."
        
        return {
            "query_type": "exploratory",
            "message": message,
            "data": years,
            "confidence": 0.95,
        }
    
    def _handle_month_range_query(self) -> Dict[str, Any]:
        """Answer questions about month coverage"""
        sql = "SELECT DISTINCT BULAN FROM penjualan ORDER BY BULAN"
        months = self.db.execute_query(sql)
        month_list = [int(r["BULAN"]) for r in months]
        
        month_names = {
            1: "Januari", 2: "Februari", 3: "Maret", 4: "April",
            5: "Mei", 6: "Juni", 7: "Juli", 8: "Agustus",
            9: "September", 10: "Oktober", 11: "November", 12: "Desember"
        }
        
        if month_list:
            months_str = ", ".join([f"{m} ({month_names.get(m, '')})" for m in sorted(month_list)])
            message = (
                f"📆 Bulan-Bulan Tersedia\n\n"
                f"Database kami memiliki data untuk bulan:\n{months_str}\n\n"
                f"Total {len(month_list)} bulan dengan data penjualan.\n\n"
                f"💡 Gunakan nama atau nomor bulan untuk memfilter data (contoh: 'Januari' atau 'bulan 1')."
            )
        else:
            message = "❌ Data bulan tidak ditemukan dalam database."
        
        return {
            "query_type": "exploratory",
            "message": message,
            "data": months,
            "confidence": 0.95,
        }
    
    def _handle_available_codes_query(self) -> Dict[str, Any]:
        """Answer questions about available product codes"""
        sql = "SELECT COUNT(DISTINCT KODE_BARANG) as total_codes FROM penjualan"
        result = self.db.execute_query(sql)
        total_codes = result[0].get("total_codes", 0) if result else 0
        
        # Get sample codes
        sql_sample = "SELECT DISTINCT KODE_BARANG FROM penjualan LIMIT 10"
        samples = self.db.execute_query(sql_sample)
        sample_codes = [r["KODE_BARANG"] for r in samples]
        
        codes_str = ", ".join(sample_codes) if sample_codes else "Tidak ada"
        
        message = (
            f"🏷️ Kode Barang Tersedia\n\n"
            f"Database kami memiliki {total_codes} kode barang yang berbeda.\n\n"
            f"Contoh beberapa kode:\n{codes_str}\n\n"
            f"💡 Format kode barang: MP000XXX (contoh: MP000197)\n"
            f"Gunakan kode spesifik untuk memfilter penjualan produk tertentu."
        )
        
        return {
            "query_type": "exploratory",
            "message": message,
            "data": samples,
            "confidence": 0.95,
        }
    
    def _handle_available_locations_query(self) -> Dict[str, Any]:
        """Answer questions about available locations"""
        sql = "SELECT COUNT(DISTINCT LOKASI) as total_locations FROM penjualan"
        result = self.db.execute_query(sql)
        total_locs = result[0].get("total_locations", 0) if result else 0
        
        # Get sample locations
        sql_sample = "SELECT DISTINCT LOKASI FROM penjualan LIMIT 10"
        samples = self.db.execute_query(sql_sample)
        sample_locs = [r["LOKASI"] for r in samples]
        
        locs_str = ", ".join(sample_locs) if sample_locs else "Tidak ada"
        
        message = (
            f"📍 Lokasi Penjualan Tersedia\n\n"
            f"Database kami memiliki data dari {total_locs} lokasi berbeda.\n\n"
            f"Contoh beberapa lokasi:\n{locs_str}\n\n"
            f"💡 Format lokasi: LO000XXX (contoh: LO000048)\n"
            f"Gunakan kode lokasi untuk melihat penjualan di area tertentu."
        )
        
        return {
            "query_type": "exploratory",
            "message": message,
            "data": samples,
            "confidence": 0.95,
        }
    
    def _handle_available_channels_query(self) -> Dict[str, Any]:
        """Answer questions about available channels"""
        sql = "SELECT DISTINCT CHANNEL FROM penjualan ORDER BY CHANNEL"
        channels = self.db.execute_query(sql)
        channel_list = [r["CHANNEL"] for r in channels]
        
        channels_str = ", ".join(str(c) for c in channel_list) if channel_list else "Tidak ada"
        
        message = (
            f"📡 Saluran Penjualan Tersedia\n\n"
            f"Database kami mencakup saluran penjualan:\n{channels_str}\n\n"
            f"Total {len(channel_list)} saluran dengan data aktif.\n\n"
            f"💡 Gunakan nomor channel untuk melihat penjualan per saluran."
        )
        
        return {
            "query_type": "exploratory",
            "message": message,
            "data": channels,
            "confidence": 0.95,
        }
    
    def _handle_data_overview_query(self) -> Dict[str, Any]:
        """Provide general overview of the database"""
        sql = "SELECT COUNT(*) as total_records FROM penjualan"
        total = self.db.execute_query(sql)
        total_records = total[0].get("total_records", 0) if total else 0
        
        sql_codes = "SELECT COUNT(DISTINCT KODE_BARANG) as count FROM penjualan"
        codes = self.db.execute_query(sql_codes)
        code_count = codes[0].get("count", 0) if codes else 0
        
        sql_locs = "SELECT COUNT(DISTINCT LOKASI) as count FROM penjualan"
        locs = self.db.execute_query(sql_locs)
        loc_count = locs[0].get("count", 0) if locs else 0
        
        sql_dates = "SELECT MIN(TANGGAL) as min_date, MAX(TANGGAL) as max_date FROM penjualan"
        dates = self.db.execute_query(sql_dates)
        date_info = dates[0] if dates else {}
        
        message = (
            f"📊 Ringkasan Data Penjualan\n\n"
            f"✓ Total transaksi: {total_records:,}\n"
            f"✓ Kode barang berbeda: {code_count}\n"
            f"✓ Lokasi berbeda: {loc_count}\n"
            f"✓ Periode: {date_info.get('min_date', 'N/A')} hingga {date_info.get('max_date', 'N/A')}\n\n"
            f"💡 Tanya tentang tahun, bulan, lokasi, atau kode barang spesifik untuk detail lebih lanjut.\n"
            f"📈 Gunakan kata kunci 'ringkasan' untuk melihat analisis per kategori."
        )
        
        return {
            "query_type": "exploratory",
            "message": message,
            "data": [
                {"metric": "Total Records", "value": total_records},
                {"metric": "Unique Codes", "value": code_count},
                {"metric": "Unique Locations", "value": loc_count},
                {"metric": "Date Range Start", "value": date_info.get("min_date", "N/A")},
                {"metric": "Date Range End", "value": date_info.get("max_date", "N/A")},
            ],
            "confidence": 0.95,
        }
    
    def _handle_detail_query(self, parsed_query: Dict) -> Dict[str, Any]:
        """Handle detail queries - return sample rows (LIMIT 5)"""
        filters = parsed_query["filters"].copy()
        confidence = parsed_query["confidence"]
        
        try:
            # Step 1: Count total matching records
            count_sql, count_params = SalesQueryBuilder.build_count_query(filters)
            count_result = self.db.execute_query(count_sql, tuple(count_params))
            total_records = count_result[0].get("total", 0) if count_result else 0
            
            # Step 2: Get sample data (LIMIT 5)
            detail_sql, detail_params = SalesQueryBuilder.build_detail_query(filters, limit=5, offset=0)
            
            print(f"[DEBUG] Count SQL: {count_sql}")
            print(f"[DEBUG] Detail SQL: {detail_sql}")
            print(f"[DEBUG] Total: {total_records}")
            
            results = self.db.execute_query(detail_sql, tuple(detail_params))
            
            # Generate response message
            if total_records > 0:
                filter_desc = self._describe_filters(filters)
                message = (
                    f"✅ Hasil Pencarian Penjualan\n\n"
                    f"Menemukan {total_records:,} transaksi penjualan"
                    f"{f' {filter_desc}' if filter_desc else ''}.\n\n"
                    f"Ditampilkan 5 data teratas sebagai contoh:\n"
                    f"{self._format_table(results)}\n\n"
                    f"📊 Total: {total_records:,} | Sampel: {len(results)}/5\n"
                    f"💡 Gunakan filter tambahan untuk mempersempit hasil.\n"
                    f"🎯 Tingkat kepercayaan: {confidence:.0%}"
                )
            else:
                filter_desc = self._describe_filters(filters)
                message = (
                    f"❌ Tidak Ada Hasil\n\n"
                    f"Maaf, saya tidak menemukan data{f' {filter_desc}' if filter_desc else ''}.\n\n"
                    f"Filter yang digunakan:\n{self._format_filters(filters)}\n\n"
                    f"💡 Coba gunakan filter berbeda atau tanya 'data apa saja' untuk melihat opsi."
                )
            
            return {
                "query_type": "detail",
                "message": message,
                "data": results,
                "sql": detail_sql,
                "filters": filters,
                "confidence": confidence,
                "count": total_records,
                "displayed": len(results),
            }
        
        except Exception as e:
            print(f"[DEBUG] Error in detail query: {str(e)}")
            return {
                "query_type": "detail",
                "message": f"❌ Error: {str(e)}",
                "data": [],
                "sql": "",
                "filters": filters,
                "confidence": 0.0,
                "error": str(e),
            }
    
    def _handle_count_query(self, parsed_query: Dict) -> Dict[str, Any]:
        """Handle count queries - return just the count"""
        filters = parsed_query["filters"].copy()
        confidence = parsed_query["confidence"]
        
        try:
            # Execute count query
            count_sql, count_params = SalesQueryBuilder.build_count_query(filters)
            
            print(f"[DEBUG] Count SQL: {count_sql}")
            
            count_result = self.db.execute_query(count_sql, tuple(count_params))
            total = count_result[0].get("total", 0) if count_result else 0
            
            filter_desc = self._describe_filters(filters)
            message = (
                f"📊 Hasil Penghitungan\n\n"
                f"Ditemukan **{total:,} transaksi penjualan**"
                f"{f' {filter_desc}' if filter_desc else ''}.\n\n"
                f"🎯 Tingkat kepercayaan: {confidence:.0%}"
            )
            
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
            print(f"[DEBUG] Error in count query: {str(e)}")
            return {
                "query_type": "count",
                "message": f"❌ Error: {str(e)}",
                "data": [],
                "sql": "",
                "filters": filters,
                "confidence": 0.0,
                "error": str(e),
            }
        """Handle filter/detail queries"""
        filters = parsed_query["filters"].copy()
        confidence = parsed_query["confidence"]
        
        # Extract pagination (but force limit to 5 for display)
        filters.pop("limit", None)  # Remove user limit
        filters.pop("offset", None)  # Remove user offset
        
        try:
            # Step 1: Count total matching records (no LIMIT)
            count_sql, count_params = SalesQueryBuilder.build_count_query(filters)
            count_result = self.db.execute_query(count_sql, count_params)
            total_records = count_result[0].get("total", 0) if count_result else 0
            
            # Step 2: Get sample data (always LIMIT 5)
            detail_sql, detail_params = SalesQueryBuilder.build_detail_query(filters, limit=5, offset=0)
            
            # DEBUG: Log SQL queries
            print(f"[DEBUG] Filters after extraction: {filters}")
            print(f"[DEBUG] Count SQL: {count_sql}")
            print(f"[DEBUG] Detail SQL: {detail_sql}")
            print(f"[DEBUG] Total Records: {total_records}")
            
            results = self.db.execute_query(detail_sql, detail_params)
            
            # Generate conversational response message
            if total_records > 0:
                filter_desc = self._describe_filters(filters)
                message = (
                    f"✅ Hasil Pencarian Penjualan\n\n"
                    f"Menemukan {total_records:,} transaksi penjualan"
                    f"{f' {filter_desc}' if filter_desc else ''}.\n\n"
                    f"Ditampilkan 5 data teratas sebagai contoh:\n"
                    f"{self._format_table(results)}\n\n"
                    f"📊 Total: {total_records:,} | Sampel: {len(results)}/5\n"
                    f"💡 Gunakan filter tambahan untuk mempersempit hasil.\n"
                    f"🎯 Tingkat kepercayaan: {confidence:.0%}"
                )
            else:
                filter_desc = self._describe_filters(filters)
                message = (
                    f"❌ Tidak Ada Hasil\n\n"
                    f"Maaf, saya tidak menemukan data{f' {filter_desc}' if filter_desc else ''}.\n\n"
                    f"Filter yang digunakan:\n{self._format_filters(filters)}\n\n"
                    f"💡 Coba gunakan filter berbeda atau tanya 'data apa saja' untuk melihat opsi yang tersedia."
                )
            
            return {
                "query_type": "filter",
                "message": message,
                "data": results,
                "sql": detail_sql,
                "filters": filters,
                "confidence": confidence,
                "count": total_records,
                "displayed": len(results),
            }
        
        except Exception as e:
            print(f"[DEBUG] Error in filter query: {str(e)}")
            return {
                "query_type": "filter",
                "message": f"❌ Error: {str(e)}",
                "data": [],
                "sql": "",
                "filters": filters,
                "confidence": 0.0,
                "error": str(e),
            }
    
    def _handle_summary_query(self, parsed_query: Dict) -> Dict[str, Any]:
        """Handle summary/aggregation queries"""
        filters = parsed_query["filters"]
        group_by = parsed_query.get("group_by") or "KODE_BARANG"
        confidence = parsed_query["confidence"]
        
        try:
            # Build and execute query
            sql, params = SalesQueryBuilder.build_summary_query(filters, group_by)
            
            print(f"[DEBUG] Summary SQL: {sql}")
            print(f"[DEBUG] Group By: {group_by}")
            
            results = self.db.execute_query(sql, tuple(params))
            
            # Generate response message
            if results:
                total_records = sum(r.get("count_records", 0) for r in results)
                total_qty = sum(r.get("total_jumlah", 0) for r in results)
                total_berat = sum(r.get("total_berat", 0) for r in results)

                avg_berat = (
                    sum(r.get("avg_berat", 0) * r.get("count_records", 0) for r in results) / total_records
                    if total_records else 0
                )

                filter_desc = self._describe_filters(filters)
                
                message = (
                    f"📊 Ringkasan Penjualan\n\n"
                    f"Berdasarkan {group_by.lower()}"
                    f"{f' {filter_desc}' if filter_desc else ''}:\n\n"
                    f"✓ Total transaksi: **{total_records:,}**\n"
                    f"✓ Total quantity: **{total_qty:,} unit**\n"
                    f"✓ Total berat: **{total_berat:,.2f} gram**\n"
                    f"✓ Rata-rata berat: **{avg_berat:.2f} gram/item**\n\n"
                    f"📋 Detail per {group_by.lower()}:\n"
                    f"{self._format_summary_table(results)}\n\n"
                    f"🎯 Tingkat kepercayaan: {confidence:.0%}"
                )

            else:
                message = "❌ Tidak ada data untuk summary."
            
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
            print(f"[DEBUG] Error in summary query: {str(e)}")
            return {
                "query_type": "summary",
                "message": f"❌ Error executing summary: {str(e)}",
                "data": [],
                "sql": "",
                "filters": filters,
                "confidence": 0.0,
                "error": str(e),
            }
    
    def _handle_help_query(self) -> Dict[str, Any]:
        """Handle help/info queries"""
        message = """
📚 JEWELRY SALES CHATBOT - BANTUAN

Saya adalah AI assistant untuk querying data penjualan perhiasan.
Database berisi informasi penjualan dengan kode-kode ID (bukan nama manusia).

🔍 CONTOH QUERY:

1. Filter Data:
   - "Tampilkan penjualan KODE_BARANG MP000197 bulan 4 tahun 2022"
   - "Cari record dengan LOKASI LO000048"
   - "Penjualan dengan berat 5 sampai 10"
   - "Tampilkan WARNA_BARANG PL000037 channel 1"

2. Summary/Aggregasi:
   - "Ringkasan penjualan per produk"
   - "Summary penjualan per lokasi"
   - "Total penjualan per bulan"
   - "Statistik per channel"

🎯 KODE-KODE YANG DIDUKUNG:

- KODE_BARANG: MP000xxx (contoh: MP000197, MP000284)
- LOKASI: LO000xxx (contoh: LO000048)
- KLASIFIKASI: KD000xxx (contoh: KD000016)
- WARNA: PL000xxx (contoh: PL000037)
- UKURAN: SZ000xxx (contoh: SZ000012)
- BULAN: 1-12 (atau nama bulan: januari, februar, dst)
- TAHUN: 2020, 2021, 2022, 2023, 2024, dst
- BERAT: numeric value (contoh: "berat 5 sampai 10")
- CHANNEL: numeric value (contoh: "channel 1")

📝 KOLOM DATA YANG TERSEDIA:

CHANNEL, LOKASI, TANGGAL, BULAN, TAHUN, KODE_BARANG, KLASIFIKASI_BARANG,
WARNA_BARANG, UKURAN_BARANG, BERAT_SATUAN, JUMLAH, BERAT_TOTAL

⚠️ PENTING:

- Semua kode harus menggunakan format ID yang benar (bukan nama manusia)
- Jika Anda tidak tahu kodenya, tanyakan kode spesifik
- Query tidak case-sensitive (BESAR/kecil bisa dicampur)
"""
        return {
            "query_type": "help",
            "message": message,
        }
    
    def _handle_unknown_query(self, parsed_query: Dict) -> Dict[str, Any]:
        """Handle unknown/unclear queries"""
        confidence = parsed_query["confidence"]
        
        message = (
            f"❓ Query tidak jelas (confidence: {confidence:.0%})\n\n"
            f"Saya tidak dapat memahami query Anda dengan baik.\n\n"
            f"💡 Tips:\n"
            f"- Gunakan kode ID spesifik (MP000xxx, LO000xxx, dll)\n"
            f"- Jelaskan filter yang diinginkan\n"
            f"- Contoh: 'Tampilkan penjualan MP000197 bulan 4 tahun 2022'\n\n"
            f"Ketik 'bantuan' untuk melihat contoh query yang benar."
        )
        
        return {
            "query_type": "unknown",
            "message": message,
            "confidence": confidence,
        }
    
    def _describe_filters(self, filters: Dict) -> str:
        """Generate human-readable description of applied filters"""
        if not filters:
            return ""
        
        descriptions = []
        
        if "kode_barang" in filters:
            descriptions.append(f"untuk kode barang {filters['kode_barang']}")
        if "lokasi" in filters:
            descriptions.append(f"di lokasi {filters['lokasi']}")
        if "bulan" in filters:
            month_names = {
                1: "Januari", 2: "Februari", 3: "Maret", 4: "April",
                5: "Mei", 6: "Juni", 7: "Juli", 8: "Agustus",
                9: "September", 10: "Oktober", 11: "November", 12: "Desember"
            }
            month_num = filters['bulan']
            month_name = month_names.get(month_num, f"bulan {month_num}")
            descriptions.append(f"pada {month_name}")
        if "tahun" in filters:
            descriptions.append(f"tahun {filters['tahun']}")
        if "channel" in filters:
            descriptions.append(f"channel {filters['channel']}")
        
        if descriptions:
            return "(" + ", ".join(descriptions) + ")"
        return ""
    
    def _format_table(self, results: List[Dict], max_rows: int = 5) -> str:
        """Format results as table string"""
        if not results:
            return "No data"
        
        # Select key columns to display
        display_cols = ["TANGGAL", "KODE_BARANG", "LOKASI", "BERAT_SATUAN", "JUMLAH", "BERAT_TOTAL"]
        available_cols = [c for c in display_cols if c in results[0]]
        
        # Build table
        lines = []
        
        # Header
        header = " | ".join(f"{col:15}" for col in available_cols)
        lines.append(header)
        lines.append("-" * len(header))
        
        # Rows
        for row in results[:max_rows]:
            values = []
            for col in available_cols:
                val = row.get(col, "")
                if isinstance(val, float):
                    values.append(f"{val:.2f}")
                else:
                    values.append(str(val)[:15])
            lines.append(" | ".join(f"{v:15}" for v in values))
        
        return "\n".join(lines)
    
    def _format_summary_table(self, results: List[Dict], max_rows: int = 10) -> str:
        """Format summary results as table"""
        if not results:
            return "No data"
        
        lines = []
        lines.append(f"{'Kategori':<20} {'Count':<8} {'Total Qty':<10} {'Avg Berat':<10}")
        lines.append("-" * 50)
        
        for row in results[:max_rows]:
            kategori = str(row.get("kategori", ""))[:20]
            count = row.get("count_records", 0)
            total_qty = row.get("total_jumlah", 0)
            avg_berat = row.get("avg_berat", 0)
            
            lines.append(f"{kategori:<20} {count:<8} {total_qty:<10} {avg_berat:<10.2f}")
        
        return "\n".join(lines)
    
    def _format_filters(self, filters: Dict) -> str:
        """Format filters for display"""
        if not filters:
            return "  (tidak ada filter)"
        
        lines = []
        for key, value in filters.items():
            lines.append(f"  • {key}: {value}")
        
        return "\n".join(lines)

def create_bot(db_path: str) -> JewelrySalesBot:
    """Factory function to create bot instance"""
    return JewelrySalesBot(db_path)
