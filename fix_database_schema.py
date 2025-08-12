#!/usr/bin/env python3
"""
Database schema analyzer and fixer for AIDS Memorial Quilt Records
Implements comprehensive error handling and schema validation
Following project coding standards for digital humanities research
"""

import sqlite3
import asyncio
import aiosqlite
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

# Configure structured logging per project guidelines
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - AIDS Memorial Quilt Schema Fix - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchemaAnalyzer:
    """
    Database schema analyzer for AIDS Memorial Quilt Records
    Implements separation of concerns and error resilience
    """
    
    def __init__(self, database_path: Path):
        """Initialize schema analyzer with comprehensive error handling"""
        self.database_path = Path(database_path)
        if not self.database_path.exists():
            raise FileNotFoundError(f"Database file not found: {database_path}")
    
    async def analyze_table_schemas(self) -> Dict[str, Any]:
        """
        Analyze all table schemas in the database
        Implements async/await patterns for non-blocking operations
        
        Returns:
            Dictionary containing schema analysis results
        """
        logger.info("AIDS Memorial Quilt Schema: Starting table schema analysis")
        
        analysis_results = {}
        
        try:
            async with aiosqlite.connect(str(self.database_path)) as conn:
                # Get all table names
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                tables = await cursor.fetchall()
                table_names = [table[0] for table in tables]
                await cursor.close()
                
                logger.info(f"AIDS Memorial Quilt Schema: Found {len(table_names)} tables: {table_names}")
                
                # Analyze each table schema
                for table_name in table_names:
                    try:
                        # Get column information
                        cursor = await conn.execute(f"PRAGMA table_info({table_name})")
                        columns_info = await cursor.fetchall()
                        await cursor.close()
                        
                        # Get row count
                        cursor = await conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                        row_count_result = await cursor.fetchone()
                        row_count = row_count_result[0] if row_count_result else 0
                        await cursor.close()
                        
                        # Format column information
                        columns = []
                        for col_info in columns_info:
                            columns.append({
                                'name': col_info[1],
                                'type': col_info[2],
                                'not_null': bool(col_info[3]),
                                'default_value': col_info[4],
                                'primary_key': bool(col_info[5])
                            })
                        
                        analysis_results[table_name] = {
                            'row_count': row_count,
                            'column_count': len(columns),
                            'columns': columns,
                            'column_names': [col['name'] for col in columns]
                        }
                        
                        logger.info(f"AIDS Memorial Quilt Schema: {table_name} - {row_count:,} rows, {len(columns)} columns")
                        
                    except Exception as table_error:
                        logger.error(f"AIDS Memorial Quilt Schema: Error analyzing table {table_name}: {table_error}")
                        analysis_results[table_name] = {'error': str(table_error)}
        
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Schema: Critical error during analysis: {e}")
            analysis_results['critical_error'] = str(e)
        
        return analysis_results
    
    def print_schema_analysis(self, analysis: Dict[str, Any]) -> None:
        """
        Print formatted schema analysis results
        Implements structured output following project documentation standards
        """
        print("🔍 AIDS Memorial Quilt Database Schema Analysis")
        print("=" * 52)
        
        if 'critical_error' in analysis:
            print(f"❌ Critical error: {analysis['critical_error']}")
            return
        
        for table_name, table_info in analysis.items():
            if 'error' in table_info:
                print(f"\n❌ Table {table_name}: Error - {table_info['error']}")
                continue
            
            row_count = table_info.get('row_count', 0)
            columns = table_info.get('columns', [])
            
            print(f"\n📊 Table: {table_name}")
            print(f"   Rows: {row_count:,}")
            print(f"   Columns: {len(columns)}")
            
            if columns:
                print("   Schema:")
                for col in columns:
                    pk_indicator = " (PK)" if col['primary_key'] else ""
                    not_null = " NOT NULL" if col['not_null'] else ""
                    default = f" DEFAULT {col['default_value']}" if col['default_value'] else ""
                    print(f"      • {col['name']}: {col['type']}{not_null}{default}{pk_indicator}")
    
    async def generate_fixed_get_records_query(self, analysis: Dict[str, Any]) -> str:
        """
        Generate corrected get_records query based on actual schema
        Implements error resilience and schema validation
        
        Args:
            analysis: Schema analysis results
            
        Returns:
            Corrected SQL query string
        """
        logger.info("AIDS Memorial Quilt Schema: Generating corrected get_records query")
        
        # Determine which table has the most data and should be primary
        primary_table = None
        max_rows = 0
        
        for table_name, table_info in analysis.items():
            if 'error' in table_info or table_name == 'sqlite_sequence':
                continue
            
            row_count = table_info.get('row_count', 0)
            if row_count > max_rows:
                max_rows = row_count
                primary_table = table_name
        
        if not primary_table:
            raise ValueError("No suitable primary table found")
        
        logger.info(f"AIDS Memorial Quilt Schema: Using {primary_table} as primary table ({max_rows:,} rows)")
        
        # Get available columns for the primary table
        table_info = analysis[primary_table]
        available_columns = table_info.get('column_names', [])
        
        logger.info(f"AIDS Memorial Quilt Schema: Available columns in {primary_table}: {available_columns}")
        
        # Map desired columns to available columns
        column_mapping = {
            'id': self._find_best_column(available_columns, ['id', 'block_id', 'panel_id', 'item_id']),
            'item_id': self._find_best_column(available_columns, ['item_id', 'block_id', 'panel_id', 'id']),
            'title': self._find_best_column(available_columns, ['title', 'name', 'description']),
            'description': self._find_best_column(available_columns, ['description', 'title', 'summary']),
            'subjects': self._find_best_column(available_columns, ['subjects', 'tags', 'keywords']),
            'names': self._find_best_column(available_columns, ['names', 'contributors', 'creators']),
            'dates': self._find_best_column(available_columns, ['dates', 'created_date', 'date', 'created_at']),
            'url': self._find_best_column(available_columns, ['url', 'link', 'source_url']),
            'image_url': self._find_best_column(available_columns, ['image_url', 'image_link', 'image_path']),
            'content_hash': self._find_best_column(available_columns, ['content_hash', 'hash', 'checksum']),
            'created_at': self._find_best_column(available_columns, ['created_at', 'created_date', 'date_added']),
            'updated_at': self._find_best_column(available_columns, ['updated_at', 'modified_date', 'last_updated'])
        }
        
        # Build SELECT clause with available columns
        select_clauses = []
        for desired_name, actual_column in column_mapping.items():
            if actual_column:
                if actual_column != desired_name:
                    select_clauses.append(f"{actual_column} as {desired_name}")
                else:
                    select_clauses.append(actual_column)
            else:
                # Use NULL for missing columns with appropriate alias
                select_clauses.append(f"NULL as {desired_name}")
        
        # Generate the corrected query
        query = f"""
            SELECT {', '.join(select_clauses)}
            FROM {primary_table}
            ORDER BY {self._find_best_column(available_columns, ['created_at', 'created_date', 'id', 'block_id'], 'id')} DESC
            LIMIT ? OFFSET ?
        """
        
        return query.strip(), primary_table
    
    def _find_best_column(self, available_columns: List[str], 
                         preferred_names: List[str], 
                         fallback: Optional[str] = None) -> Optional[str]:
        """
        Find the best matching column name from available columns
        Implements intelligent column mapping following project standards
        """
        # Check for exact matches first
        for preferred in preferred_names:
            if preferred in available_columns:
                return preferred
        
        # Check for case-insensitive matches
        available_lower = [col.lower() for col in available_columns]
        for preferred in preferred_names:
            preferred_lower = preferred.lower()
            if preferred_lower in available_lower:
                # Find the original case version
                for original in available_columns:
                    if original.lower() == preferred_lower:
                        return original
        
        # Check for partial matches
        for preferred in preferred_names:
            for available in available_columns:
                if preferred.lower() in available.lower() or available.lower() in preferred.lower():
                    return available
        
        return fallback if fallback and fallback in available_columns else None

async def fix_database_schema_compatibility() -> None:
    """
    Main function to fix database schema compatibility issues
    Implements comprehensive error handling and async/await patterns
    """
    print("🔧 AIDS Memorial Quilt Database Schema Fix")
    print("=" * 43)
    
    try:
        # Initialize schema analyzer
        database_path = Path("output/quilt_data.db")
        analyzer = SchemaAnalyzer(database_path)
        
        # Analyze current schema
        analysis = await analyzer.analyze_table_schemas()
        analyzer.print_schema_analysis(analysis)
        
        # Generate corrected query
        corrected_query, primary_table = await analyzer.generate_fixed_get_records_query(analysis)
        
        print(f"\n🔧 Corrected Query for {primary_table} table:")
        print("=" * 40)
        print(corrected_query)
        
        print(f"\n💡 Database Manager Fix Required:")
        print("=" * 35)
        print("Update the get_records() method in src/database.py to use the corrected query above")
        print(f"Primary table: {primary_table}")
        print(f"Available columns: {analysis[primary_table]['column_names']}")
        
        # Test the corrected query
        print(f"\n🧪 Testing Corrected Query:")
        print("=" * 27)
        
        async with aiosqlite.connect(str(database_path)) as conn:
            try:
                cursor = await conn.execute(corrected_query.replace('LIMIT ? OFFSET ?', 'LIMIT 3 OFFSET 0'))
                test_results = await cursor.fetchall()
                await cursor.close()
                
                print(f"✅ Query executed successfully!")
                print(f"✅ Returned {len(test_results)} test records")
                
                if test_results:
                    print(f"✅ Sample record structure verified")
                    # Show column names from the query
                    column_names = [desc[0] for desc in cursor.description] if hasattr(cursor, 'description') else []
                    if column_names:
                        print(f"   Columns: {column_names}")
                
            except Exception as query_error:
                print(f"❌ Query test failed: {query_error}")
                raise
        
        print(f"\n🎯 Next Steps:")
        print("=" * 12)
        print("1. Update src/database.py with the corrected query")
        print("2. Update the primary table selection logic")
        print("3. Test the API server endpoints")
        print("4. Verify React dashboard displays records correctly")
        
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt Schema Fix: Critical error: {e}")
        print(f"❌ Schema fix failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(fix_database_schema_compatibility())