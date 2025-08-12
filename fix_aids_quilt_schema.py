#!/usr/bin/env python3
"""
AIDS Memorial Quilt Database Schema Fix
Implements comprehensive error handling with specific exception types
Following project coding standards for digital humanities research applications
"""

import sqlite3
import asyncio
import aiosqlite
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

# Configure structured logging with appropriate log levels
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - AIDS Memorial Quilt Schema Fix - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseSchemaCompatibilityError(Exception):
    """Raised when database schema compatibility issues occur"""
    pass

class QuiltDatabaseSchemaPatcher:
    """
    Database schema compatibility patcher for AIDS Memorial Quilt Records
    Implements separation of concerns and asynchronous processing patterns
    """
    
    def __init__(self, database_path: Path) -> None:
        """
        Initialize schema patcher with comprehensive error handling
        
        Args:
            database_path: Path to SQLite database file
            
        Raises:
            DatabaseSchemaCompatibilityError: If database path is invalid
        """
        self.database_path = Path(database_path)
        if not self.database_path.exists():
            raise DatabaseSchemaCompatibilityError(f"Database file not found: {database_path}")
        
        logger.info(f"AIDS Memorial Quilt Schema Fix: Initialized with {self.database_path}")
    
    async def analyze_current_schema(self) -> Dict[str, Any]:
        """
        Analyze current database schema for AIDS Memorial Quilt Records
        Implements async/await patterns for non-blocking database operations
        
        Returns:
            Schema analysis results with table information and compatibility status
        """
        logger.info("AIDS Memorial Quilt Schema Fix: Analyzing current database schema")
        
        schema_analysis = {
            "tables": {},
            "primary_data_source": None,
            "compatibility_issues": [],
            "recommended_fixes": []
        }
        
        try:
            async with aiosqlite.connect(str(self.database_path)) as conn:
                # Get all table names
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
                tables = await cursor.fetchall()
                table_names = [table[0] for table in tables]
                await cursor.close()
                
                logger.info(f"AIDS Memorial Quilt Schema Fix: Found tables: {table_names}")
                
                # Analyze each table
                for table_name in table_names:
                    try:
                        # Get table schema
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
                        column_names = []
                        for col_info in columns_info:
                            column_data = {
                                'name': col_info[1],
                                'type': col_info[2],
                                'not_null': bool(col_info[3]),
                                'default_value': col_info[4],
                                'primary_key': bool(col_info[5])
                            }
                            columns.append(column_data)
                            column_names.append(col_info[1])
                        
                        schema_analysis["tables"][table_name] = {
                            'row_count': row_count,
                            'columns': columns,
                            'column_names': column_names,
                            'has_data': row_count > 0
                        }
                        
                        logger.info(f"AIDS Memorial Quilt Schema Fix: {table_name} - {row_count:,} rows, {len(columns)} columns")
                        
                        # Check for specific compatibility issues
                        if table_name in ['quilt_blocks', 'quilt_panels']:
                            if 'metadata' not in column_names and 'metadata_json' in column_names:
                                schema_analysis["compatibility_issues"].append({
                                    'table': table_name,
                                    'issue': 'Column name mismatch: code expects "metadata" but database has "metadata_json"',
                                    'severity': 'critical'
                                })
                    
                    except Exception as table_error:
                        logger.error(f"AIDS Memorial Quilt Schema Fix: Error analyzing table {table_name}: {table_error}")
                        schema_analysis["compatibility_issues"].append({
                            'table': table_name,
                            'issue': f'Analysis error: {table_error}',
                            'severity': 'error'
                        })
                
                # Determine primary data source based on row counts
                max_rows = 0
                for table_name, table_info in schema_analysis["tables"].items():
                    if table_info['has_data'] and table_info['row_count'] > max_rows:
                        max_rows = table_info['row_count']
                        schema_analysis["primary_data_source"] = table_name
                
                logger.info(f"AIDS Memorial Quilt Schema Fix: Primary data source: {schema_analysis['primary_data_source']}")
        
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Schema Fix: Critical error during schema analysis: {e}")
            schema_analysis["critical_error"] = str(e)
        
        return schema_analysis
    
    def generate_corrected_query(self, table_name: str, available_columns: List[str]) -> str:
        """
        Generate corrected SQL query for AIDS Memorial Quilt Records API compatibility
        Implements intelligent column mapping following digital humanities standards
        
        Args:
            table_name: Name of the database table
            available_columns: List of available column names in the table
            
        Returns:
            Corrected SQL query string
        """
        logger.info(f"AIDS Memorial Quilt Schema Fix: Generating corrected query for {table_name}")
        
        # Define API-compatible column mappings
        if table_name == "quilt_blocks":
            column_mappings = {
                "id": self._find_best_column(available_columns, ["id", "block_id"]),
                "item_id": self._find_best_column(available_columns, ["block_id", "id"]),
                "title": self._find_best_column(available_columns, ["title"]),
                "description": self._find_best_column(available_columns, ["description"]),
                "subjects": self._extract_metadata_field("subjects", available_columns),
                "names": self._extract_metadata_field("names", available_columns),
                "dates": self._find_best_column(available_columns, ["created_date", "scraped_at", "updated_at"]),
                "url": "NULL",  # Not available in blocks table
                "image_url": "NULL",  # Not available in blocks table
                "content_hash": "NULL",
                "created_at": self._find_best_column(available_columns, ["scraped_at", "created_date"]),
                "updated_at": self._find_best_column(available_columns, ["updated_at", "scraped_at"])
            }
        elif table_name == "quilt_panels":
            column_mappings = {
                "id": self._find_best_column(available_columns, ["id", "panel_id"]),
                "item_id": self._find_best_column(available_columns, ["panel_id", "block_id", "id"]),
                "title": self._find_best_column(available_columns, ["title"]),
                "description": self._find_best_column(available_columns, ["description"]),
                "subjects": self._extract_metadata_field("subjects", available_columns),
                "names": self._extract_metadata_field("names", available_columns),
                "dates": self._find_best_column(available_columns, ["scraped_at", "updated_at"]),
                "url": self._find_best_column(available_columns, ["image_urls"]),
                "image_url": self._find_best_column(available_columns, ["image_urls"]),
                "content_hash": "NULL",
                "created_at": self._find_best_column(available_columns, ["scraped_at"]),
                "updated_at": self._find_best_column(available_columns, ["updated_at", "scraped_at"])
            }
        else:
            # Default mapping for other tables
            column_mappings = {
                "id": self._find_best_column(available_columns, ["id"]),
                "item_id": self._find_best_column(available_columns, ["item_id", "id"]),
                "title": self._find_best_column(available_columns, ["title"]),
                "description": self._find_best_column(available_columns, ["description"]),
                "subjects": self._find_best_column(available_columns, ["subjects"]),
                "names": self._find_best_column(available_columns, ["names"]),
                "dates": self._find_best_column(available_columns, ["dates", "created_at"]),
                "url": self._find_best_column(available_columns, ["url"]),
                "image_url": self._find_best_column(available_columns, ["image_url"]),
                "content_hash": self._find_best_column(available_columns, ["content_hash"]),
                "created_at": self._find_best_column(available_columns, ["created_at"]),
                "updated_at": self._find_best_column(available_columns, ["updated_at"])
            }
        
        # Build SELECT clause
        select_clauses = []
        for api_column, db_column in column_mappings.items():
            if db_column and db_column != "NULL":
                if db_column != api_column:
                    select_clauses.append(f"{db_column} as {api_column}")
                else:
                    select_clauses.append(api_column)
            else:
                select_clauses.append(f"NULL as {api_column}")
        
        # Determine best ordering column
        order_column = self._find_best_column(available_columns, ["updated_at", "scraped_at", "created_at", "id"], "id")
        
        # Generate the corrected query
        corrected_query = f"""
            SELECT {', '.join(select_clauses)}
            FROM {table_name}
            ORDER BY {order_column} DESC
            LIMIT ? OFFSET ?
        """.strip()
        
        return corrected_query
    
    def _find_best_column(self, available_columns: List[str], 
                         candidates: List[str], 
                         fallback: str = "NULL") -> str:
        """
        Find the best matching column from candidates
        Implements intelligent column matching following project standards
        """
        for candidate in candidates:
            if candidate in available_columns:
                return candidate
        return fallback
    
    def _extract_metadata_field(self, field_name: str, available_columns: List[str]) -> str:
        """
        Generate SQL expression to extract metadata fields from JSON column
        Implements JSON parsing for AIDS Memorial Quilt metadata preservation
        """
        if "metadata_json" in available_columns:
            return f"json_extract(metadata_json, '$.{field_name}')"
        elif field_name in available_columns:
            return field_name
        else:
            return "NULL"
    
    async def test_corrected_query(self, table_name: str, query: str) -> Dict[str, Any]:
        """
        Test corrected query to ensure it works with the database
        Implements comprehensive testing with error handling
        
        Args:
            table_name: Name of the table to test
            query: SQL query to test
            
        Returns:
            Test results with success status and sample data
        """
        logger.info(f"AIDS Memorial Quilt Schema Fix: Testing corrected query for {table_name}")
        
        test_result = {
            "table": table_name,
            "success": False,
            "error": None,
            "sample_data": None,
            "column_names": []
        }
        
        try:
            async with aiosqlite.connect(str(self.database_path)) as conn:
                # Test with LIMIT 1 to get sample data
                test_query = query.replace("LIMIT ? OFFSET ?", "LIMIT 1 OFFSET 0")
                
                cursor = await conn.execute(test_query)
                sample_row = await cursor.fetchone()
                
                if sample_row:
                    # Get column names from cursor description
                    column_names = [desc[0] for desc in cursor.description]
                    sample_data = dict(zip(column_names, sample_row))
                    
                    test_result.update({
                        "success": True,
                        "sample_data": sample_data,
                        "column_names": column_names
                    })
                    
                    logger.info(f"AIDS Memorial Quilt Schema Fix: Query test successful for {table_name}")
                else:
                    test_result["error"] = "Query returned no results"
                
                await cursor.close()
        
        except Exception as e:
            test_result["error"] = str(e)
            logger.error(f"AIDS Memorial Quilt Schema Fix: Query test failed for {table_name}: {e}")
        
        return test_result
    
    def generate_database_manager_patch(self, schema_analysis: Dict[str, Any]) -> str:
        """
        Generate complete database manager patch code
        Implements comprehensive error handling and async/await patterns
        
        Args:
            schema_analysis: Results from schema analysis
            
        Returns:
            Complete patch code for DatabaseManager
        """
        primary_table = schema_analysis.get("primary_data_source", "quilt_blocks")
        table_info = schema_analysis["tables"].get(primary_table, {})
        available_columns = table_info.get("column_names", [])
        
        corrected_query = self.generate_corrected_query(primary_table, available_columns)
        
        patch_code = f'''#!/usr/bin/env python3
"""
AIDS Memorial Quilt Database Manager - Schema Compatibility Patch
Implements comprehensive error handling with specific exception types
Following project coding standards for digital humanities research applications
"""

async def get_records(self, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Get AIDS Memorial Quilt records with pagination (PATCHED VERSION)
    Implements error resilience and digital humanities research standards
    Addresses schema compatibility for {primary_table} table
    
    Args:
        limit: Maximum number of records to return (1-1000)
        offset: Number of records to skip (>=0)
        
    Returns:
        List of record dictionaries compatible with API format
        
    Raises:
        DatabaseConnectionError: If database connection fails
        DataValidationError: If parameters are invalid
    """
    try:
        if not self.connection:
            raise DatabaseConnectionError("Database not initialized")
        
        # Validate parameters following project error handling guidelines
        if limit <= 0 or limit > 1000:
            raise DataValidationError("Limit must be between 1 and 1000")
        if offset < 0:
            raise DataValidationError("Offset must be non-negative")
        
        # Use schema-corrected query for {primary_table} table
        query = """{corrected_query}"""
        
        cursor = await self.connection.execute(query, (limit, offset))
        rows = await cursor.fetchall()
        await cursor.close()
        
        # Convert rows to dictionaries with proper AIDS Memorial Quilt metadata handling
        columns = ["id", "item_id", "title", "description", "subjects", 
                  "names", "dates", "url", "image_url", "content_hash", 
                  "created_at", "updated_at"]
        
        records = []
        for row in rows:
            record = dict(zip(columns, row))
            
            # Parse JSON metadata fields safely for AIDS Memorial Quilt preservation
            for json_field in ["subjects", "names"]:
                field_value = record.get(json_field)
                if field_value and field_value != "NULL" and field_value is not None:
                    try:
                        # Handle various JSON formats for digital humanities metadata
                        if isinstance(field_value, str):
                            if field_value.startswith('[') or field_value.startswith('{{'):
                                record[json_field] = json.loads(field_value)
                            else:
                                record[json_field] = [field_value]  # Convert single string to list
                        elif isinstance(field_value, list):
                            record[json_field] = field_value
                        else:
                            record[json_field] = [str(field_value)] if field_value else []
                    except (json.JSONDecodeError, TypeError) as parse_error:
                        logger.warning(f"AIDS Memorial Quilt DB: Error parsing {{json_field}}: {{parse_error}}")
                        record[json_field] = []
                else:
                    record[json_field] = []
            
            # Handle image_urls field for AIDS Memorial Quilt panels
            if record.get("url") or record.get("image_url"):
                for url_field in ["url", "image_url"]:
                    url_value = record.get(url_field)
                    if url_value and url_value != "NULL" and url_value is not None:
                        try:
                            # Parse image URLs JSON array for Library of Congress images
                            if isinstance(url_value, str) and url_value.startswith('['):
                                urls_list = json.loads(url_value)
                                if isinstance(urls_list, list) and urls_list:
                                    record[url_field] = urls_list[0]  # Use first URL
                                else:
                                    record[url_field] = None
                            # Keep string URLs as-is for Library of Congress attribution
                        except (json.JSONDecodeError, TypeError):
                            # Keep original value if not valid JSON
                            pass
            
            # Ensure proper Library of Congress attribution for dates
            if record.get("dates") and record["dates"] != "NULL":
                try:
                    if isinstance(record["dates"], str) and not record["dates"].startswith('['):
                        record["dates"] = [record["dates"]]
                except Exception:
                    record["dates"] = []
            else:
                record["dates"] = []
            
            records.append(record)
        
        logger.info(f"AIDS Memorial Quilt DB: Retrieved {{len(records)}} records from {primary_table}")
        return records
        
    except (DatabaseConnectionError, DataValidationError):
        raise
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt DB: Critical error getting records: {{e}}")
        raise DatabaseConnectionError(f"Failed to get records from AIDS Memorial Quilt database: {{e}}")

# Also update the primary data source determination logic:
async def _determine_primary_data_source(self) -> str:
    """
    Determine primary data source table for AIDS Memorial Quilt Records
    Implements intelligent table selection following digital humanities standards
    
    Returns:
        Name of the primary table to use for record retrieval
    """
    try:
        # Check table row counts to determine best data source
        tables_to_check = ["collection_items", "quilt_blocks", "quilt_panels"]
        table_stats = {{}}
        
        for table_name in tables_to_check:
            try:
                cursor = await self.connection.execute(f"SELECT COUNT(*) FROM {{table_name}}")
                result = await cursor.fetchone()
                table_stats[table_name] = result[0] if result else 0
                await cursor.close()
            except Exception:
                table_stats[table_name] = 0
        
        # Prefer collection_items if it has data, otherwise use table with most records
        if table_stats.get("collection_items", 0) > 0:
            primary_source = "collection_items"
        else:
            primary_source = max(table_stats.items(), key=lambda x: x[1])[0]
        
        logger.info(f"AIDS Memorial Quilt DB: Primary data source determined as '{{primary_source}}' ({{', '.join([f'{{k}}: {{v}}' for k, v in table_stats.items()])}})")
        return primary_source
        
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt DB: Error determining primary data source: {{e}}")
        return "quilt_blocks"  # Safe fallback
'''
        
        return patch_code

async def fix_aids_quilt_database_schema() -> None:
    """
    Main function to fix AIDS Memorial Quilt database schema compatibility
    Implements comprehensive error handling and async/await patterns
    Following digital humanities research standards
    """
    print("🔧 AIDS Memorial Quilt Database Schema Compatibility Fix")
    print("=" * 56)
    print("Following digital humanities research standards with comprehensive error handling")
    
    try:
        # Initialize schema patcher
        database_path = Path("output/quilt_data.db")
        patcher = QuiltDatabaseSchemaPatcher(database_path)
        
        # Analyze current schema
        print("\\n📋 Analyzing Database Schema...")
        schema_analysis = await patcher.analyze_current_schema()
        
        if "critical_error" in schema_analysis:
            print(f"❌ Critical error during analysis: {schema_analysis['critical_error']}")
            return
        
        # Display analysis results
        tables = schema_analysis.get("tables", {})
        primary_table = schema_analysis.get("primary_data_source")
        
        print(f"\\n✅ Schema Analysis Complete")
        print(f"   Primary data source: {primary_table}")
        print(f"   Total tables analyzed: {len(tables)}")
        
        for table_name, table_info in tables.items():
            if table_info.get("has_data", False):
                row_count = table_info.get("row_count", 0)
                column_count = len(table_info.get("columns", []))
                print(f"   📊 {table_name}: {row_count:,} rows, {column_count} columns")
        
        # Show compatibility issues
        issues = schema_analysis.get("compatibility_issues", [])
        if issues:
            print(f"\\n⚠️  Found {len(issues)} compatibility issue(s):")
            for issue in issues:
                severity = issue.get("severity", "unknown")
                table = issue.get("table", "unknown")
                description = issue.get("issue", "unknown")
                print(f"   {severity.upper()}: {table} - {description}")
        
        # Generate and test corrected query
        if primary_table and primary_table in tables:
            table_info = tables[primary_table]
            available_columns = table_info.get("column_names", [])
            
            print(f"\\n🔧 Generating Corrected Query for {primary_table}...")
            corrected_query = patcher.generate_corrected_query(primary_table, available_columns)
            
            print(f"\\n📝 Corrected Query:")
            print("-" * 20)
            print(corrected_query)
            
            # Test the corrected query
            print(f"\\n🧪 Testing Corrected Query...")
            test_result = await patcher.test_corrected_query(primary_table, corrected_query)
            
            if test_result["success"]:
                print(f"✅ Query test successful!")
                print(f"   Columns: {test_result['column_names']}")
                
                sample_data = test_result.get("sample_data", {})
                if sample_data:
                    print(f"   Sample record:")
                    for key, value in list(sample_data.items())[:5]:  # Show first 5 fields
                        display_value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                        print(f"     {key}: {display_value}")
            else:
                print(f"❌ Query test failed: {test_result['error']}")
                return
            
            # Generate complete patch
            print(f"\\n📁 Generating Database Manager Patch...")
            patch_code = patcher.generate_database_manager_patch(schema_analysis)
            
            # Save patch to file
            patch_file = Path("aids_quilt_database_manager_patch.py")
            with open(patch_file, 'w', encoding='utf-8') as f:
                f.write(patch_code)
            
            print(f"✅ Patch saved to: {patch_file}")
            
            print(f"\\n🎯 Next Steps:")
            print("=" * 12)
            print("1. Review the generated patch code")
            print("2. Replace the get_records() method in src/database.py with the patched version")
            print("3. Update the _determine_primary_data_source() method if needed")
            print("4. Restart the API server: python api_server.py")
            print("5. Test the /records endpoint")
            print("6. Verify React dashboard displays AIDS Memorial Quilt records")
            
            print(f"\\n💡 Schema Fix Summary:")
            print(f"   • Fixed 'metadata' -> 'metadata_json' column mapping")
            print(f"   • Optimized for {primary_table} table with {table_info['row_count']:,} records")
            print(f"   • Maintains Library of Congress attribution standards")
            print(f"   • Implements digital humanities metadata preservation")
        
        else:
            print(f"❌ No suitable primary table found for patch generation")
    
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt Schema Fix: Critical error: {e}")
        print(f"❌ Schema fix failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(fix_aids_quilt_database_schema())