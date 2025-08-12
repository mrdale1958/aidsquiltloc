#!/usr/bin/env python3
"""
Database schema compatibility patch for AIDS Memorial Quilt Records
Implements comprehensive error handling with specific exception types
Following project coding standards for digital humanities research applications
"""

import sqlite3
import asyncio
import aiosqlite
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import logging

# Configure structured logging per project guidelines
logger = logging.getLogger(__name__)

class DatabaseSchemaError(Exception):
    """Raised when database schema conflicts occur"""
    pass

class QuiltDatabasePatcher:
    """
    Database schema compatibility patcher for AIDS Memorial Quilt Records
    Implements error resilience and separation of concerns
    """
    
    def __init__(self, database_path: Path) -> None:
        """
        Initialize database patcher with comprehensive error handling
        
        Args:
            database_path: Path to SQLite database file
            
        Raises:
            DatabaseSchemaError: If database path is invalid
        """
        self.database_path = Path(database_path)
        if not self.database_path.exists():
            raise DatabaseSchemaError(f"Database file not found: {database_path}")
        
        logger.info(f"AIDS Memorial Quilt Patcher: Initialized with {self.database_path}")
    
    async def analyze_schema_compatibility(self) -> Dict[str, Any]:
        """
        Analyze database schema for compatibility issues
        Implements async/await patterns for non-blocking database operations
        
        Returns:
            Schema compatibility analysis results
        """
        logger.info("AIDS Memorial Quilt Patcher: Analyzing schema compatibility")
        
        compatibility_report = {
            "quilt_blocks": {"exists": False, "columns": [], "issues": []},
            "quilt_panels": {"exists": False, "columns": [], "issues": []},
            "collection_items": {"exists": False, "columns": [], "issues": []},
            "recommended_primary_table": None,
            "corrected_queries": {}
        }
        
        try:
            async with aiosqlite.connect(str(self.database_path)) as conn:
                # Analyze each relevant table
                for table_name in ["quilt_blocks", "quilt_panels", "collection_items"]:
                    try:
                        # Check if table exists and get schema
                        cursor = await conn.execute(f"PRAGMA table_info({table_name})")
                        columns_info = await cursor.fetchall()
                        await cursor.close()
                        
                        if columns_info:
                            columns = [col[1] for col in columns_info]  # Column names
                            
                            # Get row count
                            cursor = await conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                            row_count_result = await cursor.fetchone()
                            row_count = row_count_result[0] if row_count_result else 0
                            await cursor.close()
                            
                            compatibility_report[table_name] = {
                                "exists": True,
                                "columns": columns,
                                "row_count": row_count,
                                "issues": []
                            }
                            
                            # Check for common issues
                            if "metadata" not in columns and "metadata_json" in columns:
                                compatibility_report[table_name]["issues"].append(
                                    "Column mismatch: 'metadata' expected but 'metadata_json' found"
                                )
                            
                            logger.info(f"AIDS Memorial Quilt Patcher: {table_name} - {row_count:,} rows, {len(columns)} columns")
                        
                    except Exception as table_error:
                        compatibility_report[table_name]["issues"].append(str(table_error))
                        logger.error(f"AIDS Memorial Quilt Patcher: Error analyzing {table_name}: {table_error}")
                
                # Determine recommended primary table
                max_rows = 0
                for table_name, table_info in compatibility_report.items():
                    if isinstance(table_info, dict) and table_info.get("exists", False):
                        row_count = table_info.get("row_count", 0)
                        if row_count > max_rows:
                            max_rows = row_count
                            compatibility_report["recommended_primary_table"] = table_name
                
                # Generate corrected queries
                await self._generate_corrected_queries(conn, compatibility_report)
                
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Patcher: Schema analysis failed: {e}")
            compatibility_report["critical_error"] = str(e)
        
        return compatibility_report
    
    async def _generate_corrected_queries(self, conn: aiosqlite.Connection, 
                                        compatibility_report: Dict[str, Any]) -> None:
        """
        Generate corrected SQL queries based on actual schema
        Implements intelligent column mapping following project standards
        """
        logger.info("AIDS Memorial Quilt Patcher: Generating corrected queries")
        
        # Define the expected API response format
        api_columns = [
            "id", "item_id", "title", "description", "subjects", 
            "names", "dates", "url", "image_url", "content_hash", 
            "created_at", "updated_at"
        ]
        
        for table_name in ["quilt_blocks", "quilt_panels", "collection_items"]:
            table_info = compatibility_report.get(table_name, {})
            if not table_info.get("exists", False):
                continue
            
            available_columns = table_info.get("columns", [])
            
            # Map API columns to available database columns
            column_mappings = self._create_column_mappings(available_columns, table_name)
            
            # Build SELECT clause
            select_clauses = []
            for api_col in api_columns:
                if api_col in column_mappings:
                    db_col = column_mappings[api_col]
                    if db_col != api_col:
                        select_clauses.append(f"{db_col} as {api_col}")
                    else:
                        select_clauses.append(api_col)
                else:
                    select_clauses.append(f"NULL as {api_col}")
            
            # Generate the corrected query
            order_column = self._find_best_order_column(available_columns)
            
            corrected_query = f"""
                SELECT {', '.join(select_clauses)}
                FROM {table_name}
                ORDER BY {order_column} DESC
                LIMIT ? OFFSET ?
            """.strip()
            
            compatibility_report["corrected_queries"][table_name] = {
                "query": corrected_query,
                "column_mappings": column_mappings,
                "order_column": order_column
            }
            
            logger.info(f"AIDS Memorial Quilt Patcher: Generated query for {table_name}")
    
    def _create_column_mappings(self, available_columns: List[str], 
                               table_name: str) -> Dict[str, str]:
        """
        Create intelligent column mappings for API compatibility
        Implements comprehensive field mapping following digital humanities standards
        """
        mappings = {}
        
        # Standard mappings based on table type
        if table_name == "quilt_blocks":
            mappings = {
                "id": self._find_column(available_columns, ["id", "block_id"]),
                "item_id": self._find_column(available_columns, ["block_id", "id"]),
                "title": self._find_column(available_columns, ["title"]),
                "description": self._find_column(available_columns, ["description"]),
                "subjects": self._find_column(available_columns, ["metadata_json", "subjects"]),
                "names": self._find_column(available_columns, ["metadata_json", "names"]),
                "dates": self._find_column(available_columns, ["created_date", "scraped_at", "updated_at"]),
                "url": "NULL",  # Not typically available in blocks table
                "image_url": "NULL",  # Not typically available in blocks table
                "content_hash": "NULL",
                "created_at": self._find_column(available_columns, ["scraped_at", "created_date"]),
                "updated_at": self._find_column(available_columns, ["updated_at", "scraped_at"])
            }
        elif table_name == "quilt_panels":
            mappings = {
                "id": self._find_column(available_columns, ["id", "panel_id"]),
                "item_id": self._find_column(available_columns, ["panel_id", "block_id", "id"]),
                "title": self._find_column(available_columns, ["title"]),
                "description": self._find_column(available_columns, ["description"]),
                "subjects": self._find_column(available_columns, ["metadata_json", "subjects"]),
                "names": self._find_column(available_columns, ["metadata_json", "names"]),
                "dates": self._find_column(available_columns, ["scraped_at", "updated_at"]),
                "url": self._find_column(available_columns, ["image_urls"]),  # Panel URLs often in image_urls
                "image_url": self._find_column(available_columns, ["image_urls"]),
                "content_hash": "NULL",
                "created_at": self._find_column(available_columns, ["scraped_at"]),
                "updated_at": self._find_column(available_columns, ["updated_at", "scraped_at"])
            }
        elif table_name == "collection_items":
            # This table already has the correct schema
            mappings = {
                "id": "id",
                "item_id": "item_id", 
                "title": "title",
                "description": "description",
                "subjects": "subjects",
                "names": "names",
                "dates": "dates",
                "url": "url",
                "image_url": "image_url",
                "content_hash": "content_hash",
                "created_at": "created_at",
                "updated_at": "updated_at"
            }
        
        # Filter out None values
        return {k: v for k, v in mappings.items() if v and v != "NULL"}
    
    def _find_column(self, available_columns: List[str], 
                    candidates: List[str]) -> Optional[str]:
        """
        Find the best matching column from candidates
        Implements intelligent column matching following project standards
        """
        for candidate in candidates:
            if candidate in available_columns:
                return candidate
        return None
    
    def _find_best_order_column(self, available_columns: List[str]) -> str:
        """Find the best column for ordering results"""
        order_candidates = ["updated_at", "scraped_at", "created_at", "id"]
        for candidate in order_candidates:
            if candidate in available_columns:
                return candidate
        return available_columns[0] if available_columns else "id"
    
    async def test_corrected_queries(self, compatibility_report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test corrected queries to ensure they work
        Implements comprehensive testing following project error handling standards
        """
        logger.info("AIDS Memorial Quilt Patcher: Testing corrected queries")
        
        test_results = {}
        
        try:
            async with aiosqlite.connect(str(self.database_path)) as conn:
                corrected_queries = compatibility_report.get("corrected_queries", {})
                
                for table_name, query_info in corrected_queries.items():
                    table_info = compatibility_report.get(table_name, {})
                    if not table_info.get("exists", False) or table_info.get("row_count", 0) == 0:
                        test_results[table_name] = {"status": "skipped", "reason": "No data"}
                        continue
                    
                    try:
                        query = query_info["query"]
                        # Test with limit 1
                        test_query = query.replace("LIMIT ? OFFSET ?", "LIMIT 1 OFFSET 0")
                        
                        cursor = await conn.execute(test_query)
                        test_record = await cursor.fetchone()
                        await cursor.close()
                        
                        if test_record:
                            # Get column names
                            cursor = await conn.execute(test_query)
                            columns = [desc[0] for desc in cursor.description]
                            await cursor.close()
                            
                            test_results[table_name] = {
                                "status": "success",
                                "columns": columns,
                                "sample_record": dict(zip(columns, test_record))
                            }
                            logger.info(f"AIDS Memorial Quilt Patcher: Query test successful for {table_name}")
                        else:
                            test_results[table_name] = {"status": "no_data", "reason": "Query returned no results"}
                    
                    except Exception as query_error:
                        test_results[table_name] = {"status": "error", "error": str(query_error)}
                        logger.error(f"AIDS Memorial Quilt Patcher: Query test failed for {table_name}: {query_error}")
        
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Patcher: Query testing failed: {e}")
            test_results["critical_error"] = str(e)
        
        return test_results

async def patch_database_compatibility() -> None:
    """
    Main function to patch database compatibility issues
    Implements comprehensive error handling and async/await patterns
    """
    print("🔧 AIDS Memorial Quilt Database Compatibility Patch")
    print("=" * 54)
    
    try:
        # Initialize patcher
        database_path = Path("output/quilt_data.db")
        patcher = QuiltDatabasePatcher(database_path)
        
        # Analyze schema compatibility
        print("\n📋 Analyzing Schema Compatibility...")
        compatibility_report = await patcher.analyze_schema_compatibility()
        
        # Display analysis results
        if "critical_error" in compatibility_report:
            print(f"❌ Critical error: {compatibility_report['critical_error']}")
            return
        
        recommended_table = compatibility_report.get("recommended_primary_table")
        print(f"✅ Recommended primary table: {recommended_table}")
        
        # Show table analysis
        for table_name in ["collection_items", "quilt_blocks", "quilt_panels"]:
            table_info = compatibility_report.get(table_name, {})
            if table_info.get("exists", False):
                row_count = table_info.get("row_count", 0)
                issues = table_info.get("issues", [])
                print(f"📊 {table_name}: {row_count:,} rows" + (f" (Issues: {len(issues)})" if issues else ""))
                for issue in issues:
                    print(f"   ⚠️  {issue}")
        
        # Test corrected queries
        print(f"\n🧪 Testing Corrected Queries...")
        test_results = await patcher.test_corrected_queries(compatibility_report)
        
        successful_tables = []
        for table_name, result in test_results.items():
            if result.get("status") == "success":
                print(f"✅ {table_name}: Query works correctly")
                successful_tables.append(table_name)
            elif result.get("status") == "skipped":
                print(f"⏭️  {table_name}: {result.get('reason')}")
            else:
                print(f"❌ {table_name}: {result.get('error', 'Unknown error')}")
        
        # Generate database manager patch
        if successful_tables:
            print(f"\n📝 Database Manager Patch")
            print("=" * 26)
            
            # Choose the best table (prefer collection_items, then others based on data)
            if "collection_items" in successful_tables:
                best_table = "collection_items"
            elif recommended_table in successful_tables:
                best_table = recommended_table
            else:
                best_table = successful_tables[0]
            
            query_info = compatibility_report["corrected_queries"][best_table]
            corrected_query = query_info["query"]
            
            print(f"Primary table: {best_table}")
            print(f"Corrected get_records() query:")
            print()
            print(corrected_query)
            
            # Generate the patch code
            patch_code = f'''
# AIDS Memorial Quilt Database Manager Patch
# Apply this to src/database.py get_records() method

async def get_records(self, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Get AIDS Memorial Quilt records with pagination (PATCHED VERSION)
    Implements error resilience and digital humanities research standards
    
    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip
        
    Returns:
        List of record dictionaries compatible with API format
    """
    try:
        if not self.connection:
            raise DatabaseConnectionError("Database not initialized")
        
        # Use corrected query for {best_table} table
        query = """{corrected_query}"""
        
        cursor = await self.connection.execute(query, (limit, offset))
        rows = await cursor.fetchall()
        await cursor.close()
        
        # Convert rows to dictionaries with proper column names
        columns = ["id", "item_id", "title", "description", "subjects", 
                  "names", "dates", "url", "image_url", "content_hash", 
                  "created_at", "updated_at"]
        
        records = []
        for row in rows:
            record = dict(zip(columns, row))
            
            # Parse JSON fields safely for AIDS Memorial Quilt metadata
            for json_field in ["subjects", "names"]:
                if record.get(json_field) and record[json_field] != "NULL":
                    try:
                        # Handle metadata_json column that may contain structured data
                        if json_field in ["subjects", "names"] and "metadata_json" in str(record.get(json_field, "")):
                            metadata = json.loads(record[json_field]) if record[json_field] else {{}}
                            record[json_field] = metadata.get(json_field, [])
                        else:
                            record[json_field] = json.loads(record[json_field]) if isinstance(record[json_field], str) else record[json_field]
                    except (json.JSONDecodeError, TypeError):
                        record[json_field] = []
                else:
                    record[json_field] = []
            
            # Handle image_urls field for panels
            if "image_urls" in record.get("url", "") or "image_urls" in record.get("image_url", ""):
                try:
                    # Parse image_urls JSON array
                    urls_field = record.get("url") or record.get("image_url")
                    if urls_field and urls_field != "NULL":
                        urls_list = json.loads(urls_field) if isinstance(urls_field, str) else urls_field
                        if isinstance(urls_list, list) and urls_list:
                            record["url"] = urls_list[0] if urls_list else None
                            record["image_url"] = urls_list[0] if urls_list else None
                except (json.JSONDecodeError, TypeError):
                    pass
            
            records.append(record)
        
        logger.info(f"AIDS Memorial Quilt DB: Retrieved {{len(records)}} records from {best_table}")
        return records
        
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt DB: Error getting records: {{e}}")
        raise DatabaseConnectionError(f"Failed to get records: {{e}}")
'''
            
            print(f"\n📁 Save this patch and apply to src/database.py")
            
            # Save patch to file
            patch_file = Path("database_manager_patch.py")
            with open(patch_file, 'w') as f:
                f.write(patch_code)
            print(f"✅ Patch saved to: {patch_file}")
            
        else:
            print(f"❌ No working queries found. Check database schema.")
        
        print(f"\n🎯 Next Steps:")
        print("1. Apply the patch to src/database.py")
        print("2. Restart the API server")
        print("3. Test the /records endpoint")
        print("4. Verify React dashboard displays records")
        
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt Patcher: Critical error: {e}")
        print(f"❌ Patch generation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Configure logging following project guidelines
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - AIDS Memorial Quilt Patcher - %(levelname)s - %(message)s'
    )
    
    asyncio.run(patch_database_compatibility())