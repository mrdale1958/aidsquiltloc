"""
Enhanced database manager for AIDS Memorial Quilt Records
Implements comprehensive error handling and async operations following project standards
Supports QuiltBlock and QuiltPanel schema with proper type safety
"""

import asyncio
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import logging
import json
import traceback
from datetime import datetime, timedelta

# Configure structured logging per project guidelines
logger = logging.getLogger(__name__)

class DatabaseConnectionError(Exception):
    """Raised when database connection operations fail"""
    pass

class DataValidationError(Exception):
    """Raised when data validation fails"""
    pass

class DatabaseManager:
    """
    Manages SQLite database operations for AIDS Memorial Quilt Records
    Implements sync operations with comprehensive error handling
    Following project standards for digital humanities research
    """
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        """
        Initialize database manager with configurable path
        
        Args:
            db_path: Optional path to database file, defaults to project data directory
        """
        if db_path is None:
            # Use project data directory following naming conventions
            project_root = Path(__file__).parent.parent
            data_dir = project_root / "data"
            data_dir.mkdir(exist_ok=True)
            self.db_path = data_dir / "aids_memorial_quilt.db"
        else:
            self.db_path = Path(db_path)
        
        self.connection = None
        self._primary_data_source = None  # Cache the primary data source
        logger.info(f"AIDS Memorial Quilt Database: Initialized with path {self.db_path}")
    
    async def initialize(self) -> None:
        """
        Initialize database connection and create tables
        Implements comprehensive error handling for database setup
        """
        try:
            logger.info("AIDS Memorial Quilt Database: Initializing database connection")
            
            # Ensure database directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create database connection
            self.connection = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.connection.row_factory = sqlite3.Row  # Enable column access by name
            
            # Create tables if they don't exist
            await self._create_tables()
            
            # Determine and cache primary data source
            await self._determine_primary_data_source()
            
            logger.info("AIDS Memorial Quilt Database: Successfully initialized")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Initialization failed: {e}")
            if self.connection:
                self.connection.close()
                self.connection = None
            raise
    
    async def _create_tables(self) -> None:
        """
        Create database tables following enhanced schema design
        Implements QuiltBlock and QuiltPanel tables with proper relationships
        """
        try:
            cursor = self.connection.cursor()
            
            # Create QuiltBlock table for block-level metadata
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quilt_blocks (
                    block_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    created_date TEXT,
                    total_panels INTEGER DEFAULT 0,
                    scraped_at TEXT,
                    updated_at TEXT,
                    metadata TEXT
                )
            """)
            
            # Create QuiltPanel table for individual panel data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quilt_panels (
                    panel_id TEXT PRIMARY KEY,
                    block_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    image_urls TEXT,
                    scraped_at TEXT,
                    updated_at TEXT,
                    metadata TEXT,
                    FOREIGN KEY (block_id) REFERENCES quilt_blocks (block_id)
                )
            """)
            
            # Create legacy table for backwards compatibility
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS collection_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    subjects TEXT,
                    names TEXT,
                    dates TEXT,
                    url TEXT NOT NULL,
                    image_url TEXT,
                    image_path TEXT,
                    content_hash TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT
                )
            """)
            
            # Create indexes for performance optimization
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_quilt_blocks_title ON quilt_blocks(title)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_quilt_blocks_scraped_at ON quilt_blocks(scraped_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_quilt_panels_block_id ON quilt_panels(block_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_quilt_panels_title ON quilt_panels(title)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_quilt_panels_scraped_at ON quilt_panels(scraped_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_collection_items_title ON collection_items(title)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_collection_items_created_at ON collection_items(created_at)")
            
            self.connection.commit()
            logger.info("AIDS Memorial Quilt Database: Tables created successfully")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Table creation failed: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    async def _determine_primary_data_source(self) -> str:
        """
        Determine and cache the primary data source for efficient record retrieval
        Implements performance optimization following project standards
        
        Returns:
            Primary data source table name
        """
        try:
            if not self.connection:
                self._primary_data_source = "none"
                return self._primary_data_source
            
            cursor = self.connection.cursor()
            
            # Check collection_items count
            cursor.execute("SELECT COUNT(*) FROM collection_items")
            collection_items_count = cursor.fetchone()[0] or 0
            
            # Check quilt_blocks count
            cursor.execute("SELECT COUNT(*) FROM quilt_blocks")
            quilt_blocks_count = cursor.fetchone()[0] or 0
            
            # Check quilt_panels count
            cursor.execute("SELECT COUNT(*) FROM quilt_panels")
            quilt_panels_count = cursor.fetchone()[0] or 0
            
            # Determine primary source based on data availability
            if collection_items_count > 0:
                self._primary_data_source = "collection_items"
            elif quilt_blocks_count > 0:
                self._primary_data_source = "quilt_blocks"
            elif quilt_panels_count > 0:
                self._primary_data_source = "quilt_panels"
            else:
                self._primary_data_source = "none"
            
            logger.info(f"AIDS Memorial Quilt Database: Primary data source determined as '{self._primary_data_source}' "
                       f"(collection_items: {collection_items_count}, quilt_blocks: {quilt_blocks_count}, "
                       f"quilt_panels: {quilt_panels_count})")
            
            return self._primary_data_source
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Error determining primary data source: {e}")
            self._primary_data_source = "none"
            return self._primary_data_source

    async def get_records(self, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get AIDS Memorial Quilt records with pagination (SYNC-COMPATIBLE VERSION)
        Implements error resilience and digital humanities research standards
        Uses synchronous database operations to avoid async/await issues
        
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
            
            # Determine which table to use for primary data source using sync operations
            table_counts = {}
            cursor = self.connection.cursor()
            
            for table_name in ["collection_items", "quilt_blocks", "quilt_panels"]:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    result = cursor.fetchone()
                    table_counts[table_name] = result[0] if result else 0
                except Exception:
                    table_counts[table_name] = 0
            
            # Choose primary table based on data availability
            if table_counts.get("collection_items", 0) > 0:
                primary_table = "collection_items"
            elif table_counts.get("quilt_blocks", 0) >= table_counts.get("quilt_panels", 0):
                primary_table = "quilt_blocks"
            else:
                primary_table = "quilt_panels"
            
            logger.info(f"AIDS Memorial Quilt Database: Attempting to fetch records from {primary_table} (limit: {limit}, offset: {offset})")
            
            # Log table counts for debugging
            for table_name, count in table_counts.items():
                logger.info(f"AIDS Memorial Quilt Database: Table {table_name} contains {count} rows")
            
            logger.info(f"AIDS Memorial Quilt Database: Fetching from {primary_table} table")
            
            # Use schema-compatible queries based on actual table structure (SYNC VERSION)
            if primary_table == "collection_items":
                cursor.execute("""
                    SELECT id, item_id, title, description, subjects, names, dates,
                           url, image_url, content_hash, created_at, updated_at
                    FROM collection_items
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset))
            elif primary_table == "quilt_blocks":
                # Check if metadata_json column exists, fall back to metadata if not
                cursor.execute("PRAGMA table_info(quilt_blocks)")
                columns_info = cursor.fetchall()
                available_columns = [col[1] for col in columns_info]
                
                if "metadata_json" in available_columns:
                    metadata_column = "metadata_json"
                else:
                    metadata_column = "metadata"
                
                cursor.execute(f"""
                    SELECT id, block_id as item_id, title, description, 
                           CASE 
                               WHEN {metadata_column} IS NOT NULL AND {metadata_column} != '{{}}' 
                               THEN json_extract({metadata_column}, '$.subjects')
                               ELSE NULL 
                           END as subjects,
                           CASE 
                               WHEN {metadata_column} IS NOT NULL AND {metadata_column} != '{{}}' 
                               THEN json_extract({metadata_column}, '$.names')
                               ELSE NULL 
                           END as names,
                           created_date as dates,
                           NULL as url, NULL as image_url, NULL as content_hash,
                           scraped_at as created_at, updated_at
                    FROM quilt_blocks
                    ORDER BY id DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset))
            else:  # quilt_panels
                # Check if metadata_json column exists, fall back to metadata if not
                cursor.execute("PRAGMA table_info(quilt_panels)")
                columns_info = cursor.fetchall()
                available_columns = [col[1] for col in columns_info]
                
                if "metadata_json" in available_columns:
                    metadata_column = "metadata_json"
                else:
                    metadata_column = "metadata"
                
                cursor.execute(f"""
                    SELECT id, panel_id as item_id, title, description,
                           CASE 
                               WHEN {metadata_column} IS NOT NULL AND {metadata_column} != '{{}}' 
                               THEN json_extract({metadata_column}, '$.subjects')
                               ELSE NULL 
                           END as subjects,
                           CASE 
                               WHEN {metadata_column} IS NOT NULL AND {metadata_column} != '{{}}' 
                               THEN json_extract({metadata_column}, '$.names')
                               ELSE NULL 
                           END as names,
                           scraped_at as dates,
                           image_urls as url, image_urls as image_url, NULL as content_hash,
                           scraped_at as created_at, updated_at
                    FROM quilt_panels
                    ORDER BY id DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset))
            
            rows = cursor.fetchall()
            
            # Convert to API-compatible format following digital humanities standards
            columns = ["id", "item_id", "title", "description", "subjects", 
                      "names", "dates", "url", "image_url", "content_hash", 
                      "created_at", "updated_at"]
            
            records = []
            for row in rows:
                record = dict(zip(columns, row))
                
                # Parse JSON fields safely for AIDS Memorial Quilt metadata preservation
                for json_field in ["subjects", "names"]:
                    field_value = record.get(json_field)
                    if field_value and field_value != "NULL" and field_value is not None:
                        try:
                            if isinstance(field_value, str):
                                if field_value.startswith('[') or field_value.startswith('{'):
                                    parsed_value = json.loads(field_value)
                                    record[json_field] = parsed_value if isinstance(parsed_value, list) else [str(parsed_value)]
                                else:
                                    record[json_field] = [field_value]
                            else:
                                record[json_field] = [str(field_value)] if field_value else []
                        except Exception as parse_error:
                            logger.warning(f"AIDS Memorial Quilt Database: Error parsing {json_field}: {parse_error}")
                            record[json_field] = []
                    else:
                        record[json_field] = []
                
                # Handle image URLs for Library of Congress attribution
                for url_field in ["url", "image_url"]:
                    url_value = record.get(url_field)
                    if url_value and url_value != "NULL" and url_value is not None:
                        try:
                            if isinstance(url_value, str) and url_value.startswith('['):
                                urls_list = json.loads(url_value)
                                record[url_field] = urls_list[0] if isinstance(urls_list, list) and urls_list else None
                        except Exception:
                            pass
                    else:
                        record[url_field] = None
                
                # Handle dates for Library of Congress attribution
                dates_value = record.get("dates")
                if dates_value and dates_value != "NULL":
                    try:
                        if isinstance(dates_value, str) and not dates_value.startswith('['):
                            record["dates"] = [dates_value]
                        elif isinstance(dates_value, str) and dates_value.startswith('['):
                            record["dates"] = json.loads(dates_value)
                        else:
                            record["dates"] = [str(dates_value)] if dates_value else []
                    except Exception:
                        record["dates"] = []
                else:
                    record["dates"] = []
                
                # Ensure string fields are properly handled
                for string_field in ["title", "description"]:
                    if record.get(string_field) is None:
                        record[string_field] = ""
                
                records.append(record)
            
            logger.info(f"AIDS Memorial Quilt Database: Retrieved {len(records)} records from {primary_table}")
            return records
            
        except (DatabaseConnectionError, DataValidationError):
            raise
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Critical error getting records: {e}")
            logger.error(f"AIDS Memorial Quilt Database: Full traceback: {traceback.format_exc()}")
            raise DatabaseConnectionError(f"Failed to get records from AIDS Memorial Quilt database: {e}")

    async def get_database_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive database statistics with enhanced image tracking
        Implements accurate image digitization progress calculation
        Following digital humanities research standards for archival data
        
        Returns:
            Dictionary containing database statistics with accurate image metrics
        """
        try:
            if not self.connection:
                logger.warning("AIDS Memorial Quilt Database: No connection available for stats")
                return self._get_empty_stats()
            
            cursor = self.connection.cursor()
            
            # Get total blocks count
            cursor.execute("SELECT COUNT(*) as total_blocks FROM quilt_blocks")
            result = cursor.fetchone()
            total_blocks = int(result[0]) if result and result[0] is not None else 0
            
            # Get total panels count
            cursor.execute("SELECT COUNT(*) as total_panels FROM quilt_panels")
            result = cursor.fetchone()
            total_panels = int(result[0]) if result and result[0] is not None else 0
            
            # Get legacy items count for backwards compatibility
            cursor.execute("SELECT COUNT(*) as total_items FROM collection_items")
            result = cursor.fetchone()
            total_items = int(result[0]) if result and result[0] is not None else 0
            
            # Use the higher count for total_blocks if legacy data exists
            if total_items > total_blocks:
                total_blocks = total_items
            
            # Calculate image digitization metrics more accurately
            blocks_with_images = 0
            total_potential_images = 0
            downloaded_images = 0
            
            try:
                # Count blocks that have image URLs available (not necessarily downloaded)
                cursor.execute("""
                    SELECT COUNT(DISTINCT block_id) 
                    FROM quilt_panels 
                    WHERE image_urls IS NOT NULL AND image_urls != '' AND image_urls != '[]'
                """)
                result = cursor.fetchone()
                blocks_with_image_urls = int(result[0]) if result and result[0] is not None else 0
                
                # Count blocks with actually downloaded images (have image_path)
                cursor.execute("""
                    SELECT COUNT(DISTINCT COALESCE(qb.block_id, ci.item_id))
                    FROM collection_items ci
                    LEFT JOIN quilt_blocks qb ON ci.item_id = qb.block_id
                    WHERE ci.image_path IS NOT NULL AND ci.image_path != ''
                """)
                result = cursor.fetchone()
                blocks_with_downloaded_images = int(result[0]) if result and result[0] is not None else 0
                
                # Count total potential images (panels with URLs)
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM quilt_panels 
                    WHERE image_urls IS NOT NULL AND image_urls != '' AND image_urls != '[]'
                """)
                result = cursor.fetchone()
                total_potential_images = int(result[0]) if result and result[0] is not None else 0
                
                # Count actually downloaded images
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM collection_items 
                    WHERE image_path IS NOT NULL AND image_path != ''
                """)
                result = cursor.fetchone()
                downloaded_images = int(result[0]) if result and result[0] is not None else 0
                
                # Use the more accurate downloaded images count
                blocks_with_images = blocks_with_downloaded_images
                
                logger.info(f"AIDS Memorial Quilt Database: Image metrics - blocks_with_urls: {blocks_with_image_urls}, "
                           f"blocks_with_downloads: {blocks_with_downloaded_images}, "
                           f"potential_images: {total_potential_images}, downloaded: {downloaded_images}")
                
            except Exception as e:
                logger.warning(f"AIDS Memorial Quilt Database: Error calculating image metrics: {e}")
                blocks_with_images = 0
                total_potential_images = 0
                downloaded_images = 0
            
            # Calculate recent blocks (last 24 hours) - check both tables
            recent_blocks = 0
            try:
                # Calculate 24 hours ago timestamp
                twenty_four_hours_ago = (datetime.now() - timedelta(hours=24)).isoformat()
                
                # Count recent quilt_blocks
                cursor.execute("""
                    SELECT COUNT(*) FROM quilt_blocks 
                    WHERE scraped_at > ? OR updated_at > ?
                """, (twenty_four_hours_ago, twenty_four_hours_ago))
                recent_quilt_blocks = cursor.fetchone()[0] or 0
                
                # Count recent collection_items  
                cursor.execute("""
                    SELECT COUNT(*) FROM collection_items 
                    WHERE created_at > ? OR updated_at > ?
                """, (twenty_four_hours_ago, twenty_four_hours_ago))
                recent_collection_items = cursor.fetchone()[0] or 0
                
                # Use the higher count
                recent_blocks = max(recent_quilt_blocks, recent_collection_items)
                
                logger.info(f"AIDS Memorial Quilt Database: Recent blocks calculation - quilt_blocks: {recent_quilt_blocks}, collection_items: {recent_collection_items}")
                
            except Exception as e:
                logger.warning(f"AIDS Memorial Quilt Database: Error calculating recent blocks: {e}")
            
            # Get database file size
            database_size_bytes = 0
            try:
                if self.db_path.exists():
                    database_size_bytes = int(self.db_path.stat().st_size)
            except Exception as e:
                logger.warning(f"AIDS Memorial Quilt Database: Failed to get file size: {e}")
            
            # Determine database health following project standards
            if total_blocks == 0 and total_items == 0:
                health = "empty"
            elif total_panels == 0 and total_items == 0:
                health = "no_panels"
            elif total_blocks > 100 or total_items > 100:
                health = "healthy"
            elif total_blocks > 10 or total_items > 10:
                health = "limited"
            else:
                health = "minimal"
            
            # Get last updated timestamp
            last_updated = None
            try:
                # Check both tables for most recent update
                cursor.execute("""
                    SELECT MAX(scraped_at) as max_scraped FROM quilt_blocks
                    UNION ALL
                    SELECT MAX(updated_at) as max_updated FROM quilt_blocks
                    UNION ALL  
                    SELECT MAX(created_at) as max_created FROM collection_items
                    UNION ALL
                    SELECT MAX(updated_at) as max_updated FROM collection_items
                    ORDER BY max_scraped DESC LIMIT 1
                """)
                result = cursor.fetchone()
                if result and result[0]:
                    last_updated = result[0]
            except Exception as e:
                logger.warning(f"AIDS Memorial Quilt Database: Error getting last updated: {e}")
            
            # Calculate image digitization progress percentage
            image_digitization_progress = 0
            if total_potential_images > 0:
                image_digitization_progress = round((downloaded_images / total_potential_images) * 100, 1)
            
            stats = {
                'total_blocks': total_blocks,
                'total_panels': total_panels,
                'blocks_with_images': blocks_with_images,  # Now represents actually downloaded images
                'recent_blocks': recent_blocks,
                'database_size_bytes': database_size_bytes,
                'database_health': health,
                'last_updated': last_updated,
                # Enhanced image tracking metrics
                'total_potential_images': total_potential_images,
                'downloaded_images': downloaded_images,
                'image_digitization_progress': image_digitization_progress,
                'blocks_with_image_urls': blocks_with_image_urls if 'blocks_with_image_urls' in locals() else 0
            }
            
            logger.info(f"AIDS Memorial Quilt Database: Enhanced stats calculated: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Error getting statistics: {e}")
            return self._get_empty_stats()
    
    def _get_empty_stats(self) -> Dict[str, Any]:
        """Return safe default statistics following error resilience guidelines"""
        return {
            'total_blocks': 0,
            'total_panels': 0,
            'blocks_with_images': 0,
            'recent_blocks': 0,
            'database_size_bytes': 0,
            'database_health': 'error',
            'last_updated': None,
            'total_potential_images': 0,
            'downloaded_images': 0,
            'image_digitization_progress': 0,
            'blocks_with_image_urls': 0
        }
    
    def _extract_first_image_url(self, image_urls_json: Any) -> Optional[str]:
        """
        Extract the first image URL from JSON array for display purposes
        Implements safe JSON parsing following project error resilience guidelines
        """
        try:
            urls = self._safe_json_parse(image_urls_json)
            if urls and isinstance(urls, list) and len(urls) > 0:
                return urls[0]
            return None
        except Exception:
            return None
    
    async def get_total_records(self) -> int:
        """
        Get total count of records from the primary data source
        Uses cached primary source for efficient counting
        Following project error resilience patterns
        
        Returns:
            Total number of records available
        """
        try:
            if not self.connection:
                logger.warning("AIDS Memorial Quilt Database: No connection available for count")
                return 0
            
            # Use cached primary data source if available
            if not self._primary_data_source:
                await self._determine_primary_data_source()
            
            cursor = self.connection.cursor()
            primary_source = self._primary_data_source
            
            if primary_source == "collection_items":
                cursor.execute("SELECT COUNT(*) FROM collection_items")
                count = cursor.fetchone()[0] or 0
            elif primary_source == "quilt_blocks":
                cursor.execute("SELECT COUNT(*) FROM quilt_blocks")
                count = cursor.fetchone()[0] or 0
            elif primary_source == "quilt_panels":
                # Count distinct blocks when using panels as source
                cursor.execute("SELECT COUNT(DISTINCT block_id) FROM quilt_panels")
                count = cursor.fetchone()[0] or 0
            else:
                count = 0
            
            logger.info(f"AIDS Memorial Quilt Database: Total records count: {count} from {primary_source}")
            return count
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Error counting records: {e}")
            return 0

    async def diagnose_data_availability(self) -> Dict[str, Any]:
        """
        Comprehensive diagnostic method to identify data availability issues
        Implements detailed analysis following digital humanities research standards
        
        Returns:
            Dictionary containing detailed diagnostic information about data tables
        """
        try:
            if not self.connection:
                return {"error": "No database connection available"}
            
            cursor = self.connection.cursor()
            diagnosis = {}
            
            # Check all tables and their data
            tables_to_check = ['quilt_blocks', 'quilt_panels', 'collection_items']
            
            for table in tables_to_check:
                try:
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0] or 0
                    
                    # Get sample data
                    cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                    sample_rows = cursor.fetchall()
                    
                    # Get column info
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]
                    
                    diagnosis[table] = {
                        "row_count": count,
                        "columns": columns,
                        "sample_data": [dict(row) for row in sample_rows] if sample_rows else [],
                        "has_data": count > 0
                    }
                    
                except Exception as e:
                    diagnosis[table] = {"error": str(e)}
            
            # Check for any data at all
            total_data_rows = sum(
                table_info.get("row_count", 0) 
                for table_info in diagnosis.values() 
                if isinstance(table_info, dict) and "row_count" in table_info
            )
            
            diagnosis["summary"] = {
                "total_rows_across_all_tables": total_data_rows,
                "cached_primary_data_source": self._primary_data_source,
                "recommendations": self._generate_data_recommendations(diagnosis)
            }
            
            logger.info(f"AIDS Memorial Quilt Database: Diagnostic completed - {total_data_rows} total rows found")
            return diagnosis
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Error during diagnosis: {e}")
            return {"error": str(e)}
    
    def _generate_data_recommendations(self, diagnosis: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on data availability analysis"""
        recommendations = []
        
        collection_items_count = diagnosis.get('collection_items', {}).get('row_count', 0)
        quilt_blocks_count = diagnosis.get('quilt_blocks', {}).get('row_count', 0)
        quilt_panels_count = diagnosis.get('quilt_panels', {}).get('row_count', 0)
        
        if collection_items_count == 0 and quilt_blocks_count == 0 and quilt_panels_count == 0:
            recommendations.append("No data found in any table - run data scraping to populate database")
        elif collection_items_count == 0 and quilt_blocks_count > 0:
            recommendations.append("Use quilt_blocks as primary data source for API responses")
        elif collection_items_count == 0 and quilt_panels_count > 0:
            recommendations.append("Consider aggregating quilt_panels data for API responses")
        elif collection_items_count > 0:
            recommendations.append("collection_items table has data and should work for API responses")
        
        return recommendations

    async def search_records(self, query: str, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search records with optimized data source selection
        Uses cached primary source for efficient searching
        Implements comprehensive search across multiple fields per project standards
        
        Args:
            query: Search query string for AIDS Memorial Quilt records
            limit: Maximum number of results to return
            offset: Number of results to skip for pagination
            
        Returns:
            List of matching record dictionaries
        """
        try:
            if not self.connection:
                logger.warning("AIDS Memorial Quilt Database: No connection available for search")
                return []
            
            # Use cached primary data source
            if not self._primary_data_source:
                await self._determine_primary_data_source()
            
            primary_source = self._primary_data_source
            logger.info(f"AIDS Memorial Quilt Database: Searching {primary_source} for '{query}'")
            
            search_term = f"%{query}%"
            records = []
            cursor = self.connection.cursor()
            
            if primary_source == "collection_items":
                cursor.execute("""
                    SELECT id, item_id, title, description, subjects, names, dates,
                           url, image_url, content_hash, created_at, updated_at
                    FROM collection_items 
                    WHERE title LIKE ? OR description LIKE ? OR subjects LIKE ? OR names LIKE ?
                    ORDER BY title
                    LIMIT ? OFFSET ?
                """, (search_term, search_term, search_term, search_term, limit, offset))
                
                for row in cursor.fetchall():
                    record = {
                        'id': str(row[0]),
                        'item_id': row[1],
                        'title': row[2] or 'Untitled AIDS Memorial Quilt Item',
                        'description': row[3],
                        'subjects': self._safe_json_parse(row[4]),
                        'names': self._safe_json_parse(row[5]),
                        'dates': self._safe_json_parse(row[6]),
                        'url': row[7],
                        'image_url': row[8],
                        'image_path': row[9],
                        'content_hash': row[10],
                        'created_at': row[11],
                        'updated_at': row[12]
                    }
                    records.append(record)
                    
            elif primary_source == "quilt_blocks":
                cursor.execute("""
                    SELECT block_id, title, description, created_date, total_panels,
                           scraped_at, updated_at, metadata
                    FROM quilt_blocks 
                    WHERE title LIKE ? OR description LIKE ?
                    ORDER BY title
                    LIMIT ? OFFSET ?
                """, (search_term, search_term, limit, offset))
                
                for row in cursor.fetchall():
                    record = {
                        'id': row[0],
                        'item_id': row[0],
                        'title': row[1] or 'Untitled AIDS Memorial Quilt Block',
                        'description': row[2],
                        'subjects': [],
                        'names': [],
                        'dates': [row[3]] if row[3] else [],
                        'url': f"https://www.loc.gov/collections/aids-memorial-quilt-records/{row[0]}/",
                        'image_url': None,
                        'image_path': None,
                        'content_hash': None,
                        'created_at': row[5],
                        'updated_at': row[6],
                        'total_panels': row[4] or 0,
                        'metadata': self._safe_json_parse(row[7])
                    }
                    records.append(record)
            
            logger.info(f"AIDS Memorial Quilt Database: Search '{query}' found {len(records)} records")
            return records
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Search error: {e}")
            return []
    
    async def get_search_count(self, query: str) -> int:
        """
        Get total count of search results from primary data source
        Uses cached primary source for efficient counting
        Implements efficient counting for pagination support
        
        Args:
            query: Search query string
            
        Returns:
            Total number of matching records
        """
        try:
            if not self.connection:
                logger.warning("AIDS Memorial Quilt Database: No connection available for search count")
                return 0
            
            # Use cached primary data source
            if not self._primary_data_source:
                await self._determine_primary_data_source()
            
            cursor = self.connection.cursor()
            search_term = f"%{query}%"
            primary_source = self._primary_data_source
            
            if primary_source == "collection_items":
                cursor.execute("""
                    SELECT COUNT(*) FROM collection_items 
                    WHERE title LIKE ? OR description LIKE ? OR subjects LIKE ? OR names LIKE ?
                """, (search_term, search_term, search_term, search_term))
                count = cursor.fetchone()[0] or 0
            elif primary_source == "quilt_blocks":
                cursor.execute("""
                    SELECT COUNT(*) FROM quilt_blocks 
                    WHERE title LIKE ? OR description LIKE ?
                """, (search_term, search_term))
                count = cursor.fetchone()[0] or 0
            else:
                count = 0
            
            logger.info(f"AIDS Memorial Quilt Database: Search count for '{query}': {count}")
            return count
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Search count error: {e}")
            return 0
    
    def _safe_json_parse(self, value: Any) -> Any:
        """
        Safely parse JSON values with comprehensive error handling
        Following project error resilience guidelines for digital humanities data
        
        Args:
            value: Value to parse (may be JSON string, list, or other)
            
        Returns:
            Parsed value or safe default
        """
        if value is None:
            return []
        
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return [value] if value else []
        elif isinstance(value, list):
            return value
        elif value is not None:
            return [str(value)]
        
        return []
    
    async def close(self) -> None:
        """
        Close database connection safely
        Implements proper resource cleanup per project standards
        """
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                logger.info("AIDS Memorial Quilt Database: Connection closed successfully")
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Error closing connection: {e}")

    async def get_record_by_id(self, record_id: str) -> Optional[Dict[str, Any]]:
        """
        Get specific AIDS Memorial Quilt record by ID from any table
        Implements comprehensive record lookup with error handling
        Following Library of Congress data standards
        
        Args:
            record_id: The ID to search for in AIDS Memorial Quilt collection
            
        Returns:
            Record dictionary if found, None otherwise
        """
        try:
            if not self.connection:
                return None
                
            cursor = self.connection.cursor()
            
            # Try collection_items table first
            cursor.execute("""
                SELECT id, item_id, title, description, subjects, names, dates,
                       url, image_url, content_hash, created_at, updated_at
                FROM collection_items WHERE item_id = ? OR id = ?
            """, (record_id, record_id))
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': str(row[0]),
                    'item_id': row[1],
                    'title': row[2] or 'Untitled AIDS Memorial Quilt Item',
                    'description': row[3],
                    'subjects': self._safe_json_parse(row[4]),
                    'names': self._safe_json_parse(row[5]),
                    'dates': self._safe_json_parse(row[6]),
                    'url': row[7],
                    'image_url': row[8],
                    'image_path': row[9],
                    'content_hash': row[10],
                    'created_at': row[11],
                    'updated_at': row[12]
                }
            
            # Try QuiltBlock table
            cursor.execute("""
                SELECT block_id, title, description, created_date, total_panels,
                       scraped_at, updated_at, metadata
                FROM quilt_blocks WHERE block_id = ?
            """, (record_id,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'item_id': row[0],
                    'title': row[1] or 'Untitled AIDS Memorial Quilt Block',
                    'description': row[2],
                    'subjects': [],
                    'names': [],
                    'dates': [row[3]] if row[3] else [],
                    'url': f"https://www.loc.gov/collections/aids-memorial-quilt-records/{row[0]}/",
                    'image_url': None,
                    'image_path': None,
                    'content_hash': None,
                    'created_at': row[5],
                    'updated_at': row[6],
                    'total_panels': row[4] or 0,
                    'metadata': self._safe_json_parse(row[7])
                }
            
            return None
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Database: Error getting record {record_id}: {e}")
            return None
