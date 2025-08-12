#!/usr/bin/env python3
"""
FastAPI server to serve AIDS Memorial Quilt data to the React dashboard
Following project coding standards with comprehensive error handling and type safety
Implements async/await patterns for non-blocking database operations
Supports digital humanities research standards with proper Library of Congress attribution
"""

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import sqlite3
import os

# Import only existing components following separation of concerns
try:
    from src.database import DatabaseManager
    from config.settings import ScraperConfig
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running from the project root directory")
    print("Required: src.database.DatabaseManager, config.settings.ScraperConfig")
    sys.exit(1)

# Configure structured logging per project guidelines
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - AIDS Memorial Quilt API - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="AIDS Memorial Quilt Records API",
    description="Enhanced API for accessing Library of Congress AIDS Memorial Quilt Records collection",
    version="2.0.0"
)

# CORS configuration for dashboard integration following project standards
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # React development server
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://localhost:3002", 
        "http://127.0.0.1:3002"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Initialize configuration following centralized settings management
config = ScraperConfig()
db_manager = None

# Pydantic models for API responses following project type safety standards
class QuiltRecordResponse(BaseModel):
    """
    Response model for AIDS Memorial Quilt records with comprehensive field support
    Implements type safety and validation for digital humanities data
    """
    id: str
    item_id: str
    title: str
    description: Optional[str] = None
    subjects: List[str] = []
    names: List[str] = []
    dates: List[str] = []
    url: Optional[str] = None
    image_url: Optional[str] = None
    image_path: Optional[str] = None
    content_hash: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class StatsResponse(BaseModel):
    """Enhanced statistics response with comprehensive AIDS Memorial Quilt metrics"""
    total_blocks: int
    total_panels: int
    blocks_with_images: int
    recent_blocks: int
    database_size_bytes: int
    database_health: str
    last_updated: Optional[str] = None

class SearchResponse(BaseModel):
    """Paginated search response for AIDS Memorial Quilt records"""
    records: List[QuiltRecordResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_previous: bool

# Helper functions following project coding standards
def safe_json_parse(field_value: Any, default: Any = None) -> Any:
    """
    Safely parse JSON field values with comprehensive error handling
    Implements error resilience for digital humanities data processing
    
    Args:
        field_value: The value to parse (may be JSON string, list, or other)
        default: Default value if parsing fails
        
    Returns:
        Parsed value or safe default
    """
    if field_value is None:
        return default or []
        
    if isinstance(field_value, str):
        try:
            return json.loads(field_value)
        except json.JSONDecodeError:
            return [str(field_value)] if field_value else (default or [])
    elif isinstance(field_value, list):
        return field_value
    elif field_value is not None:
        return [str(field_value)]
        
    return default or []

def convert_db_record_to_response(record: Dict[str, Any]) -> QuiltRecordResponse:
    """
    Convert database record to API response format
    Implements comprehensive field mapping with error handling
    Following Library of Congress data standards
    
    Args:
        record: Database record dictionary
        
    Returns:
        QuiltRecordResponse for API consumption
    """
    try:
        return QuiltRecordResponse(
            id=str(record.get('id', 'unknown')),
            item_id=str(record.get('item_id', record.get('id', 'unknown'))),
            title=record.get('title', 'AIDS Memorial Quilt Record'),
            description=record.get('description'),
            subjects=safe_json_parse(record.get('subjects'), []),
            names=safe_json_parse(record.get('names'), []),
            dates=safe_json_parse(record.get('dates'), []),
            url=record.get('url', "https://www.loc.gov/collections/aids-memorial-quilt-records/"),
            image_url=record.get('image_url'),
            image_path=record.get('image_path'),
            content_hash=record.get('content_hash', ''),
            created_at=record.get('created_at'),
            updated_at=record.get('updated_at')
        )
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt API: Error converting record to response: {e}")
        # Return minimal safe response per error resilience guidelines
        return QuiltRecordResponse(
            id=str(record.get('id', 'unknown')),
            item_id=str(record.get('item_id', 'unknown')),
            title=record.get('title', 'AIDS Memorial Quilt Record'),
            description=record.get('description')
        )

# Global database manager with lazy initialization
async def get_db_manager() -> Optional[DatabaseManager]:
    """
    Get database manager instance with proper initialization
    Implements lazy loading and error handling per project standards
    
    Returns:
        DatabaseManager instance or None if initialization fails
    """
    global db_manager
    
    if db_manager is None:
        try:
            logger.info("AIDS Memorial Quilt API: Initializing database manager")
            db_manager = DatabaseManager(config.database_path)
            await db_manager.initialize()
            
            # Test basic connectivity with detailed logging per project guidelines
            stats = await db_manager.get_database_stats()
            total_records = await db_manager.get_total_records()
            
            logger.info(f"AIDS Memorial Quilt API: Database initialized successfully")
            logger.info(f"   Total records: {total_records:,}")
            logger.info(f"   Database health: {stats.get('database_health', 'unknown')}")
            logger.info(f"   Total blocks: {stats.get('total_blocks', 0):,}")
            logger.info(f"   Total panels: {stats.get('total_panels', 0):,}")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt API: Database initialization failed: {e}")
            import traceback
            logger.error(f"AIDS Memorial Quilt API: Full traceback: {traceback.format_exc()}")
            db_manager = None
    
    return db_manager

# Event handlers following project patterns
@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup with comprehensive error handling"""
    try:
        logger.info("AIDS Memorial Quilt API: Starting up server")
        db = await get_db_manager()
        
        if db:
            # Test database functionality with detailed reporting
            total_records = await db.get_total_records()
            stats = await db.get_database_stats()
            
            logger.info(f"AIDS Memorial Quilt API: Startup complete")
            logger.info(f"   Database ready with {total_records:,} records")
            logger.info(f"   Database health: {stats.get('database_health', 'unknown')}")
            
            if total_records == 0:
                logger.warning("AIDS Memorial Quilt API: Database is empty - records endpoint will return no data")
                logger.warning("   Run 'python -m src.main' to populate the database")
        else:
            logger.warning("AIDS Memorial Quilt API: Starting without database connection")
        
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt API: Startup error: {e}")
        # Continue startup even if database fails per error resilience guidelines

@app.on_event("shutdown") 
async def shutdown_event():
    """Clean up database connection on shutdown"""
    try:
        global db_manager
        if db_manager:
            await db_manager.close()
            db_manager = None
            logger.info("AIDS Memorial Quilt API: Database connections closed successfully")
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt API: Error during shutdown: {e}")

# API endpoints following project standards
@app.get("/", response_model=dict)
async def root():
    """Root endpoint with comprehensive API information"""
    try:
        db = await get_db_manager()
        
        if db:
            stats = await db.get_database_stats()
            total_records = await db.get_total_records()
            
            database_info = {
                "total_blocks": stats.get('total_blocks', 0),
                "total_panels": stats.get('total_panels', 0),
                "total_records": total_records,
                "database_size_mb": round(stats.get('database_size_bytes', 0) / 1024 / 1024, 2),
                "database_health": stats.get('database_health', 'unknown')
            }
        else:
            database_info = {
                "total_blocks": 0,
                "total_panels": 0,
                "total_records": 0,
                "database_size_mb": 0,
                "database_health": "unavailable"
            }
        
        return {
            "message": "AIDS Memorial Quilt Records API",
            "version": "2.0.0",
            "description": "Enhanced API for accessing AIDS Memorial Quilt Records from Library of Congress",
            "database_info": database_info,
            "endpoints": {
                "/stats": "Get comprehensive database statistics",
                "/records": "Get paginated list of quilt records",
                "/records/{record_id}": "Get specific record by ID",
                "/search": "Search records by query",
                "/health": "Health check endpoint",
                "/debug": "Database diagnostic information"
            },
            "documentation": "/docs"
        }
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt API: Error in root endpoint: {e}")
        return {
            "message": "AIDS Memorial Quilt Records API",
            "version": "2.0.0",
            "status": "error",
            "error": str(e)
        }

@app.get("/health")
async def health_check():
    """Health check endpoint for API connectivity testing with verbose logging"""
    db_path = "output/quilt_data.db"
    health_obj = {
        "status": "unhealthy",
        "service": "AIDS Memorial Quilt Records API",
        "database": {
            "connected": False,
            "blocks": None,
            "panels": None,
            "records": None,
            "health": "error",
            "error": None
        }
    }
    try:
        if not os.path.exists(db_path):
            health_obj["database"]["error"] = "Database file not found"
            health_obj["database"]["health"] = "empty"
            health_obj["status"] = "empty_database"
        else:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM quilt_blocks")
            blocks = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM quilt_panels")
            panels = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM quilt_blocks WHERE image_path IS NOT NULL AND image_path != ''")
            blocks_with_images = cur.fetchone()[0]
            health_obj["database"].update({
                "connected": True,
                "blocks": blocks,
                "panels": panels,
                "blocks_with_images": blocks_with_images,
                "health": "healthy" if blocks > 0 and panels > 0 else "limited",
                "error": None
            })
            health_obj["status"] = "healthy" if blocks > 0 and panels > 0 else "limited"
            conn.close()
    except Exception as e:
        health_obj["database"]["error"] = str(e)
        health_obj["database"]["health"] = "error"
        health_obj["status"] = "error"
    return JSONResponse(health_obj)

@app.get("/records", response_model=SearchResponse)
async def get_records(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of records per page")
):
    """
    Get AIDS Memorial Quilt records with pagination and enhanced debugging
    Implements graceful error handling for database connectivity issues
    Following digital humanities research standards
    """
    try:
        logger.info(f"AIDS Memorial Quilt API: Records endpoint called - page {page}, size {page_size}")
        db = await get_db_manager()
        
        if not db:
            logger.warning("AIDS Memorial Quilt API: Database not available for records")
            return SearchResponse(
                records=[],
                total=0,
                page=page,
                page_size=page_size,
                total_pages=0,
                has_next=False,
                has_previous=False
            )
        
        # Get records and total count using existing database methods
        logger.info("AIDS Memorial Quilt API: Calling db.get_records()...")
        records = await db.get_records(limit=page_size, offset=(page - 1) * page_size)
        logger.info(f"AIDS Memorial Quilt API: get_records returned {len(records)} records")
        
        logger.info("AIDS Memorial Quilt API: Calling db.get_total_records()...")
        total = await db.get_total_records()
        logger.info(f"AIDS Memorial Quilt API: get_total_records returned {total}")
        
        # Log first record for debugging if available
        if records:
            logger.info(f"AIDS Memorial Quilt API: Sample record keys: {list(records[0].keys())}")
            logger.info(f"AIDS Memorial Quilt API: Sample record title: {records[0].get('title', 'No title')}")
        else:
            logger.warning("AIDS Memorial Quilt API: No records returned from database!")
        
        # Convert database records to API response format
        record_responses = []
        for i, record in enumerate(records):
            try:
                converted = convert_db_record_to_response(record)
                record_responses.append(converted)
            except Exception as convert_error:
                logger.error(f"AIDS Memorial Quilt API: Error converting record {i}: {convert_error}")
                logger.error(f"AIDS Memorial Quilt API: Problematic record: {record}")
        
        logger.info(f"AIDS Memorial Quilt API: Successfully converted {len(record_responses)} records")
        
        # Calculate pagination metadata
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0
        has_next = page < total_pages
        has_previous = page > 1
        
        return SearchResponse(
            records=record_responses,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            has_next=has_next,
            has_previous=has_previous
        )
        
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt API: Critical error getting records: {e}")
        import traceback
        logger.error(f"AIDS Memorial Quilt API: Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve AIDS Memorial Quilt records: {str(e)}")

@app.get("/records/{record_id}", response_model=QuiltRecordResponse)
async def get_record(record_id: str):
    """
    Get specific AIDS Memorial Quilt record by ID with enhanced error handling
    Implements graceful degradation and detailed logging
    """
    try:
        logger.info(f"AIDS Memorial Quilt API: Record detail endpoint called for ID {record_id}")
        db = await get_db_manager()
        
        if not db:
            logger.warning("AIDS Memorial Quilt API: Database not available for record retrieval")
            raise HTTPException(status_code=503, detail="Database service unavailable")
        
        # Retrieve record by ID
        record = await db.get_record_by_id(record_id)
        
        if not record:
            logger.warning(f"AIDS Memorial Quilt API: Record not found for ID {record_id}")
            raise HTTPException(status_code=404, detail=f"Record not found: {record_id}")
        
        logger.info(f"AIDS Memorial Quilt API: Record found: {record}")
        
        # Convert to response model
        response = convert_db_record_to_response(record)
        logger.info(f"AIDS Memorial Quilt API: Record response: {response}")
        
        return response
    except HTTPException as http_ex:
        logger.warning(f"AIDS Memorial Quilt API: HTTP exception: {http_ex.detail}")
        raise
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt API: Error retrieving record {record_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving record: {str(e)}")

@app.get("/search", response_model=SearchResponse)
async def search_records(
    query: str = Query(..., description="Search query string"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of records per page")
):
    """
    Search AIDS Memorial Quilt records by query string with pagination
    Implements comprehensive error handling and logging
    """
    try:
        logger.info(f"AIDS Memorial Quilt API: Search endpoint called - query: '{query}', page {page}, size {page_size}")
        db = await get_db_manager()
        
        if not db:
            logger.warning("AIDS Memorial Quilt API: Database not available for search")
            return SearchResponse(
                records=[],
                total=0,
                page=page,
                page_size=page_size,
                total_pages=0,
                has_next=False,
                has_previous=False
            )
        
        # Perform search using database manager
        logger.info("AIDS Memorial Quilt API: Calling db.search_records()...")
        records = await db.search_records(query, limit=page_size, offset=(page - 1) * page_size)
        logger.info(f"AIDS Memorial Quilt API: search_records returned {len(records)} records")
        
        logger.info("AIDS Memorial Quilt API: Calling db.get_total_records() for total count...")
        total = await db.get_total_records()
        logger.info(f"AIDS Memorial Quilt API: Total records found: {total}")
        
        # Convert database records to API response format
        record_responses = [convert_db_record_to_response(record) for record in records]
        
        # Calculate pagination metadata
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0
        has_next = page < total_pages
        has_previous = page > 1
        
        return SearchResponse(
            records=record_responses,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            has_next=has_next,
            has_previous=has_previous
        )
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt API: Error in search endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Error searching records: {str(e)}")

@app.get("/stats")
async def get_stats():
    """
    Returns summary statistics for the AIDS Memorial Quilt database.
    """
    db_path = "output/quilt_data.db"
    try:
        if not os.path.exists(db_path):
            health = "empty"
            total_blocks = total_panels = panels_with_images = recent_blocks = database_size_bytes = 0
        else:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            try:
                cur.execute("SELECT COUNT(*) FROM quilt_blocks")
                total_blocks = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM quilt_panels")
                total_panels = cur.fetchone()[0]
                # Count panels where image_urls is a non-empty JSON array
                cur.execute("""
                    SELECT COUNT(*) FROM quilt_panels
                    WHERE image_urls IS NOT NULL
                      AND json_array_length(image_urls) > 0
                """)
                panels_with_images = cur.fetchone()[0]
                recent_blocks = total_blocks  # Or implement date logic if you have a date field
                database_size_bytes = os.path.getsize(db_path)
                if total_blocks == 0:
                    health = "empty"
                elif total_panels == 0:
                    health = "no_panels"
                else:
                    health = "healthy"
            except Exception as e:
                logger.error(f"AIDS Memorial Quilt API: Error in /stats SQL: {e}")
                health = "limited"
                total_blocks = total_panels = panels_with_images = recent_blocks = database_size_bytes = 0
            finally:
                conn.close()
        return {
            "total_blocks": total_blocks,
            "total_panels": total_panels,
            "panels_with_images": panels_with_images,
            "recent_blocks": recent_blocks,
            "database_size_bytes": database_size_bytes,
            "database_health": health,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt API: Error in /stats: {e}")
        return {
            "total_blocks": 0,
            "total_panels": 0,
            "panels_with_images": 0,
            "recent_blocks": 0,
            "database_size_bytes": 0,
            "database_health": "error",
            "last_updated": datetime.now(timezone.utc).isoformat()
        }

if __name__ == "__main__":
    # Set up comprehensive logging following project guidelines
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('api_server.log'),
            logging.StreamHandler()
        ]
    )
    
    # Run the server with proper configuration
    uvicorn.run(
        "api_server:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        log_level="info"
    )
