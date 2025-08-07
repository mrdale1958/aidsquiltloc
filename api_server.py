#!/usr/bin/env python3
"""
FastAPI server to serve AIDS Memorial Quilt data to the React dashboard
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from src.database import DatabaseManager, QuiltRecord
from config.settings import Settings


app = FastAPI(
    title="AIDS Memorial Quilt API",
    description="API for accessing AIDS Memorial Quilt Records from Library of Congress",
    version="1.0.0"
)

# Enable CORS for React development server
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://127.0.0.1:3002"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database
settings = Settings()
db_manager = DatabaseManager(settings.base_dir / "quilt_records.db")

# Pydantic models for API responses
class QuiltRecordResponse(BaseModel):
    id: int  # We'll use row ID or generate one
    item_id: str
    title: str
    description: Optional[str]
    subjects: Optional[List[str]]
    names: Optional[List[str]]
    dates: Optional[List[str]]
    url: str
    image_url: Optional[str]
    image_path: Optional[str]
    content_hash: str
    created_at: str
    updated_at: Optional[str]

class StatsResponse(BaseModel):
    total_records: int
    records_with_images: int
    recent_records: int
    last_updated: Optional[str]

# Helper function to convert database record to frontend format
def db_record_to_response(record, row_id: int) -> QuiltRecordResponse:
    """Convert a database record to the format expected by the frontend."""
    import json
    
    # Parse JSON fields safely
    def safe_json_parse(field_value, default=None):
        if field_value is None:
            return default or []
        if isinstance(field_value, str):
            try:
                parsed = json.loads(field_value)
                return parsed if isinstance(parsed, list) else [parsed] if parsed else []
            except json.JSONDecodeError:
                return [field_value] if field_value else []
        return field_value if isinstance(field_value, list) else []
    
    # Extract subjects
    subjects = safe_json_parse(record.subject, [])
    
    # Extract names (combine memorial_names and contributors)
    names = []
    if record.memorial_names:
        memorial_names = safe_json_parse(record.memorial_names, [])
        names.extend(memorial_names)
    if record.contributor:
        contributors = safe_json_parse(record.contributor, [])
        names.extend(contributors)
    if record.panel_maker:
        names.append(record.panel_maker)
    
    # Extract dates
    dates = []
    if record.date_created:
        dates.append(str(record.date_created))
    
    # Get first image URL
    image_urls = safe_json_parse(record.image_urls, [])
    first_image_url = image_urls[0] if image_urls else None
    
    # Parse description
    description = ""
    if record.description:
        desc_list = safe_json_parse(record.description, [])
        description = desc_list[0] if desc_list and desc_list[0] else ""
    
    return QuiltRecordResponse(
        id=row_id,
        item_id=record.item_id,
        title=record.title or "",
        description=description,
        subjects=subjects,
        names=list(set(names)) if names else [],  # Remove duplicates
        dates=dates,
        url=record.loc_url,
        image_url=first_image_url,
        image_path=None,  # We don't have local paths yet
        content_hash=record.content_hash,
        created_at=record.first_seen.isoformat() if record.first_seen else "",
        updated_at=record.last_updated.isoformat() if record.last_updated else None
    )

class RecordsResponse(BaseModel):
    recent_updates: int

class SearchResponse(BaseModel):
    records: List[QuiltRecordResponse]
    total: int
    page: int
    page_size: int

@app.on_event("startup")
async def startup_event():
    """Initialize database connection on startup"""
    try:
        db_manager.initialize_database()
        logging.info("Database initialized for API server")
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up database connection on shutdown"""
    db_manager.close()

@app.get("/", response_model=dict)
async def root():
    """Root endpoint with API information"""
    return {
        "message": "AIDS Memorial Quilt Records API",
        "version": "1.0.0",
        "description": "API for accessing AIDS Memorial Quilt Records from Library of Congress",
        "endpoints": {
            "/stats": "Get database statistics",
            "/records": "Get paginated list of records",
            "/records/{item_id}": "Get specific record by ID",
            "/search": "Search records by query"
        }
    }

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """Get database statistics"""
    try:
        if not db_manager.Session:
            raise HTTPException(status_code=500, detail="Database not initialized")
        
        with db_manager.Session() as session:
            total_records = session.query(QuiltRecord).count()
            records_with_images = session.query(QuiltRecord).filter(
                QuiltRecord.images_downloaded == True
            ).count()
            
            # Get recent records (last 30 days)
            from datetime import datetime, timedelta
            thirty_days_ago = datetime.utcnow() - timedelta(days=30)
            recent_records = session.query(QuiltRecord).filter(
                QuiltRecord.first_seen > thirty_days_ago
            ).count()
            
            # Get last updated timestamp
            latest_record = session.query(QuiltRecord).order_by(
                QuiltRecord.last_updated.desc()
            ).first()
            last_updated = latest_record.last_updated.isoformat() if latest_record and latest_record.last_updated else None
            
            return StatsResponse(
                total_records=total_records,
                records_with_images=records_with_images,
                recent_records=recent_records,
                last_updated=last_updated
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting statistics: {str(e)}")

@app.get("/records", response_model=SearchResponse)
async def get_records(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of records per page"),
    sort_by: str = Query("last_updated", description="Sort field"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order")
):
    """Get paginated list of records"""
    try:
        if not db_manager.Session:
            raise HTTPException(status_code=500, detail="Database not initialized")
        
        with db_manager.Session() as session:
            # Get total count
            total = session.query(QuiltRecord).count()
            
            # Build query with sorting
            query = session.query(QuiltRecord)
            
            if hasattr(QuiltRecord, sort_by):
                sort_column = getattr(QuiltRecord, sort_by)
                if sort_order == "desc":
                    query = query.order_by(sort_column.desc())
                else:
                    query = query.order_by(sort_column.asc())
            
            # Apply pagination
            offset = (page - 1) * page_size
            records = query.offset(offset).limit(page_size).all()
            
            # Convert to response format with row IDs
            record_responses = []
            for i, record in enumerate(records, start=offset + 1):
                record_responses.append(db_record_to_response(record, i))
            
            return SearchResponse(
                records=record_responses,
                total=total,
                page=page,
                page_size=page_size
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting records: {str(e)}")

@app.get("/records/{item_id}", response_model=QuiltRecordResponse)
async def get_record(item_id: str):
    """Get specific record by ID"""
    try:
        if not db_manager.Session:
            raise HTTPException(status_code=500, detail="Database not initialized")
        
        with db_manager.Session() as session:
            record = session.query(QuiltRecord).filter_by(item_id=item_id).first()
            
            if not record:
                raise HTTPException(status_code=404, detail=f"Record not found: {item_id}")
            
            return db_record_to_response(record, 1)  # Use 1 as ID for single record
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting record: {str(e)}")

@app.get("/records/search", response_model=SearchResponse)
async def search_records(
    q: str = Query(..., description="Search query"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=50, description="Number of records per page")
):
    """Search records by query"""
    try:
        if not db_manager.Session:
            raise HTTPException(status_code=500, detail="Database not initialized")
        
        with db_manager.Session() as session:
            # Simple text search across title, description, and memorial names
            search_term = f"%{q}%"
            
            query = session.query(QuiltRecord).filter(
                QuiltRecord.title.ilike(search_term) |
                QuiltRecord.description.ilike(search_term) |
                QuiltRecord.memorial_names.ilike(search_term) |
                QuiltRecord.panel_maker.ilike(search_term)
            )
            
            total = query.count()
            
            # Apply pagination
            offset = (page - 1) * page_size
            records = query.offset(offset).limit(page_size).all()
            
            # Convert to response format with row IDs
            record_responses = []
            for i, record in enumerate(records, start=offset + 1):
                record_responses.append(db_record_to_response(record, i))
            
            return SearchResponse(
                records=record_responses,
                total=total,
                page=page,
                page_size=page_size
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching records: {str(e)}")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the server
    uvicorn.run(
        "api_server:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        log_level="info"
    )
