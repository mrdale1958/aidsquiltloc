"""
Database models and operations for AIDS Memorial Quilt Records
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from sqlalchemy import create_engine, Column, String, Text, DateTime, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()


class QuiltRecord(Base):
    """SQLAlchemy model for AIDS Memorial Quilt records"""
    __tablename__ = 'quilt_records'
    
    # Primary fields
    item_id = Column(String(255), primary_key=True)  # e.g., afc2019048_0001
    loc_url = Column(String(500), nullable=False)    # Full LOC URL
    title = Column(Text, nullable=True)
    
    # Metadata
    description = Column(Text, nullable=True)
    subject = Column(Text, nullable=True)            # JSON array of subjects
    contributor = Column(Text, nullable=True)        # JSON array of contributors
    date_created = Column(String(100), nullable=True)
    location = Column(String(500), nullable=True)
    format_info = Column(Text, nullable=True)        # JSON array of formats
    
    # Quilt-specific fields
    quilt_block_number = Column(String(20), nullable=True)
    memorial_names = Column(Text, nullable=True)     # JSON array of names on the quilt
    panel_maker = Column(String(500), nullable=True)
    
    # Resource tracking
    image_urls = Column(Text, nullable=True)         # JSON array of image URLs
    resource_urls = Column(Text, nullable=True)      # JSON array of other resources
    local_images = Column(Text, nullable=True)       # JSON array of local image paths
    
    # Change tracking
    content_hash = Column(String(64), nullable=False)  # Hash of metadata for change detection
    first_seen = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_updated = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_checked = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Status flags
    images_downloaded = Column(Boolean, default=False)
    metadata_complete = Column(Boolean, default=False)
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_last_checked', 'last_checked'),
        Index('idx_last_updated', 'last_updated'),
        Index('idx_quilt_block_number', 'quilt_block_number'),
        Index('idx_content_hash', 'content_hash'),
    )


class DatabaseManager:
    """Manages database operations for AIDS Memorial Quilt records"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_url = f"sqlite:///{db_path}"
        self.engine = None
        self.Session = None
        
    def initialize_database(self):
        """Initialize the database and create tables"""
        try:
            # Ensure directory exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create engine and tables
            self.engine = create_engine(self.db_url, echo=False)
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)
            
            logger.info("Database initialized at: %s", self.db_path)
            
        except Exception as e:
            logger.error("Failed to initialize database: %s", e)
            raise
    
    def _calculate_content_hash(self, metadata: Dict[str, Any]) -> str:
        """Calculate a hash of the metadata for change detection"""
        # Create a stable representation of the metadata
        stable_data = {
            'title': metadata.get('title', ''),
            'description': metadata.get('description', []),
            'subject': sorted(metadata.get('subject', [])),
            'contributor': sorted(metadata.get('contributor', [])),
            'date': metadata.get('date', ''),
            'image_urls': sorted(metadata.get('image_url', [])),
        }
        
        # Convert to JSON and hash
        json_str = json.dumps(stable_data, sort_keys=True)
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()
    
    def _extract_quilt_info(self, metadata: Dict[str, Any]) -> Tuple[Optional[str], Optional[List[str]]]:
        """Extract quilt-specific information from metadata"""
        title = metadata.get('title', '')
        quilt_block_number = None
        memorial_names = []
        
        # Extract block number from title like "AIDS Quilt Block 2621 Panel Maker Records"
        if 'AIDS Quilt Block' in title:
            parts = title.split()
            for i, part in enumerate(parts):
                if part == 'Block' and i + 1 < len(parts):
                    try:
                        quilt_block_number = parts[i + 1]
                        break
                    except (ValueError, IndexError):
                        pass
        
        # Look for memorial names in various fields
        description = metadata.get('description', [])
        if isinstance(description, list) and description:
            # Names might be in the description
            desc_text = ' '.join(description)
            # This is a simple extraction - could be enhanced with NLP
            if 'memorial' in desc_text.lower() or 'rememb' in desc_text.lower():
                # Simple name extraction - look for patterns like "In memory of John Doe"
                # For now, we'll rely on manual extraction or enhancement later
                memorial_names = []  # Placeholder for future name extraction
        
        return quilt_block_number, memorial_names
    
    async def upsert_record(self, item_id: str, metadata: Dict[str, Any], 
                           image_urls: List[str] = None, 
                           resource_urls: List[str] = None) -> bool:
        """
        Insert or update a quilt record
        
        Args:
            item_id: The LOC item identifier
            metadata: Raw metadata from LOC API
            image_urls: List of image URLs for this item
            resource_urls: List of other resource URLs
            
        Returns:
            True if record was updated (changed), False if no change
        """
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        try:
            # Calculate content hash
            content_hash = self._calculate_content_hash(metadata)
            
            # Extract quilt-specific info
            quilt_block_number, memorial_names = self._extract_quilt_info(metadata)
            
            with self.Session() as session:
                # Check if record exists
                existing = session.query(QuiltRecord).filter_by(item_id=item_id).first()
                
                now = datetime.utcnow()
                
                if existing:
                    # Update last_checked
                    existing.last_checked = now
                    
                    # Check if content has changed
                    if existing.content_hash != content_hash:
                        logger.info("Content changed for item: %s", item_id)
                        
                        # Update all fields
                        existing.title = metadata.get('title', '')
                        existing.description = json.dumps(metadata.get('description', []))
                        existing.subject = json.dumps(metadata.get('subject', []))
                        existing.contributor = json.dumps(metadata.get('contributor', []))
                        existing.date_created = metadata.get('date', '')
                        existing.location = json.dumps(metadata.get('location', []))
                        existing.format_info = json.dumps(metadata.get('original_format', []))
                        
                        existing.quilt_block_number = quilt_block_number
                        existing.memorial_names = json.dumps(memorial_names)
                        
                        existing.image_urls = json.dumps(image_urls or [])
                        existing.resource_urls = json.dumps(resource_urls or [])
                        
                        existing.content_hash = content_hash
                        existing.last_updated = now
                        existing.metadata_complete = True
                        
                        session.commit()
                        return True
                    else:
                        # No content change, just update last_checked
                        session.commit()
                        return False
                else:
                    # Create new record
                    logger.info("Creating new record for item: %s", item_id)
                    
                    new_record = QuiltRecord(
                        item_id=item_id,
                        loc_url=f"https://www.loc.gov/item/{item_id}/",
                        title=metadata.get('title', ''),
                        description=json.dumps(metadata.get('description', [])),
                        subject=json.dumps(metadata.get('subject', [])),
                        contributor=json.dumps(metadata.get('contributor', [])),
                        date_created=metadata.get('date', ''),
                        location=json.dumps(metadata.get('location', [])),
                        format_info=json.dumps(metadata.get('original_format', [])),
                        
                        quilt_block_number=quilt_block_number,
                        memorial_names=json.dumps(memorial_names),
                        
                        image_urls=json.dumps(image_urls or []),
                        resource_urls=json.dumps(resource_urls or []),
                        
                        content_hash=content_hash,
                        first_seen=now,
                        last_updated=now,
                        last_checked=now,
                        metadata_complete=True,
                        images_downloaded=False
                    )
                    
                    session.add(new_record)
                    session.commit()
                    return True
                    
        except Exception as e:
            logger.error("Error upserting record %s: %s", item_id, e)
            raise
    
    async def mark_images_downloaded(self, item_id: str, local_image_paths: List[str]):
        """Mark that images have been downloaded for an item"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        try:
            with self.Session() as session:
                record = session.query(QuiltRecord).filter_by(item_id=item_id).first()
                if record:
                    record.local_images = json.dumps(local_image_paths)
                    record.images_downloaded = True
                    session.commit()
                    logger.debug("Marked images downloaded for: %s", item_id)
                    
        except Exception as e:
            logger.error("Error marking images downloaded for %s: %s", item_id, e)
            raise
    
    async def get_records_needing_updates(self, hours_since_check: int = 24) -> List[str]:
        """Get item IDs that haven't been checked recently"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_since_check)
            
            with self.Session() as session:
                records = session.query(QuiltRecord.item_id).filter(
                    QuiltRecord.last_checked < cutoff_time
                ).all()
                
                return [record.item_id for record in records]
                
        except Exception as e:
            logger.error("Error getting records needing updates: %s", e)
            raise
    
    async def get_records_without_images(self) -> List[str]:
        """Get item IDs that don't have images downloaded"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        try:
            with self.Session() as session:
                records = session.query(QuiltRecord.item_id).filter(
                    QuiltRecord.images_downloaded == False
                ).all()
                
                return [record.item_id for record in records]
                
        except Exception as e:
            logger.error("Error getting records without images: %s", e)
            raise
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        try:
            with self.Session() as session:
                total_records = session.query(QuiltRecord).count()
                records_with_images = session.query(QuiltRecord).filter(
                    QuiltRecord.images_downloaded == True
                ).count()
                recent_updates = session.query(QuiltRecord).filter(
                    QuiltRecord.last_updated > datetime.utcnow() - timedelta(days=7)
                ).count()
                
                return {
                    'total_records': total_records,
                    'records_with_images': records_with_images,
                    'records_without_images': total_records - records_with_images,
                    'recent_updates': recent_updates
                }
                
        except Exception as e:
            logger.error("Error getting statistics: %s", e)
            raise
    
    def close(self):
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.debug("Database connections closed")
