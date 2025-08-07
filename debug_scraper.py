#!/usr/bin/env python3
"""
Debug scraper to identify why items aren't being processed correctly
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src and config to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from src.loc_api_client import LOCAPIClient
from src.metadata_extractor_enhanced import MetadataExtractor
from src.database import DatabaseManager
from config.settings import Settings


async def debug_single_item():
    """Debug processing of a single item"""
    
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    settings = Settings()
    # Increase rate limit delay for debugging
    settings.rate_limit_delay = 3.0
    
    # Initialize components
    api_client = LOCAPIClient(settings)
    db_manager = DatabaseManager(settings.base_dir / "debug_records.db")
    db_manager.initialize_database()
    metadata_extractor = MetadataExtractor(settings, db_manager)
    
    try:
        # Get just a few items to test
        print("Fetching test items from LOC API...")
        items = await api_client.get_collection_items(start=0, count=5)
        print(f"Retrieved {len(items)} items")
        
        for i, item in enumerate(items):
            print(f"\n=== Processing item {i+1}/{len(items)} ===")
            print(f"Item data keys: {list(item.keys())}")
            print(f"Item ID: {item.get('id', 'NO_ID')}")
            
            # Extract item ID
            item_id = item.get('id', '').split('/item/')[-1].rstrip('/')
            if not item_id:
                print(f"⚠️  Could not extract item ID from: {item.get('id', 'unknown')}")
                continue
            
            print(f"Extracted item ID: {item_id}")
            
            # Get detailed information
            try:
                print("Getting item details...")
                item_details = await api_client.get_item_details(item_id)
                print(f"Got item details with keys: {list(item_details.keys()) if item_details else 'None'}")
            except Exception as e:
                print(f"⚠️  Could not get details for item {item_id}: {e}")
                item_details = None
            
            # Get resources
            try:
                print("Getting item resources...")
                resources = await api_client.get_item_resources(item_id)
                print(f"Got {len(resources) if resources else 0} resources")
            except Exception as e:
                print(f"⚠️  Could not get resources for item {item_id}: {e}")
                resources = []
            
            # Process metadata
            try:
                print("Processing metadata...")
                was_updated = await metadata_extractor.process_item_metadata(
                    item, item_details, resources
                )
                print(f"✅ Processing result: {'Updated' if was_updated else 'No change'}")
                
            except Exception as e:
                print(f"❌ Error processing metadata: {e}")
                import traceback
                traceback.print_exc()
    
    finally:
        await api_client.close()
        db_manager.close()


async def check_database_state():
    """Check what's actually in the database"""
    settings = Settings()
    db_manager = DatabaseManager(settings.base_dir / "quilt_records.db")
    db_manager.initialize_database()
    
    try:
        stats = await db_manager.get_statistics()
        print(f"Database stats: {stats}")
        
        # Get a few recent records - let's use a direct query instead
        from src.database import QuiltRecord
        from sqlalchemy.orm import sessionmaker
        
        Session = sessionmaker(bind=db_manager.engine)
        with Session() as session:
            recent_records = session.query(QuiltRecord).order_by(QuiltRecord.last_updated.desc()).limit(5).all()
        print(f"\nRecent records:")
        for record in recent_records:
            print(f"  - {record.item_id}: {record.title}")
            print(f"    Last updated: {record.last_updated}")
            print(f"    Content hash: {record.content_hash[:10]}...")
            
    finally:
        db_manager.close()


if __name__ == "__main__":
    print("=== Checking current database state ===")
    asyncio.run(check_database_state())
    
    print("\n=== Running debug scraper ===")
    asyncio.run(debug_single_item())
