#!/usr/bin/env python3
"""
Import metadata JSON files into the database for dashboard viewing
"""

import asyncio
import json
import logging
from pathlib import Path
import sys

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from src.database import DatabaseManager
from config.settings import Settings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def import_metadata_files():
    """Import all metadata JSON files into the database"""
    settings = Settings()
    db_manager = DatabaseManager(settings.database_path)
    
    metadata_dir = Path("output/metadata")
    if not metadata_dir.exists():
        logger.error("Metadata directory not found: %s", metadata_dir)
        return
    
    # Find all metadata JSON files
    metadata_files = list(metadata_dir.glob("afc2019048_*_metadata.json"))
    
    if not metadata_files:
        logger.warning("No metadata files found in %s", metadata_dir)
        return
    
    logger.info("Found %d metadata files to import", len(metadata_files))
    
    imported_count = 0
    error_count = 0
    
    for metadata_file in metadata_files:
        try:
            # Extract item ID from filename (e.g., afc2019048_0001_metadata.json -> afc2019048_0001)
            item_id = metadata_file.stem.replace('_metadata', '')
            
            logger.info("Importing %s", item_id)
            
            # Load metadata
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Import into database
            changed = await db_manager.upsert_record(
                item_id=item_id,
                metadata=metadata,
                image_urls=metadata.get('image_urls', []),
                resource_urls=metadata.get('resource_urls', [])
            )
            
            if changed:
                logger.info("✅ Imported %s", item_id)
                imported_count += 1
            else:
                logger.info("ℹ️  %s already up to date", item_id)
            
        except Exception as e:
            logger.error("❌ Error importing %s: %s", metadata_file.name, e)
            error_count += 1
    
    logger.info("🏁 Import complete:")
    logger.info("   📊 Total files: %d", len(metadata_files))
    logger.info("   ✅ Imported: %d", imported_count)
    logger.info("   💥 Errors: %d", error_count)

if __name__ == "__main__":
    asyncio.run(import_metadata_files())
