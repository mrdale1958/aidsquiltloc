#!/usr/bin/env python3
"""
Unlimited AIDS Memorial Quilt Records Scraper
Scrapes the complete collection from the Library of Congress with no limits
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

# Add src and config to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from src.loc_api_client import LOCAPIClient
from src.image_downloader import ImageDownloader
from src.metadata_extractor_enhanced import MetadataExtractor
from src.database import DatabaseManager
from config.settings import Settings


class UnlimitedAIDSQuiltScraper:
    """Unlimited scraper for the complete AIDS Memorial Quilt collection"""
    
    def __init__(self):
        self.settings = Settings()
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.api_client = LOCAPIClient(self.settings)
        self.db_manager = DatabaseManager(self.settings.base_dir / "quilt_records.db")
        self.metadata_extractor = MetadataExtractor(self.settings, self.db_manager)
        self.image_downloader = ImageDownloader(self.settings)
        
        # Statistics
        self.stats = {
            'items_processed': 0,
            'items_updated': 0,
            'items_new': 0,
            'items_skipped': 0,
            'images_downloaded': 0,
            'errors': 0,
            'batches_processed': 0
        }
    
    def setup_logging(self):
        """Set up comprehensive logging"""
        logging.basicConfig(
            level=getattr(logging, self.settings.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.settings.log_file),
                logging.StreamHandler()
            ]
        )
    
    async def run_unlimited_scrape(self, download_images: bool = False):
        """
        Run unlimited scrape of the entire collection
        
        Args:
            download_images: Whether to download images after metadata collection
        """
        try:
            # Initialize database
            self.db_manager.initialize_database()
            
            self.logger.info("🚀 Starting UNLIMITED scrape of AIDS Memorial Quilt collection")
            self.logger.info("📊 This will collect the entire collection - estimated 5,000+ records")
            
            # Get initial stats
            initial_stats = await self.db_manager.get_statistics()
            self.logger.info("📈 Initial database stats: %s", initial_stats)
            
            # Scrape all metadata with no limits
            await self._scrape_all_metadata()
            
            # Download images if requested
            if download_images:
                self.logger.info("🖼️  Starting image download phase...")
                await self._download_missing_images()
            
            # Get final stats
            final_stats = await self.db_manager.get_statistics()
            self.logger.info("🎉 UNLIMITED SCRAPE COMPLETED!")
            self.logger.info("📊 Final database stats: %s", final_stats)
            self.logger.info("📈 Processing statistics: %s", self.stats)
            
            # Calculate improvements
            records_added = final_stats['total_records'] - initial_stats['total_records']
            self.logger.info("✨ Records added this session: %d", records_added)
            
        except Exception as e:
            self.logger.error("💥 Error in unlimited scrape: %s", e)
            raise
        finally:
            await self._cleanup()
    
    async def _scrape_all_metadata(self):
        """Scrape metadata from ALL items in the collection"""
        
        start = 0
        batch_size = 50  # Conservative batch size for stability
        consecutive_empty_batches = 0
        consecutive_no_updates_batches = 0
        max_empty_batches = 3  # Stop after 3 consecutive empty batches
        max_no_updates_batches = 10  # Continue for up to 10 batches with no updates
        
        while True:
            try:
                # Get batch of items
                self.logger.info("📦 Fetching batch %d: start=%d, count=%d", 
                                self.stats['batches_processed'] + 1, start, batch_size)
                
                items = await self.api_client.get_collection_items(
                    start=start, 
                    count=batch_size,
                    max_items=None  # No limits!
                )
                
                if not items:
                    consecutive_empty_batches += 1
                    self.logger.warning("⚠️  Empty batch %d (consecutive: %d/%d)", 
                                      self.stats['batches_processed'] + 1, 
                                      consecutive_empty_batches, max_empty_batches)
                    
                    if consecutive_empty_batches >= max_empty_batches:
                        self.logger.info("🏁 Reached end of collection after %d consecutive empty batches", 
                                        consecutive_empty_batches)
                        break
                    
                    # Try next batch
                    start += batch_size
                    continue
                else:
                    consecutive_empty_batches = 0  # Reset counter
                
                self.logger.info("✅ Retrieved %d items in batch %d", 
                               len(items), self.stats['batches_processed'] + 1)
                
                # Process each item in the batch
                batch_updates = 0
                batch_errors = 0
                batch_new_items = 0
                
                for i, item in enumerate(items):
                    try:
                        was_updated = await self._process_single_item(item)
                        self.stats['items_processed'] += 1
                        
                        if was_updated:
                            self.stats['items_updated'] += 1
                            batch_updates += 1
                            batch_new_items += 1
                        else:
                            self.stats['items_skipped'] += 1
                        
                        # Progress logging every 10 items
                        if (i + 1) % 10 == 0:
                            self.logger.debug("📊 Batch progress: %d/%d items processed", 
                                            i + 1, len(items))
                        
                    except Exception as e:
                        self.logger.error("❌ Error processing item %d in batch: %s", i + 1, e)
                        self.stats['errors'] += 1
                        batch_errors += 1
                
                self.stats['batches_processed'] += 1
                
                # Track consecutive batches with no updates
                if batch_updates == 0:
                    consecutive_no_updates_batches += 1
                else:
                    consecutive_no_updates_batches = 0
                
                # Batch summary
                self.logger.info("📊 Batch %d complete: %d items, %d updates (%d new), %d errors", 
                               self.stats['batches_processed'], len(items), 
                               batch_updates, batch_new_items, batch_errors)
                
                # Overall progress
                self.logger.info("🎯 Overall progress: %d items processed, %d total updates", 
                               self.stats['items_processed'], self.stats['items_updated'])
                
                # Check if we should continue despite no updates
                if consecutive_no_updates_batches >= max_no_updates_batches:
                    self.logger.warning("⚠️  %d consecutive batches with no updates - might be processing existing records", 
                                      consecutive_no_updates_batches)
                    self.logger.info("🔄 Continuing to search for new records...")
                
                start += len(items)
                
                # If we got fewer items than requested, we might be near the end
                if len(items) < batch_size:
                    self.logger.info("📉 Received fewer items than requested (%d < %d), approaching collection end", 
                                   len(items), batch_size)
                
            except Exception as e:
                self.logger.error("💥 Error processing batch starting at %d: %s", start, e)
                self.stats['errors'] += 1
                
                # Continue with next batch, but with a delay
                await asyncio.sleep(5)
                start += batch_size
    
    async def _process_single_item(self, item_data: Dict[str, Any]) -> bool:
        """Process a single item: extract metadata and store in database"""
        try:
            # Extract item ID
            item_id = item_data.get('id', '').split('/item/')[-1].rstrip('/')
            if not item_id:
                self.logger.warning("⚠️  Could not extract item ID from: %s", 
                                  item_data.get('id', 'unknown'))
                return False
            
            # Check if item already exists (for logging purposes)
            from src.database import QuiltRecord
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=self.db_manager.engine)
            with Session() as session:
                existing = session.query(QuiltRecord).filter_by(item_id=item_id).first()
                is_existing = existing is not None
            
            # Get detailed information
            item_details = None
            try:
                item_details = await self.api_client.get_item_details(item_id)
            except Exception as e:
                self.logger.warning("⚠️  Could not get details for item %s: %s", item_id, e)
            
            # Get resources
            resources = []
            try:
                resources = await self.api_client.get_item_resources(item_id)
            except Exception as e:
                self.logger.warning("⚠️  Could not get resources for item %s: %s", item_id, e)
            
            # Process metadata
            was_updated = await self.metadata_extractor.process_item_metadata(
                item_data, item_details, resources
            )
            
            # Enhanced logging
            if was_updated:
                if is_existing:
                    self.logger.debug("🔄 Updated existing item: %s", item_id)
                else:
                    self.logger.info("✨ Added NEW item: %s", item_id)
                    self.stats['items_new'] += 1
            else:
                self.logger.debug("📋 No changes for existing item: %s", item_id)
            
            return was_updated
            
        except Exception as e:
            self.logger.error("❌ Error processing item: %s", e)
            return False
    
    async def _download_missing_images(self):
        """Download images for items that don't have them yet"""
        
        # This is a placeholder for future image downloading
        # The ImageDownloader integration can be added here
        self.logger.info("🖼️  Image downloading feature coming soon!")
        pass
    
    async def _cleanup(self):
        """Clean up resources"""
        try:
            await self.api_client.close()
            self.db_manager.close()
            self.logger.info("🧹 Cleanup completed")
        except Exception as e:
            self.logger.error("❌ Error during cleanup: %s", e)


async def main():
    """Main entry point for unlimited scraping"""
    print("🏳️‍🌈 AIDS Memorial Quilt - Unlimited Collection Scraper")
    print("📚 Collecting the complete Library of Congress collection")
    print("⏱️  This may take several hours depending on collection size")
    print("🔄 The scraper will process ALL available records")
    print()
    
    scraper = UnlimitedAIDSQuiltScraper()
    
    # Run unlimited scrape (no image download for now)
    await scraper.run_unlimited_scrape(download_images=False)


if __name__ == "__main__":
    asyncio.run(main())
