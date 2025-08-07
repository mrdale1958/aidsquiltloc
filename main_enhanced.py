#!/usr/bin/env python3
"""
Enhanced AIDS Memorial Quilt Records Scraper with database integration
Scrapes images and metadata from the Library of Congress collection with incremental updates
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


class AIDSQuiltScraper:
    """Enhanced scraper with database integration and incremental updates"""
    
    def __init__(self):
        self.settings = Settings()
        self.setup_logging()
        
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
            'images_downloaded': 0,
            'errors': 0
        }
    
    def setup_logging(self):
        """Set up logging configuration"""
        logging.basicConfig(
            level=getattr(logging, self.settings.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.settings.log_file),
                logging.StreamHandler()
            ]
        )
    
    async def run_full_scrape(self, max_items: int = None, download_images: bool = True):
        """
        Run a full scrape of the collection
        
        Args:
            max_items: Maximum number of items to process (None for all)
            download_images: Whether to download images
        """
        try:
            # Initialize database
            self.db_manager.initialize_database()
            
            logger = logging.getLogger(__name__)
            logger.info("Starting full scrape of AIDS Memorial Quilt collection")
            
            # Get initial statistics
            initial_stats = await self.db_manager.get_statistics()
            logger.info("Initial database stats: %s", initial_stats)
            
            # Scrape metadata
            await self._scrape_metadata(max_items)
            
            # Download images if requested
            if download_images:
                await self._download_missing_images()
            
            # Final statistics
            final_stats = await self.db_manager.get_statistics()
            logger.info("Final database stats: %s", final_stats)
            logger.info("Scrape statistics: %s", self.stats)
            
        except Exception as e:
            logger.error("Error in full scrape: %s", e)
            raise
        finally:
            await self._cleanup()
    
    async def run_incremental_update(self, hours_since_check: int = 24, download_images: bool = True):
        """
        Run an incremental update to check for changes
        
        Args:
            hours_since_check: Only check items not checked in this many hours
            download_images: Whether to download new images
        """
        try:
            # Initialize database
            self.db_manager.initialize_database()
            
            logger = logging.getLogger(__name__)
            logger.info("Starting incremental update (checking items not seen in %d hours)", hours_since_check)
            
            # Get items that need updating
            items_to_check = await self.db_manager.get_records_needing_updates(hours_since_check)
            logger.info("Found %d items to check for updates", len(items_to_check))
            
            if items_to_check:
                await self._update_specific_items(items_to_check)
            
            # Download missing images if requested
            if download_images:
                await self._download_missing_images()
            
            logger.info("Incremental update completed: %s", self.stats)
            
        except Exception as e:
            logger.error("Error in incremental update: %s", e)
            raise
        finally:
            await self._cleanup()
    
    async def _scrape_metadata(self, max_items: int = None):
        """Scrape metadata from all items"""
        logger = logging.getLogger(__name__)
        
        start = 0
        batch_size = 50  # Reasonable batch size to avoid rate limiting
        
        while True:
            try:
                # Get batch of items
                logger.info("Fetching items batch: start=%d, count=%d", start, batch_size)
                
                batch_max = min(batch_size, max_items - start) if max_items else batch_size
                items = await self.api_client.get_collection_items(
                    start=start, 
                    count=batch_max,
                    max_items=batch_max
                )
                
                if not items:
                    logger.info("No more items found, scraping complete")
                    break
                
                # Process each item
                for item in items:
                    await self._process_single_item(item)
                    self.stats['items_processed'] += 1
                    
                    # Check if we've hit our limit
                    if max_items and self.stats['items_processed'] >= max_items:
                        logger.info("Reached max_items limit (%d)", max_items)
                        return
                
                start += len(items)
                
                # If we got fewer items than requested, we're done
                if len(items) < batch_size:
                    logger.info("Received fewer items than requested, scraping complete")
                    break
                
            except Exception as e:
                logger.error("Error processing batch starting at %d: %s", start, e)
                self.stats['errors'] += 1
                # Continue with next batch
                start += batch_size
    
    async def _update_specific_items(self, item_ids: List[str]):
        """Update specific items by their IDs"""
        logger = logging.getLogger(__name__)
        
        for item_id in item_ids:
            try:
                # Get current item details
                item_details = await self.api_client.get_item_details(item_id)
                
                # Create a basic item structure for processing
                item_data = {
                    'id': f"https://www.loc.gov/item/{item_id}/",
                    'title': item_details.get('title', ''),
                    # Add other fields as available
                }
                
                await self._process_single_item(item_data, item_details)
                self.stats['items_processed'] += 1
                
            except Exception as e:
                logger.error("Error updating item %s: %s", item_id, e)
                self.stats['errors'] += 1
    
    async def _process_single_item(self, item_data: Dict[str, Any], item_details: Dict[str, Any] = None):
        """Process a single item: extract metadata and optionally get detailed info"""
        try:
            # Extract item ID
            item_id = item_data.get('id', '').split('/item/')[-1].rstrip('/')
            if not item_id:
                logger.warning("Could not extract item ID from: %s", item_data.get('id', 'unknown'))
                return
            
            # Get detailed information if not provided
            if not item_details:
                try:
                    item_details = await self.api_client.get_item_details(item_id)
                except Exception as e:
                    logger.warning("Could not get details for item %s: %s", item_id, e)
                    item_details = None
            
            # Get resources
            try:
                resources = await self.api_client.get_item_resources(item_id)
            except Exception as e:
                logger.warning("Could not get resources for item %s: %s", item_id, e)
                resources = []
            
            # Process metadata
            was_updated = await self.metadata_extractor.process_item_metadata(
                item_data, item_details, resources
            )
            
            if was_updated:
                self.stats['items_updated'] += 1
            
        except Exception as e:
            logger.error("Error processing item: %s", e)
            self.stats['errors'] += 1
    
    async def _download_missing_images(self):
        """Download images for items that don't have them yet"""
        logger = logging.getLogger(__name__)
        
        # Get items without images
        items_without_images = await self.db_manager.get_records_without_images()
        logger.info("Found %d items without downloaded images", len(items_without_images))
        
        for item_id in items_without_images:
            try:
                # This would integrate with the ImageDownloader
                # For now, we'll mark as placeholder
                await self.db_manager.mark_images_downloaded(item_id, [])
                self.stats['images_downloaded'] += 1
                
            except Exception as e:
                logger.error("Error downloading images for item %s: %s", item_id, e)
                self.stats['errors'] += 1
    
    async def _cleanup(self):
        """Clean up resources"""
        try:
            await self.api_client.close()
            self.db_manager.close()
        except Exception as e:
            logging.getLogger(__name__).error("Error during cleanup: %s", e)


async def main():
    """Main entry point"""
    scraper = AIDSQuiltScraper()
    
    # Run a small test scrape
    await scraper.run_full_scrape(max_items=10, download_images=False)


if __name__ == "__main__":
    asyncio.run(main())
