#!/usr/bin/env python3
"""
Direct ID scraper with enhanced image downloading
Iterates through all AIDS Memorial Quilt block IDs (afc2019048_0001 to afc2019048_7164)
and downloads images using the enhanced ImageDownloader with manuscript organization
"""

import asyncio
import logging
import time
from pathlib import Path

from src.loc_api_client import LOCAPIClient
from src.image_downloader import ImageDownloader
from src.metadata_extractor import MetadataExtractor
from config.settings import Settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('direct_id_scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class DirectIDScraperWithImages:
    """Direct ID scraper with enhanced image downloading"""
    
    def __init__(self):
        self.settings = Settings()
        self.api_client = LOCAPIClient(self.settings)
        self.image_downloader = ImageDownloader(self.settings)
        self.metadata_extractor = MetadataExtractor(self.settings)
        
        # Statistics
        self.stats = {
            'items_processed': 0,
            'items_found': 0,
            'items_not_found': 0,
            'images_downloaded': 0,
            'errors': 0,
            'start_time': time.time()
        }
    
    async def scrape_range(self, start_id: int = 1, end_id: int = 7164):
        """
        Scrape AIDS Memorial Quilt items by direct ID range
        
        Args:
            start_id: Starting block number (1 for afc2019048_0001)
            end_id: Ending block number (7164 for afc2019048_7164)
        """
        logger.info("🏳️‍🌈 Starting Direct ID Scraper with Enhanced Images")
        logger.info(f"📊 Processing blocks {start_id:04d} to {end_id:04d}")
        logger.info(f"⏱️  Rate limit: {self.settings.rate_limit_delay}s between requests")
        
        # Ensure output directories exist
        self.settings.images_dir.mkdir(parents=True, exist_ok=True)
        self.settings.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            for block_num in range(start_id, end_id + 1):
                item_id = f"afc2019048_{block_num:04d}"
                
                try:
                    success = await self._process_item(item_id)
                    self.stats['items_processed'] += 1
                    
                    if success:
                        self.stats['items_found'] += 1
                    else:
                        self.stats['items_not_found'] += 1
                    
                    # Progress report every 25 items
                    if self.stats['items_processed'] % 25 == 0:
                        await self._print_progress()
                
                except Exception as e:
                    logger.error(f"❌ Error processing {item_id}: {e}")
                    self.stats['errors'] += 1
                    continue
            
            # Final summary
            await self._print_final_summary()
            
        except KeyboardInterrupt:
            logger.info("🛑 Scraping interrupted by user")
            await self._print_final_summary()
        except Exception as e:
            logger.error(f"💥 Critical error: {e}")
            raise
        finally:
            await self._cleanup()
    
    async def _process_item(self, item_id: str) -> bool:
        """
        Process a single item: fetch metadata and download images
        
        Args:
            item_id: The item ID (e.g., afc2019048_0001)
            
        Returns:
            True if item exists and was processed, False if not found
        """
        try:
            # Try to get item details
            item_details = await self.api_client.get_item_details(item_id)
            
            if not item_details or 'item' not in item_details:
                logger.debug(f"📋 Item {item_id} not found")
                return False
            
            logger.info(f"✅ Processing: {item_id}")
            
            # Also get resources to find all available images
            resources = await self.api_client.get_item_resources(item_id)
            logger.debug(f"🔍 Found {len(resources)} resources for {item_id}")
            
            # Extract metadata including resources
            metadata = await self.metadata_extractor.extract_metadata({
                'id': item_id,  # Use clean ID for file naming
                'url': f"https://www.loc.gov/item/{item_id}/",  # Keep full URL as separate field
                'resources': resources,  # Add resources data
                **item_details.get('item', {})
            })
            
            # Download images if available
            if 'image_urls' in metadata and metadata['image_urls']:
                logger.info(f"🖼️  Downloading {len(metadata['image_urls'])} images for {item_id}")
                downloaded_paths = await self.image_downloader.download_images(
                    metadata['image_urls'], 
                    item_id,
                    metadata
                )
                self.stats['images_downloaded'] += len(downloaded_paths)
                logger.info(f"📥 Downloaded {len(downloaded_paths)} images for {item_id}")
            else:
                logger.debug(f"📋 No images found for {item_id}")
            
            # Save metadata
            await self.metadata_extractor.save_metadata(metadata)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing {item_id}: {e}")
            raise
    
    async def _print_progress(self):
        """Print progress statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['items_processed'] / elapsed if elapsed > 0 else 0
        
        logger.info(f"🎯 Progress: {self.stats['items_processed']} processed, "
                   f"{self.stats['items_found']} found, "
                   f"{self.stats['images_downloaded']} images downloaded, "
                   f"{rate:.2f} items/sec")
    
    async def _print_final_summary(self):
        """Print final statistics"""
        elapsed = time.time() - self.stats['start_time']
        
        logger.info("🏁 Final Summary:")
        logger.info(f"   📊 Items processed: {self.stats['items_processed']}")
        logger.info(f"   ✅ Items found: {self.stats['items_found']}")
        logger.info(f"   ❌ Items not found: {self.stats['items_not_found']}")
        logger.info(f"   🖼️  Images downloaded: {self.stats['images_downloaded']}")
        logger.info(f"   💥 Errors: {self.stats['errors']}")
        logger.info(f"   ⏱️  Total time: {elapsed:.1f} seconds")
        logger.info(f"   📈 Average rate: {self.stats['items_processed']/elapsed:.2f} items/sec")
    
    async def _cleanup(self):
        """Clean up resources"""
        await self.api_client.close()
        await self.image_downloader.close()


async def main():
    """Main entry point"""
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='AIDS Memorial Quilt Direct ID Scraper with Enhanced Images')
    parser.add_argument('--start-id', type=int, default=1, help='Starting ID number (default: 1)')
    parser.add_argument('--end-id', type=int, default=7164, help='Ending ID number (default: 7164)')
    parser.add_argument('--test', action='store_true', help='Test mode (no effect on processing)')
    
    args = parser.parse_args()
    
    scraper = DirectIDScraperWithImages()
    
    # Use command-line arguments for range
    await scraper.scrape_range(start_id=args.start_id, end_id=args.end_id)


if __name__ == "__main__":
    asyncio.run(main())
