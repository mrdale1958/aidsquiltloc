#!/usr/bin/env python3
"""
Enhanced Direct ID scraper with threaded image downloading
Separates metadata retrieval (30s API clock) from image downloads (separate thread)
"""

import asyncio
import logging
import time
import threading
import queue
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional

from src.loc_api_client import LOCAPIClient
from src.image_downloader_enhanced import ImageDownloader
from src.metadata_extractor import MetadataExtractor
from config.settings import Settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class EnhancedDirectIDScraper:
    """Enhanced scraper with separate metadata and image downloading threads"""
    
    def __init__(self):
        self.settings = Settings()
        self.api_client = LOCAPIClient(self.settings)
        self.image_downloader = ImageDownloader(self.settings)
        self.metadata_extractor = MetadataExtractor(self.settings)
        
        # Threading for image downloads
        self.image_queue = queue.Queue()
        self.download_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ImageDownloader")
        self.download_shutdown = threading.Event()
        
        # Statistics
        self.stats = {
            'items_processed': 0,
            'items_found': 0,
            'items_not_found': 0,
            'images_queued': 0,
            'images_downloaded': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # Start image download worker thread
        self.download_thread = threading.Thread(target=self._image_download_worker, daemon=True)
        self.download_thread.start()
    
    def _image_download_worker(self):
        """Worker thread that processes image download queue"""
        logger.info("🧵 Image download worker thread started")
        
        while not self.download_shutdown.is_set():
            try:
                # Get download task from queue (with timeout to check shutdown)
                try:
                    download_task = self.image_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                
                if download_task is None:  # Shutdown signal
                    break
                
                item_id, image_urls, metadata = download_task
                
                # Run async download in thread's event loop
                asyncio.run(self._download_images_async(item_id, image_urls, metadata))
                
                self.image_queue.task_done()
                
            except Exception as e:
                logger.error("Error in image download worker: %s", e)
                self.stats['errors'] += 1
        
        logger.info("🧵 Image download worker thread stopped")
    
    async def _download_images_async(self, item_id: str, image_urls: List[str], metadata: Dict[str, Any]):
        """Async wrapper for image downloads in worker thread"""
        try:
            logger.info(f"🖼️  Downloading {len(image_urls)} images for {item_id}")
            downloaded_paths = await self.image_downloader.download_images(
                image_urls, 
                item_id,
                metadata
            )
            self.stats['images_downloaded'] += len(downloaded_paths)
            logger.info(f"📥 Downloaded {len(downloaded_paths)} images for {item_id}")
            
        except Exception as e:
            logger.error(f"Error downloading images for {item_id}: {e}")
            self.stats['errors'] += 1
        finally:
            # Close session in this thread
            await self.image_downloader.close()
    
    async def scrape_range(self, start_id: int = 1, end_id: int = 7164):
        """
        Scrape a range of AIDS Memorial Quilt block IDs
        
        Args:
            start_id: Starting block number (1)
            end_id: Ending block number (7164)
        """
        logger.info(f"🏳️‍🌈 Starting Enhanced Direct ID Scraper")
        logger.info(f"📊 Processing blocks {start_id:04d} to {end_id:04d}")
        logger.info(f"⏱️  Metadata rate limit: {self.settings.rate_limit_delay}s between requests")
        logger.info(f"🖼️  Image downloads: separate thread with {getattr(self.settings, 'image_download_delay', 1.0)}s delays")
        
        try:
            for block_num in range(start_id, end_id + 1):
                # Generate item ID
                item_id = f"afc2019048_{block_num:04d}"
                
                # Process metadata on main thread with API rate limiting
                found = await self._process_metadata(item_id)
                
                self.stats['items_processed'] += 1
                if found:
                    self.stats['items_found'] += 1
                else:
                    self.stats['items_not_found'] += 1
                
                # Progress update every 10 items
                if self.stats['items_processed'] % 10 == 0:
                    self._log_progress()
        
        except KeyboardInterrupt:
            logger.info("🛑 Interrupted by user")
            raise  # Re-raise to ensure cleanup
        except Exception as e:
            logger.error(f"💥 Fatal error: {e}")
            self.stats['errors'] += 1
            raise  # Re-raise to ensure cleanup
        finally:
            await self._cleanup()
    
    async def _process_metadata(self, item_id: str) -> bool:
        """
        Process metadata for a single item and queue images for download
        
        Args:
            item_id: The item ID (e.g., afc2019048_0001)
            
        Returns:
            True if item exists and was processed, False if not found
        """
        try:
            # Try to get item details (this follows the 30s API rate limit)
            item_details = await self.api_client.get_item_details(item_id)
            
            if not item_details or 'item' not in item_details:
                logger.debug(f"📋 Item {item_id} not found")
                return False
            
            logger.info(f"✅ Processing metadata: {item_id}")
            
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
            
            # Save metadata immediately
            await self.metadata_extractor.save_metadata(metadata)
            
            # Queue images for download if available
            if 'image_urls' in metadata and metadata['image_urls']:
                logger.info(f"📋 Queuing {len(metadata['image_urls'])} images for {item_id}")
                self.image_queue.put((item_id, metadata['image_urls'], metadata))
                self.stats['images_queued'] += len(metadata['image_urls'])
            else:
                logger.debug(f"📋 No images found for {item_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"💥 Error processing metadata for {item_id}: {e}")
            self.stats['errors'] += 1
            return False
    
    def _log_progress(self):
        """Log current progress statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['items_processed'] / elapsed if elapsed > 0 else 0
        
        logger.info(f"📊 Progress: {self.stats['items_processed']} processed, "
                   f"{self.stats['items_found']} found, "
                   f"{self.stats['items_not_found']} not found, "
                   f"{self.stats['images_queued']} images queued, "
                   f"{self.stats['images_downloaded']} downloaded, "
                   f"{self.stats['errors']} errors, "
                   f"{rate:.3f} items/sec")
    
    async def _cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Cleaning up...")
        
        # Wait for remaining image downloads to complete
        if not self.image_queue.empty():
            logger.info(f"⏳ Waiting for {self.image_queue.qsize()} remaining image downloads...")
            self.image_queue.join()
        
        # Shutdown image download thread
        self.download_shutdown.set()
        self.image_queue.put(None)  # Shutdown signal
        self.download_thread.join(timeout=30)
        
        # Shutdown thread pool
        self.download_executor.shutdown(wait=True)
        
        # Close API client
        await self.api_client.close()
        
        # Final statistics
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['items_processed'] / elapsed if elapsed > 0 else 0
        
        logger.info("🏁 Final Summary:")
        logger.info(f"   📊 Items processed: {self.stats['items_processed']}")
        logger.info(f"   ✅ Items found: {self.stats['items_found']}")
        logger.info(f"   ❌ Items not found: {self.stats['items_not_found']}")
        logger.info(f"   📋 Images queued: {self.stats['images_queued']}")
        logger.info(f"   🖼️  Images downloaded: {self.stats['images_downloaded']}")
        logger.info(f"   💥 Errors: {self.stats['errors']}")
        logger.info(f"   ⏱️  Total time: {elapsed:.1f} seconds")
        logger.info(f"   📈 Average rate: {rate:.3f} items/sec")


async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced AIDS Memorial Quilt Direct ID Scraper')
    parser.add_argument('--start-id', type=int, default=1, help='Starting block number')
    parser.add_argument('--end-id', type=int, default=7164, help='Ending block number')
    
    args = parser.parse_args()
    
    scraper = EnhancedDirectIDScraper()
    await scraper.scrape_range(args.start_id, args.end_id)


if __name__ == "__main__":
    asyncio.run(main())
