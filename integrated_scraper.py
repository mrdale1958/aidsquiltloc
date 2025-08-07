#!/usr/bin/env python3
"""
Integrated AIDS Memorial Quilt scraper that combines metadata extraction and image downloading
"""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor

import aiofiles
import aiohttp

from src.database import DatabaseManager
from src.metadata_extractor_enhanced import MetadataExtractor
from src.image_downloader import ImageDownloader

logger = logging.getLogger(__name__)

import asyncio
import json
import logging
import aiohttp
import aiofiles
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime
import time

from src.loc_api_client import LOCAPIClient
from src.metadata_extractor_enhanced import MetadataExtractor
from src.database import DatabaseManager
from config.settings import Settings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('integrated_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IntegratedScraper:
    """
    Integrated scraper that handles metadata collection and image downloading in parallel
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = LOCAPIClient(settings)
        
        # Initialize components
        self.db_manager = DatabaseManager(self.settings.database_path)
        self.metadata_extractor = MetadataExtractor(self.settings, self.db_manager)
        self.image_downloader = ImageDownloader(self.settings)
        
        # Session management
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Statistics tracking
        self.stats = {
            'items_processed': 0,
            'items_saved': 0,
            'images_downloaded': 0,
            'errors': 0,
            'start_time': time.time(),
            'last_progress_time': time.time()
        }
        
        # Track downloaded images to avoid duplicates
        self.downloaded_images: Set[str] = set()
        
        # API rate limiting
        self.api_delay = self.settings.rate_limit_delay
        self.last_api_call = 0
        
        # Directory paths
        self.images_dir = self.settings.images_dir
        self.metadata_dir = self.settings.metadata_dir
    
    async def ensure_database_initialized(self):
        """Ensure database is initialized"""
        if not hasattr(self.db_manager, 'Session') or not self.db_manager.Session:
            self.db_manager.initialize_database()
    
    async def save_metadata(self, item_id: str, metadata: Dict) -> bool:
        """Save metadata to JSON file, organized by block"""
        try:
            # Extract block number and create block directory
            block_number = self.extract_block_number(item_id, metadata)
            block_metadata_dir = self.metadata_dir / f"block_{block_number}"
            block_metadata_dir.mkdir(parents=True, exist_ok=True)
            
            metadata_file = block_metadata_dir / f"{item_id}.json"
            async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False))
            return True
        except Exception as e:
            logger.error(f"Error saving metadata for {item_id}: {e}")
            return False
    
    def extract_block_number(self, item_id: str, metadata: Dict) -> str:
        """Extract block number from item ID or metadata"""
        # Try to extract from item ID first (e.g., afc2019048_2621 -> 2621)
        parts = item_id.split('_')
        if len(parts) >= 2:
            try:
                block_num = int(parts[-1])
                return str(block_num)
            except ValueError:
                pass
        
        # Try to extract from title or other metadata
        title = metadata.get('title', '')
        if 'Block' in title:
            title_parts = title.split()
            for i, part in enumerate(title_parts):
                if part == 'Block' and i + 1 < len(title_parts):
                    try:
                        return str(int(title_parts[i + 1]))
                    except ValueError:
                        pass
        
        # Fallback to item ID suffix
        return parts[-1] if parts else item_id

    async def process_item_images(self, item_id: str, metadata: Dict) -> List[str]:
        """Download all images for an item using the enhanced ImageDownloader"""
        try:
            # Get image URLs from metadata
            image_urls = metadata.get('image_urls', [])
            
            if not image_urls:
                logger.debug(f"No image URLs found for {item_id}")
                return []
            
            logger.info(f"Processing {len(image_urls)} images for {item_id}")
            
            # Use the enhanced ImageDownloader to download all images
            downloaded_files = await self.image_downloader.download_images(
                image_urls=image_urls,
                item_id=item_id,
                metadata=metadata
            )
            
            # Update statistics
            self.stats['images_downloaded'] += len(downloaded_files)
            
            # Return list of downloaded file paths as strings
            return [str(f) for f in downloaded_files]
            
        except Exception as e:
            logger.error(f"Error processing images for {item_id}: {e}")
            self.stats['errors'] += 1
            return []
    
    async def save_metadata(self, item_id: str, metadata: Dict) -> bool:
        """Save metadata to JSON file"""
        try:
            metadata_file = self.metadata_dir / f"{item_id}.json"
            async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False))
            return True
        except Exception as e:
            logger.error(f"Error saving metadata for {item_id}: {e}")
            return False
    
    async def rate_limited_api_call(self, coro):
        """Execute API call with rate limiting"""
        async with self.api_semaphore:
            # Check if we need to wait
            now = time.time()
            time_since_last = now - self.last_api_call
            
            if time_since_last < self.api_delay:
                wait_time = self.api_delay - time_since_last
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            
            try:
                result = await coro
                self.last_api_call = time.time()
                return result
            except aiohttp.ClientResponseError as e:
                if e.status == 429:  # Rate limited
                    self.api_delay = min(self.api_delay * 2, 120)  # Exponential backoff, max 2 minutes
                    logger.warning(f"Rate limited! Increasing delay to {self.api_delay}s")
                    raise
                else:
                    raise
    
    async def process_single_item(self, item_id: str) -> bool:
        """Process a single item - metadata extraction and image downloading"""
        try:
            # Ensure database is initialized
            await self.ensure_database_initialized()
            
            logger.info(f"Processing item: {item_id}")
            
            # Check if already processed
            if await self.db_manager.item_exists(item_id):
                logger.debug(f"Item {item_id} already in database, skipping metadata collection")
                # But we might still want to download images and save metadata if not already done
                metadata = await self.db_manager.get_item_metadata(item_id)
                if metadata:
                    # Save metadata to file if not already done
                    await self.save_metadata(item_id, metadata)
                    
                    # Download images if they exist
                    if metadata.get('image_urls'):
                        downloaded_files = await self.process_item_images(item_id, metadata)
                        logger.info(f"Downloaded {len(downloaded_files)} images for existing item {item_id}")
                    
                    # Count as processed
                    self.stats['items_processed'] += 1
                return True
            
            # Fetch item details with rate limiting
            item_details = await self.rate_limited_api_call(
                self.client.get_item_details(item_id)
            )
            
            if not item_details:
                logger.warning(f"No details found for item {item_id}")
                return False
            
            # Fetch item resources with rate limiting
            item_resources = await self.rate_limited_api_call(
                self.client.get_item_resources(item_id)
            )
            
            # Create basic item data in the format expected by MetadataExtractor
            item_data = {
                'id': f'https://www.loc.gov/item/{item_id}/',  # Full URL format for ID extraction
                'url': f'https://www.loc.gov/item/{item_id}/',
                'title': item_details.get('title', f'Item {item_id}')
            }
            
            # Extract metadata
            metadata = await self.extractor.extract_item_metadata(item_data, item_details, item_resources)
            
            if not metadata:
                logger.warning(f"No metadata extracted for item {item_id}")
                return False
            
            # Start image downloads in parallel with database save
            image_task = None
            if metadata.get('image_urls'):
                image_task = asyncio.create_task(self.process_item_images(item_id, metadata))
            
            # Save to database
            saved = await self.db_manager.upsert_record(
                item_id=metadata['item_id'],
                metadata=metadata,
                image_urls=metadata.get('image_urls', []),
                resource_urls=metadata.get('resource_urls', [])
            )
            if saved:
                self.stats['items_saved'] += 1
                logger.info(f"Saved metadata for {item_id}")
            
            # Save metadata to file
            await self.save_metadata(item_id, metadata)
            
            # Wait for image downloads to complete
            if image_task:
                downloaded_files = await image_task
                logger.info(f"Downloaded {len(downloaded_files)} images for {item_id}")
            
            self.stats['items_processed'] += 1
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error processing item {item_id}: {e}")
            return False
    
    def print_progress(self, current_id: int, total_ids: int):
        """Print progress statistics"""
        now = time.time()
        elapsed = now - self.stats['start_time']
        
        # Only print every 30 seconds to avoid spam
        if now - self.stats['last_progress_time'] < 30:
            return
        
        self.stats['last_progress_time'] = now
        
        rate = self.stats['items_processed'] / elapsed if elapsed > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"📊 PROGRESS UPDATE - ID {current_id:,}/{total_ids:,}")
        print(f"{'='*60}")
        print(f"⏱️  Elapsed time: {elapsed/3600:.1f} hours")
        print(f"📝 Items processed: {self.stats['items_processed']:,}")
        print(f"💾 Items saved: {self.stats['items_saved']:,}")
        print(f"🖼️  Images downloaded: {self.stats['images_downloaded']:,}")
        print(f"❌ Errors: {self.stats['errors']:,}")
        print(f"📈 Processing rate: {rate:.2f} items/second")
        print(f"🐌 Current API delay: {self.api_delay:.1f}s")
        
        if rate > 0:
            remaining = total_ids - current_id
            eta_seconds = remaining / rate
            eta_hours = eta_seconds / 3600
            print(f"⏰ ETA: {eta_hours:.1f} hours")
        
        print(f"{'='*60}\n")
    
    def format_item_id(self, block_number: int) -> str:
        """Format block number into correct AIDS Memorial Quilt item ID"""
        if block_number < 1000:
            # Low numbers need 4-digit padding
            return f"afc2019048_{block_number:04d}"
        else:
            # High numbers use no padding  
            return f"afc2019048_{block_number}"
    
    async def scrape_collection(self, start_id: int = 1, end_id: int = 6100):
        """Scrape the entire collection with integrated metadata and image downloading"""
        
        # Initialize database first
        await self.ensure_database_initialized()
        
        print(f"🏳️‍🌈 Starting Integrated AIDS Memorial Quilt Scraper")
        print(f"📊 ID range: {start_id:,} to {end_id:,} ({end_id - start_id + 1:,} items)")
        print(f"🔄 Parallel processing: metadata + images")
        print(f"⏱️  Starting API delay: {self.api_delay}s")
        print(f"📁 Output directories:")
        print(f"   🖼️  Images: {self.images_dir}")
        print(f"   📄 Metadata: {self.metadata_dir}")
        print(f"   💾 Database: {self.db_manager.db_path}")
        print(f"\n{'='*60}\n")
        
        try:
            for current_id in range(start_id, end_id + 1):
                item_id = self.format_item_id(current_id)
                
                success = await self.process_single_item(item_id)
                
                # Print progress periodically
                self.print_progress(current_id, end_id)
                
                # Adaptive delay based on success
                if not success:
                    # Add a small delay after errors
                    await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            print(f"\n⚠️  Scraping interrupted by user")
        except Exception as e:
            logger.error(f"Scraping error: {e}")
        
        finally:
            # Final statistics
            elapsed = time.time() - self.stats['start_time']
            rate = self.stats['items_processed'] / elapsed if elapsed > 0 else 0
            
            print(f"\n{'='*60}")
            print(f"📊 FINAL STATISTICS")
            print(f"{'='*60}")
            print(f"⏱️  Total time: {elapsed/3600:.2f} hours")
            print(f"📝 Items processed: {self.stats['items_processed']:,}")
            print(f"💾 Items saved: {self.stats['items_saved']:,}")
            print(f"🖼️  Images downloaded: {self.stats['images_downloaded']:,}")
            print(f"❌ Errors: {self.stats['errors']:,}")
            print(f"📈 Average rate: {rate:.2f} items/second")
            print(f"💿 Database: {self.db_manager.db_path}")
            print(f"📁 Output: {self.output_dir}")
            print(f"{'='*60}")
            
            await self.client.close()
            await self.image_downloader.close()
            await self.db_manager.close()

async def main():
    """Main function"""
    settings = Settings()
    scraper = IntegratedScraper(settings)
    
    # Start from where we left off or from the beginning
    # You can adjust these ranges based on what's already been processed
    await scraper.scrape_collection(start_id=2001, end_id=7164)

if __name__ == "__main__":
    asyncio.run(main())
