#!/usr/bin/env python3
"""
Direct ID range scraper - bypasses broken LOC search pagination
Directly scrapes known ID ranges since the search API pagination is broken
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
from src.metadata_extractor_enhanced import MetadataExtractor
from src.database import DatabaseManager
from config.settings import Settings


class DirectIDScraper:
    """Direct ID range scraper - bypasses broken search pagination"""
    
    def __init__(self):
        self.settings = Settings()
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.api_client = LOCAPIClient(self.settings)
        self.db_manager = DatabaseManager(self.settings.base_dir / "quilt_records.db")
        self.metadata_extractor = MetadataExtractor(self.settings, self.db_manager)
        
        # Statistics
        self.stats = {
            'items_processed': 0,
            'items_found': 0,
            'items_new': 0,
            'items_missing': 0,
            'errors': 0
        }
    
    def setup_logging(self):
        """Set up logging"""
        logging.basicConfig(
            level=getattr(logging, self.settings.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.settings.log_file),
                logging.StreamHandler()
            ]
        )
    
    async def scrape_by_id_ranges(self, start_id: int = 1, end_id: int = 5164):
        """
        Scrape by directly accessing known ID ranges
        
        Args:
            start_id: Starting ID number (1-5164)
            end_id: Ending ID number (1-5164)
        """
        try:
            # Initialize database
            self.db_manager.initialize_database()
            
            self.logger.info("🚀 Starting DIRECT ID RANGE scraper")
            self.logger.info("📊 Scraping IDs %d to %d (total: %d)", start_id, end_id, end_id - start_id + 1)
            self.logger.info("🔧 Bypassing broken LOC search pagination")
            
            # Get initial stats
            initial_stats = await self.db_manager.get_statistics()
            self.logger.info("📈 Initial database stats: %s", initial_stats)
            
            # Process each ID directly
            batch_size = 100
            for batch_start in range(start_id, end_id + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, end_id)
                
                self.logger.info("📦 Processing ID batch: %d to %d", batch_start, batch_end)
                
                batch_found = 0
                batch_new = 0
                batch_errors = 0
                
                for item_id_num in range(batch_start, batch_end + 1):
                    item_id = f"afc2019048_{item_id_num:04d}"
                    
                    try:
                        success = await self._process_direct_id(item_id)
                        self.stats['items_processed'] += 1
                        
                        if success:
                            self.stats['items_found'] += 1
                            batch_found += 1
                        else:
                            self.stats['items_missing'] += 1
                        
                        # Progress every 25 items
                        if self.stats['items_processed'] % 25 == 0:
                            self.logger.info("🎯 Progress: %d processed, %d found, %d new", 
                                           self.stats['items_processed'], 
                                           self.stats['items_found'],
                                           self.stats['items_new'])
                        
                    except Exception as e:
                        self.logger.error("❌ Error processing %s: %s", item_id, e)
                        self.stats['errors'] += 1
                        batch_errors += 1
                
                # Batch summary
                self.logger.info("📊 Batch complete: %d found, %d new, %d errors", 
                               batch_found, batch_new, batch_errors)
                
                # Small delay between batches to be respectful
                await asyncio.sleep(1)
            
            # Final stats
            final_stats = await self.db_manager.get_statistics()
            self.logger.info("🎉 DIRECT ID SCRAPE COMPLETED!")
            self.logger.info("📊 Final database stats: %s", final_stats)
            self.logger.info("📈 Processing statistics: %s", self.stats)
            
            # Calculate improvements
            records_added = final_stats['total_records'] - initial_stats['total_records']
            self.logger.info("✨ Records added this session: %d", records_added)
            
        except Exception as e:
            self.logger.error("💥 Error in direct ID scrape: %s", e)
            raise
        finally:
            await self._cleanup()
    
    async def _process_direct_id(self, item_id: str) -> bool:
        """
        Process a single item by ID directly
        
        Args:
            item_id: The item ID (e.g., afc2019048_0001)
            
        Returns:
            True if item exists and was processed, False if not found
        """
        try:
            # Try to get item details directly
            item_details = await self.api_client.get_item_details(item_id)
            
            if not item_details or 'item' not in item_details:
                self.logger.debug("📋 Item %s not found", item_id)
                return False
            
            # Create basic item data structure for metadata processor
            item_data = {
                'id': f"https://www.loc.gov/item/{item_id}/",
                'title': item_details.get('item', {}).get('title', ''),
                'date': item_details.get('item', {}).get('date', ''),
                'description': item_details.get('item', {}).get('summary', []),
                'contributor': item_details.get('item', {}).get('contributors', []),
                'subject': item_details.get('item', {}).get('subjects', []),
                'location': item_details.get('item', {}).get('location', []),
            }
            
            # Get resources
            resources = []
            try:
                resources = await self.api_client.get_item_resources(item_id)
            except Exception as e:
                self.logger.debug("⚠️  Could not get resources for %s: %s", item_id, e)
            
            # Process metadata
            was_updated = await self.metadata_extractor.process_item_metadata(
                item_data, item_details, resources
            )
            
            if was_updated:
                self.logger.info("✨ Processed: %s", item_id)
                self.stats['items_new'] += 1
            else:
                self.logger.debug("📋 No changes: %s", item_id)
            
            return True
            
        except Exception as e:
            # Item likely doesn't exist - this is normal for some ID ranges
            self.logger.debug("📋 Item %s: %s", item_id, str(e)[:50])
            return False
    
    async def _cleanup(self):
        """Clean up resources"""
        try:
            await self.api_client.close()
            self.db_manager.close()
            self.logger.info("🧹 Cleanup completed")
        except Exception as e:
            self.logger.error("❌ Error during cleanup: %s", e)


async def main():
    """Main entry point for direct ID scraping"""
    print("🏳️‍🌈 AIDS Memorial Quilt - Direct ID Range Scraper")
    print("🔧 Bypassing broken LOC search API pagination")
    print("📊 Directly accessing 5,164 known item IDs")
    print("⏱️  This will process all individual quilt block records")
    print()
    
    scraper = DirectIDScraper()
    
    # Start from where we left off (after 2670) and go to the end
    await scraper.scrape_by_id_ranges(start_id=2671, end_id=5164)


if __name__ == "__main__":
    asyncio.run(main())
