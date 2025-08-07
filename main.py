#!/usr/bin/env python3
"""
AIDS Memorial Quilt Records Scraper
Scrapes images and metadata from the Library of Congress AIDS Memorial Quilt Records collection
using the LOC APIs: https://www.loc.gov/apis/
Collection URL: https://www.loc.gov/collections/aids-memorial-quilt-records/about-this-collection/
"""

import asyncio
import logging
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
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def main():
    """Main entry point for the AIDS Memorial Quilt Records scraper."""
    logger.info("Starting AIDS Memorial Quilt Records scraper...")
    
    # Initialize settings
    settings = Settings()
    
    # Create output directories
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    settings.images_dir.mkdir(parents=True, exist_ok=True)
    settings.metadata_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize components
    api_client = LOCAPIClient(settings)
    image_downloader = ImageDownloader(settings)
    metadata_extractor = MetadataExtractor(settings)
    
    try:
        # Get collection items from LOC API
        logger.info("Fetching collection items from LOC API...")
        items = await api_client.get_collection_items()
        logger.info(f"Found {len(items)} items in the collection")
        
        # Process each item
        for item in items:
            logger.info(f"Processing item: {item.get('id', 'Unknown')}")
            
            # Extract metadata
            metadata = await metadata_extractor.extract_metadata(item)
            
            # Download images
            if 'image_urls' in metadata:
                await image_downloader.download_images(
                    metadata['image_urls'], 
                    metadata['id']
                )
            
            # Save metadata
            await metadata_extractor.save_metadata(metadata)
            
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise
    finally:
        await api_client.close()
        await image_downloader.close()
    
    logger.info("Scraping completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
