#!/usr/bin/env python3
"""
Command-line interface for the AIDS Memorial Quilt Records scraper
"""

import asyncio
import sys
from pathlib import Path

import click

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from src.loc_api_client import LOCAPIClient
from src.image_downloader import ImageDownloader
from src.metadata_extractor import MetadataExtractor
from config.settings import Settings


@click.group()
def cli():
    """AIDS Memorial Quilt Records Scraper CLI"""
    pass


@cli.command()
@click.option('--count', default=10, help='Number of items to fetch (default: 10)')
@click.option('--start', default=0, help='Starting index (default: 0)')
@click.option('--download-images/--no-download-images', default=False, help='Download images (default: False)')
@click.option('--save-metadata/--no-save-metadata', default=True, help='Save metadata (default: True)')
def scrape(count, start, download_images, save_metadata):
    """Scrape items from the AIDS Memorial Quilt collection"""
    
    async def run_scrape():
        settings = Settings()
        
        # Ensure output directories exist
        settings.output_dir.mkdir(parents=True, exist_ok=True)
        settings.images_dir.mkdir(parents=True, exist_ok=True)
        settings.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        client = LOCAPIClient(settings)
        downloader = ImageDownloader(settings)
        extractor = MetadataExtractor(settings)
        
        try:
            click.echo(f"🔍 Fetching {count} items starting from index {start}...")
            items = await client.get_collection_items(start=start, count=count)
            click.echo(f"✅ Retrieved {len(items)} items")
            
            with click.progressbar(items, label="Processing items") as bar:
                for item in bar:
                    item_id = item.get('id', 'unknown').replace('https://www.loc.gov/item/', '').rstrip('/')
                    
                    # Extract metadata
                    if save_metadata:
                        metadata = await extractor.extract_metadata(item)
                        await extractor.save_metadata(metadata)
                    else:
                        metadata = await extractor.extract_metadata(item)
                    
                    # Download images if requested
                    if download_images and 'image_urls' in metadata and metadata['image_urls']:
                        await downloader.download_images(metadata['image_urls'], item_id, metadata)
            
            click.echo("🎉 Scraping completed!")
            
        except Exception as e:
            click.echo(f"❌ Error during scraping: {e}", err=True)
            sys.exit(1)
        finally:
            await client.close()
            await downloader.close()
    
    asyncio.run(run_scrape())


@cli.command()
def test():
    """Test API connectivity"""
    
    async def run_test():
        settings = Settings()
        client = LOCAPIClient(settings)
        
        try:
            click.echo("🔍 Testing API connection...")
            items = await client.get_collection_items(start=0, count=3)
            click.echo(f"✅ Successfully retrieved {len(items)} test items")
            
            if items:
                first_item = items[0]
                item_id = first_item.get('id', '').replace('https://www.loc.gov/item/', '').rstrip('/')
                click.echo(f"📄 Sample item: {first_item.get('title', 'No title')}")
                
                if item_id:
                    details = await client.get_item_details(item_id)
                    click.echo(f"✅ Retrieved item details")
                    
                    resources = await client.get_item_resources(item_id)
                    click.echo(f"✅ Found {len(resources)} resources")
            
            click.echo("🎉 API test completed successfully!")
            
        except Exception as e:
            click.echo(f"❌ API test failed: {e}", err=True)
            sys.exit(1)
        finally:
            await client.close()
    
    asyncio.run(run_test())


@cli.command()
@click.argument('item_id')
@click.option('--download-images/--no-download-images', default=True, help='Download images (default: True)')
def item(item_id, download_images):
    """Get details for a specific item"""
    
    async def run_item():
        settings = Settings()
        client = LOCAPIClient(settings)
        downloader = ImageDownloader(settings)
        extractor = MetadataExtractor(settings)
        
        try:
            click.echo(f"🔍 Fetching details for item: {item_id}")
            
            # Get item details
            details = await client.get_item_details(item_id)
            resources = await client.get_item_resources(item_id)
            
            # Combine details and resources
            item_data = details.copy() if details else {}
            item_data['resources'] = resources
            
            # Extract metadata
            metadata = await extractor.extract_metadata(item_data)
            await extractor.save_metadata(metadata)
            
            click.echo(f"✅ Title: {metadata.get('title', 'No title')}")
            click.echo(f"✅ Found {len(metadata.get('image_urls', []))} image URLs")
            
            # Download images if requested
            if download_images and metadata.get('image_urls'):
                await downloader.download_images(metadata['image_urls'], item_id, metadata)
                click.echo(f"✅ Downloaded images for item: {item_id}")
            
            click.echo("🎉 Item processing completed!")
            
        except Exception as e:
            click.echo(f"❌ Error processing item: {e}", err=True)
            sys.exit(1)
        finally:
            await client.close()
            await downloader.close()
    
    asyncio.run(run_item())


@cli.command()
def info():
    """Show information about the collection and settings"""
    settings = Settings()
    
    click.echo("📊 AIDS Memorial Quilt Records Scraper")
    click.echo("=" * 40)
    click.echo(f"Collection URL: {settings.aids_quilt_collection_url}")
    click.echo(f"API Base URL: {settings.loc_api_base_url}")
    click.echo(f"Output directory: {settings.output_dir}")
    click.echo(f"Images directory: {settings.images_dir}")
    click.echo(f"Metadata directory: {settings.metadata_dir}")
    click.echo(f"Max concurrent downloads: {settings.max_concurrent_downloads}")
    click.echo(f"Rate limit delay: {settings.rate_limit_delay}s")
    click.echo(f"Max image size: {settings.max_image_size_mb}MB")
    click.echo(f"Supported formats: {', '.join(settings.supported_image_formats)}")


if __name__ == '__main__':
    cli()
