#!/usr/bin/env python3
"""
Enhanced CLI for AIDS Memorial Quilt Records Scraper with database features
"""

import asyncio
import click
import sys
from pathlib import Path

# Add src and config to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from main_enhanced import AIDSQuiltScraper
from src.database import DatabaseManager
from config.settings import Settings


@click.group()
def cli():
    """AIDS Memorial Quilt Records Scraper with database integration"""
    pass


@cli.command()
@click.option('--max-items', type=int, default=None, 
              help='Maximum number of items to process (default: all)')
@click.option('--download-images/--no-images', default=False, 
              help='Download images for the items (default: metadata only)')
@click.option('--batch-size', type=int, default=50, 
              help='Number of items to process per batch (default: 50)')
def scrape(max_items, download_images, batch_size):
    """Run a full scrape of the AIDS Memorial Quilt collection"""
    
    async def run_scrape():
        scraper = AIDSQuiltScraper()
        await scraper.run_full_scrape(max_items=max_items, download_images=download_images)
    
    click.echo(f"🚀 Starting full scrape...")
    if max_items:
        click.echo(f"📊 Max items: {max_items}")
    else:
        click.echo(f"📊 Processing all items in collection")
    
    click.echo(f"🖼️  Download images: {'Yes' if download_images else 'No'}")
    
    asyncio.run(run_scrape())


@cli.command()
@click.option('--hours', type=int, default=24, 
              help='Check items not updated in this many hours (default: 24)')
@click.option('--download-images/--no-images', default=False, 
              help='Download missing images (default: False)')
def update(hours, download_images):
    """Run an incremental update to check for changes"""
    
    async def run_update():
        scraper = AIDSQuiltScraper()
        await scraper.run_incremental_update(hours_since_check=hours, download_images=download_images)
    
    click.echo(f"🔄 Starting incremental update...")
    click.echo(f"⏰ Checking items not seen in {hours} hours")
    click.echo(f"🖼️  Download images: {'Yes' if download_images else 'No'}")
    
    asyncio.run(run_update())


@cli.command()
def stats():
    """Show database statistics"""
    
    async def show_stats():
        settings = Settings()
        db_manager = DatabaseManager(settings.base_dir / "quilt_records.db")
        
        try:
            db_manager.initialize_database()
            stats = await db_manager.get_statistics()
            
            click.echo("📊 Database Statistics:")
            click.echo("=" * 30)
            click.echo(f"Total records: {stats['total_records']}")
            click.echo(f"Records with images: {stats['records_with_images']}")
            click.echo(f"Records without images: {stats['records_without_images']}")
            click.echo(f"Recent updates (7 days): {stats['recent_updates']}")
            
        except Exception as e:
            click.echo(f"❌ Error getting statistics: {e}")
        finally:
            db_manager.close()
    
    asyncio.run(show_stats())


@cli.command()
@click.option('--item-id', required=True, help='Item ID to check (e.g., afc2019048_0001)')
def check_item(item_id):
    """Check details for a specific item"""
    
    async def check_single_item():
        from src.loc_api_client import LOCAPIClient
        from config.settings import Settings
        
        settings = Settings()
        client = LOCAPIClient(settings)
        
        try:
            click.echo(f"🔍 Checking item: {item_id}")
            
            # Get item details
            details = await client.get_item_details(item_id)
            click.echo(f"✅ Found item: {details.get('title', 'No title')}")
            
            # Get resources
            resources = await client.get_item_resources(item_id)
            click.echo(f"📁 Found {len(resources)} resources")
            
            # Show some basic info
            if 'image_url' in details:
                image_count = len(details['image_url']) if isinstance(details['image_url'], list) else 1
                click.echo(f"🖼️  Images available: {image_count}")
            
        except Exception as e:
            click.echo(f"❌ Error checking item: {e}")
        finally:
            await client.close()
    
    asyncio.run(check_single_item())


@cli.command()
@click.option('--force', is_flag=True, help='Force recreation of database')
def init_db(force):
    """Initialize the database"""
    
    settings = Settings()
    db_path = settings.base_dir / "quilt_records.db"
    
    if db_path.exists() and not force:
        click.echo(f"⚠️  Database already exists at {db_path}")
        click.echo("Use --force to recreate it")
        return
    
    if force and db_path.exists():
        click.echo(f"🗑️  Removing existing database...")
        db_path.unlink()
    
    try:
        db_manager = DatabaseManager(db_path)
        db_manager.initialize_database()
        click.echo(f"✅ Database initialized at {db_path}")
        
    except Exception as e:
        click.echo(f"❌ Error initializing database: {e}")
    finally:
        db_manager.close()


@cli.command()
def test_connection():
    """Test connection to Library of Congress API"""
    
    async def test_api():
        from src.loc_api_client import LOCAPIClient
        from config.settings import Settings
        
        settings = Settings()
        client = LOCAPIClient(settings)
        
        try:
            click.echo("🔍 Testing LOC API connection...")
            
            # Test getting a few items
            items = await client.get_collection_items(start=0, count=3, max_items=3)
            
            if items:
                click.echo(f"✅ Successfully retrieved {len(items)} items")
                click.echo(f"📄 First item: {items[0].get('title', 'No title')}")
            else:
                click.echo("⚠️  No items retrieved")
                
        except Exception as e:
            click.echo(f"❌ API test failed: {e}")
        finally:
            await client.close()
    
    asyncio.run(test_api())


if __name__ == '__main__':
    cli()
