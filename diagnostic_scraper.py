#!/usr/bin/env python3
"""
Slow diagnostic scraper to understand what resources are actually available
"""

import asyncio
import json
import logging
from src.loc_api_client import LOCAPIClient
from src.metadata_extractor_enhanced import MetadataExtractor
from src.database import DatabaseManager
from config.settings import Settings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def diagnostic_scrape():
    """Run a very slow diagnostic scrape of a few items"""
    settings = Settings()
    client = LOCAPIClient(settings)
    
    # Initialize database manager with proper path
    db_path = settings.base_dir / "quilt_records.db"
    db_manager = DatabaseManager(db_path)
    db_manager.initialize_database()
    
    extractor = MetadataExtractor(settings, db_manager)
    
    # Test a range of items, including some manuscript ones
    test_items = [
        'afc2019048_2728',  # Known manuscript item
        'afc2019048_2700',  # Earlier item
        'afc2019048_2750',  # Recent manuscript item
        'afc2019048_2800',  # Later item
        'afc2019048_2900',  # Much later item
    ]
    
    print(f"🏳️‍🌈 AIDS Memorial Quilt - Diagnostic Resource Check")
    print(f"🔍 Testing {len(test_items)} items for available resources")
    print(f"⏱️  Using 15-second delays to avoid rate limiting")
    
    results = []
    
    for i, item_id in enumerate(test_items):
        print(f"\n{'='*60}")
        print(f"📋 Testing item {i+1}/{len(test_items)}: {item_id}")
        print(f"🌐 URL: https://www.loc.gov/item/{item_id}/")
        print(f"{'='*60}")
        
        try:
            # Create basic item data
            item_data = {
                'id': item_id,
                'url': f'https://www.loc.gov/item/{item_id}/',
                'title': f'Test item {item_id}'
            }
            
            # Get details and resources
            print(f"📊 Fetching item details...")
            item_details = await client.get_item_details(item_id)
            
            print(f"📁 Fetching item resources...")
            item_resources = await client.get_item_resources(item_id)
            
            # Extract metadata
            print(f"🧠 Extracting metadata...")
            metadata = await extractor.process_item_metadata(item_data, item_details, item_resources)
            
            # Analyze results
            resource_urls = metadata.get('resource_urls', [])
            image_urls = metadata.get('image_urls', [])
            subjects = metadata.get('subject', [])
            
            result = {
                'item_id': item_id,
                'subjects': subjects,
                'resource_count': len(resource_urls),
                'image_count': len(image_urls),
                'resource_urls': resource_urls,
                'has_manuscripts': any('manuscript' in str(s).lower() for s in subjects) if subjects else False
            }
            results.append(result)
            
            # Report findings
            print(f"✅ Processing complete:")
            print(f"   📝 Subjects: {len(subjects) if subjects else 0}")
            if subjects:
                subject_list = subjects if isinstance(subjects, list) else [subjects]
                for subject in subject_list[:3]:  # Show first 3 subjects
                    print(f"      - {subject}")
            print(f"   📁 Resources found: {len(resource_urls)}")
            print(f"   🖼️  Images found: {len(image_urls)}")
            print(f"   📄 Has manuscripts tag: {result['has_manuscripts']}")
            
            if resource_urls:
                print(f"   🔗 Resource URLs:")
                for url in resource_urls[:3]:  # Show first 3 URLs
                    print(f"      - {url}")
            
            # Long delay to avoid rate limiting
            if i < len(test_items) - 1:  # Don't wait after the last item
                print(f"⏱️  Waiting 15 seconds before next request...")
                await asyncio.sleep(15)
                
        except Exception as e:
            print(f"❌ Error processing {item_id}: {e}")
            result = {
                'item_id': item_id,
                'error': str(e),
                'resource_count': 0,
                'image_count': 0
            }
            results.append(result)
            
            # Still wait on error
            if i < len(test_items) - 1:
                print(f"⏱️  Waiting 15 seconds before next request...")
                await asyncio.sleep(15)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 DIAGNOSTIC SUMMARY")
    print(f"{'='*60}")
    
    total_resources = sum(r.get('resource_count', 0) for r in results)
    total_images = sum(r.get('image_count', 0) for r in results)
    manuscript_items = sum(1 for r in results if r.get('has_manuscripts', False))
    items_with_resources = sum(1 for r in results if r.get('resource_count', 0) > 0)
    
    print(f"🔍 Items tested: {len(results)}")
    print(f"📄 Items tagged as manuscripts: {manuscript_items}")
    print(f"📁 Total resources found: {total_resources}")
    print(f"🖼️  Total images found: {total_images}")
    print(f"✅ Items with resources: {items_with_resources}")
    
    if total_resources == 0:
        print(f"\n❌ NO RESOURCES FOUND")
        print(f"   This suggests that the AIDS Memorial Quilt collection")
        print(f"   at the Library of Congress may not have downloadable")
        print(f"   PDF files or documents available through the API.")
        print(f"   The 'manuscripts' tag likely refers to historical")
        print(f"   context rather than available digital documents.")
    
    await client.close()
    return results

if __name__ == "__main__":
    asyncio.run(diagnostic_scrape())
