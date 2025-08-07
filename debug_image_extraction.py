#!/usr/bin/env python3
"""
Debug image URL extraction for AIDS Memorial Quilt items
"""
import asyncio
import json
from src.loc_api_client import LOCAPIClient
from src.metadata_extractor_enhanced import MetadataExtractor
from src.database import DatabaseManager
from config.settings import Settings

async def debug_image_extraction():
    """Debug image extraction for specific item"""
    
    # Initialize components
    settings = Settings()
    client = LOCAPIClient(settings)
    db_manager = DatabaseManager(settings)
    extractor = MetadataExtractor(db_manager, settings)
    
    item_id = "afc2019048_0001"
    
    print(f"🔍 Debugging image extraction for {item_id}")
    
    try:
        # Get item details
        print("📥 Fetching item details...")
        item_details = await client.get_item_details(item_id)
        
        # Get item resources  
        print("📥 Fetching item resources...")
        item_resources = await client.get_item_resources(item_id)
        
        # Create item_data structure
        item_data = {
            'id': f'https://www.loc.gov/item/{item_id}/',
            'url': f'https://www.loc.gov/item/{item_id}/',
            'title': item_details.get('title', f'Item {item_id}')
        }
        
        print(f"\n📊 DATA STRUCTURES:")
        print(f"   item_data keys: {list(item_data.keys())}")
        print(f"   item_details keys: {list(item_details.keys()) if item_details else 'None'}")
        print(f"   item_resources type: {type(item_resources)}, length: {len(item_resources) if item_resources else 0}")
        
        # Check for image_url in different locations
        if item_details:
            print(f"\n🔍 Looking for image URLs in item_details:")
            if 'image_url' in item_details:
                print(f"   ✅ Found 'image_url' at top level")
                image_urls = item_details['image_url']
                if isinstance(image_urls, list):
                    print(f"      Array with {len(image_urls)} URLs")
                    for i, url in enumerate(image_urls[:2]):
                        print(f"      [{i}]: {url[:80]}...")
            
            # Check in item structure
            if 'item' in item_details and isinstance(item_details['item'], dict):
                item_section = item_details['item']
                print(f"   Checking item section with keys: {list(item_section.keys())[:10]}...")
                if 'image_url' in item_section:
                    print(f"   ✅ Found 'image_url' in item section")
                    image_urls = item_section['image_url']
                    if isinstance(image_urls, list):
                        print(f"      Array with {len(image_urls)} URLs")
            
            # Check more_like_this section  
            if 'more_like_this' in item_details and isinstance(item_details['more_like_this'], dict):
                more_section = item_details['more_like_this']
                print(f"   Checking more_like_this section with keys: {list(more_section.keys())[:10]}...")
                if 'item' in more_section and isinstance(more_section['item'], list):
                    print(f"   Found item array with {len(more_section['item'])} items")
                    for i, sub_item in enumerate(more_section['item'][:3]):
                        if isinstance(sub_item, dict) and 'image_url' in sub_item:
                            print(f"      Item {i} has image_url array with {len(sub_item['image_url'])} URLs")
                
        # Extract metadata
        print(f"\n🔄 Running metadata extraction...")
        metadata = await extractor.extract_item_metadata(item_data, item_details, item_resources)
        
        if metadata:
            print(f"\n✅ Extracted metadata:")
            print(f"   item_id: {metadata.get('item_id')}")
            print(f"   image_urls: {len(metadata.get('image_urls', []))} URLs")
            print(f"   resource_urls: {len(metadata.get('resource_urls', []))} URLs")
            
            # Show some image URLs
            image_urls = metadata.get('image_urls', [])
            if image_urls:
                print(f"\n🖼️  Image URLs found:")
                for i, url in enumerate(image_urls[:3]):
                    print(f"   [{i}]: {url}")
                if len(image_urls) > 3:
                    print(f"   ... and {len(image_urls) - 3} more")
            else:
                print("   ❌ No image URLs found")
        else:
            print("❌ Failed to extract metadata")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(debug_image_extraction())
