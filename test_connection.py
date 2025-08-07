#!/usr/bin/env python3
"""
Test script to verify LOC API connectivity and basic functionality
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from src.loc_api_client import LOCAPIClient
from config.settings import Settings


async def test_api_connection():
    """Test basic API connectivity"""
    print("🔍 Testing Library of Congress API connection...")
    
    settings = Settings()
    client = LOCAPIClient(settings)
    
    try:
        # Test getting a small number of items
        print("📚 Fetching first 5 items from AIDS Memorial Quilt collection...")
        items = await client.get_collection_items(start=0, count=5, max_items=5)
        
        print(f"✅ Successfully retrieved {len(items)} items")
        
        if items:
            first_item = items[0]
            print(f"📄 First item ID: {first_item.get('id', 'Unknown')}")
            print(f"📄 First item title: {first_item.get('title', 'No title')}")
            
            # Test getting item details
            item_id_raw = first_item.get('id', '')
            print(f"🔍 Raw item ID: {item_id_raw}")
            
            # Extract the actual identifier from the item URL
            # Format: http://www.loc.gov/item/afc2019048_2621/
            if '/item/' in item_id_raw:
                item_id = item_id_raw.split('/item/')[-1].rstrip('/')
            else:
                item_id = item_id_raw
                
            if item_id:
                print(f"🔍 Getting details for item: {item_id}")
                details = await client.get_item_details(item_id)
                print(f"✅ Retrieved item details: {len(str(details))} characters")
                
                # Test getting resources
                print(f"🖼️  Getting resources for item: {item_id}")
                resources = await client.get_item_resources(item_id)
                print(f"✅ Found {len(resources)} resources")
        
        print("🎉 All API tests passed!")
        
    except Exception as e:
        print(f"❌ API test failed: {e}")
        raise
    finally:
        await client.close()


async def test_directory_creation():
    """Test output directory creation"""
    print("📁 Testing directory creation...")
    
    settings = Settings()
    
    # This should create directories automatically
    print(f"✅ Output directory: {settings.output_dir}")
    print(f"✅ Images directory: {settings.images_dir}")
    print(f"✅ Metadata directory: {settings.metadata_dir}")
    
    # Verify they exist
    assert settings.output_dir.exists(), "Output directory not created"
    assert settings.images_dir.exists(), "Images directory not created"
    assert settings.metadata_dir.exists(), "Metadata directory not created"
    
    print("🎉 Directory creation test passed!")


async def main():
    """Run all tests"""
    print("🚀 Starting AIDS Memorial Quilt Scraper tests...\n")
    
    try:
        await test_directory_creation()
        print()
        await test_api_connection()
        print("\n🎉 All tests completed successfully!")
        print("\n💡 You can now run the full scraper with: python main.py")
        
    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
