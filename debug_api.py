#!/usr/bin/env python3
"""
Debug script to examine LOC API responses
"""

import asyncio
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from src.loc_api_client import LOCAPIClient
from config.settings import Settings


async def debug_api():
    """Debug the API responses"""
    print("🔍 Debugging Library of Congress API responses...")
    
    settings = Settings()
    client = LOCAPIClient(settings)
    
    try:
        # Get just 2 items to see the structure
        print("📚 Fetching 2 items from AIDS Memorial Quilt collection...")
        items = await client.get_collection_items(start=0, count=2, max_items=2)
        
        print(f"✅ Successfully retrieved {len(items)} items")
        
        if items:
            print("\n📄 First item structure:")
            print(json.dumps(items[0], indent=2))
            
            print("\n🔍 Looking for image/resource references...")
            first_item = items[0]
            
            # Check for different possible fields that might contain images
            possible_image_fields = ['image_url', 'resource', 'resources', 'files', 'url', 'online_format', 'digitized']
            for field in possible_image_fields:
                if field in first_item:
                    print(f"Found field '{field}': {first_item[field]}")
        
        print("\n🎉 Debug completed!")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        raise
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(debug_api())
