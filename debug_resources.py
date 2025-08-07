#!/usr/bin/env python3
"""
Debug the actual resource structure for AIDS Memorial Quilt items
"""
import asyncio
import json
from src.loc_api_client import LOCAPIClient
from config.settings import Settings

async def debug_resources():
    """Debug actual resources available for block 0001"""
    
    settings = Settings()
    client = LOCAPIClient(settings)
    
    item_id = "afc2019048_0001"
    
    print(f"🔍 Debugging resources for {item_id}")
    
    try:
        print("📥 Fetching item details...")
        item_details = await client.get_item_details(item_id)
        
        print("📥 Fetching item resources...")
        item_resources = await client.get_item_resources(item_id)
        
        print(f"\n📊 RESOURCES ENDPOINT RESPONSE:")
        print(f"   Type: {type(item_resources)}")
        print(f"   Length: {len(item_resources) if item_resources else 0}")
        
        if item_resources:
            for i, resource in enumerate(item_resources):
                print(f"\n   Resource [{i}]:")
                if isinstance(resource, dict):
                    print(f"      Keys: {list(resource.keys())}")
                    if 'files' in resource:
                        files = resource['files']
                        print(f"      Files type: {type(files)}")
                        if isinstance(files, list):
                            print(f"      Files count: {len(files)}")
                            for j, file_item in enumerate(files[:3]):  # Show first 3
                                print(f"         File [{j}]: {file_item}")
                            if len(files) > 3:
                                print(f"         ... and {len(files) - 3} more files")
                        else:
                            print(f"      Files value: {files}")
                else:
                    print(f"      Resource: {resource}")
        
        # Also check if resources are in item_details
        print(f"\n📊 ITEM DETAILS - RESOURCES SECTION:")
        if 'resources' in item_details:
            resources_section = item_details['resources']
            print(f"   Type: {type(resources_section)}")
            if isinstance(resources_section, list):
                print(f"   Count: {len(resources_section)}")
                for i, res in enumerate(resources_section):
                    if isinstance(res, dict):
                        print(f"   Resource [{i}] keys: {list(res.keys())}")
                        if 'files' in res:
                            print(f"      files: {res['files']}")
                        if 'url' in res:
                            print(f"      url: {res['url']}")
            else:
                print(f"   Value: {resources_section}")
        else:
            print("   No 'resources' section found in item_details")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(debug_resources())
