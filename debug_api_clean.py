#!/usr/bin/env python3
"""
Debug the exact API response structure for resource extraction
"""

import asyncio
import json
import aiohttp
import time

async def check_api_formats():
    """Check different API endpoint formats to understand resource structure"""
    
    item_id = "afc2019048_2728"
    base_urls = [
        f"https://www.loc.gov/item/{item_id}/?fo=json",
        f"https://www.loc.gov/resource/{item_id}/?fo=json", 
        f"https://www.loc.gov/item/{item_id}/resources/?fo=json",
        f"https://www.loc.gov/{item_id}/?fo=json",
    ]
    
    print(f"🔍 Testing different LOC API endpoints for item: {item_id}")
    print(f"🎯 Looking for PDF/manuscript resource patterns")
    
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(base_urls):
            print(f"\n{'='*60}")
            print(f"📡 Testing endpoint {i+1}/{len(base_urls)}")
            print(f"🌐 URL: {url}")
            print(f"{'='*60}")
            
            try:
                headers = {'User-Agent': 'AIDS-Quilt-Research/1.0'}
                async with session.get(url, timeout=30, headers=headers) as response:
                    print(f"📊 Status: {response.status}")
                    
                    if response.status == 200:
                        try:
                            data = await response.json()
                            print(f"✅ JSON response received")
                            
                            # Look for resource-related keys
                            def find_keys(obj, target_keys=None, path=""):
                                """Recursively find keys that might contain resources"""
                                if target_keys is None:
                                    target_keys = ['resource', 'pdf', 'file', 'download', 'url', 'link']
                                
                                found = []
                                if isinstance(obj, dict):
                                    for key, value in obj.items():
                                        current_path = f"{path}.{key}" if path else key
                                        
                                        # Check if key matches our targets
                                        if any(target in key.lower() for target in target_keys):
                                            found.append((current_path, type(value).__name__, str(value)[:200]))
                                        
                                        # Recurse into nested structures
                                        if isinstance(value, (dict, list)):
                                            found.extend(find_keys(value, target_keys, current_path))
                                
                                elif isinstance(obj, list):
                                    for idx, item in enumerate(obj):
                                        current_path = f"{path}[{idx}]"
                                        if isinstance(item, (dict, list)):
                                            found.extend(find_keys(item, target_keys, current_path))
                                
                                return found
                            
                            # Find all resource-related keys
                            resource_keys = find_keys(data)
                            
                            if resource_keys:
                                print(f"🔗 Found {len(resource_keys)} potential resource keys:")
                                for path, type_name, value in resource_keys[:10]:  # Show first 10
                                    print(f"   📂 {path} ({type_name}): {value}")
                                if len(resource_keys) > 10:
                                    print(f"   ... and {len(resource_keys) - 10} more")
                            else:
                                print(f"❌ No resource-related keys found")
                                
                                # Show top-level structure
                                if isinstance(data, dict):
                                    print(f"📋 Top-level keys: {list(data.keys())}")
                            
                            # Save response for manual inspection
                            filename = f"api_response_{item_id}_{i+1}.json"
                            with open(filename, 'w') as f:
                                json.dump(data, f, indent=2)
                            print(f"💾 Saved response to {filename}")
                            
                        except json.JSONDecodeError:
                            print(f"❌ Failed to parse JSON")
                            text = await response.text()
                            print(f"📄 Response preview: {text[:300]}...")
                    
                    else:
                        print(f"❌ HTTP {response.status}")
                        
            except asyncio.TimeoutError:
                print(f"⏱️  Request timeout")
            except Exception as e:
                print(f"❌ Error: {e}")
            
            # Delay between requests
            if i < len(base_urls) - 1:
                print(f"⏱️  Waiting 10 seconds...")
                await asyncio.sleep(10)

if __name__ == "__main__":
    print("🏳️‍🌈 AIDS Memorial Quilt - API Structure Debug")
    print("🔬 Understanding LOC API resource patterns")
    
    asyncio.run(check_api_formats())
