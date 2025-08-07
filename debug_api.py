#!/usr/bin/env python3
"""
Debug the exact API response structure for resource extraction
"""

import asyncio
import json
import aiohttp
from urllib.parse import urljoin

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
            
            # Small delay between requests
            if i < len(base_urls) - 1:
                print(f"⏱️  Waiting 10 seconds...")
                await asyncio.sleep(10)

if __name__ == "__main__":
    print("🏳️‍🌈 AIDS Memorial Quilt - API Structure Debug")
    print("🔬 Understanding LOC API resource patterns")
    
    asyncio.run(check_api_formats())
    
    settings = Settings()
    settings.rate_limit_delay = 1.0  # Faster for testing
    api_client = LOCAPIClient(settings)
    
    try:
        print("🔍 Diagnosing AIDS Memorial Quilt Collection...")
        print()
        
        # Test 1: Check different batch sizes and starts
        print("📊 Test 1: Pagination behavior")
        for start in [0, 50, 100, 200, 500, 1000]:
            try:
                items = await api_client.get_collection_items(start=start, count=10)
                if items:
                    first_id = items[0].get('id', '').split('/item/')[-1].rstrip('/')
                    last_id = items[-1].get('id', '').split('/item/')[-1].rstrip('/')
                    print(f"   Start {start:4d}: {len(items):2d} items, first={first_id}, last={last_id}")
                else:
                    print(f"   Start {start:4d}: No items returned")
            except Exception as e:
                print(f"   Start {start:4d}: Error - {e}")
        
        print()
        
        # Test 2: Try different search queries
        print("🔍 Test 2: Different search approaches")
        
        search_queries = [
            ('partof:"aids memorial quilt records"', 'Current query'),
            ('aids quilt', 'Simple query'),
            ('collection:"aids memorial quilt records"', 'Collection field'),
            ('afc2019048', 'ID prefix search'),
            ('quilt panel', 'Panel search'),
        ]
        
        for query, description in search_queries:
            try:
                # Make direct API call with different query
                search_url = f"{api_client.base_url}/search/"
                params = {
                    'q': query,
                    'c': 10,
                    's': 0,
                    'fo': 'json'
                }
                
                data = await api_client._make_request(search_url, params)
                if 'results' in data and data['results']:
                    total_items = data.get('pagination', {}).get('total', 'unknown')
                    print(f"   {description:25s}: {len(data['results']):2d} items (total: {total_items})")
                    
                    # Show a few IDs
                    sample_ids = []
                    for item in data['results'][:3]:
                        item_id = item.get('id', '').split('/item/')[-1].rstrip('/')
                        if item_id:
                            sample_ids.append(item_id)
                    if sample_ids:
                        print(f"                              Sample IDs: {', '.join(sample_ids)}")
                else:
                    print(f"   {description:25s}: No results")
                    
            except Exception as e:
                print(f"   {description:25s}: Error - {e}")
        
        print()
        
        # Test 3: Check total collection size
        print("📈 Test 3: Total collection information")
        try:
            # Get first batch with pagination info
            search_url = f"{api_client.base_url}/search/"
            params = {
                'q': 'partof:"aids memorial quilt records"',
                'c': 1,
                's': 0,
                'fo': 'json'
            }
            
            data = await api_client._make_request(search_url, params)
            if 'pagination' in data:
                pagination = data['pagination']
                print(f"   Total items in collection: {pagination.get('total', 'unknown')}")
                print(f"   Items per page: {pagination.get('perpage', 'unknown')}")
                print(f"   Current page: {pagination.get('current', 'unknown')}")
                print(f"   Total pages: {pagination.get('pages', 'unknown')}")
            else:
                print("   No pagination information available")
                
        except Exception as e:
            print(f"   Error getting pagination info: {e}")
        
        print()
        
        # Test 4: Sample specific ID ranges
        print("🎯 Test 4: Testing specific ID ranges")
        test_ids = [
            'afc2019048_0001',
            'afc2019048_0100', 
            'afc2019048_1000',
            'afc2019048_2621',  # Our current start
            'afc2019048_2670',  # Our current end
            'afc2019048_3000',
            'afc2019048_5000',
        ]
        
        for test_id in test_ids:
            try:
                # Try to get details for the ID
                item_details = await api_client.get_item_details(test_id)
                if item_details:
                    title = item_details.get('item', {}).get('title', 'No title')
                    print(f"   {test_id}: ✅ EXISTS - {title[:50]}")
                else:
                    print(f"   {test_id}: ❌ No details")
            except Exception as e:
                print(f"   {test_id}: ❌ Error - {str(e)[:50]}")
        
    finally:
        await api_client.close()
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
