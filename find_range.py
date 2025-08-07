#!/usr/bin/env python3
"""
Quick test to find the actual valid ID range for AIDS Memorial Quilt
"""

import asyncio
from src.loc_api_client import LOCAPIClient
from config.settings import Settings

async def find_valid_range():
    """Test to find the actual valid ID range"""
    
    print("🔍 Finding valid AIDS Memorial Quilt ID range")
    
    settings = Settings()
    client = LOCAPIClient(settings)
    
    # Test some key points to determine the range
    test_points = [
        1, 10, 100, 500, 1000, 1500, 2000, 2500, 2621,  # Lower range
        3000, 3500, 4000, 4500, 4965, 5000, 5500, 6000  # Upper range
    ]
    
    print("Testing key ID points...")
    valid_ids = []
    
    for num in test_points:
        item_id = f"afc2019048_{num}"
        try:
            print(f"Testing {item_id}...", end=" ")
            
            item_details = await client.get_item_details(item_id)
            if item_details:
                print("✅ EXISTS")
                valid_ids.append(num)
            else:
                print("❌ MISSING")
                
        except Exception as e:
            if "404" in str(e):
                print("❌ 404")
            else:
                print(f"⚠️ ERROR: {e}")
        
        await asyncio.sleep(1)  # Small delay
    
    await client.close()
    
    print(f"\n📊 Valid IDs found: {valid_ids}")
    
    if valid_ids:
        min_valid = min(valid_ids)
        max_valid = max(valid_ids)
        print(f"🎯 Suggested range: {min_valid} to {max_valid}")
        print(f"   Format: afc2019048_{min_valid} to afc2019048_{max_valid}")
        
        # Calculate a safe range
        start_range = max(1, min_valid - 100)  # Start a bit before first valid
        end_range = max_valid + 500  # End a bit after last valid
        
        print(f"💡 Recommended scraping range: {start_range} to {end_range}")
        print(f"   This covers potential gaps and future additions")
    else:
        print("❌ No valid IDs found in test range")

if __name__ == "__main__":
    asyncio.run(find_valid_range())
