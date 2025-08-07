#!/usr/bin/env python3
"""
Resume AIDS Memorial Quilt Collection
Resume from where the previous run failed
"""

import asyncio
from integrated_scraper import IntegratedScraper
from config.settings import Settings

async def resume_collection():
    """Resume the AIDS Memorial Quilt collection from where it failed"""
    
    print("🏳️‍🌈 AIDS Memorial Quilt - RESUME COLLECTION")
    print("🔄 This will resume from where the previous run failed")
    print("🐛 Fixed issues:")
    print("   • ✅ Fixed 'extract_metadata' method name error")
    print("   • ✅ Fixed DatabaseManager initialization")
    print("   • ✅ Improved error handling for missing items")
    print("")
    
    # Based on ID format discovery: 
    # - Low numbers (1-999): afc2019048_0001, afc2019048_0002, etc. (4-digit padding)
    # - High numbers (1000+): afc2019048_1000, afc2019048_2621, etc. (no padding)
    # - Blocks start from 1 and go up to ~6100
    
    start_id = 1      # Start from block 1 with proper formatting
    end_id = 6100     # Cover full range including future blocks
    
    print(f"📊 Correct range: Block {start_id:,} to {end_id:,}")
    print("   (Uses proper ID formatting: 0001, 0002...0999, 1000, 1001...)")
    print("   (Covers all AIDS Memorial Quilt blocks from the beginning)")
    print("")
    
    print("🔍 Previous run used incorrect range (2001-7164) and wrong format")
    print("   Database shows we have 2621-4028 (properly formatted high numbers)")
    print("   But we're missing blocks 1-999 (need 4-digit padding)")
    print("   And blocks 1000-2620 (no padding needed)")
    print("")
    
    # Get user confirmation
    response = input("🚀 Ready to resume collection? (y/N): ").strip().lower()
    if response != 'y':
        print("❌ Collection cancelled by user")
        return
    
    print("\n🚀 Resuming collection with correct ID formatting...")
    
    settings = Settings()
    scraper = IntegratedScraper(settings)
    
    # Resume with the corrected range and formatting
    await scraper.scrape_collection(start_id=start_id, end_id=end_id)
    
    print("\n🎉 Collection completed!")
    print("📁 Images saved to: output/images/block_XXXX/")
    print("📄 Metadata saved to: output/metadata/block_XXXX/")
    print("💾 Database: quilt_records.db")

if __name__ == "__main__":
    asyncio.run(resume_collection())
