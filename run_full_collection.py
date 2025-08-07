#!/usr/bin/env python3
"""
Production AIDS Memorial Quilt Scraper
Ready for full collection with block-organized directories
"""

import asyncio
from integrated_scraper import IntegratedScraper
from config.settings import Settings

async def run_full_collection():
    """Run the full AIDS Memorial Quilt collection scraper"""
    
    print("🏳️‍🌈 AIDS Memorial Quilt - FULL COLLECTION SCRAPER")
    print("📊 This will collect the entire LOC AIDS Memorial Quilt collection")
    print("🔄 Features:")
    print("   • Block-organized directories (output/images/block_XXXX/)")
    print("   • Parallel metadata + image downloading")
    print("   • Adaptive rate limiting (starts at 2s, increases if rate limited)")
    print("   • Smart deduplication (skips existing items)")
    print("   • Real-time progress tracking")
    print("   • Comprehensive error handling")
    print("")
    
    # Get user confirmation
    response = input("🚀 Ready to start full collection? (y/N): ").strip().lower()
    if response != 'y':
        print("❌ Collection cancelled by user")
        return
    
    print("\n🚀 Starting full collection...")
    
    settings = Settings()
    scraper = IntegratedScraper(settings)
    
    # Run the full collection (IDs 1-6100 with proper formatting)
    await scraper.scrape_collection(start_id=1, end_id=6100)
    
    print("\n🎉 Full collection completed!")
    print("📁 Images saved to: output/images/block_XXXX/")
    print("📄 Metadata saved to: output/metadata/block_XXXX/")
    print("💾 Database: quilt_records.db")

if __name__ == "__main__":
    asyncio.run(run_full_collection())
