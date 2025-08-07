#!/usr/bin/env python3
"""
Monitor the progress of the unlimited scraper
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add src and config to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

from src.database import DatabaseManager
from config.settings import Settings


async def monitor_progress():
    """Monitor scraper progress"""
    settings = Settings()
    db_manager = DatabaseManager(settings.base_dir / "quilt_records.db")
    db_manager.initialize_database()
    
    try:
        stats = await db_manager.get_statistics()
        
        print(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Database Statistics:")
        print(f"   Total Records: {stats['total_records']}")
        print(f"   Records with Images: {stats['records_with_images']}")
        print(f"   Records without Images: {stats['records_without_images']}")
        print(f"   Recent Updates: {stats['recent_updates']}")
        
        # Get latest records
        from src.database import QuiltRecord
        from sqlalchemy.orm import sessionmaker
        
        Session = sessionmaker(bind=db_manager.engine)
        with Session() as session:
            latest_records = session.query(QuiltRecord).order_by(
                QuiltRecord.last_updated.desc()
            ).limit(3).all()
            
            if latest_records:
                print(f"\n📋 Latest Records:")
                for record in latest_records:
                    print(f"   {record.item_id}: {record.title[:50]}...")
                    print(f"      Updated: {record.last_updated}")
                
                # Show ID range
                first_record = session.query(QuiltRecord).order_by(
                    QuiltRecord.item_id.asc()
                ).first()
                last_record = session.query(QuiltRecord).order_by(
                    QuiltRecord.item_id.desc()
                ).first()
                
                if first_record and last_record:
                    print(f"\n🔢 ID Range: {first_record.item_id} → {last_record.item_id}")
        
    finally:
        db_manager.close()


if __name__ == "__main__":
    asyncio.run(monitor_progress())
