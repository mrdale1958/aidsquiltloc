#!/usr/bin/env python3
"""
Import verification and cleanup script for AIDS Memorial Quilt Records
Identifies and resolves import conflicts following project coding standards
Implements comprehensive error handling with specific exception types
"""

import sys
import importlib
from pathlib import Path
from typing import List, Optional

# Add project paths following separation of concerns
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "config"))

def verify_import_structure() -> None:
    """
    Verify and diagnose import structure for AIDS Memorial Quilt project
    Implements systematic debugging following project error handling guidelines
    """
    print("🔍 AIDS Memorial Quilt Import Verification")
    print("=" * 45)
    
    # Check if required modules exist
    required_files = [
        project_root / "src" / "database.py",
        project_root / "config" / "settings.py"
    ]
    
    for file_path in required_files:
        if file_path.exists():
            print(f"✅ Found: {file_path}")
        else:
            print(f"❌ Missing: {file_path}")
            return
    
    # Clear any cached imports
    modules_to_clear = [
        'src.database',
        'config.settings',
        'database',
        'settings'
    ]
    
    for module_name in modules_to_clear:
        if module_name in sys.modules:
            del sys.modules[module_name]
            print(f"🧹 Cleared cached module: {module_name}")
    
    # Test individual imports with detailed error reporting
    try:
        print("\n🔧 Testing DatabaseManager import...")
        from src.database import DatabaseManager
        print("✅ Successfully imported DatabaseManager")
        
        # Check what's actually in the database module
        import src.database as db_module
        available_items = [item for item in dir(db_module) if not item.startswith('_')]
        print(f"📋 Available items in src.database: {available_items}")
        
        # Check for problematic imports
        problematic_items = ['QuiltBlock', 'QuiltPanel']
        for item in problematic_items:
            if hasattr(db_module, item):
                print(f"⚠️  Found unexpected item: {item}")
            else:
                print(f"✅ Confirmed {item} not in database module")
        
    except ImportError as e:
        print(f"❌ DatabaseManager import failed: {e}")
        return
    except Exception as e:
        print(f"❌ Unexpected error importing DatabaseManager: {e}")
        return
    
    try:
        print("\n🔧 Testing ScraperConfig import...")
        from config.settings import ScraperConfig
        print("✅ Successfully imported ScraperConfig")
        
    except ImportError as e:
        print(f"❌ ScraperConfig import failed: {e}")
        return
    except Exception as e:
        print(f"❌ Unexpected error importing ScraperConfig: {e}")
        return
    
    print("\n✅ All imports verified successfully!")
    print("🎯 Ready to run API server")

def clean_python_cache() -> None:
    """
    Remove Python cache files that might contain stale imports
    Implements comprehensive cleanup following project standards
    """
    print("\n🧹 Cleaning Python Cache Files")
    print("=" * 32)
    
    cache_patterns = [
        "**/__pycache__",
        "**/*.pyc", 
        "**/*.pyo"
    ]
    
    cleaned_count = 0
    for pattern in cache_patterns:
        for cache_file in project_root.glob(pattern):
            try:
                if cache_file.is_file():
                    cache_file.unlink()
                    cleaned_count += 1
                elif cache_file.is_dir():
                    import shutil
                    shutil.rmtree(cache_file)
                    cleaned_count += 1
            except Exception as e:
                print(f"⚠️  Could not remove {cache_file}: {e}")
    
    print(f"✅ Cleaned {cleaned_count} cache files/directories")

if __name__ == "__main__":
    clean_python_cache()
    verify_import_structure()
