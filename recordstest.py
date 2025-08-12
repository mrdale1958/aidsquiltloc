#!/usr/bin/env python3
"""
Database diagnostic script for AIDS Memorial Quilt Records
Comprehensive diagnostic tool following project coding standards
Implements digital humanities research best practices with async/await patterns
"""

import asyncio
import sqlite3
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

# Configure structured logging per project guidelines
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - AIDS Memorial Quilt Diagnostic - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "config"))

# Import only what actually exists - following separation of concerns
try:
    from src.database import DatabaseManager
    from config.settings import ScraperConfig
    logger.info("AIDS Memorial Quilt Diagnostic: Successfully imported required modules")
except ImportError as e:
    logger.error(f"AIDS Memorial Quilt Diagnostic: Import error - {e}")
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the project root directory")
    print("Required modules: src.database.DatabaseManager, config.settings.ScraperConfig")
    sys.exit(1)

class AIDSQuiltDiagnostic:
    """
    Comprehensive diagnostic tool for AIDS Memorial Quilt Records database
    Implements error resilience and structured logging per project standards
    """
    
    def __init__(self):
        """Initialize diagnostic tool with proper configuration management"""
        self.config = ScraperConfig()
        self.db_manager: Optional[DatabaseManager] = None
        self.results: Dict[str, Any] = {}
        
    async def run_complete_diagnosis(self) -> Dict[str, Any]:
        """
        Execute comprehensive database and API diagnosis
        Implements async/await patterns for non-blocking operations
        
        Returns:
            Complete diagnostic results dictionary
        """
        logger.info("AIDS Memorial Quilt Diagnostic: Starting comprehensive diagnosis")
        
        try:
            # Step 1: Database file analysis
            await self._analyze_database_file()
            
            # Step 2: Database manager initialization
            await self._initialize_database_manager()
            
            # Step 3: Database content analysis
            await self._analyze_database_content()
            
            # Step 4: Database manager method testing
            await self._test_database_methods()
            
            # Step 5: API server connectivity testing
            await self._test_api_connectivity()
            
            # Step 6: Generate recommendations
            self._generate_recommendations()
            
            return self.results
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: Critical error during diagnosis: {e}")
            self.results["critical_error"] = str(e)
            return self.results
        finally:
            # Cleanup database connection following resource management best practices
            if self.db_manager:
                try:
                    await self.db_manager.close()
                    logger.info("AIDS Memorial Quilt Diagnostic: Database connection closed")
                except Exception as e:
                    logger.warning(f"AIDS Memorial Quilt Diagnostic: Error closing database: {e}")
    
    async def _analyze_database_file(self) -> None:
        """Analyze database file existence and basic properties"""
        logger.info("AIDS Memorial Quilt Diagnostic: Analyzing database file")
        
        try:
            db_path = self.config.database_path
            file_info = {
                "path": str(db_path),
                "exists": db_path.exists(),
                "size_bytes": 0,
                "size_mb": 0
            }
            
            if db_path.exists():
                size_bytes = db_path.stat().st_size
                file_info.update({
                    "size_bytes": size_bytes,
                    "size_mb": round(size_bytes / 1024 / 1024, 2),
                    "readable": db_path.is_file()
                })
                logger.info(f"AIDS Memorial Quilt Diagnostic: Database file found - {size_bytes:,} bytes")
            else:
                logger.warning("AIDS Memorial Quilt Diagnostic: Database file does not exist")
                
            self.results["database_file"] = file_info
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: Error analyzing database file: {e}")
            self.results["database_file"] = {"error": str(e)}
    
    async def _initialize_database_manager(self) -> None:
        """Initialize and test database manager following project error handling patterns"""
        logger.info("AIDS Memorial Quilt Diagnostic: Initializing database manager")
        
        try:
            self.db_manager = DatabaseManager(self.config.database_path)
            await self.db_manager.initialize()
            
            self.results["database_manager"] = {
                "initialized": True,
                "connection_established": self.db_manager.connection is not None
            }
            logger.info("AIDS Memorial Quilt Diagnostic: Database manager initialized successfully")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: Database manager initialization failed: {e}")
            self.results["database_manager"] = {
                "initialized": False,
                "error": str(e)
            }
    
    async def _analyze_database_content(self) -> None:
        """Analyze database content using raw SQLite queries for accuracy"""
        logger.info("AIDS Memorial Quilt Diagnostic: Analyzing database content")
        
        if not self.config.database_path.exists():
            self.results["database_content"] = {"error": "Database file does not exist"}
            return
        
        try:
            # Use raw SQLite connection for accurate table analysis
            conn = sqlite3.connect(str(self.config.database_path))
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            table_analysis = {}
            total_rows = 0
            
            for table_name in tables:
                try:
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]
                    total_rows += row_count
                    
                    # Get column info
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = [{"name": col[1], "type": col[2]} for col in cursor.fetchall()]
                    
                    # Get sample record if available
                    sample_record = None
                    if row_count > 0:
                        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                        sample_row = cursor.fetchone()
                        if sample_row:
                            column_names = [col["name"] for col in columns]
                            sample_record = dict(zip(column_names, sample_row))
                    
                    table_analysis[table_name] = {
                        "row_count": row_count,
                        "columns": columns,
                        "has_data": row_count > 0,
                        "sample_record": sample_record
                    }
                    
                    logger.info(f"AIDS Memorial Quilt Diagnostic: Table {table_name}: {row_count:,} rows")
                    
                except Exception as table_error:
                    logger.error(f"AIDS Memorial Quilt Diagnostic: Error analyzing table {table_name}: {table_error}")
                    table_analysis[table_name] = {"error": str(table_error)}
            
            conn.close()
            
            self.results["database_content"] = {
                "tables": table_analysis,
                "total_tables": len(tables),
                "total_rows_all_tables": total_rows,
                "has_any_data": total_rows > 0
            }
            
            logger.info(f"AIDS Memorial Quilt Diagnostic: Found {len(tables)} tables with {total_rows:,} total rows")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: Error analyzing database content: {e}")
            self.results["database_content"] = {"error": str(e)}
    
    async def _test_database_methods(self) -> None:
        """Test database manager methods for functionality verification"""
        logger.info("AIDS Memorial Quilt Diagnostic: Testing database manager methods")
        
        if not self.db_manager:
            self.results["database_methods"] = {"error": "Database manager not initialized"}
            return
        
        method_results = {}
        
        try:
            # Test get_total_records
            logger.info("AIDS Memorial Quilt Diagnostic: Testing get_total_records()")
            total_records = await self.db_manager.get_total_records()
            method_results["get_total_records"] = {
                "success": True,
                "result": total_records
            }
            logger.info(f"AIDS Memorial Quilt Diagnostic: get_total_records() returned {total_records:,}")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: get_total_records() failed: {e}")
            method_results["get_total_records"] = {
                "success": False,
                "error": str(e)
            }
        
        try:
            # Test get_database_stats
            logger.info("AIDS Memorial Quilt Diagnostic: Testing get_database_stats()")
            stats = await self.db_manager.get_database_stats()
            method_results["get_database_stats"] = {
                "success": True,
                "result": stats
            }
            logger.info(f"AIDS Memorial Quilt Diagnostic: get_database_stats() completed successfully")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: get_database_stats() failed: {e}")
            method_results["get_database_stats"] = {
                "success": False,
                "error": str(e)
            }
        
        try:
            # Test get_records
            logger.info("AIDS Memorial Quilt Diagnostic: Testing get_records(limit=3)")
            records = await self.db_manager.get_records(limit=3)
            method_results["get_records"] = {
                "success": True,
                "records_returned": len(records),
                "sample_record": records[0] if records else None
            }
            logger.info(f"AIDS Memorial Quilt Diagnostic: get_records() returned {len(records)} records")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: get_records() failed: {e}")
            method_results["get_records"] = {
                "success": False,
                "error": str(e)
            }
        
        try:
            # Test diagnose_data_availability
            logger.info("AIDS Memorial Quilt Diagnostic: Testing diagnose_data_availability()")
            diagnosis = await self.db_manager.diagnose_data_availability()
            method_results["diagnose_data_availability"] = {
                "success": True,
                "result": diagnosis
            }
            logger.info("AIDS Memorial Quilt Diagnostic: diagnose_data_availability() completed successfully")
            
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: diagnose_data_availability() failed: {e}")
            method_results["diagnose_data_availability"] = {
                "success": False,
                "error": str(e)
            }
        
        self.results["database_methods"] = method_results
    
    async def _test_api_connectivity(self) -> None:
        """Test API server connectivity using aiohttp following project async patterns"""
        logger.info("AIDS Memorial Quilt Diagnostic: Testing API server connectivity")
        
        try:
            import aiohttp
            
            base_url = "http://127.0.0.1:8000"
            timeout = aiohttp.ClientTimeout(total=10)
            api_results = {}
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Test health endpoint
                try:
                    async with session.get(f"{base_url}/health") as response:
                        if response.status == 200:
                            data = await response.json()
                            api_results["health"] = {
                                "success": True,
                                "status": data.get("status"),
                                "database_connected": data.get("database", {}).get("connected", False)
                            }
                            logger.info(f"AIDS Memorial Quilt Diagnostic: Health endpoint successful - {data.get('status')}")
                        else:
                            api_results["health"] = {
                                "success": False,
                                "http_status": response.status
                            }
                except Exception as e:
                    api_results["health"] = {
                        "success": False,
                        "error": str(e)
                    }
                
                # Test records endpoint (critical test)
                try:
                    async with session.get(f"{base_url}/records?page=1&page_size=3") as response:
                        if response.status == 200:
                            data = await response.json()
                            records = data.get("records", [])
                            api_results["records"] = {
                                "success": True,
                                "records_returned": len(records),
                                "total_available": data.get("total", 0),
                                "has_records": len(records) > 0
                            }
                            logger.info(f"AIDS Memorial Quilt Diagnostic: Records endpoint returned {len(records)} records")
                        else:
                            api_results["records"] = {
                                "success": False,
                                "http_status": response.status
                            }
                except Exception as e:
                    api_results["records"] = {
                        "success": False,
                        "error": str(e)
                    }
                
                # Test debug endpoint
                try:
                    async with session.get(f"{base_url}/debug") as response:
                        if response.status == 200:
                            data = await response.json()
                            api_results["debug"] = {
                                "success": True,
                                "database_exists": data.get("database_exists", False),
                                "test_retrieval": data.get("test_record_retrieval", {})
                            }
                            logger.info("AIDS Memorial Quilt Diagnostic: Debug endpoint successful")
                        else:
                            api_results["debug"] = {
                                "success": False,
                                "http_status": response.status
                            }
                except Exception as e:
                    api_results["debug"] = {
                        "success": False,
                        "error": str(e)
                    }
            
            self.results["api_connectivity"] = {
                "server_reachable": True,
                "endpoints": api_results
            }
            
        except ImportError:
            logger.warning("AIDS Memorial Quilt Diagnostic: aiohttp not available for API testing")
            self.results["api_connectivity"] = {
                "server_reachable": False,
                "error": "aiohttp not available - install with: pip install aiohttp"
            }
        except Exception as e:
            logger.error(f"AIDS Memorial Quilt Diagnostic: API connectivity test failed: {e}")
            self.results["api_connectivity"] = {
                "server_reachable": False,
                "error": str(e)
            }
    
    def _generate_recommendations(self) -> None:
        """Generate actionable recommendations based on diagnostic results"""
        logger.info("AIDS Memorial Quilt Diagnostic: Generating recommendations")
        
        recommendations: List[str] = []
        
        # Database file recommendations
        db_file = self.results.get("database_file", {})
        if not db_file.get("exists", False):
            recommendations.append("Database file does not exist - run 'python -m src.main' to create and populate database")
        elif db_file.get("size_bytes", 0) == 0:
            recommendations.append("Database file is empty - run data scraping to populate with AIDS Memorial Quilt records")
        
        # Database content recommendations  
        db_content = self.results.get("database_content", {})
        if not db_content.get("has_any_data", False):
            recommendations.append("Database contains no data - execute scraper to collect AIDS Memorial Quilt records from Library of Congress")
        
        # Database methods recommendations
        db_methods = self.results.get("database_methods", {})
        get_records_result = db_methods.get("get_records", {})
        if get_records_result.get("success", False) and get_records_result.get("records_returned", 0) == 0:
            recommendations.append("Database manager returns no records - check data source configuration and primary table selection")
        
        # API connectivity recommendations
        api_connectivity = self.results.get("api_connectivity", {})
        if not api_connectivity.get("server_reachable", False):
            recommendations.append("API server not reachable - start server with 'python api_server.py'")
        else:
            records_endpoint = api_connectivity.get("endpoints", {}).get("records", {})
            if records_endpoint.get("success", False) and not records_endpoint.get("has_records", False):
                recommendations.append("API server running but returns no records - check database manager integration")
        
        # Dashboard recommendations
        if (api_connectivity.get("server_reachable", False) and 
            api_connectivity.get("endpoints", {}).get("records", {}).get("has_records", False)):
            recommendations.append("API returning records successfully - check React dashboard frontend for display issues")
        
        self.results["recommendations"] = recommendations
        
        for i, rec in enumerate(recommendations, 1):
            logger.info(f"AIDS Memorial Quilt Diagnostic: Recommendation {i}: {rec}")

async def main() -> None:
    """
    Main diagnostic execution function
    Implements comprehensive error handling and structured output
    """
    print("🚀 AIDS Memorial Quilt Records - Comprehensive Diagnostic Tool")
    print("=" * 65)
    print("Following digital humanities research standards with async/await patterns")
    print("Implementing comprehensive error handling and structured logging\n")
    
    # Initialize and run diagnostic
    diagnostic = AIDSQuiltDiagnostic()
    results = await diagnostic.run_complete_diagnosis()
    
    # Display results in structured format
    print("\n📋 DIAGNOSTIC RESULTS SUMMARY")
    print("=" * 35)
    
    # Database file status
    db_file = results.get("database_file", {})
    if db_file.get("exists", False):
        print(f"✅ Database file: {db_file['path']} ({db_file.get('size_mb', 0)} MB)")
    else:
        print(f"❌ Database file: Not found at {db_file.get('path', 'unknown')}")
    
    # Database content status
    db_content = results.get("database_content", {})
    if db_content.get("has_any_data", False):
        total_rows = db_content.get("total_rows_all_tables", 0)
        total_tables = db_content.get("total_tables", 0)
        print(f"✅ Database content: {total_rows:,} rows across {total_tables} tables")
        
        # Show table breakdown
        tables = db_content.get("tables", {})
        for table_name, table_info in tables.items():
            if table_info.get("has_data", False):
                row_count = table_info.get("row_count", 0)
                print(f"   📊 {table_name}: {row_count:,} rows")
    else:
        print("❌ Database content: No data found")
    
    # Database manager status
    db_manager = results.get("database_manager", {})
    if db_manager.get("initialized", False):
        print("✅ Database manager: Initialized successfully")
        
        # Show method test results
        db_methods = results.get("database_methods", {})
        for method_name, method_result in db_methods.items():
            if method_result.get("success", False):
                if method_name == "get_records":
                    records_count = method_result.get("records_returned", 0)
                    print(f"   ✅ {method_name}: {records_count} records returned")
                elif method_name == "get_total_records":
                    total = method_result.get("result", 0)
                    print(f"   ✅ {method_name}: {total:,} total records")
                else:
                    print(f"   ✅ {method_name}: Success")
            else:
                print(f"   ❌ {method_name}: {method_result.get('error', 'Failed')}")
    else:
        print(f"❌ Database manager: {db_manager.get('error', 'Failed to initialize')}")
    
    # API server status
    api_connectivity = results.get("api_connectivity", {})
    if api_connectivity.get("server_reachable", False):
        print("✅ API server: Reachable")
        
        endpoints = api_connectivity.get("endpoints", {})
        for endpoint_name, endpoint_result in endpoints.items():
            if endpoint_result.get("success", False):
                if endpoint_name == "records":
                    records_count = endpoint_result.get("records_returned", 0)
                    total_available = endpoint_result.get("total_available", 0)
                    print(f"   ✅ /{endpoint_name}: {records_count} records returned (of {total_available:,} total)")
                else:
                    print(f"   ✅ /{endpoint_name}: Success")
            else:
                print(f"   ❌ /{endpoint_name}: {endpoint_result.get('error', 'Failed')}")
    else:
        print(f"❌ API server: {api_connectivity.get('error', 'Not reachable')}")
    
    # Recommendations
    recommendations = results.get("recommendations", [])
    if recommendations:
        print(f"\n🎯 ACTIONABLE RECOMMENDATIONS")
        print("=" * 30)
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
    
    # Save results for further analysis
    results_file = Path("aids_quilt_diagnostic_results.json")
    try:
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n📁 Detailed results saved to: {results_file}")
    except Exception as e:
        print(f"⚠️  Could not save diagnostic results: {e}")
    
    print(f"\n🔍 For detailed logs, check the console output above.")
    print("💡 If issues persist, check individual component logs and error messages.")

if __name__ == "__main__":
    # Run the comprehensive diagnostic tool
    asyncio.run(main())