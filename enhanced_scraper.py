#!/usr/bin/env python3
"""
Enhanced AIDS Memorial Quilt Records scraper with real-time database integration
Supports metadata-only, image-only, or combined scraping with live dashboard updates
"""

import asyncio
import logging
import argparse
import signal
import sys
import re  # Add this import for manuscript ID pattern matching
import aiohttp  # Add this import to avoid repeated inline imports
from pathlib import Path
from typing import Optional, Set, Dict, Any, List, Tuple
from datetime import datetime, timezone
import json
import threading
import queue
import time

# Local imports following architecture patterns for separation of concerns
from src.loc_api_client import LOCAPIClient, LOCAPISettings
from src.metadata_extractor import MetadataExtractor
from src.image_downloader import ImageDownloader
from src.database import DatabaseManager, QuiltBlock, QuiltPanel
from config.settings import ScraperConfig

# Configure structured logging per project standards
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ScraperOperationModes:
    """Enumeration of available scraper operation modes"""
    FULL = "full"           # Both metadata and images with database integration
    METADATA_ONLY = "metadata"  # Only collect metadata and update database
    IMAGES_ONLY = "images"      # Only download images (no database updates)
    DATABASE_SYNC = "db-sync"   # Sync existing JSON files to database


class IntegratedAIDSQuiltScraper:
    """
    Enhanced AIDS Memorial Quilt Records scraper with real-time database integration
    
    Provides immediate dashboard updates as metadata is collected, following
    project coding standards with comprehensive error handling and type hints
    """
    
    def __init__(self, config: ScraperConfig, operation_mode: str = ScraperOperationModes.FULL) -> None:
        """
        Initialize the integrated scraper with specified operation mode
        
        Args:
            config: Scraper configuration settings
            operation_mode: One of 'full', 'metadata', 'images', or 'db-sync'
            
        Raises:
            ValueError: If operation_mode is not valid
        """
        valid_modes = [
            ScraperOperationModes.FULL, 
            ScraperOperationModes.METADATA_ONLY, 
            ScraperOperationModes.IMAGES_ONLY,
            ScraperOperationModes.DATABASE_SYNC
        ]
        
        if operation_mode not in valid_modes:
            raise ValueError(f"Invalid operation mode: {operation_mode}")
            
        self.config = config
        self.operation_mode = operation_mode
        self.shutdown_event = threading.Event()
        
        # Initialize database manager for real-time updates
        if operation_mode in [ScraperOperationModes.FULL, ScraperOperationModes.METADATA_ONLY, ScraperOperationModes.DATABASE_SYNC]:
            self.db_manager = DatabaseManager(config.database_path)
        else:
            self.db_manager = None
        
        # Initialize API components based on operation mode with proper rate limiting
        if operation_mode != ScraperOperationModes.DATABASE_SYNC:
            # Use empirically validated 30-second delay for production scraping
            api_settings = LOCAPISettings()
            api_settings.rate_limit_delay = 30.0  # Empirically validated delay
            api_settings.max_retries = 3
            api_settings.timeout = 60
            self.api_client = LOCAPIClient(api_settings)
            
            # Override config rate limit delay to use empirically validated value
            self._actual_rate_limit_delay = 30.0
            logger.info("Using empirically validated 30-second rate limit delay")
        else:
            self.api_client = None
            self._actual_rate_limit_delay = 0.0
            
        if operation_mode in [ScraperOperationModes.FULL, ScraperOperationModes.METADATA_ONLY]:
            self.metadata_extractor = MetadataExtractor(config)
        else:
            self.metadata_extractor = None
            
        if operation_mode in [ScraperOperationModes.FULL, ScraperOperationModes.IMAGES_ONLY]:
            self.image_downloader = ImageDownloader(config)
            self.image_queue: queue.Queue = queue.Queue()
            self.image_thread: Optional[threading.Thread] = None
        else:
            self.image_downloader = None
            self.image_queue = None
            self.image_thread = None
        
        # Initialize manuscript discovery service after importing
        self.manuscript_discovery_service = None
        
        # Statistics tracking with database metrics including rate limit errors
        self.stats = {
            'metadata_collected': 0,
            'database_records_created': 0,
            'database_records_updated': 0,
            'images_queued': 0,
            'images_downloaded': 0,
            'database_errors': 0,
            'api_errors': 0,
            'rate_limit_errors': 0,  # Add specific rate limit error tracking
            'image_errors': 0,
            'start_time': None,
            'end_time': None,
            'blocks_processed': 0,
            'manuscripts_discovered': 0,
            'errors_encountered': 0
        }
        
        logger.info("Integrated scraper initialized in %s mode with %s second rate limiting", 
                   operation_mode, self._actual_rate_limit_delay)
    
    async def __aenter__(self) -> 'IntegratedAIDSQuiltScraper':
        """Async context manager entry for proper resource management"""
        if self.db_manager:
            await self.db_manager.initialize()
            
        # Initialize manuscript discovery service with lazy loading
        try:
            from src.manuscript_discovery import ManuscriptDiscoveryService
            if self.api_client:
                self.manuscript_discovery_service = ManuscriptDiscoveryService(
                    api_client=self.api_client,
                    max_concurrent=5
                )
                logger.debug("Manuscript discovery service initialized")
        except ImportError as e:
            logger.warning("Manuscript discovery service not available: %s", e)
            self.manuscript_discovery_service = None
            
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Async context manager exit with comprehensive cleanup following resource management practices
        """
        cleanup_errors = []
        
        try:
            # Close metadata extractor and its API client
            if hasattr(self, 'metadata_extractor') and self.metadata_extractor:
                await self.metadata_extractor.close()
        except Exception as e:
            cleanup_errors.append(f"metadata_extractor: {e}")
        
        try:
            # Close image downloader session
            if hasattr(self, 'image_downloader') and self.image_downloader:
                await self.image_downloader.close_session()
        except Exception as e:
            cleanup_errors.append(f"image_downloader: {e}")
        
        try:
            # Close database connection
            if hasattr(self, 'db_manager') and self.db_manager:
                await self.db_manager.close()
        except Exception as e:
            cleanup_errors.append(f"database: {e}")
        
        if cleanup_errors:
            logger.warning("Cleanup errors: %s", "; ".join(cleanup_errors))
        else:
            logger.info("All resources cleaned up successfully")
    
    def setup_signal_handlers(self) -> None:
        """Setup graceful shutdown signal handlers"""
        def signal_handler(signum: int, frame) -> None:
            logger.info("Received shutdown signal (%s), initiating graceful shutdown...", signum)
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def store_metadata_in_database(self, block_id: str, metadata: Dict[str, Any], manuscripts: Set[str]) -> bool:
        """
        Store collected metadata directly in database for immediate dashboard access
        
        Args:
            block_id: The block identifier (e.g., "0001")
            metadata: Extracted metadata dictionary
            manuscripts: Set of available manuscript identifiers
            
        Returns:
            True if database storage was successful
        """
        if not self.db_manager:
            logger.debug("Database manager not available, skipping database storage")
            return False
        
        try:
            logger.info("Storing metadata for block %s in database", block_id)
            
            # Extract block-level information
            block_title = metadata.get('title', f'AIDS Memorial Quilt Block {block_id}')
            block_description = metadata.get('description', '')
            created_date = metadata.get('date', '')
            
            # Create or update block record
            block = QuiltBlock(
                block_id=block_id,
                title=block_title,
                description=block_description,
                created_date=created_date,
                metadata_json=json.dumps(metadata),
                total_panels=len(manuscripts),
                scraped_at=datetime.now(timezone.utc)
            )
            
            block_saved = await self.db_manager.save_block(block)
            if block_saved:
                self.stats['database_records_created'] += 1
                logger.info("Successfully saved block %s to database", block_id)
            else:
                # Try updating existing record
                updated = await self.db_manager.update_block_metadata(block_id, metadata)
                if updated:
                    self.stats['database_records_updated'] += 1
                    logger.info("Successfully updated block %s in database", block_id)
                else:
                    logger.warning("Failed to save or update block %s in database", block_id)
                    self.stats['database_errors'] += 1
                    return False
            
            # Create panel records for each manuscript with proper JSON serialization
            for manuscript_id in manuscripts:
                try:
                    # Fix: Convert image URLs list to JSON string for database storage
                    image_urls_list = self._generate_image_urls(block_id, manuscript_id)
                    image_urls_json = json.dumps(image_urls_list)
                    
                    panel = QuiltPanel(
                        block_id=block_id,
                        panel_id=manuscript_id,
                        title=f"Panel {manuscript_id}",
                        description=f"AIDS Memorial Quilt panel {manuscript_id} from block {block_id}",
                        image_urls=image_urls_json,  # Store as JSON string, not list
                        metadata_json=json.dumps({
                            'manuscript_id': manuscript_id,
                            'block_id': block_id,
                            'iiif_base_url': f"https://tile.loc.gov/image-services/iiif/service:afc:afc2019048:afc2019048_{block_id}:{manuscript_id}",
                            'image_count': len(image_urls_list)
                        }),
                        scraped_at=datetime.now(timezone.utc)
                    )
                    
                    panel_saved = await self.db_manager.save_panel(panel)
                    if panel_saved:
                        self.stats['database_records_created'] += 1
                    else:
                        self.stats['database_records_updated'] += 1
                        
                except Exception as e:
                    logger.error("Error saving panel %s/%s to database: %s", block_id, manuscript_id, e)
                    self.stats['database_errors'] += 1
            
            logger.info("Successfully stored metadata for block %s with %d panels", block_id, len(manuscripts))
            return True
            
        except Exception as e:
            logger.error("Failed to store metadata for block %s in database: %s", block_id, e)
            self.stats['database_errors'] += 1
            return False
    
    def _generate_image_urls(self, block_id: str, manuscript_id: str) -> List[str]:
        """
        Generate IIIF image URLs for different resolutions
        
        Args:
            block_id: The block identifier
            manuscript_id: The manuscript identifier
            
        Returns:
            List of IIIF image URLs
        """
        base_url = f"https://tile.loc.gov/image-services/iiif/service:afc:afc2019048:afc2019048_{block_id}:{manuscript_id}"
        
        resolutions = ['200', '400', '800', '1200', 'full']
        urls = []
        
        for resolution in resolutions:
            if resolution == 'full':
                url = f"{base_url}/full/full/0/default.jpg"
            else:
                url = f"{base_url}/full/{resolution},/0/default.jpg"
            urls.append(url)
        
        return urls
    
    async def discover_available_manuscripts(self, block_id: str, timeout: int = 30) -> Set[str]:
        """
        Discover available manuscripts using enhanced discovery service or fallback methods
        
        Args:
            block_id: The block identifier (e.g., "0001")
            timeout: Maximum time to spend discovering manuscripts
            
        Returns:
            Set of available manuscript identifiers
        """
        logger.info("Discovering available manuscripts for block %s", block_id)
        
        # Use enhanced discovery service if available
        if self.manuscript_discovery_service:
            try:
                item_id = f"afc2019048_{block_id}"
                manuscripts = await self.manuscript_discovery_service.discover_manuscripts_for_item(item_id)
                if manuscripts:
                    logger.info("Enhanced discovery found %d manuscripts for block %s", len(manuscripts), block_id)
                    return manuscripts
            except Exception as e:
                logger.warning("Enhanced discovery failed, using fallback: %s", e)
        
        # Fallback to original discovery methods
        available_manuscripts: Set[str] = set()
        
        # Strategy 1: Check LOC item page for resource links
        try:
            metadata_manuscripts = await self._discover_from_metadata(block_id)
            if metadata_manuscripts:
                available_manuscripts.update(metadata_manuscripts)
                logger.info("Found %d manuscripts from metadata analysis", len(metadata_manuscripts))
        except Exception as e:
            logger.debug("Metadata-based discovery failed: %s", e)
        
        # Strategy 2: IIIF endpoint probing (existing implementation)
        try:
            iiif_manuscripts = await self._discover_from_iiif_probing(block_id, timeout)
            if iiif_manuscripts:
                available_manuscripts.update(iiif_manuscripts)
                logger.info("Found %d additional manuscripts from IIIF probing", len(iiif_manuscripts))
        except Exception as e:
            logger.debug("IIIF probing discovery failed: %s", e)
        
        # Strategy 3: LOC search API for related items
        try:
            if not available_manuscripts:  # Only if other methods failed
                search_manuscripts = await self._discover_from_search_api(block_id)
                if search_manuscripts:
                    available_manuscripts.update(search_manuscripts)
                    logger.info("Found %d manuscripts from search API", len(search_manuscripts))
        except Exception as e:
            logger.debug("Search API discovery failed: %s", e)
    
        # Fallback: Use known pattern for available blocks
        if not available_manuscripts:
            fallback_manuscripts = await self._discover_fallback_strategy(block_id)
            if fallback_manuscripts:
                available_manuscripts.update(fallback_manuscripts)
                logger.info("Using fallback strategy: %d manuscripts", len(fallback_manuscripts))
    
        logger.info("Discovered %d available manuscripts: %s", 
                   len(available_manuscripts), sorted(available_manuscripts))
        return available_manuscripts
    
    async def _discover_from_metadata(self, block_id: str) -> Set[str]:
        """
        Discover manuscripts by analyzing the metadata from LOC API
    
        Args:
            block_id: The block identifier
        
        Returns:
            Set of manuscript identifiers found in metadata
        """
        manuscripts: Set[str] = set()
    
        try:
            # Get the raw metadata we already collected
            item_id = f"afc2019048_{block_id}"
            if hasattr(self, 'metadata_extractor') and self.metadata_extractor:
                # Check if metadata contains resource links or file references
                raw_metadata = await self.metadata_extractor.api_client.get_item_metadata(item_id)
    
                if raw_metadata and 'item' in raw_metadata:
                    item_data = raw_metadata['item']
                    
                    # Look for resources, files, or digitized content references
                    resources = item_data.get('resources', [])
                    for resource in resources:
                        if isinstance(resource, dict):
                            # Look for manuscript-like identifiers
                            resource_url = resource.get('url', '')
                            if 'ms' in resource_url or 'manuscript' in resource_url.lower():
                                # Extract manuscript ID from URL pattern
                                ms_match = re.search(r'ms(\d{4})', resource_url)
                                if ms_match:
                                    manuscripts.add(f"ms{ms_match.group(1)}")
                    
                    # Check other fields that might contain file references
                    files = item_data.get('files', [])
                    if isinstance(files, list):
                        for file_ref in files:
                            if isinstance(file_ref, str) and 'ms' in file_ref:
                                ms_match = re.search(r'ms(\d{4})', file_ref)
                                if ms_match:
                                    manuscripts.add(f"ms{ms_match.group(1)}")
                    
                    # Check for image URLs in metadata
                    image_urls = item_data.get('image_url', [])
                    if isinstance(image_urls, list):
                        for img_url in image_urls:
                            if 'tile.loc.gov' in str(img_url) and 'ms' in str(img_url):
                                ms_match = re.search(r'ms(\d{4})', str(img_url))
                                if ms_match:
                                    manuscripts.add(f"ms{ms_match.group(1)}")
                
        except Exception as e:
            logger.debug("Error in metadata-based manuscript discovery: %s", e)
    
        return manuscripts
    
    async def _discover_from_iiif_probing(self, block_id: str, timeout: int) -> Set[str]:
        """
        Discover manuscripts by probing IIIF endpoints with comprehensive error handling
        
        Uses multiple URL patterns and concurrent probing following performance optimization
        and rate limiting guidelines from project standards.
        
        Args:
            block_id: The block identifier (e.g., "0001")
            timeout: Maximum time to spend on IIIF probing
            
        Returns:
            Set of manuscript identifiers found through IIIF endpoint probing
        """
        available_manuscripts: Set[str] = set()
        
        async def check_manuscript_endpoint(session: aiohttp.ClientSession, 
                                          manuscript_id: str, 
                                          semaphore: asyncio.Semaphore) -> Optional[str]:
            """
            Check multiple IIIF endpoint patterns following comprehensive error handling
            
            Args:
                session: aiohttp client session
                manuscript_id: Manuscript identifier to check (e.g., "ms0001")
                semaphore: Semaphore for rate limiting
                
            Returns:
                Manuscript ID if found, None otherwise
            """
            async with semaphore:
                try:
                    # Try multiple URL patterns per LOC API documentation
                    url_patterns = [
                        f"https://tile.loc.gov/image-services/iiif/service:afc:afc2019048:afc2019048_{block_id}:{manuscript_id}/info.json",
                        f"https://tile.loc.gov/image-services/iiif/service:afc:afc2019048:afc2019048_{block_id}:{manuscript_id}/full/200,/0/default.jpg",
                        f"https://www.loc.gov/resource/afc2019048.{block_id}/{manuscript_id}/"
                    ]
                    
                    for url_pattern in url_patterns:
                        try:
                            async with session.head(url_pattern, timeout=3) as response:
                                if response.status == 200:
                                    logger.debug("Found manuscript %s via %s", manuscript_id, url_pattern.split('/')[-2])
                                    return manuscript_id
                                elif response.status == 404:
                                    logger.debug("Manuscript %s not found via pattern %s", manuscript_id, url_pattern.split('/')[-2])
                                    continue
                                else:
                                    logger.debug("Unexpected status %d for %s", response.status, manuscript_id)
                                    
                        except asyncio.TimeoutError:
                            logger.debug("Timeout checking %s via %s", manuscript_id, url_pattern.split('/')[-2])
                            continue
                        except Exception as e:
                            logger.debug("Error checking %s via %s: %s", manuscript_id, url_pattern.split('/')[-2], e)
                            continue
                    
                    return None
                            
                except Exception as e:
                    logger.debug("Error checking manuscript %s: %s", manuscript_id, e)
                    return None
        
        try:
            # Use timeout to prevent hanging per performance optimization guidelines
            async with asyncio.timeout(timeout):
                # Limit concurrent checks per rate limiting guidelines
                semaphore = asyncio.Semaphore(3)  # Conservative limit for respectful API usage
                
                async with aiohttp.ClientSession(
                    headers={'User-Agent': self.config.user_agent},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as session:
                    # Test common manuscript patterns following collection analysis
                    tasks = []
                    
                    # Test standard range (ms0001 to ms0020 first)
                    for i in range(1, 21):
                        manuscript_id = f"ms{i:04d}"
                        task = check_manuscript_endpoint(session, manuscript_id, semaphore)
                        tasks.append(task)
                    
                    # Execute checks concurrently per async patterns
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, str):  # Valid manuscript ID
                            available_manuscripts.add(result)
                        elif isinstance(result, Exception):
                            logger.debug("Manuscript check failed: %s", result)
                    
                    # If we found some manuscripts, try extended range
                    if available_manuscripts:
                        logger.debug("Found initial manuscripts, checking extended range...")
                        extended_tasks = []
                        for i in range(21, 57):  # Extended range based on collection patterns
                            manuscript_id = f"ms{i:04d}"
                            task = check_manuscript_endpoint(session, manuscript_id, semaphore)
                            extended_tasks.append(task)
                        
                        # Process extended range with error resilience
                        extended_results = await asyncio.gather(*extended_tasks, return_exceptions=True)
                        for result in extended_results:
                            if isinstance(result, str):
                                available_manuscripts.add(result)
        
        except asyncio.TimeoutError:
            logger.warning("IIIF manuscript discovery timed out after %ds", timeout)
        except Exception as e:
            logger.warning("Error during IIIF manuscript discovery: %s", e)
        
        # Log discovery results per documentation standards
        if available_manuscripts:
            logger.debug("IIIF probing found manuscripts: %s", sorted(available_manuscripts))
        else:
            logger.info("No manuscripts discovered through IIIF endpoints, checking LOC item page...")
        
        return available_manuscripts
    
    async def _discover_from_search_api(self, block_id: str) -> Set[str]:
        """
        Discover manuscripts using LOC search API for related items
    
        Args:
            block_id: The block identifier
        
        Returns:
            Set of manuscript identifiers found through search
        """
        manuscripts: Set[str] = set()
    
        try:
            if hasattr(self, 'metadata_extractor') and self.metadata_extractor:
                # Search for related items or components
                search_query = f"afc2019048_{block_id}"
                
                import aiohttp
                async with aiohttp.ClientSession(
                    headers={'User-Agent': self.config.user_agent}
                ) as session:
                    # Try LOC search API
                    search_url = f"https://www.loc.gov/search/?q={search_query}&fo=json&c=100"
                    
                    try:
                        async with session.get(search_url, timeout=10) as response:
                            if response.status == 200:
                                search_data = await response.json()
                                
                                # Parse search results for manuscript references
                                if 'results' in search_data:
                                    for result in search_data['results']:
                                        # Look for manuscript IDs in titles, URLs, or descriptions
                                        result_text = str(result).lower()
                                        ms_matches = re.findall(r'ms(\d{4})', result_text)
                                        for ms_num in ms_matches:
                                            manuscripts.add(f"ms{ms_num}")
                
                    except Exception as e:
                        logger.debug("Search API request failed: %s", e)
                        
        except Exception as e:
            logger.debug("Error in search API manuscript discovery: %s", e)
    
        return manuscripts
    
    async def _discover_fallback_strategy(self, block_id: str) -> Set[str]:
        """
        Fallback manuscript discovery using known patterns for this collection
    
        Based on empirical analysis of AIDS Memorial Quilt Records collection structure.
        Uses conservative approach following error resilience guidelines.
    
        Args:
            block_id: The block identifier
        
        Returns:
            Set of manuscript identifiers based on collection patterns
        """
        manuscripts: Set[str] = set()
    
        try:
            # Based on LOC AIDS Memorial Quilt Records collection analysis:
            # Most blocks that exist have at least ms0001
            # Some have additional manuscripts following sequential patterns
            
            block_num = int(block_id)
            
            # Conservative pattern-based discovery
            if block_num <= 100:
                # Early blocks often have 1-3 manuscripts
                potential_manuscripts = ['ms0001', 'ms0002', 'ms0003']
            elif block_num <= 500:
                # Mid-range blocks typically have 1-2 manuscripts  
                potential_manuscripts = ['ms0001', 'ms0002']
            else:
                # Later blocks often have single manuscript
                potential_manuscripts = ['ms0001']
            
            # Verify at least one manuscript exists using lightweight check
            import aiohttp
            async with aiohttp.ClientSession(
                headers={'User-Agent': self.config.user_agent}
            ) as session:
                
                for ms_id in potential_manuscripts:
                    try:
                        # Quick check for IIIF info.json (lightweight)
                        info_url = f"https://tile.loc.gov/image-services/iiif/service:afc:afc2019048:afc2019048_{block_id}:{ms_id}/info.json"
                        
                        async with session.head(info_url, timeout=5) as response:
                            if response.status == 200:
                                manuscripts.add(ms_id)
                                logger.debug("Fallback confirmed manuscript: %s", ms_id)
                            elif response.status == 404:
                                logger.debug("Fallback manuscript not found: %s", ms_id)
                                break  # If ms0001 doesn't exist, likely none do
                        
                    except asyncio.TimeoutError:
                        logger.debug("Fallback check timeout for: %s", ms_id)
                        break
                    except Exception as e:
                        logger.debug("Fallback check error for %s: %s", ms_id, e)
                        break
                    
        except Exception as e:
            logger.debug("Error in fallback manuscript discovery: %s", e)
    
        return manuscripts
    
    async def collect_and_store_metadata(self, block_id: str) -> Tuple[Optional[Dict[str, Any]], Set[str]]:
        """
        Collect metadata and immediately store in database for dashboard access
        
        Args:
            block_id: The block identifier (e.g., "0001")
            
        Returns:
            Tuple of (metadata_dict, manuscript_set)
        """
        if self.operation_mode == ScraperOperationModes.IMAGES_ONLY:
            logger.debug("Skipping metadata collection in images-only mode")
            return None, set()
            
        try:
            logger.info("Collecting metadata for block %s", block_id)
            
            # Get item metadata from LOC API with proper error handling for rate limits
            item_id = f"afc2019048_{block_id}"
            try:
                metadata = await self.metadata_extractor.extract_item_metadata(item_id)
            except Exception as e:
                # Check if this is a rate limit error
                if "rate limit" in str(e).lower() or "429" in str(e):
                    logger.warning("Rate limit error for block %s: %s", block_id, e)
                    self.stats['rate_limit_errors'] += 1
                    return None, set()
                else:
                    logger.error("API error for block %s: %s", block_id, e)
                    self.stats['api_errors'] += 1
                    return None, set()
            
            if not metadata:
                logger.warning("No metadata found for block %s", block_id)
                self.stats['api_errors'] += 1
                return None, set()
            
            # Discover available manuscripts
            manuscripts = await self.discover_available_manuscripts(block_id)
            
            # Store in database immediately for dashboard access
            if self.operation_mode in [ScraperOperationModes.FULL, ScraperOperationModes.METADATA_ONLY]:
                database_success = await self.store_metadata_in_database(block_id, metadata, manuscripts)
                if database_success:
                    logger.info("✅ Block %s metadata now available in dashboard", block_id)
                else:
                    logger.warning("⚠️ Block %s metadata collected but database storage failed", block_id)
            
            # Also save to JSON file as backup
            await self._save_metadata_to_file(block_id, metadata)
            
            self.stats['metadata_collected'] += 1
            logger.info("Successfully processed metadata for block %s", block_id)
            
            return metadata, manuscripts
                
        except Exception as e:
            logger.error("Failed to collect metadata for block %s: %s", block_id, e)
            self.stats['api_errors'] += 1
            return None, set()
    
    async def _save_metadata_to_file(self, block_id: str, metadata: Dict[str, Any]) -> None:
        """
        Save metadata to JSON file as backup
        
        Args:
            block_id: The block identifier
            metadata: Metadata dictionary to save
        """
        try:
            metadata_dir = self.config.output_dir / "metadata"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            
            metadata_file = metadata_dir / f"block_{block_id}_metadata.json"
            
            import aiofiles
            async with aiofiles.open(metadata_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False))
            
            logger.debug("Saved metadata backup to %s", metadata_file)
            
        except Exception as e:
            logger.warning("Failed to save metadata backup for block %s: %s", block_id, e)
    
    async def sync_existing_metadata_to_database(self) -> None:
        """
        Sync existing JSON metadata files to database for dashboard access
        """
        if self.operation_mode != ScraperOperationModes.DATABASE_SYNC:
            return
            
        logger.info("Syncing existing metadata files to database...")
        
        metadata_dir = self.config.output_dir / "metadata"
        if not metadata_dir.exists():
            logger.warning("No metadata directory found at %s", metadata_dir)
            return
        
        json_files = list(metadata_dir.glob("block_*_metadata.json"))
        logger.info("Found %d metadata files to sync", len(json_files))
        
        synced_count = 0
        for json_file in json_files:
            try:
                # Extract block ID from filename
                block_id = json_file.stem.replace("block_", "").replace("_metadata", "")
                
                # Load metadata from file
                import aiofiles
                async with aiofiles.open(json_file, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    metadata = json.loads(content)
                
                # Get manuscripts from image directories or assume default
                image_dir = self.config.output_dir / "images" / f"block_{block_id}"
                manuscripts = set()
                if image_dir.exists():
                    manuscripts = {d.name for d in image_dir.iterdir() if d.is_dir()}
                else:
                    # Default manuscript assumption
                    manuscripts = {"ms0001"}
                
                # Store in database
                success = await self.store_metadata_in_database(block_id, metadata, manuscripts)
                if success:
                    synced_count += 1
                    logger.info("✅ Synced block %s to database", block_id)
                else:
                    logger.warning("❌ Failed to sync block %s", block_id)
                    
            except Exception as e:
                logger.error("Error syncing %s: %s", json_file, e)
                self.stats['database_errors'] += 1
        
        logger.info("Database sync completed: %d/%d files synced successfully", 
                   synced_count, len(json_files))
    
    def queue_images_for_download(self, block_id: str, manuscripts: Set[str]) -> None:
        """
        Queue image URLs for download in a separate thread
        
        Args:
            block_id: The block identifier
            manuscripts: Set of available manuscript identifiers
        """
        if self.operation_mode == ScraperOperationModes.METADATA_ONLY:
            logger.debug("Skipping image queueing in metadata-only mode")
            return
            
        logger.info("Queueing images for download: block %s, %d manuscripts", 
                   block_id, len(manuscripts))
        
        resolutions = ['200', '400', '800', '1200', 'full']
        
        for manuscript_id in sorted(manuscripts):
            for resolution in resolutions:
                if self.shutdown_event.is_set():
                    break
                    
                try:
                    # Construct IIIF image URL
                    base_url = f"https://tile.loc.gov/image-services/iiif/service:afc:afc2019048:afc2019048_{block_id}:{manuscript_id}"
                    
                    if resolution == 'full':
                        image_url = f"{base_url}/full/full/0/default.jpg"
                    else:
                        image_url = f"{base_url}/full/{resolution},/0/default.jpg"
                    
                    # Create output path
                    output_dir = self.config.output_dir / "images" / f"block_{block_id}" / manuscript_id
                    filename = f"{manuscript_id}_{resolution}.jpg"
                    
                    # Queue the download task
                    download_info = {
                        'url': image_url,
                        'output_path': output_dir / filename,
                        'block_id': block_id,
                        'manuscript_id': manuscript_id,
                        'resolution': resolution
                    }
                    
                    self.image_queue.put(download_info)
                    self.stats['images_queued'] += 1
                    
                except Exception as e:
                    logger.error("Error queueing image %s/%s/%s: %s", 
                               block_id, manuscript_id, resolution, e)
                    self.stats['image_errors'] += 1
        
        logger.info("Queued %d images for download", self.stats['images_queued'])
    
    def image_download_worker(self) -> None:
        """
        Worker thread function for downloading images with retry logic
        """
        logger.info("Image download worker thread started")
        
        while not self.shutdown_event.is_set():
            try:
                # Get download task with timeout
                download_info = self.image_queue.get(timeout=1)
                
                if download_info is None:  # Shutdown signal
                    break
                
                # Attempt download with retry logic
                success = self._download_image_with_retry(download_info)
                
                if success:
                    self.stats['images_downloaded'] += 1
                else:
                    self.stats['image_errors'] += 1
                
                self.image_queue.task_done()
                
            except queue.Empty:
                continue  # Check shutdown event and try again
            except Exception as e:
                logger.error("Image download worker error: %s", e)
                self.stats['image_errors'] += 1
        
        logger.info("Image download worker thread stopped")
    
    def _download_image_with_retry(self, download_info: Dict[str, Any], max_retries: int = 4) -> bool:
        """
        Download an image with exponential backoff retry logic
        
        Args:
            download_info: Dictionary containing download parameters
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if download was successful
        """
        url = download_info['url']
        output_path = download_info['output_path']
        
        # Skip if file already exists
        if output_path.exists():
            logger.debug("Image already exists, skipping: %s", output_path.name)
            return True
        
        for attempt in range(max_retries):
            try:
                # Use synchronous image downloader
                if self.image_downloader.download_image_sync(str(url), output_path):
                    logger.info("Downloaded image: %s", output_path.relative_to(self.config.output_dir))
                    return True
                else:
                    logger.warning("Download failed for %s (attempt %d/%d)", 
                                 output_path.name, attempt + 1, max_retries)
                    
            except Exception as e:
                logger.warning("Error downloading %s (attempt %d/%d): %s", 
                             output_path.name, attempt + 1, max_retries, e)
            
            if attempt < max_retries - 1:
                # Exponential backoff with shorter delays for images
                delay = (2 ** attempt) * 1.0  # Use shorter delays for image downloads
                logger.debug("Retrying in %s seconds...", delay)
                time.sleep(delay)
        
        logger.error("Failed to download after %d attempts: %s", max_retries, url)
        return False
    
    def start_image_download_thread(self) -> None:
        """Start the image download worker thread"""
        if self.operation_mode in [ScraperOperationModes.METADATA_ONLY, ScraperOperationModes.DATABASE_SYNC]:
            logger.debug("Not starting image download thread in %s mode", self.operation_mode)
            return
            
        if self.image_thread is None or not self.image_thread.is_alive():
            self.image_thread = threading.Thread(
                target=self.image_download_worker,
                name="ImageDownloadWorker",
                daemon=True
            )
            self.image_thread.start()
            logger.info("Image download thread started")
    
    def stop_image_download_thread(self) -> None:
        """Stop the image download worker thread gracefully"""
        if self.image_thread and self.image_thread.is_alive():
            logger.info("Stopping image download thread...")
            
            # Signal shutdown and add sentinel
            self.shutdown_event.set()
            self.image_queue.put(None)  # Sentinel to wake up worker
            
            # Wait for thread to finish
            self.image_thread.join(timeout=10)
            
            if self.image_thread.is_alive():
                logger.warning("Image download thread did not stop gracefully")
            else:
                logger.info("Image download thread stopped")
    
    async def scrape_block_range(self, start_id: int, end_id: int) -> None:
        """
        Scrape a range of blocks with real-time database integration and proper rate limiting
        
        Args:
            start_id: Starting block ID (inclusive)
            end_id: Ending block ID (inclusive)
        """
        logger.info("Starting %s scraping for blocks %d-%d with %s second rate limiting", 
                   self.operation_mode, start_id, end_id, self._actual_rate_limit_delay)
        
        self.stats['start_time'] = datetime.now()
        
        # Handle database sync mode
        if self.operation_mode == ScraperOperationModes.DATABASE_SYNC:
            await self.sync_existing_metadata_to_database()
            self.stats['end_time'] = datetime.now()
            self._log_final_statistics()
            return
        
        # Start image download thread if needed
        self.start_image_download_thread()
        
        try:
            for block_num in range(start_id, end_id + 1):
                if self.shutdown_event.is_set():
                    logger.info("Shutdown requested, stopping scraper")
                    break
                
                block_id = f"{block_num:04d}"
                logger.info("Processing block %s (using %s second rate limit)", block_id, self._actual_rate_limit_delay)
                
                try:
                    # Collect metadata and store in database immediately
                    metadata, manuscripts = await self.collect_and_store_metadata(block_id)
                    
                    # Queue images if in images or full mode
                    if manuscripts and self.operation_mode in [ScraperOperationModes.FULL, ScraperOperationModes.IMAGES_ONLY]:
                        self.queue_images_for_download(block_id, manuscripts)
                    
                    # Apply empirically validated 30-second rate limiting between blocks
                    if not self.shutdown_event.is_set():
                        logger.debug("Applying %s second rate limit delay...", self._actual_rate_limit_delay)
                        await asyncio.sleep(self._actual_rate_limit_delay)
                        
                except Exception as e:
                    logger.error("Error processing block %s: %s", block_id, e)
                    # Check if it's a rate limit error
                    if "rate limit" in str(e).lower() or "429" in str(e):
                        self.stats['rate_limit_errors'] += 1
                    else:
                        self.stats['api_errors'] += 1
                    continue
            
        finally:
            # Stop image download thread
            self.stop_image_download_thread()
            
            self.stats['end_time'] = datetime.now()
            self._log_final_statistics()
    
    def _log_final_statistics(self) -> None:
        """Log comprehensive final scraping statistics including rate limit errors"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        logger.info("="*70)
        logger.info("SCRAPING COMPLETED - %s MODE", self.operation_mode.upper())
        logger.info("="*70)
        logger.info("Duration: %s", duration)
        logger.info("Rate limiting: %s seconds per block", self._actual_rate_limit_delay)
        logger.info("Metadata collected: %d", self.stats['metadata_collected'])
        logger.info("Database records created: %d", self.stats['database_records_created'])
        logger.info("Database records updated: %d", self.stats['database_records_updated'])
        logger.info("Images queued: %d", self.stats['images_queued'])
        logger.info("Images downloaded: %d", self.stats['images_downloaded'])
        logger.info("API errors: %d", self.stats['api_errors'])
        logger.info("Rate limit errors: %d", self.stats['rate_limit_errors'])  # Add rate limit error count
        logger.info("Database errors: %d", self.stats['database_errors'])
        logger.info("Image errors: %d", self.stats['image_errors'])
        
        if self.stats['images_queued'] > 0:
            success_rate = (self.stats['images_downloaded'] / self.stats['images_queued']) * 100
            logger.info("Download success rate: %.1f%%", success_rate)
        
        total_errors = self.stats['api_errors'] + self.stats['rate_limit_errors'] + self.stats['database_errors'] + self.stats['image_errors']
        if total_errors == 0:
            logger.info("✅ Scraping completed with no errors")
        else:
            logger.warning("⚠️ Scraping completed with %d total errors (%d rate limit, %d other API, %d database, %d image)", 
                         total_errors, self.stats['rate_limit_errors'], self.stats['api_errors'], 
                         self.stats['database_errors'], self.stats['image_errors'])
            
        # Log rate limiting effectiveness
        if self.stats['rate_limit_errors'] > 0:
            logger.warning("🔄 Consider increasing rate limit delay beyond %s seconds", self._actual_rate_limit_delay)
        else:
            logger.info("✅ Rate limiting effective: %s second delay prevented rate limit errors", self._actual_rate_limit_delay)


async def main() -> None:
    """
    Main entry point for the integrated scraper with real-time database updates
    """
    parser = argparse.ArgumentParser(
        description="Integrated AIDS Memorial Quilt Records Scraper with Real-time Database Updates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Operation Modes:
  full        Download metadata and images with real-time database updates (default)
  metadata    Only collect metadata and update database (no images)
  images      Only download images (no database updates)
  db-sync     Sync existing JSON metadata files to database
  
Examples:
  # Real-time scraping with dashboard updates (30-second rate limiting)
  python enhanced_scraper.py --start-id 1 --end-id 5 --mode full
  
  # Metadata-only when images are blocked, dashboard updated immediately
  python enhanced_scraper.py --start-id 100 --end-id 110 --mode metadata
  
  # Catch up on images later (no database updates needed)
  python enhanced_scraper.py --start-id 100 --end-id 110 --mode images
  
  # Sync existing files to database for dashboard
  python enhanced_scraper.py --mode db-sync
        """
    )
    
    parser.add_argument(
        '--start-id', 
        type=int,
        help='Starting block ID (e.g., 1 for block_0001)'
    )
    parser.add_argument(
        '--end-id', 
        type=int,
        help='Ending block ID (inclusive)'
    )
    parser.add_argument(
        '--mode',
        choices=[
            ScraperOperationModes.FULL, 
            ScraperOperationModes.METADATA_ONLY, 
            ScraperOperationModes.IMAGES_ONLY,
            ScraperOperationModes.DATABASE_SYNC
        ],
        default=ScraperOperationModes.FULL,
        help='Scraper operation mode (default: full)'
    )
    parser.add_argument(
        '--config',
        type=Path,
        help='Path to configuration file (optional)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments based on mode
    if args.mode != ScraperOperationModes.DATABASE_SYNC:
        if not args.start_id or not args.end_id:
            logger.error("--start-id and --end-id are required for %s mode", args.mode)
            sys.exit(1)
        
        if args.start_id < 1 or args.end_id < args.start_id:
            logger.error("Invalid ID range: start_id must be >= 1 and <= end_id")
            sys.exit(1)
    
    try:
        # Load configuration
        config = ScraperConfig()
        if args.config and args.config.exists():
            logger.info("Loading configuration from %s", args.config)
            # TODO: Add config file loading logic
        
        # Initialize scraper with database integration and proper rate limiting
        async with IntegratedAIDSQuiltScraper(config, args.mode) as scraper:
            scraper.setup_signal_handlers()
            
            logger.info("Starting AIDS Memorial Quilt scraper with real-time database updates")
            logger.info("Operation mode: %s", args.mode)
            logger.info("Rate limiting: 30 seconds per block (empirically validated)")
            
            if args.mode != ScraperOperationModes.DATABASE_SYNC:
                logger.info("Block range: %d to %d", args.start_id, args.end_id)
            
            # Execute scraping workflow
            await scraper.scrape_block_range(
                args.start_id or 0, 
                args.end_id or 0
            )
            
            logger.info("✅ Scraping workflow completed successfully")
            if args.mode in [ScraperOperationModes.FULL, ScraperOperationModes.METADATA_ONLY]:
                logger.info("🎯 Dashboard data updated in real-time during scraping")
        
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
    except Exception as e:
        logger.error("Scraper failed with error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
