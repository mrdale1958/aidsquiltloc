"""
Manuscript discovery module for AIDS Memorial Quilt Records scraper
Integrates with existing scraper architecture following project standards
"""

import asyncio
import logging
from typing import Set, Dict, Any, List, Optional, Tuple
from pathlib import Path

# Use absolute imports for proper module structure
from src.loc_api_client import LOCAPIClient

logger = logging.getLogger(__name__)


class ManuscriptDiscoveryService:
    """
    Service for discovering manuscript files from AIDS Memorial Quilt Records
    
    Implements comprehensive error handling and follows async/await patterns
    per project architecture guidelines. Integrates with existing LOCAPIClient
    for consistency with scraper infrastructure.
    """
    
    def __init__(self, api_client: LOCAPIClient, max_concurrent: int = 5):
        """
        Initialize manuscript discovery service
        
        Args:
            api_client: Existing LOCAPIClient instance for API consistency
            max_concurrent: Maximum concurrent discovery operations
        """
        self.api_client = api_client
        self.max_concurrent = max_concurrent
        self.discovery_cache: Dict[str, Set[str]] = {}
        
    async def discover_manuscripts_for_item(self, item_id: str, 
                                           item_metadata: Optional[Dict[str, Any]] = None) -> Set[str]:
        """
        Discover available manuscripts for a specific item
        
        Uses existing scraper patterns for data extraction and caching.
        Implements separation of concerns by leveraging existing API client.
        
        Args:
            item_id: Full item identifier (e.g., "afc2019048_0001")
            item_metadata: Pre-fetched item metadata to avoid duplicate API calls
            
        Returns:
            Set of manuscript identifiers (e.g., {"ms0001", "ms0002", ...})
        """
        # Check cache first for performance optimization
        if item_id in self.discovery_cache:
            logger.debug("Using cached manuscript discovery for %s", item_id)
            return self.discovery_cache[item_id]
        
        logger.info("Discovering manuscripts for item %s", item_id)
        manuscripts: Set[str] = set()
        
        try:
            # Strategy 1: Use provided metadata if available (performance optimization)
            if item_metadata:
                manuscripts = self._extract_from_metadata(item_metadata, item_id)
            
            # Strategy 2: Fetch fresh metadata if needed (error resilience)
            if not manuscripts:
                fresh_metadata = await self.api_client.get_item_metadata(item_id)
                if fresh_metadata:
                    manuscripts = self._extract_from_metadata(fresh_metadata, item_id)
            
            # Cache successful results
            if manuscripts:
                self.discovery_cache[item_id] = manuscripts
                
        except Exception as e:
            logger.error("Error discovering manuscripts for item %s: %s", item_id, e)
            # Return empty set for graceful error handling
            manuscripts = set()
        
        logger.info("Discovered %d manuscripts for %s: %s", 
                   len(manuscripts), item_id, sorted(manuscripts))
        return manuscripts
    
    def _extract_from_metadata(self, metadata: Dict[str, Any], item_id: str) -> Set[str]:
        """
        Extract manuscript identifiers from item metadata
        
        Implements data validation practices per project standards.
        
        Args:
            metadata: Item metadata from LOC API
            item_id: Item identifier for logging context
            
        Returns:
            Set of discovered manuscript identifiers
        """
        manuscripts: Set[str] = set()
        
        try:
            if not isinstance(metadata, dict) or 'item' not in metadata:
                logger.debug("Invalid metadata structure for %s", item_id)
                return manuscripts
            
            item = metadata['item']
            
            # Extract from resources field (primary strategy)
            resources = item.get('resources', [])
            if isinstance(resources, list):
                manuscripts = self._extract_from_resources(resources, item_id)
            
            # Fallback: extract from other metadata fields
            if not manuscripts:
                manuscripts = self._extract_from_item_fields(item, item_id)
                
        except Exception as e:
            logger.error("Error extracting manuscripts from metadata for %s: %s", item_id, e)
        
        return manuscripts
    
    def _extract_from_resources(self, resources: List[Dict[str, Any]], item_id: str) -> Set[str]:
        """
        Extract manuscripts from resources field following data validation practices
        
        Args:
            resources: Resources array from item metadata
            item_id: Item identifier for logging context
            
        Returns:
            Set of manuscript identifiers
        """
        manuscripts: Set[str] = set()
        
        try:
            for resource in resources:
                if not isinstance(resource, dict):
                    continue
                
                # Check for files count (most reliable indicator)
                files_count = resource.get('files', 0)
                if isinstance(files_count, int) and files_count > 0:
                    # Generate manuscript IDs based on file count
                    # Each file typically represents a manuscript page/document
                    for i in range(1, min(files_count + 1, 101)):  # Reasonable upper limit
                        manuscript_id = f"ms{i:04d}"
                        manuscripts.add(manuscript_id)
                    
                    logger.debug("Generated %d manuscripts from %d files for %s", 
                               len(manuscripts), files_count, item_id)
                    break  # Use first resource with valid file count
                
        except Exception as e:
            logger.error("Error extracting from resources for %s: %s", item_id, e)
        
        return manuscripts
    
    def _extract_from_item_fields(self, item: Dict[str, Any], item_id: str) -> Set[str]:
        """
        Extract manuscripts from other item metadata fields (fallback strategy)
        
        Args:
            item: Item metadata dictionary
            item_id: Item identifier for logging context
            
        Returns:
            Set of manuscript identifiers
        """
        manuscripts: Set[str] = set()
        
        try:
            # Check various fields that might indicate digitized content count
            potential_count_fields = ['segments', 'digitized_items', 'image_count', 'pages']
            
            for field in potential_count_fields:
                if field in item:
                    value = item[field]
                    if isinstance(value, int) and value > 0:
                        for i in range(1, min(value + 1, 101)):
                            manuscripts.add(f"ms{i:04d}")
                        logger.debug("Generated %d manuscripts from %s field for %s", 
                                   value, field, item_id)
                        break
                    elif isinstance(value, str) and value.isdigit():
                        count = int(value)
                        if count > 0:
                            for i in range(1, min(count + 1, 101)):
                                manuscripts.add(f"ms{i:04d}")
                            logger.debug("Generated %d manuscripts from %s field for %s", 
                                       count, field, item_id)
                            break
            
            # If no count fields found, provide minimal fallback
            if not manuscripts:
                # Assume at least one manuscript exists for digitized items
                manuscripts.add("ms0001")
                logger.debug("Applied fallback strategy for %s: single manuscript", item_id)
                
        except Exception as e:
            logger.error("Error extracting from item fields for %s: %s", item_id, e)
        
        return manuscripts
    
    def generate_iiif_urls(self, item_id: str, manuscripts: Set[str]) -> List[str]:
        """
        Generate IIIF image URLs for discovered manuscripts
        
        Follows performance optimization guidelines by generating multiple
        resolution options for flexible usage.
        
        Args:
            item_id: Item identifier (e.g., "afc2019048_0001")
            manuscripts: Set of manuscript identifiers
            
        Returns:
            List of IIIF URLs at multiple resolutions
        """
        urls: List[str] = []
        
        try:
            # Extract block ID from item ID for IIIF URL construction
            block_id = item_id.split('_')[-1] if '_' in item_id else item_id
            
            # Base IIIF service pattern for AIDS Memorial Quilt collection
            # Based on successful discovery patterns from investigation
            base_service = f"https://tile.loc.gov/image-services/iiif/service:afc:afc2019048:af:c2:01:90:48:_{block_id}:{item_id}"
            
            # Multiple resolution options following IIIF Image API 2.1 specification
            resolutions = [
                'pct:100',   # Full resolution (highest quality)
                'pct:50',    # 50% scale (good balance of quality/size)
                'pct:25',    # 25% scale (medium thumbnail)
                'pct:12.5',  # 12.5% scale (small thumbnail)
                'pct:6.25'   # 6.25% scale (very small thumbnail)
            ]
            
            # Generate URLs for each manuscript at each resolution
            for manuscript in sorted(manuscripts):
                for resolution in resolutions:
                    # IIIF Image API URL format: {service_base}/{manuscript}/full/{size}/0/default.jpg
                    url = f"{base_service}_{manuscript}/full/{resolution}/0/default.jpg"
                    urls.append(url)
            
            logger.debug("Generated %d IIIF URLs for %d manuscripts (%s)", 
                        len(urls), len(manuscripts), item_id)
            
        except Exception as e:
            logger.error("Error generating IIIF URLs for %s: %s", item_id, e)
        
        return urls
    
    def get_priority_urls(self, urls: List[str], max_manuscripts: int = 3) -> List[str]:
        """
        Extract priority URLs for most important manuscripts at high resolution
        
        Implements performance optimization by prioritizing key manuscripts.
        
        Args:
            urls: Complete list of IIIF URLs
            max_manuscripts: Maximum number of manuscripts to prioritize
            
        Returns:
            List of priority URLs (high-resolution, first few manuscripts)
        """
        priority_urls: List[str] = []
        
        try:
            # Filter for high-resolution URLs of first few manuscripts
            for i in range(1, max_manuscripts + 1):
                manuscript_pattern = f"_ms{i:04d}"
                high_res_pattern = "pct:100"
                
                for url in urls:
                    if manuscript_pattern in url and high_res_pattern in url:
                        priority_urls.append(url)
                        break  # One URL per manuscript
            
            logger.debug("Selected %d priority URLs from %d total", 
                        len(priority_urls), len(urls))
            
        except Exception as e:
            logger.error("Error selecting priority URLs: %s", e)
        
        return priority_urls
    
    def clear_cache(self) -> None:
        """Clear discovery cache for memory management"""
        self.discovery_cache.clear()
        logger.debug("Cleared manuscript discovery cache")