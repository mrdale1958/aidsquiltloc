"""
Library of Congress API client for AIDS Memorial Quilt Records collection
Handles API authentication, rate limiting, and data retrieval
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from urllib.parse import urlencode
import aiohttp
from aiohttp import ClientSession, ClientTimeout, ClientError

logger = logging.getLogger(__name__)


class LOCAPISettings:
    """
    Library of Congress API specific settings
    
    Attributes:
        base_url: Base URL for LOC JSON API
        collection_name: AIDS Memorial Quilt collection identifier
        items_per_page: Number of items to request per API call
        iiif_base_url: Base URL for IIIF image service
    """
    
    def __init__(self) -> None:
        """Initialize LOC API settings with default values"""
        self.base_url: str = "https://www.loc.gov"
        self.collection_name: str = "aids-memorial-quilt-records"
        self.items_per_page: int = 100
        self.iiif_base_url: str = "https://tile.loc.gov/image-services/iiif"
    
    @property
    def search_url(self) -> str:
        """Get the search API URL for the collection"""
        return f"{self.base_url}/collections/{self.collection_name}/"
    
    @property
    def item_url_template(self) -> str:
        """Get the template for individual item URLs"""
        return f"{self.base_url}/item/{{item_id}}/"


class LOCAPIError(Exception):
    """Base exception for Library of Congress API errors"""
    pass


class LOCAPIRateLimitError(LOCAPIError):
    """Raised when API rate limit is exceeded"""
    pass


class LOCAPIClient:
    """
    Asynchronous client for Library of Congress JSON API
    
    Provides methods for retrieving collection items, individual item metadata,
    and handling pagination with proper rate limiting and error handling
    """
    
    def __init__(self, settings: LOCAPISettings) -> None:
        """
        Initialize the LOC API client
        
        Args:
            settings: API configuration settings
        """
        self.settings = settings
        self.session: Optional[ClientSession] = None
        self._semaphore = asyncio.Semaphore(5)  # Limit concurrent requests
        
    async def __aenter__(self) -> 'LOCAPIClient':
        """Async context manager entry"""
        await self.initialize_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        await self.close_session()
    
    async def initialize_session(self) -> None:
        """Initialize the aiohttp session with proper configuration"""
        if self.session is None:
            timeout = ClientTimeout(total=30, connect=10)
            headers = {
                'User-Agent': 'AIDS-Memorial-Quilt-Scraper/1.0 (Educational Research)',
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip, deflate'
            }
            
            self.session = ClientSession(
                timeout=timeout,
                headers=headers,
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
            logger.info("LOC API client session initialized")
    
    async def close_session(self) -> None:
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("LOC API client session closed")
    
    async def get_item_metadata(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a specific item
        
        Args:
            item_id: The LOC item identifier (e.g., "afc2019048_0001")
            
        Returns:
            Item metadata dictionary or None if not found
            
        Raises:
            LOCAPIError: If API request fails
        """
        if not self.session:
            await self.initialize_session()
        
        async with self._semaphore:
            try:
                url = f"{self.settings.base_url}/item/{item_id}/?fo=json"
                logger.debug("Fetching item metadata: %s", url)
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.debug("Successfully retrieved metadata for item %s", item_id)
                        return data
                    elif response.status == 404:
                        logger.warning("Item not found: %s", item_id)
                        return None
                    elif response.status == 429:
                        logger.warning("Rate limit exceeded for item %s", item_id)
                        raise LOCAPIRateLimitError(f"Rate limit exceeded for item {item_id}")
                    else:
                        logger.error("API request failed for item %s: HTTP %d", item_id, response.status)
                        raise LOCAPIError(f"API request failed: HTTP {response.status}")
                        
            except ClientError as e:
                logger.error("Network error retrieving item %s: %s", item_id, e)
                raise LOCAPIError(f"Network error: {e}")
            except Exception as e:
                logger.error("Unexpected error retrieving item %s: %s", item_id, e)
                raise LOCAPIError(f"Unexpected error: {e}")
    
    async def search_collection(self, 
                              query: str = "", 
                              page: int = 1, 
                              per_page: int = 100) -> Optional[Dict[str, Any]]:
        """
        Search the AIDS Memorial Quilt collection
        
        Args:
            query: Search query string
            page: Page number (1-based)
            per_page: Items per page
            
        Returns:
            Search results dictionary or None if request fails
            
        Raises:
            LOCAPIError: If API request fails
        """
        if not self.session:
            await self.initialize_session()
        
        async with self._semaphore:
            try:
                params = {
                    'fo': 'json',
                    'c': self.settings.items_per_page,
                    'sp': page
                }
                
                if query:
                    params['q'] = query
                
                url = f"{self.settings.search_url}?{urlencode(params)}"
                logger.debug("Searching collection: %s", url)
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.debug("Successfully retrieved search results (page %d)", page)
                        return data
                    elif response.status == 429:
                        logger.warning("Rate limit exceeded for collection search")
                        raise LOCAPIRateLimitError("Rate limit exceeded for collection search")
                    else:
                        logger.error("Collection search failed: HTTP %d", response.status)
                        raise LOCAPIError(f"Collection search failed: HTTP {response.status}")
                        
            except ClientError as e:
                logger.error("Network error during collection search: %s", e)
                raise LOCAPIError(f"Network error: {e}")
            except Exception as e:
                logger.error("Unexpected error during collection search: %s", e)
                raise LOCAPIError(f"Unexpected error: {e}")
    
    async def get_collection_items(self, max_items: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retrieve all items from the AIDS Memorial Quilt collection
        
        Args:
            max_items: Maximum number of items to retrieve (None for all)
            
        Returns:
            List of collection item dictionaries
            
        Raises:
            LOCAPIError: If API requests fail
        """
        logger.info("Retrieving collection items (max: %s)", max_items or "unlimited")
        
        items = []
        page = 1
        
        while True:
            try:
                # Add delay between requests to be respectful
                if page > 1:
                    await asyncio.sleep(0.5)
                
                results = await self.search_collection(page=page, per_page=self.settings.items_per_page)
                
                if not results or 'results' not in results:
                    logger.warning("No results found on page %d", page)
                    break
                
                page_items = results['results']
                if not page_items:
                    logger.info("No more items found, stopping at page %d", page)
                    break
                
                items.extend(page_items)
                logger.info("Retrieved %d items from page %d (total: %d)", 
                           len(page_items), page, len(items))
                
                # Check if we've reached the maximum
                if max_items and len(items) >= max_items:
                    items = items[:max_items]
                    logger.info("Reached maximum items limit: %d", max_items)
                    break
                
                # Check if this was the last page
                if len(page_items) < self.settings.items_per_page:
                    logger.info("Reached end of collection at page %d", page)
                    break
                
                page += 1
                
            except LOCAPIRateLimitError:
                logger.warning("Rate limit hit, waiting before retry...")
                await asyncio.sleep(5)  # Wait longer for rate limit
                continue
            except LOCAPIError as e:
                logger.error("API error on page %d: %s", page, e)
                break
        
        logger.info("Collection retrieval completed: %d total items", len(items))
        return items
