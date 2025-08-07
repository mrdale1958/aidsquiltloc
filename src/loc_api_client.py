"""
Library of Congress API client for accessing AIDS Memorial Quilt Records
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import aiohttp
from aiohttp import ClientSession, ClientTimeout

logger = logging.getLogger(__name__)


class LOCAPIClient:
    """Client for interacting with the Library of Congress APIs"""
    
    def __init__(self, settings):
        self.settings = settings
        self.session: Optional[ClientSession] = None
        self.base_url = settings.loc_api_base_url
        self.collection_name = "aids-memorial-quilt-records"
        
    async def _get_session(self) -> ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=self.settings.request_timeout)
            headers = {
                'User-Agent': 'AIDS-Quilt-Scraper/1.0 (Educational Research)'
            }
            if self.settings.api_key:
                headers['Authorization'] = f'Bearer {self.settings.api_key}'
                
            self.session = ClientSession(
                timeout=timeout,
                headers=headers
            )
        return self.session
    
    async def _make_request(self, url: str, params: Optional[Dict] = None, max_retries: int = 3) -> Dict[str, Any]:
        """Make an API request with rate limiting and retry logic"""
        session = await self._get_session()
        
        # Add format parameter for JSON response
        if params is None:
            params = {}
        params['fo'] = 'json'  # LOC API format parameter
        
        for attempt in range(max_retries + 1):
            try:
                logger.debug("Making request to: %s with params: %s", url, params)
                
                async with session.get(url, params=params) as response:
                    if response.status == 429:  # Too Many Requests
                        if attempt < max_retries:
                            # Exponential backoff
                            delay = (2 ** attempt) * self.settings.rate_limit_delay
                            logger.warning("Rate limited (429), retrying in %s seconds (attempt %d/%d)", 
                                         delay, attempt + 1, max_retries + 1)
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.error("Rate limited after all retries")
                            response.raise_for_status()
                    
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Rate limiting for successful requests
                    await asyncio.sleep(self.settings.rate_limit_delay)
                    
                    return data
                    
            except aiohttp.ClientError as e:
                if attempt < max_retries:
                    delay = (2 ** attempt) * self.settings.rate_limit_delay
                    logger.warning("HTTP error for %s: %s, retrying in %s seconds", url, e, delay)
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error("HTTP error for %s: %s", url, e)
                    raise
            except Exception as e:
                logger.error("Unexpected error for %s: %s", url, e)
                raise
    
    async def get_collection_items(self, start: int = 0, count: int = 100, max_items: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get items from the AIDS Memorial Quilt Records collection
        
        Based on the LOC website, there are 5,164 individual quilt block records
        with IDs like afc2019048_0001, afc2019048_0002, etc.
        
        Args:
            start: Starting index for pagination
            count: Number of items to retrieve per request
            max_items: Maximum total items to retrieve (None for all)
            
        Returns:
            List of collection items
        """
        # Search for individual quilt block items using the collection identifier
        search_url = f"{self.base_url}/search/"
        
        params = {
            'q': 'partof:"aids memorial quilt records"',  # This should find the individual items
            'c': count,
            's': start,
            'fo': 'json'
        }
        
        try:
            logger.info("Fetching AIDS Memorial Quilt items (start=%d, count=%d)", start, count)
            data = await self._make_request(search_url, params)
            
            if 'results' in data and data['results']:
                items = data['results']
                logger.info("Retrieved %d items in this batch", len(items))
                
                # Stop if we have reached max_items
                if max_items and len(items) >= max_items:
                    return items[:max_items]
                    
                return items
            else:
                logger.warning("No items found in search results")
                return []
            
        except Exception as e:
            logger.error("Error fetching collection items: %s", e)
            raise
    
    async def get_item_details(self, item_id: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific item
        
        Args:
            item_id: The LOC item identifier
            
        Returns:
            Detailed item information
        """
        # LOC item URL format
        item_url = f"{self.base_url}/item/{item_id}/"
        
        try:
            logger.debug(f"Fetching details for item: {item_id}")
            data = await self._make_request(item_url)
            return data
            
        except Exception as e:
            logger.error(f"Error fetching item details for {item_id}: {e}")
            raise
    
    async def get_item_resources(self, item_id: str) -> List[Dict[str, Any]]:
        """
        Get downloadable resources (images, documents) for an item
        
        Args:
            item_id: The LOC item identifier
            
        Returns:
            List of available resources
        """
        # LOC resources URL format
        resources_url = f"{self.base_url}/item/{item_id}/resources/"
        
        try:
            logger.debug(f"Fetching resources for item: {item_id}")
            data = await self._make_request(resources_url)
            
            resources = []
            if isinstance(data, dict) and 'resource' in data:
                if isinstance(data['resource'], list):
                    resources = data['resource']
                else:
                    resources = [data['resource']]
                    
            return resources
            
        except Exception as e:
            logger.error(f"Error fetching resources for {item_id}: {e}")
            return []
    
    async def close(self):
        """Close the HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.debug("HTTP session closed")
