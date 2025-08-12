"""
Image downloader for AIDS Memorial Quilt Records
Handles IIIF image downloads with validation and retry logic
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path
import aiohttp
import aiofiles
from PIL import Image
import io

from config.settings import ScraperConfig

logger = logging.getLogger(__name__)


class ImageDownloadError(Exception):
    """Exception raised during image download process"""
    pass


class ImageDownloader:
    """
    Downloads and validates images from IIIF endpoints
    
    Provides asynchronous image downloading with proper validation,
    retry logic, and file management for the AIDS Memorial Quilt collection
    """
    
    def __init__(self, config: ScraperConfig) -> None:
        """
        Initialize the image downloader
        
        Args:
            config: Scraper configuration settings
        """
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.max_concurrent_downloads)
        
        # Supported image formats
        self.supported_formats = {'.jpg', '.jpeg', '.png', '.tiff', '.tif'}
        
        # Quality settings for different resolutions
        self.quality_settings = {
            '200': {'quality': 85, 'optimize': True},
            '400': {'quality': 90, 'optimize': True},
            '800': {'quality': 95, 'optimize': True},
            '1200': {'quality': 95, 'optimize': False},
            'full': {'quality': 100, 'optimize': False}
        }
    
    async def __aenter__(self) -> 'ImageDownloader':
        """Async context manager entry"""
        await self.initialize_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        await self.close_session()
    
    async def initialize_session(self) -> None:
        """Initialize the aiohttp session for image downloads"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)
            headers = {
                'User-Agent': self.config.user_agent,
                'Accept': 'image/*'
            }
            
            connector = aiohttp.TCPConnector(
                limit=self.config.max_concurrent_downloads * 2,
                limit_per_host=self.config.max_concurrent_downloads
            )
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
                connector=connector
            )
            logger.info("Image downloader session initialized")
    
    async def close_session(self) -> None:
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("Image downloader session closed")
    
    async def download_image(self, 
                           url: str, 
                           output_path: Path,
                           max_retries: int = 3) -> bool:
        """
        Download an image from URL with validation and retry logic
        
        Args:
            url: Image URL to download
            output_path: Local path to save the image
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if download was successful
            
        Raises:
            ImageDownloadError: If download fails after all retries
        """
        if not self.session:
            await self.initialize_session()
        
        async with self._semaphore:
            # Skip if file already exists and is valid
            if output_path.exists() and await self._validate_existing_image(output_path):
                logger.debug("Image already exists and is valid: %s", output_path.name)
                return True
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            for attempt in range(max_retries):
                try:
                    logger.debug("Downloading image (attempt %d/%d): %s", 
                               attempt + 1, max_retries, url)
                    
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            # Download and validate image
                            image_data = await response.read()
                            
                            if await self._validate_image_data(image_data):
                                # Save image to file
                                async with aiofiles.open(output_path, 'wb') as f:
                                    await f.write(image_data)
                                
                                logger.info("Successfully downloaded: %s", output_path.name)
                                return True
                            else:
                                logger.warning("Downloaded image failed validation: %s", url)
                                
                        elif response.status == 404:
                            logger.warning("Image not found: %s", url)
                            return False
                        elif response.status == 429:
                            # Rate limited - wait longer before retry
                            wait_time = (2 ** attempt) * 2
                            logger.warning("Rate limited, waiting %ds before retry", wait_time)
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.warning("HTTP %d downloading %s (attempt %d/%d)", 
                                         response.status, url, attempt + 1, max_retries)
                
                except asyncio.TimeoutError:
                    logger.warning("Timeout downloading %s (attempt %d/%d)", 
                                 url, attempt + 1, max_retries)
                except Exception as e:
                    logger.warning("Error downloading %s (attempt %d/%d): %s", 
                                 url, attempt + 1, max_retries, e)
                
                # Wait before retry (exponential backoff)
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * self.config.rate_limit_delay
                    await asyncio.sleep(wait_time)
            
            logger.error("Failed to download after %d attempts: %s", max_retries, url)
            raise ImageDownloadError(f"Failed to download after {max_retries} attempts: {url}")
    
    async def _validate_existing_image(self, image_path: Path) -> bool:
        """
        Validate an existing image file
        
        Args:
            image_path: Path to the image file
            
        Returns:
            True if image is valid
        """
        try:
            if not image_path.exists() or image_path.stat().st_size == 0:
                return False
            
            # Read and validate image
            async with aiofiles.open(image_path, 'rb') as f:
                image_data = await f.read()
            
            return await self._validate_image_data(image_data)
            
        except Exception as e:
            logger.debug("Error validating existing image %s: %s", image_path, e)
            return False
    
    async def _validate_image_data(self, image_data: bytes) -> bool:
        """
        Validate image data using PIL
        
        Args:
            image_data: Raw image bytes
            
        Returns:
            True if image data is valid
        """
        try:
            if len(image_data) < 100:  # Too small to be a valid image
                return False
            
            # Use PIL to validate image
            image = Image.open(io.BytesIO(image_data))
            image.verify()  # Verify the image is not corrupted
            
            # Check minimum dimensions
            if image.size[0] < 10 or image.size[1] < 10:
                return False
            
            return True
            
        except Exception as e:
            logger.debug("Image validation failed: %s", e)
            return False
    
    def download_image_sync(self, url: str, output_path: Path) -> bool:
        """
        Synchronous wrapper for download_image (for use in threads)
        
        Args:
            url: Image URL to download
            output_path: Local path to save the image
            
        Returns:
            True if download was successful
        """
        try:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            
            # Skip if file already exists
            if output_path.exists() and self._validate_image_sync(output_path):
                logger.debug("Image already exists and is valid: %s", output_path.name)
                return True
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Configure session with retries
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            # Set headers
            session.headers.update({
                'User-Agent': self.config.user_agent,
                'Accept': 'image/*'
            })
            
            # Download image
            response = session.get(url, timeout=self.config.request_timeout)
            response.raise_for_status()
            
            # Validate image data
            if self._validate_image_data_sync(response.content):
                # Save to file
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                
                logger.info("Successfully downloaded: %s", output_path.name)
                return True
            else:
                logger.warning("Downloaded image failed validation: %s", url)
                return False
                
        except Exception as e:
            logger.error("Error in synchronous download of %s: %s", url, e)
            return False
    
    def _validate_image_sync(self, image_path: Path) -> bool:
        """
        Synchronous image validation
        
        Args:
            image_path: Path to the image file
            
        Returns:
            True if image is valid
        """
        try:
            if not image_path.exists() or image_path.stat().st_size == 0:
                return False
            
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            return self._validate_image_data_sync(image_data)
            
        except Exception as e:
            logger.debug("Error validating existing image %s: %s", image_path, e)
            return False
    
    def _validate_image_data_sync(self, image_data: bytes) -> bool:
        """
        Synchronous image data validation
        
        Args:
            image_data: Raw image bytes
            
        Returns:
            True if image data is valid
        """
        try:
            if len(image_data) < 100:
                return False
            
            image = Image.open(io.BytesIO(image_data))
            image.verify()
            
            if image.size[0] < 10 or image.size[1] < 10:
                return False
            
            return True
            
        except Exception:
            return False
    
    async def download_iiif_resolutions(self, 
                                      block_id: str, 
                                      manuscript_id: str,
                                      resolutions: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        Download multiple resolutions of a IIIF image
        
        Args:
            block_id: Block identifier (e.g., "0001")
            manuscript_id: Manuscript identifier (e.g., "ms0001")
            resolutions: List of resolutions to download (default: all)
            
        Returns:
            Dictionary mapping resolutions to download success status
        """
        if resolutions is None:
            resolutions = ['200', '400', '800', '1200', 'full']
        
        logger.info("Downloading IIIF resolutions for %s/%s: %s", 
                   block_id, manuscript_id, resolutions)
        
        results = {}
        base_url = f"https://tile.loc.gov/image-services/iiif/service:afc:afc2019048:afc2019048_{block_id}:{manuscript_id}"
        
        # Create output directory
        output_dir = self.config.output_dir / "images" / f"block_{block_id}" / manuscript_id
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Download each resolution
        for resolution in resolutions:
            try:
                # Construct IIIF URL
                if resolution == 'full':
                    image_url = f"{base_url}/full/full/0/default.jpg"
                else:
                    image_url = f"{base_url}/full/{resolution},/0/default.jpg"
                
                # Create output path
                filename = f"{manuscript_id}_{resolution}.jpg"
                output_path = output_dir / filename
                
                # Download image
                success = await self.download_image(image_url, output_path)
                results[resolution] = success
                
                if success:
                    logger.debug("Downloaded %s resolution for %s/%s", 
                               resolution, block_id, manuscript_id)
                else:
                    logger.warning("Failed to download %s resolution for %s/%s", 
                                 resolution, block_id, manuscript_id)
                
                # Rate limiting between downloads
                await asyncio.sleep(self.config.rate_limit_delay)
                
            except Exception as e:
                logger.error("Error downloading %s resolution for %s/%s: %s", 
                           resolution, block_id, manuscript_id, e)
                results[resolution] = False
        
        success_count = sum(results.values())
        logger.info("Downloaded %d/%d resolutions for %s/%s", 
                   success_count, len(resolutions), block_id, manuscript_id)
        
        return results
