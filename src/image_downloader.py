"""
Image downloader for AIDS Memorial Quilt Records
Downloads and saves images from the Library of Congress collection
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, unquote
import hashlib

import aiohttp
import aiofiles
from PIL import Image

logger = logging.getLogger(__name__)


class ImageDownloader:
    """Downloads and processes images from LOC collection"""
    
    def __init__(self, settings):
        self.settings = settings
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(settings.max_concurrent_downloads)
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout)
            headers = {
                'User-Agent': 'AIDS-Quilt-Scraper/1.0 (Educational Research)'
            }
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers
            )
        return self.session
    
    def _get_safe_filename(self, url: str, item_id: str) -> str:
        """Generate a safe filename from URL and item ID"""
        parsed_url = urlparse(url)
        original_filename = Path(unquote(parsed_url.path)).name
        
        # If no filename in URL, generate one
        if not original_filename or '.' not in original_filename:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            original_filename = f"{item_id}_{url_hash}.jpg"
        
        # Ensure safe filename
        safe_filename = "".join(c for c in original_filename if c.isalnum() or c in '.-_')
        
        # Add item_id prefix if not already present
        if not safe_filename.startswith(item_id):
            name_part = Path(safe_filename).stem
            ext_part = Path(safe_filename).suffix
            safe_filename = f"{item_id}_{name_part}{ext_part}"
            
        return safe_filename
    
    def _is_valid_image_format(self, filename: str) -> bool:
        """Check if the file has a supported image format"""
        return any(filename.lower().endswith(fmt) for fmt in self.settings.supported_image_formats)
    
    async def _download_single_image(self, url: str, item_id: str, metadata: Dict[str, Any]) -> Optional[Path]:
        """Download a single image with concurrency control"""
        async with self.semaphore:
            session = await self._get_session()
            
            try:
                # Generate safe filename
                filename = self._get_safe_filename(url, item_id)
                
                # Skip if not a supported image format
                if not self._is_valid_image_format(filename):
                    logger.warning("Skipping unsupported format: %s", filename)
                    return None
                
                filepath = self.settings.images_dir / filename
                
                # Skip if file already exists
                if filepath.exists():
                    logger.info("Image already exists: %s", filename)
                    return filepath
                
                logger.info("Downloading image: %s", url)
                
                async with session.get(url) as response:
                    response.raise_for_status()
                    
                    # Check content type
                    content_type = response.headers.get('content-type', '')
                    if not content_type.startswith('image/'):
                        logger.warning("URL does not appear to be an image: %s", url)
                        return None
                    
                    # Check file size
                    content_length = response.headers.get('content-length')
                    if content_length:
                        size_mb = int(content_length) / (1024 * 1024)
                        if size_mb > self.settings.max_image_size_mb:
                            logger.warning("Image too large (%s MB): %s", size_mb, url)
                            return None
                    
                    # Download and save
                    async with aiofiles.open(filepath, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                
                # Verify the image is valid
                try:
                    with Image.open(filepath) as img:
                        img.verify()
                    logger.info("Successfully downloaded image: %s", filename)
                    return filepath
                    
                except Exception as e:
                    logger.error("Invalid image file, removing: %s (%s)", filename, e)
                    filepath.unlink(missing_ok=True)
                    return None
                
            except aiohttp.ClientError as e:
                logger.error("HTTP error downloading %s: %s", url, e)
                return None
            except Exception as e:
                logger.error("Unexpected error downloading %s: %s", url, e)
                return None
            finally:
                # Rate limiting
                await asyncio.sleep(self.settings.rate_limit_delay)
    
    async def download_images(self, image_urls: List[str], item_id: str, metadata: Optional[Dict[str, Any]] = None) -> List[Path]:
        """
        Download multiple images for an item
        
        Args:
            image_urls: List of image URLs to download
            item_id: The item identifier
            metadata: Optional metadata about the item
            
        Returns:
            List of successfully downloaded image file paths
        """
        if metadata is None:
            metadata = {}
            
        logger.info("Downloading %d images for item: %s", len(image_urls), item_id)
        
        # Create download tasks
        tasks = [
            self._download_single_image(url, item_id, metadata)
            for url in image_urls
        ]
        
        # Execute downloads concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful downloads
        downloaded_files = []
        for result in results:
            if isinstance(result, Path):
                downloaded_files.append(result)
            elif isinstance(result, Exception):
                logger.error("Download task failed: %s", result)
        
        logger.info("Successfully downloaded %d of %d images for item: %s", 
                   len(downloaded_files), len(image_urls), item_id)
        
        return downloaded_files
    
    async def close(self):
        """Close the HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.debug("Image downloader session closed")
