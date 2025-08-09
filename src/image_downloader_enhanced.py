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
        # Use image-specific concurrency settings
        image_concurrency = getattr(settings, 'max_concurrent_image_downloads', settings.max_concurrent_downloads)
        self.semaphore = asyncio.Semaphore(image_concurrency)
        
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
    
    def _parse_manuscript_info(self, url: str) -> tuple[Optional[str], Optional[str]]:
        """Extract manuscript page and resolution from URL"""
        manuscript = None
        resolution = None
        
        # Extract manuscript page (e.g., ms0001, ms0007, ms0020)
        if '_ms' in url:
            parts = url.split('_ms')
            if len(parts) > 1:
                # Get everything after _ms until the next /
                ms_part = parts[1].split('/')[0]
                manuscript = f"ms{ms_part}"
        
        # Extract resolution (e.g., pct:25, pct:50, pct:100)
        if '/pct:' in url:
            pct_part = url.split('/pct:')[1].split('/')[0]
            resolution = f"pct{pct_part.replace('.', '_')}"
        elif '/full/' in url:
            # Handle full resolution
            resolution = "full"
        
        return manuscript, resolution
    
    def _get_safe_filename(self, url: str, item_id: str) -> str:
        """Generate a safe filename from URL and item ID"""
        parsed_url = urlparse(url)
        path = unquote(parsed_url.path)
        
        # Extract manuscript and resolution info for better naming
        manuscript, resolution = self._parse_manuscript_info(url)
        
        # Try to determine file extension
        if '.jpg' in path.lower():
            ext = '.jpg'
        elif '.jpeg' in path.lower():
            ext = '.jpg'
        elif '.png' in path.lower():
            ext = '.png'
        elif '.tiff' in path.lower() or '.tif' in path.lower():
            ext = '.tiff'
        else:
            # Default to jpg for IIIF images
            ext = '.jpg'
        
        # Build filename with manuscript and resolution info
        if manuscript and resolution:
            # Format: afc2019048_0001_ms0001_pct25.jpg
            safe_filename = f"{item_id}_{manuscript}_{resolution}{ext}"
        elif manuscript:
            # Format: afc2019048_0001_ms0001.jpg
            safe_filename = f"{item_id}_{manuscript}{ext}"
        else:
            # Fallback to hash-based naming
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            safe_filename = f"{item_id}_{url_hash}{ext}"
        
        # Ensure safe characters only
        safe_filename = "".join(c for c in safe_filename if c.isalnum() or c in '.-_')
        
        return safe_filename
    
    def _is_valid_image_format(self, filename: str) -> bool:
        """Check if the file has a supported image format"""
        return any(filename.lower().endswith(fmt) for fmt in self.settings.supported_image_formats)
    
    async def _download_single_image(self, url: str, item_id: str, metadata: Dict[str, Any], max_retries: int = 3) -> Optional[Path]:
        """Download a single image with concurrency control and retry logic"""
        async with self.semaphore:
            session = await self._get_session()
            
            # Parse manuscript and resolution info
            manuscript, resolution = self._parse_manuscript_info(url)
            
            # Generate safe filename
            filename = self._get_safe_filename(url, item_id)
            
            # Skip if not a supported image format
            if not self._is_valid_image_format(filename):
                logger.warning("Skipping unsupported format: %s", filename)
                return None
            
            # Create organized directory structure
            block_num = item_id.split('_')[-1]  # Extract 0001 from afc2019048_0001
            block_dir = self.settings.images_dir / f"block_{block_num}"
            
            # Create manuscript subdirectory if we have manuscript info
            if manuscript:
                target_dir = block_dir / manuscript
            else:
                target_dir = block_dir
            
            # Ensure directory exists
            target_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = target_dir / filename
            
            # Skip if file already exists
            if filepath.exists():
                logger.info("Image already exists: %s", filepath.relative_to(self.settings.images_dir))
                return filepath
            
            # Retry logic
            for attempt in range(max_retries + 1):
                try:
                    logger.debug("Downloading image (attempt %d/%d): %s -> %s", 
                               attempt + 1, max_retries + 1, url, filepath.relative_to(self.settings.images_dir))
                    
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
                        
                        # Log with size info
                        file_size = filepath.stat().st_size
                        logger.info("Downloaded image: %s (%s bytes)", 
                                  filepath.relative_to(self.settings.images_dir), 
                                  f"{file_size:,}")
                        
                        # Rate limiting - only delay on successful download
                        image_delay = getattr(self.settings, 'image_download_delay', 1.0)
                        await asyncio.sleep(image_delay)
                        
                        return filepath
                        
                    except Exception as e:
                        logger.error("Invalid image file, removing: %s (%s)", filename, e)
                        filepath.unlink(missing_ok=True)
                        # Don't return None here, continue to retry
                        if attempt < max_retries:
                            continue
                        return None
                
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt < max_retries:
                        delay = (2 ** attempt) * 1.0  # Exponential backoff: 1s, 2s, 4s
                        logger.warning("Error downloading %s (attempt %d/%d): %s. Retrying in %ss...", 
                                     url, attempt + 1, max_retries + 1, e, delay)
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error("Failed to download after %d attempts: %s (%s)", max_retries + 1, url, e)
                        return None
                except Exception as e:
                    logger.error("Unexpected error downloading %s: %s", url, e)
                    return None
            
            return None
    
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
