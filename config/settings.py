"""
Configuration settings for the AIDS Memorial Quilt Records scraper
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class Settings:
    """Configuration settings for the scraper"""
    
    # API Settings
    loc_api_base_url: str = "https://www.loc.gov"
    aids_quilt_collection_url: str = "https://www.loc.gov/collections/aids-memorial-quilt-records/"
    
    # Request settings
    request_timeout: int = 30
    max_concurrent_downloads: int = 3  # Reduced for more conservative API usage
    rate_limit_delay: float = 2.0  # Increased delay between requests
    
    # File paths
    base_dir: Path = Path(__file__).parent.parent
    output_dir: Path = base_dir / "output"
    images_dir: Path = output_dir / "images"
    metadata_dir: Path = output_dir / "metadata"
    
    # Image settings
    max_image_size_mb: int = 50
    supported_image_formats: tuple = ('.jpg', '.jpeg', '.png', '.tiff', '.tif')
    
    # Metadata settings
    metadata_format: str = "json"  # json or csv
    
    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_file: str = "scraper.log"
    
    # Optional API key (if needed for higher rate limits)
    api_key: Optional[str] = os.getenv("LOC_API_KEY")
    
    def __post_init__(self):
        """Ensure directories exist"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
