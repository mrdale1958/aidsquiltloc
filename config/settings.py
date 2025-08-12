"""
Configuration settings for AIDS Memorial Quilt Records scraper
Centralized configuration management with environment variable support
"""

import os
from pathlib import Path
from dataclasses import dataclass

# Load environment variables if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, continue without it


@dataclass
class ScraperConfig:
    """
    Main configuration class for the AIDS Memorial Quilt scraper
    
    Attributes:
        output_dir: Base directory for storing scraped data
        database_path: Path to SQLite database file
        rate_limit_delay: Delay between API requests in seconds
        max_concurrent_downloads: Maximum concurrent image downloads
        request_timeout: Timeout for HTTP requests in seconds
        user_agent: User agent string for HTTP requests
        max_retries: Maximum retry attempts for failed operations
        chunk_size: File download chunk size in bytes
    """
    
    output_dir: Path = Path("output")
    database_path: Path = Path("output/quilt_data.db")
    rate_limit_delay: float = 1.0
    max_concurrent_downloads: int = 5
    request_timeout: int = 30
    user_agent: str = "AIDS-Memorial-Quilt-Scraper/1.0 (Educational Research)"
    max_retries: int = 3
    chunk_size: int = 8192
    
    def __post_init__(self) -> None:
        """Initialize configuration with environment variable overrides following project standards"""
        # Override with environment variables if present
        if env_output_dir := os.getenv("SCRAPER_OUTPUT_DIR"):
            self.output_dir = Path(env_output_dir)
        
        if env_db_path := os.getenv("SCRAPER_DATABASE_PATH"):
            self.database_path = Path(env_db_path)
        
        if env_rate_limit := os.getenv("SCRAPER_RATE_LIMIT_DELAY"):
            try:
                self.rate_limit_delay = float(env_rate_limit)
            except ValueError:
                pass  # Keep default value per error handling guidelines
        
        if env_max_downloads := os.getenv("SCRAPER_MAX_CONCURRENT_DOWNLOADS"):
            try:
                self.max_concurrent_downloads = int(env_max_downloads)
            except ValueError:
                pass  # Keep default value
        
        if env_timeout := os.getenv("SCRAPER_REQUEST_TIMEOUT"):
            try:
                self.request_timeout = int(env_timeout)
            except ValueError:
                pass  # Keep default value
        
        # Ensure required directories exist following safe file handling practices
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
    
    @property
    def images_dir(self) -> Path:
        """Get the images output directory"""
        return self.output_dir / "images"
    
    @property
    def metadata_dir(self) -> Path:
        """Get the metadata output directory"""
        return self.output_dir / "metadata"
    
    @property
    def logs_dir(self) -> Path:
        """Get the logs output directory"""
        return self.output_dir / "logs"


# Export only what's defined in this module
__all__ = ['ScraperConfig']
