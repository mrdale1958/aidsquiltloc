"""
AIDS Memorial Quilt Records Scraper Package
Following project coding standards with comprehensive error handling and type safety
Implements separation of concerns for digital humanities research applications
"""

from .database import DatabaseManager

# Only export what actually exists and is needed
__all__ = [
    'DatabaseManager'
]

# Version information following project documentation standards
__version__ = "2.0.0"
__author__ = "AIDS Memorial Quilt Records Project"
__description__ = "Enhanced scraper for Library of Congress AIDS Memorial Quilt Records collection"