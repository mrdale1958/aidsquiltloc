"""
Metadata extractor for AIDS Memorial Quilt Records
Processes and normalizes metadata from Library of Congress API responses
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import json

from src.loc_api_client import LOCAPIClient, LOCAPISettings
from config.settings import ScraperConfig

logger = logging.getLogger(__name__)


class MetadataExtractionError(Exception):
    """Exception raised during metadata extraction process following error handling guidelines"""
    pass


class MetadataExtractor:
    """
    Extracts and normalizes metadata from LOC API responses
    
    Handles the complex nested structure of LOC JSON responses and extracts
    relevant information for AIDS Memorial Quilt blocks and panels following
    project architecture patterns.
    """
    
    def __init__(self, config: ScraperConfig) -> None:
        """
        Initialize the metadata extractor
        
        Args:
            config: Scraper configuration settings
        """
        self.config = config
        self.api_client: Optional[LOCAPIClient] = None
        
        # Metadata field mappings per project domain knowledge
        self.field_mappings = {
            'title': ['title', 'item.title'],
            'description': ['description', 'item.summary', 'summary'],
            'date': ['date', 'item.date', 'created_published'],
            'creator': ['creator', 'item.contributors'],
            'subject': ['subject', 'item.subjects'],
            'type': ['type', 'item.original_format'],
            'identifier': ['id', 'item.id'],
            'rights': ['rights', 'item.rights'],
            'language': ['language', 'item.language'],
            'location': ['location', 'repository']
        }
    
    async def extract_item_metadata(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Extract comprehensive metadata for a specific item with error resilience
        
        Args:
            item_id: The LOC item identifier
            
        Returns:
            Normalized metadata dictionary or None if extraction fails
            
        Raises:
            MetadataExtractionError: If metadata extraction fails
        """
        try:
            # Initialize API client if needed per resource management practices
            if not self.api_client:
                self.api_client = LOCAPIClient(LOCAPISettings())
                await self.api_client.initialize_session()
            
            logger.info("Extracting metadata for item: %s", item_id)
            
            # Get raw metadata from API with error handling
            raw_metadata = await self.api_client.get_item_metadata(item_id)
            
            if not raw_metadata:
                logger.warning("No metadata returned for item: %s", item_id)
                return None
            
            # Extract and normalize metadata per project standards
            normalized_metadata = self._normalize_metadata(raw_metadata, item_id)
            
            # Add extraction timestamp following documentation standards - FIXED deprecation warning
            normalized_metadata['extracted_at'] = datetime.now(timezone.utc).isoformat()
            normalized_metadata['source_item_id'] = item_id
            
            logger.info("Successfully extracted metadata for item: %s", item_id)
            return normalized_metadata
            
        except Exception as e:
            logger.error("Unexpected error extracting metadata for %s: %s", item_id, e)
            raise MetadataExtractionError(f"Extraction error: {e}")
    
    def _normalize_metadata(self, raw_data: Dict[str, Any], item_id: str) -> Dict[str, Any]:
        """
        Normalize raw LOC metadata into a consistent structure following data validation practices
        
        Args:
            raw_data: Raw metadata from LOC API
            item_id: Item identifier for context
            
        Returns:
            Normalized metadata dictionary
        """
        normalized = {
            'item_id': item_id,
            'raw_metadata': raw_data  # Keep original for reference per archival practices
        }
        
        # Extract item-level metadata following domain knowledge
        item_data = raw_data.get('item', {})
        
        # Map standard fields using field mappings per configuration management
        for field, possible_keys in self.field_mappings.items():
            value = self._extract_field_value(raw_data, item_data, possible_keys)
            if value:
                normalized[field] = value
        
        return normalized
    
    def _extract_field_value(self, 
                           raw_data: Dict[str, Any], 
                           item_data: Dict[str, Any], 
                           possible_keys: List[str]) -> Optional[Any]:
        """
        Extract field value from multiple possible locations in metadata
        
        Args:
            raw_data: Top-level raw metadata
            item_data: Item-specific metadata
            possible_keys: List of possible field names to check
            
        Returns:
            Extracted field value or None
        """
        for key in possible_keys:
            # Check item-level data first per API structure knowledge
            if '.' in key:
                # Handle nested keys like 'item.title'
                parts = key.split('.')
                current = raw_data
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        current = None
                        break
                if current:
                    return self._clean_field_value(current)
            else:
                # Check both item_data and raw_data
                if key in item_data:
                    return self._clean_field_value(item_data[key])
                elif key in raw_data:
                    return self._clean_field_value(raw_data[key])
        
        return None
    
    def _clean_field_value(self, value: Any) -> Any:
        """
        Clean and normalize field values following data validation practices
        
        Args:
            value: Raw field value
            
        Returns:
            Cleaned field value
        """
        if isinstance(value, list):
            if len(value) == 1:
                return self._clean_field_value(value[0])
            else:
                return [self._clean_field_value(v) for v in value if v]
        elif isinstance(value, str):
            return value.strip()
        else:
            return value
    
    async def close(self) -> None:
        """Clean up resources following proper resource management and error resilience"""
        if self.api_client:
            try:
                await self.api_client.close_session()
                self.api_client = None
                logger.debug("Metadata extractor resources cleaned up")
            except Exception as e:
                logger.warning("Error during metadata extractor cleanup: %s", e)


# Export classes following project naming conventions
__all__ = ['MetadataExtractor', 'MetadataExtractionError']
