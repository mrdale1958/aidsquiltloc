"""
Enhanced metadata extractor for AIDS Memorial Quilt Records with database integration
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from .database import DatabaseManager

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Enhanced metadata extractor with database storage and change tracking"""
    
    def __init__(self, settings, db_manager: DatabaseManager):
        self.settings = settings
        self.db_manager = db_manager
        
    async def process_item_metadata(self, item_data: Dict[str, Any], 
                                   item_details: Dict[str, Any] = None,
                                   resources: List[Dict[str, Any]] = None) -> bool:
        """
        Process and store metadata for a single item
        
        Args:
            item_data: Basic item data from search results
            item_details: Detailed item information
            resources: List of available resources
            
        Returns:
            True if the item was updated (new or changed), False if no change
        """
        try:
            # Extract item ID
            item_id = self._extract_item_id(item_data)
            if not item_id:
                logger.warning("Could not extract item ID from: %s", item_data.get('id', 'unknown'))
                return False
            
            # Merge metadata from different sources
            merged_metadata = self._merge_metadata(item_data, item_details)
            
            # Extract image and resource URLs
            image_urls = self._extract_image_urls(item_data, item_details, resources)
            resource_urls = self._extract_resource_urls(resources)
            
            # Store in database
            was_updated = await self.db_manager.upsert_record(
                item_id=item_id,
                metadata=merged_metadata,
                image_urls=image_urls,
                resource_urls=resource_urls
            )
            
            if was_updated:
                logger.info("Updated metadata for item: %s", item_id)
            else:
                logger.debug("No changes for item: %s", item_id)
            
            return was_updated
            
        except Exception as e:
            logger.error("Error processing metadata for item: %s", e)
            return False
    
    def _extract_item_id(self, item_data: Dict[str, Any]) -> Optional[str]:
        """Extract the item ID from item data"""
        item_url = item_data.get('id', '')
        
        if '/item/' in item_url:
            return item_url.split('/item/')[-1].rstrip('/')
        
        return None
    
    def _merge_metadata(self, item_data: Dict[str, Any], 
                       item_details: Dict[str, Any] = None) -> Dict[str, Any]:
        """Merge metadata from search results and detailed item info"""
        merged = item_data.copy()
        
        if item_details:
            # Merge item details, preferring more detailed information
            for key, value in item_details.items():
                if key not in merged or not merged[key]:
                    merged[key] = value
                elif isinstance(value, dict) and 'item' in value:
                    # Handle nested item details
                    item_info = value['item']
                    for item_key, item_value in item_info.items():
                        if item_key not in merged or not merged[item_key]:
                            merged[item_key] = item_value
        
        return merged
    
    def _extract_image_urls(self, item_data: Dict[str, Any], 
                           item_details: Dict[str, Any] = None,
                           resources: List[Dict[str, Any]] = None) -> List[str]:
        """Extract all available image URLs"""
        image_urls = []
        
        # From basic item data
        if 'image_url' in item_data:
            image_urls.extend(item_data['image_url'])
        
        # From item details
        if item_details and 'image_url' in item_details:
            image_urls.extend(item_details['image_url'])
        
        # From resources
        if resources:
            for resource in resources:
                if isinstance(resource, dict):
                    # Look for image files
                    if 'files' in resource:
                        for file_info in resource['files']:
                            if isinstance(file_info, list) and len(file_info) >= 2:
                                url, file_type = file_info[0], file_info[1]
                                if 'image' in file_type.lower() or url.lower().endswith(('.jpg', '.jpeg', '.png', '.tiff', '.tif')):
                                    image_urls.append(url)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in image_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        return unique_urls
    
    def _extract_resource_urls(self, resources: List[Dict[str, Any]] = None) -> List[str]:
        """Extract non-image resource URLs"""
        resource_urls = []
        
        if resources:
            for resource in resources:
                if isinstance(resource, dict) and 'files' in resource:
                    for file_info in resource['files']:
                        if isinstance(file_info, list) and len(file_info) >= 2:
                            url, file_type = file_info[0], file_info[1]
                            # Include PDFs, text files, etc., but not images
                            if not ('image' in file_type.lower() or url.lower().endswith(('.jpg', '.jpeg', '.png', '.tiff', '.tif'))):
                                resource_urls.append(url)
        
        return resource_urls
    
    async def extract_memorial_names(self, item_data: Dict[str, Any]) -> List[str]:
        """
        Extract memorial names from quilt block metadata
        
        This is a sophisticated extraction that looks for names in various fields
        """
        names = []
        
        # Look in title
        title = item_data.get('title', '')
        if title:
            # Pattern like "AIDS Quilt Block 2621 Panel Maker Records"
            # The actual names are often in description or other fields
            pass
        
        # Look in description
        descriptions = item_data.get('description', [])
        if isinstance(descriptions, list):
            for desc in descriptions:
                if isinstance(desc, str):
                    # Look for patterns like "In memory of...", "Remembering..."
                    names.extend(self._extract_names_from_text(desc))
        
        # Look in subject fields
        subjects = item_data.get('subject', [])
        if isinstance(subjects, list):
            for subject in subjects:
                if isinstance(subject, str) and not any(keyword in subject.lower() for keyword in ['aids', 'quilt', 'memorial', 'disease']):
                    # Subject might contain a person's name
                    names.append(subject.strip())
        
        # Clean and deduplicate names
        cleaned_names = []
        for name in names:
            name = name.strip()
            if name and len(name) > 1 and name not in cleaned_names:
                cleaned_names.append(name)
        
        return cleaned_names
    
    def _extract_names_from_text(self, text: str) -> List[str]:
        """Extract potential names from descriptive text"""
        names = []
        
        # Simple patterns for name extraction
        # This could be enhanced with NLP libraries like spaCy
        text_lower = text.lower()
        
        # Pattern: "in memory of [name]"
        if 'in memory of' in text_lower:
            start_idx = text_lower.find('in memory of') + len('in memory of')
            remaining = text[start_idx:].strip()
            # Extract until punctuation or common stop words
            name_part = remaining.split('.')[0].split(',')[0].split(';')[0]
            if name_part:
                names.append(name_part.strip())
        
        # Pattern: "remembering [name]"
        if 'remembering' in text_lower:
            start_idx = text_lower.find('remembering') + len('remembering')
            remaining = text[start_idx:].strip()
            name_part = remaining.split('.')[0].split(',')[0].split(';')[0]
            if name_part:
                names.append(name_part.strip())
        
        return names
    
    async def get_update_statistics(self) -> Dict[str, Any]:
        """Get statistics about metadata updates"""
        return await self.db_manager.get_statistics()
