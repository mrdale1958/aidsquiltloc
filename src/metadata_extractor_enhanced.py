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
        
    async def extract_item_metadata(self, item_data: Dict[str, Any], 
                                   item_details: Dict[str, Any] = None,
                                   resources: List[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Extract metadata for a single item and return it (without saving to database)
        
        Args:
            item_data: Basic item data from search results
            item_details: Detailed item information  
            resources: List of available resources
            
        Returns:
            Dictionary containing extracted metadata, or None if extraction failed
        """
        try:
            # Extract item ID
            item_id = self._extract_item_id(item_data)
            if not item_id:
                logger.warning("Could not extract item ID from: %s", item_data.get('id', 'unknown'))
                return None
            
            # Merge metadata from different sources
            merged_metadata = self._merge_metadata(item_data, item_details)
            
            # Extract different types of content
            image_urls = self._extract_image_urls(merged_metadata, item_details, resources)
            resource_urls = self._extract_resource_urls(merged_metadata, item_details, resources)
            
            # Add extracted URLs to metadata
            merged_metadata['image_urls'] = image_urls
            merged_metadata['resource_urls'] = resource_urls
            merged_metadata['item_id'] = item_id
            
            return merged_metadata
            
        except Exception as e:
            logger.error("Error extracting metadata for item: %s", e)
            return None

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
            
            # Extract different types of content
            image_urls = self._extract_image_urls(merged_metadata, item_details, resources)
            resource_urls = self._extract_resource_urls(merged_metadata, item_details, resources)
            
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
            if isinstance(item_data['image_url'], list):
                image_urls.extend(item_data['image_url'])
            elif item_data['image_url']:
                image_urls.append(item_data['image_url'])
        
        # From item details
        if item_details and 'image_url' in item_details:
            if isinstance(item_details['image_url'], list):
                image_urls.extend(item_details['image_url'])
            elif item_details['image_url']:
                image_urls.append(item_details['image_url'])
        
        # From nested item section in item_details
        if item_details and 'item' in item_details and isinstance(item_details['item'], dict):
            item_section = item_details['item']
            if 'image_url' in item_section:
                if isinstance(item_section['image_url'], list):
                    image_urls.extend(item_section['image_url'])
                elif item_section['image_url']:
                    image_urls.append(item_section['image_url'])
        
        # From item_details resources section (contains the actual file arrays)
        if item_details and 'resources' in item_details and isinstance(item_details['resources'], list):
            for resource in item_details['resources']:
                if isinstance(resource, dict):
                    # Look for direct image URLs
                    if 'image' in resource and resource['image']:
                        image_urls.append(resource['image'])
                    
                    # Look for files array (nested format from LOC API)
                    if 'files' in resource:
                        files = resource['files']
                        if isinstance(files, list):
                            # Handle nested array structure [[manuscript1_files], [manuscript2_files], ...]
                            for file_group in files:
                                if isinstance(file_group, list):
                                    for file_info in file_group:
                                        if isinstance(file_info, dict) and 'url' in file_info and 'mimetype' in file_info:
                                            # Only extract image files (JPEG, JP2, etc.)
                                            mimetype = file_info['mimetype'].lower()
                                            if 'image' in mimetype:
                                                image_urls.append(file_info['url'])
        
        # From resources
        if resources:
            for resource in resources:
                if isinstance(resource, dict):
                    # Look for direct image URLs
                    if 'image' in resource and resource['image']:
                        image_urls.append(resource['image'])
                    
                    # Look for files array (new nested format from item_details)
                    if 'files' in resource:
                        files = resource['files']
                        if isinstance(files, list):
                            # Handle nested array structure [[manuscript1_files], [manuscript2_files], ...]
                            for file_group in files:
                                if isinstance(file_group, list):
                                    for file_info in file_group:
                                        if isinstance(file_info, dict) and 'url' in file_info and 'mimetype' in file_info:
                                            # Only extract image files (JPEG, JP2, etc.)
                                            mimetype = file_info['mimetype'].lower()
                                            if 'image' in mimetype:
                                                image_urls.append(file_info['url'])
                                elif isinstance(file_group, list) and len(file_group) >= 2:
                                    # Legacy format: [url, file_type]
                                    url, file_type = file_group[0], file_group[1]
                                    if 'image' in file_type.lower() or url.lower().endswith(('.jpg', '.jpeg', '.png', '.tiff', '.tif')):
                                        image_urls.append(url)
                        elif isinstance(files, int):
                            # Just a count - not actual file URLs
                            pass
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in image_urls:
            if url and url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        return unique_urls
    
    def _extract_resource_urls(self, item_data: Dict[str, Any] = None, item_details: Dict[str, Any] = None, resources: List[Dict[str, Any]] = None) -> List[str]:
        """Extract non-image resource URLs from all available sources"""
        resource_urls = []
        
        # Debug logging to understand API structure
        item_id = item_data.get('id', 'unknown') if item_data else 'unknown'
        logger.debug(f"Extracting resources for {item_id}")
        logger.debug(f"item_data keys: {list(item_data.keys()) if item_data else 'None'}")
        logger.debug(f"item_details keys: {list(item_details.keys()) if item_details else 'None'}")
        logger.debug(f"resources type: {type(resources)}, length: {len(resources) if resources else 0}")
        
        # Check item_data for resources
        if item_data:
            # Look for direct resource fields
            for key in ['resources', 'files', 'documents', 'pdf', 'url']:
                if key in item_data:
                    value = item_data[key]
                    logger.debug(f"Found {key} in item_data: {type(value)} = {value}")
                    if isinstance(value, str) and (value.startswith('http') or value.startswith('https')):
                        if not self._is_image_url(value):
                            resource_urls.append(value)
                            logger.info(f"Found resource URL in item_data[{key}]: {value}")
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, str) and (item.startswith('http') or item.startswith('https')):
                                if not self._is_image_url(item):
                                    resource_urls.append(item)
                                    logger.info(f"Found resource URL in item_data[{key}] list: {item}")
        
        # Check item_details for resources  
        if item_details:
            # Check top-level for resources
            for key in ['resources', 'files', 'documents', 'pdf', 'url']:
                if key in item_details:
                    value = item_details[key]
                    logger.debug(f"Found {key} in item_details: {type(value)} = {value}")
                    if isinstance(value, str) and (value.startswith('http') or value.startswith('https')):
                        if not self._is_image_url(value):
                            resource_urls.append(value)
                            logger.info(f"Found resource URL in item_details[{key}]: {value}")
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, str) and (item.startswith('http') or item.startswith('https')):
                                if not self._is_image_url(item):
                                    resource_urls.append(item)
                                    logger.info(f"Found resource URL in item_details[{key}] list: {item}")
            
            # Check nested 'item' object
            if 'item' in item_details and isinstance(item_details['item'], dict):
                item_obj = item_details['item']
                logger.debug(f"Found nested item object with keys: {list(item_obj.keys())}")
                for key in ['resources', 'files', 'documents', 'pdf', 'url', 'online_format']:
                    if key in item_obj:
                        value = item_obj[key]
                        logger.debug(f"Found {key} in nested item: {type(value)} = {value}")
                        if isinstance(value, str) and (value.startswith('http') or value.startswith('https')):
                            if not self._is_image_url(value):
                                resource_urls.append(value)
                                logger.info(f"Found resource URL in nested item[{key}]: {value}")
                        elif isinstance(value, list):
                            for item in value:
                                if isinstance(item, str) and (item.startswith('http') or item.startswith('https')):
                                    if not self._is_image_url(item):
                                        resource_urls.append(item)
                                        logger.info(f"Found resource URL in nested item[{key}] list: {item}")
                                elif isinstance(item, dict):
                                    # Check nested objects for URLs
                                    for nested_key, nested_value in item.items():
                                        if isinstance(nested_value, str) and (nested_value.startswith('http') or nested_value.startswith('https')):
                                            if not self._is_image_url(nested_value):
                                                resource_urls.append(nested_value)
                                                logger.info(f"Found resource URL in nested item[{key}][{nested_key}]: {nested_value}")
        
        # Check dedicated resources array
        if resources:
            logger.debug(f"Processing {len(resources)} resources")
            for i, resource in enumerate(resources):
                if isinstance(resource, dict):
                    logger.debug(f"Resource {i} keys: {list(resource.keys())}")
                    # Look for direct PDF URLs
                    if 'pdf' in resource and resource['pdf']:
                        resource_urls.append(resource['pdf'])
                        logger.info(f"Found PDF URL in resources[{i}]: {resource['pdf']}")
                    
                    # Look for general resource URLs
                    if 'url' in resource and resource['url']:
                        if not self._is_image_url(resource['url']):
                            resource_urls.append(resource['url'])
                            logger.info(f"Found resource URL in resources[{i}]: {resource['url']}")
                    
                    # Look for files array (legacy format)
                    if 'files' in resource:
                        files = resource['files']
                        logger.debug(f"Found files in resource {i}: {type(files)} = {files}")
                        if isinstance(files, list):
                            for file_info in files:
                                if isinstance(file_info, list) and len(file_info) >= 2:
                                    url, file_type = file_info[0], file_info[1]
                                    # Include PDFs, text files, etc., but not images
                                    if not self._is_image_url(url):
                                        resource_urls.append(url)
                                        logger.info(f"Found resource URL in files array: {url} ({file_type})")
                                elif isinstance(file_info, dict):
                                    # Handle dict format files
                                    for key, value in file_info.items():
                                        if isinstance(value, str) and (value.startswith('http') or value.startswith('https')):
                                            if not self._is_image_url(value):
                                                resource_urls.append(value)
                                                logger.info(f"Found resource URL in file dict[{key}]: {value}")
                    
                    # Look for other potential resource fields
                    for key, value in resource.items():
                        if key not in ['pdf', 'url', 'files', 'image', 'representative_index', 'segments', 'search', 'word_coordinates']:
                            if isinstance(value, str) and (value.startswith('http') or value.startswith('https')):
                                if not self._is_image_url(value):
                                    resource_urls.append(value)
                                    logger.info(f"Found resource URL in resource[{key}]: {value}")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in resource_urls:
            if url and url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        logger.info(f"Total resource URLs found for {item_id}: {len(unique_urls)}")
        if unique_urls:
            for url in unique_urls:
                logger.info(f"  - {url}")
        
        return unique_urls
    
    def _is_image_url(self, url: str) -> bool:
        """Check if a URL points to an image file"""
        if not url:
            return False
        
        url_lower = url.lower()
        image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.tiff', '.tif', '.bmp', '.webp')
        
        # Check file extension
        if url_lower.endswith(image_extensions):
            return True
        
        # Check for image-related keywords in URL
        image_keywords = ['image', 'img', 'photo', 'picture', 'thumbnail']
        if any(keyword in url_lower for keyword in image_keywords):
            return True
        
        return False
    
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
