"""
Metadata extractor for AIDS Memorial Quilt Records
Extracts and processes metadata from LOC API responses
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

import aiofiles

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Extracts and saves metadata from LOC collection items"""
    
    def __init__(self, settings):
        self.settings = settings
        
    def _extract_basic_info(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic information from an item"""
        return {
            'id': item.get('id', ''),
            'title': item.get('title', ''),
            'url': item.get('url', ''),
            'permalink': item.get('permalink', ''),
            'created_date': item.get('date', ''),
            'description': item.get('description', ''),
            'subjects': item.get('subject', []),
            'contributors': item.get('contributor', []),
            'creators': item.get('creator', []),
            'language': item.get('language', []),
            'location': item.get('location', []),
            'original_format': item.get('original_format', []),
            'online_format': item.get('online_format', []),
        }
    
    def _extract_image_urls(self, item: Dict[str, Any]) -> List[str]:
        """Extract image URLs from an item"""
        image_urls = []
        
        # Check for image URLs in various fields
        if 'image_url' in item:
            urls = item['image_url']
            if isinstance(urls, str):
                image_urls.append(urls)
            elif isinstance(urls, list):
                image_urls.extend(urls)
        
        # Check for resources with image links
        if 'resources' in item:
            for resource in item.get('resources', []):
                if isinstance(resource, dict):
                    # Look for image files in resource
                    for key, value in resource.items():
                        if 'image' in key.lower() and isinstance(value, str):
                            if any(ext in value.lower() for ext in ['.jpg', '.jpeg', '.png', '.tiff', '.tif']):
                                image_urls.append(value)
        
        # Check for thumbnail or preview images
        for field in ['thumbnail', 'preview', 'image']:
            if field in item:
                value = item[field]
                if isinstance(value, str) and value.startswith('http'):
                    image_urls.append(value)
                elif isinstance(value, list):
                    image_urls.extend([url for url in value if isinstance(url, str) and url.startswith('http')])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in image_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        return unique_urls
    
    def _extract_relationships(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relationship information"""
        return {
            'is_part_of': item.get('is_part_of', []),
            'related_items': item.get('related_items', []),
            'collection': item.get('collection', {}),
            'series': item.get('series', []),
        }
    
    def _extract_technical_metadata(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract technical metadata"""
        return {
            'file_size': item.get('file_size', ''),
            'dimensions': item.get('dimensions', ''),
            'color': item.get('color', ''),
            'medium': item.get('medium', []),
            'digital_id': item.get('digital_id', ''),
            'call_number': item.get('call_number', []),
            'reproduction_number': item.get('reproduction_number', ''),
        }
    
    def _extract_rights_info(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract rights and access information"""
        return {
            'rights': item.get('rights', ''),
            'access_restricted': item.get('access_restricted', False),
            'usage_rights': item.get('usage_rights', ''),
            'rights_holder': item.get('rights_holder', ''),
        }
    
    async def extract_metadata(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from a collection item
        
        Args:
            item: The item data from LOC API
            
        Returns:
            Processed metadata dictionary
        """
        metadata = {
            'extraction_timestamp': datetime.now().isoformat(),
            'source': 'Library of Congress AIDS Memorial Quilt Records',
            'api_version': 'v1',
        }
        
        # Extract different types of metadata
        metadata.update(self._extract_basic_info(item))
        metadata['image_urls'] = self._extract_image_urls(item)
        metadata['relationships'] = self._extract_relationships(item)
        metadata['technical'] = self._extract_technical_metadata(item)
        metadata['rights'] = self._extract_rights_info(item)
        
        # Add raw data for completeness
        metadata['raw_data'] = item
        
        logger.info("Extracted metadata for item: %s", metadata.get('id', 'Unknown'))
        
        return metadata
    
    async def save_metadata(self, metadata: Dict[str, Any]) -> Path:
        """
        Save metadata to file
        
        Args:
            metadata: The metadata dictionary to save
            
        Returns:
            Path to the saved metadata file
        """
        item_id = metadata.get('id', 'unknown')
        
        if self.settings.metadata_format.lower() == 'json':
            filename = f"{item_id}_metadata.json"
            filepath = self.settings.metadata_dir / filename
            
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(metadata, indent=2, ensure_ascii=False))
                
        else:
            # CSV format (simplified)
            filename = f"{item_id}_metadata.csv"
            filepath = self.settings.metadata_dir / filename
            
            # Flatten metadata for CSV
            flattened = self._flatten_dict(metadata)
            
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                # Write header
                await f.write(','.join(flattened.keys()) + '\n')
                # Write data
                await f.write(','.join(str(v) for v in flattened.values()) + '\n')
        
        logger.info("Saved metadata to: %s", filename)
        return filepath
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested dictionary for CSV export"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert lists to string representation
                items.append((new_key, '; '.join(str(item) for item in v)))
            else:
                items.append((new_key, v))
        return dict(items)
