#!/usr/bin/env python3
"""
AIDS Memorial Quilt Database Manager - Schema Compatibility Patch
Implements comprehensive error handling with specific exception types
Following project coding standards for digital humanities research applications
"""

async def get_records(self, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Get AIDS Memorial Quilt records with pagination (PATCHED VERSION)
    Implements error resilience and digital humanities research standards
    Addresses schema compatibility for quilt_panels table
    
    Args:
        limit: Maximum number of records to return (1-1000)
        offset: Number of records to skip (>=0)
        
    Returns:
        List of record dictionaries compatible with API format
        
    Raises:
        DatabaseConnectionError: If database connection fails
        DataValidationError: If parameters are invalid
    """
    try:
        if not self.connection:
            raise DatabaseConnectionError("Database not initialized")
        
        # Validate parameters following project error handling guidelines
        if limit <= 0 or limit > 1000:
            raise DataValidationError("Limit must be between 1 and 1000")
        if offset < 0:
            raise DataValidationError("Offset must be non-negative")
        
        # Use schema-corrected query for quilt_panels table
        query = """SELECT id, panel_id as item_id, title, description, json_extract(metadata_json, '$.subjects') as subjects, json_extract(metadata_json, '$.names') as names, scraped_at as dates, image_urls as url, image_urls as image_url, NULL as content_hash, scraped_at as created_at, updated_at
            FROM quilt_panels
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?"""
        
        cursor = await self.connection.execute(query, (limit, offset))
        rows = await cursor.fetchall()
        await cursor.close()
        
        # Convert rows to dictionaries with proper AIDS Memorial Quilt metadata handling
        columns = ["id", "item_id", "title", "description", "subjects", 
                  "names", "dates", "url", "image_url", "content_hash", 
                  "created_at", "updated_at"]
        
        records = []
        for row in rows:
            record = dict(zip(columns, row))
            
            # Parse JSON metadata fields safely for AIDS Memorial Quilt preservation
            for json_field in ["subjects", "names"]:
                field_value = record.get(json_field)
                if field_value and field_value != "NULL" and field_value is not None:
                    try:
                        # Handle various JSON formats for digital humanities metadata
                        if isinstance(field_value, str):
                            if field_value.startswith('[') or field_value.startswith('{'):
                                record[json_field] = json.loads(field_value)
                            else:
                                record[json_field] = [field_value]  # Convert single string to list
                        elif isinstance(field_value, list):
                            record[json_field] = field_value
                        else:
                            record[json_field] = [str(field_value)] if field_value else []
                    except (json.JSONDecodeError, TypeError) as parse_error:
                        logger.warning(f"AIDS Memorial Quilt DB: Error parsing {json_field}: {parse_error}")
                        record[json_field] = []
                else:
                    record[json_field] = []
            
            # Handle image_urls field for AIDS Memorial Quilt panels
            if record.get("url") or record.get("image_url"):
                for url_field in ["url", "image_url"]:
                    url_value = record.get(url_field)
                    if url_value and url_value != "NULL" and url_value is not None:
                        try:
                            # Parse image URLs JSON array for Library of Congress images
                            if isinstance(url_value, str) and url_value.startswith('['):
                                urls_list = json.loads(url_value)
                                if isinstance(urls_list, list) and urls_list:
                                    record[url_field] = urls_list[0]  # Use first URL
                                else:
                                    record[url_field] = None
                            # Keep string URLs as-is for Library of Congress attribution
                        except (json.JSONDecodeError, TypeError):
                            # Keep original value if not valid JSON
                            pass
            
            # Ensure proper Library of Congress attribution for dates
            if record.get("dates") and record["dates"] != "NULL":
                try:
                    if isinstance(record["dates"], str) and not record["dates"].startswith('['):
                        record["dates"] = [record["dates"]]
                except Exception:
                    record["dates"] = []
            else:
                record["dates"] = []
            
            records.append(record)
        
        logger.info(f"AIDS Memorial Quilt DB: Retrieved {len(records)} records from quilt_panels")
        return records
        
    except (DatabaseConnectionError, DataValidationError):
        raise
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt DB: Critical error getting records: {e}")
        raise DatabaseConnectionError(f"Failed to get records from AIDS Memorial Quilt database: {e}")

# Also update the primary data source determination logic:
async def _determine_primary_data_source(self) -> str:
    """
    Determine primary data source table for AIDS Memorial Quilt Records
    Implements intelligent table selection following digital humanities standards
    
    Returns:
        Name of the primary table to use for record retrieval
    """
    try:
        # Check table row counts to determine best data source
        tables_to_check = ["collection_items", "quilt_blocks", "quilt_panels"]
        table_stats = {}
        
        for table_name in tables_to_check:
            try:
                cursor = await self.connection.execute(f"SELECT COUNT(*) FROM {table_name}")
                result = await cursor.fetchone()
                table_stats[table_name] = result[0] if result else 0
                await cursor.close()
            except Exception:
                table_stats[table_name] = 0
        
        # Prefer collection_items if it has data, otherwise use table with most records
        if table_stats.get("collection_items", 0) > 0:
            primary_source = "collection_items"
        else:
            primary_source = max(table_stats.items(), key=lambda x: x[1])[0]
        
        logger.info(f"AIDS Memorial Quilt DB: Primary data source determined as '{primary_source}' ({', '.join([f'{k}: {v}' for k, v in table_stats.items()])})")
        return primary_source
        
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt DB: Error determining primary data source: {e}")
        return "quilt_blocks"  # Safe fallback
