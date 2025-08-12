# AIDS Memorial Quilt Database Manager - get_records() Method Patch
# Replace the existing get_records() method in src/database.py with this version


async def get_records(self, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Get AIDS Memorial Quilt records with pagination (SCHEMA-COMPATIBLE VERSION)
    Implements error resilience and digital humanities research standards
    Addresses metadata_json column compatibility for quilt_blocks table
    
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
        
        # Determine primary data source with error resilience
        try:
            cursor = await self.connection.execute("SELECT COUNT(*) FROM collection_items")
            collection_items_count = (await cursor.fetchone())[0]
            await cursor.close()
            
            cursor = await self.connection.execute("SELECT COUNT(*) FROM quilt_blocks")
            quilt_blocks_count = (await cursor.fetchone())[0]
            await cursor.close()
            
            cursor = await self.connection.execute("SELECT COUNT(*) FROM quilt_panels")
            quilt_panels_count = (await cursor.fetchone())[0]
            await cursor.close()
            
            # Use collection_items if it has data, otherwise use table with most records
            if collection_items_count > 0:
                primary_table = "collection_items"
                logger.info(f"AIDS Memorial Quilt DB: Using collection_items as primary source ({collection_items_count:,} records)")
            elif quilt_blocks_count >= quilt_panels_count:
                primary_table = "quilt_blocks"
                logger.info(f"AIDS Memorial Quilt DB: Using quilt_blocks as primary source ({quilt_blocks_count:,} records)")
            else:
                primary_table = "quilt_panels"
                logger.info(f"AIDS Memorial Quilt DB: Using quilt_panels as primary source ({quilt_panels_count:,} records)")
                
        except Exception as source_error:
            logger.warning(f"AIDS Memorial Quilt DB: Error determining data source: {source_error}")
            primary_table = "quilt_blocks"  # Safe fallback
        
        # Execute schema-compatible query based on primary table
        if primary_table == "collection_items":
            # collection_items has the correct schema already
            query = """
                SELECT id, item_id, title, description, subjects, names, dates,
                       url, image_url, content_hash, created_at, updated_at
                FROM collection_items
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """
        elif primary_table == "quilt_blocks":
            # quilt_blocks requires column mapping for compatibility
            query = """
                SELECT id, block_id as item_id, title, description, 
                       CASE 
                           WHEN metadata_json IS NOT NULL AND metadata_json != '{}' 
                           THEN json_extract(metadata_json, '$.subjects')
                           ELSE NULL 
                       END as subjects,
                       CASE 
                           WHEN metadata_json IS NOT NULL AND metadata_json != '{}' 
                           THEN json_extract(metadata_json, '$.names')
                           ELSE NULL 
                       END as names,
                       created_date as dates,
                       NULL as url, NULL as image_url, NULL as content_hash,
                       scraped_at as created_at, updated_at
                FROM quilt_blocks
                ORDER BY id DESC
                LIMIT ? OFFSET ?
            """
        else:  # quilt_panels
            # quilt_panels requires different column mapping
            query = """
                SELECT id, panel_id as item_id, title, description,
                       CASE 
                           WHEN metadata_json IS NOT NULL AND metadata_json != '{}' 
                           THEN json_extract(metadata_json, '$.subjects')
                           ELSE NULL 
                       END as subjects,
                       CASE 
                           WHEN metadata_json IS NOT NULL AND metadata_json != '{}' 
                           THEN json_extract(metadata_json, '$.names')
                           ELSE NULL 
                       END as names,
                       scraped_at as dates,
                       image_urls as url, image_urls as image_url, NULL as content_hash,
                       scraped_at as created_at, updated_at
                FROM quilt_panels
                ORDER BY id DESC
                LIMIT ? OFFSET ?
            """
        
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
                                parsed_value = json.loads(field_value)
                                if isinstance(parsed_value, list):
                                    record[json_field] = parsed_value
                                elif isinstance(parsed_value, dict):
                                    record[json_field] = [str(parsed_value)]
                                else:
                                    record[json_field] = [str(parsed_value)]
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
            for url_field in ["url", "image_url"]:
                url_value = record.get(url_field)
                if url_value and url_value != "NULL" and url_value is not None:
                    try:
                        # Parse image URLs JSON array for Library of Congress images
                        if isinstance(url_value, str) and url_value.startswith('['):
                            urls_list = json.loads(url_value)
                            if isinstance(urls_list, list) and urls_list:
                                record[url_field] = urls_list[0]  # Use first URL for compatibility
                            else:
                                record[url_field] = None
                        # Keep string URLs as-is for Library of Congress attribution
                    except (json.JSONDecodeError, TypeError):
                        # Keep original value if not valid JSON
                        pass
                else:
                    record[url_field] = None
            
            # Ensure proper Library of Congress attribution for dates
            dates_value = record.get("dates")
            if dates_value and dates_value != "NULL":
                try:
                    if isinstance(dates_value, str) and not dates_value.startswith('['):
                        record["dates"] = [dates_value]
                    elif isinstance(dates_value, str) and dates_value.startswith('['):
                        record["dates"] = json.loads(dates_value)
                    else:
                        record["dates"] = [str(dates_value)] if dates_value else []
                except Exception:
                    record["dates"] = []
            else:
                record["dates"] = []
            
            # Ensure string fields are properly handled
            for string_field in ["title", "description"]:
                if record.get(string_field) is None:
                    record[string_field] = ""
            
            records.append(record)
        
        logger.info(f"AIDS Memorial Quilt DB: Retrieved {len(records)} records from {primary_table}")
        return records
        
    except (DatabaseConnectionError, DataValidationError):
        raise
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt DB: Critical error getting records: {e}")
        import traceback
        logger.error(f"AIDS Memorial Quilt DB: Full traceback: {traceback.format_exc()}")
        raise DatabaseConnectionError(f"Failed to get records from AIDS Memorial Quilt database: {e}")
