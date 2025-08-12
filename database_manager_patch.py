
# AIDS Memorial Quilt Database Manager Patch
# Apply this to src/database.py get_records() method

async def get_records(self, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Get AIDS Memorial Quilt records with pagination (PATCHED VERSION)
    Implements error resilience and digital humanities research standards
    
    Args:
        limit: Maximum number of records to return
        offset: Number of records to skip
        
    Returns:
        List of record dictionaries compatible with API format
    """
    try:
        if not self.connection:
            raise DatabaseConnectionError("Database not initialized")
        
        # Use corrected query for quilt_panels table
        query = """SELECT id, panel_id as item_id, title, description, metadata_json as subjects, metadata_json as names, scraped_at as dates, image_urls as url, image_urls as image_url, NULL as content_hash, scraped_at as created_at, updated_at
                FROM quilt_panels
                ORDER BY updated_at DESC
                LIMIT ? OFFSET ?"""
        
        cursor = await self.connection.execute(query, (limit, offset))
        rows = await cursor.fetchall()
        await cursor.close()
        
        # Convert rows to dictionaries with proper column names
        columns = ["id", "item_id", "title", "description", "subjects", 
                  "names", "dates", "url", "image_url", "content_hash", 
                  "created_at", "updated_at"]
        
        records = []
        for row in rows:
            record = dict(zip(columns, row))
            
            # Parse JSON fields safely for AIDS Memorial Quilt metadata
            for json_field in ["subjects", "names"]:
                if record.get(json_field) and record[json_field] != "NULL":
                    try:
                        # Handle metadata_json column that may contain structured data
                        if json_field in ["subjects", "names"] and "metadata_json" in str(record.get(json_field, "")):
                            metadata = json.loads(record[json_field]) if record[json_field] else {}
                            record[json_field] = metadata.get(json_field, [])
                        else:
                            record[json_field] = json.loads(record[json_field]) if isinstance(record[json_field], str) else record[json_field]
                    except (json.JSONDecodeError, TypeError):
                        record[json_field] = []
                else:
                    record[json_field] = []
            
            # Handle image_urls field for panels
            if "image_urls" in record.get("url", "") or "image_urls" in record.get("image_url", ""):
                try:
                    # Parse image_urls JSON array
                    urls_field = record.get("url") or record.get("image_url")
                    if urls_field and urls_field != "NULL":
                        urls_list = json.loads(urls_field) if isinstance(urls_field, str) else urls_field
                        if isinstance(urls_list, list) and urls_list:
                            record["url"] = urls_list[0] if urls_list else None
                            record["image_url"] = urls_list[0] if urls_list else None
                except (json.JSONDecodeError, TypeError):
                    pass
            
            records.append(record)
        
        logger.info(f"AIDS Memorial Quilt DB: Retrieved {len(records)} records from quilt_panels")
        return records
        
    except Exception as e:
        logger.error(f"AIDS Memorial Quilt DB: Error getting records: {e}")
        raise DatabaseConnectionError(f"Failed to get records: {e}")
