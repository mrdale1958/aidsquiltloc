# AIDS Memorial Quilt Records Scraper - Enhanced Database Version

## 🎯 Overview

This enhanced version of the AIDS Memorial Quilt Records scraper now features **database integration** with **incremental update capabilities**, making it much more efficient and suitable for periodic runs to keep your local data synchronized with the Library of Congress collection.

## 🚀 New Features

### 1. **SQLite Database Storage**
- Metadata stored in a structured SQLite database instead of individual JSON files
- Efficient querying and indexing for fast access
- Change tracking with content hashing
- Timestamps for first seen, last updated, and last checked

### 2. **Incremental Updates**
- Only checks items that haven't been verified recently
- Detects content changes using SHA-256 hashing
- Avoids unnecessary API calls and downloads
- Perfect for scheduled/automated runs

### 3. **Enhanced CLI Interface**
```bash
# Available commands
python cli_enhanced.py scrape        # Full collection scrape
python cli_enhanced.py update        # Incremental update
python cli_enhanced.py stats         # Database statistics
python cli_enhanced.py check-item    # Check specific item
python cli_enhanced.py init-db       # Initialize database
python cli_enhanced.py test-connection  # Test API connectivity
```

### 4. **Smart Change Detection**
- Tracks metadata changes using content hashing
- Only updates records when actual content changes
- Maintains historical timestamps for tracking

## 📊 Database Schema

The system stores detailed information for each AIDS Memorial Quilt record:

### Core Fields
- `item_id`: Unique identifier (e.g., afc2019048_2621)
- `loc_url`: Full Library of Congress URL
- `title`: Item title
- `description`: Item description

### Metadata Fields
- `subject`: JSON array of subjects/topics
- `contributor`: JSON array of contributors
- `date_created`: Creation date
- `location`: Geographic information
- `format_info`: Format details

### Quilt-Specific Fields
- `quilt_block_number`: Extracted block number
- `memorial_names`: Names being memorialized (JSON array)
- `panel_maker`: Panel creator information

### Resource Tracking
- `image_urls`: Available image URLs (JSON array)
- `resource_urls`: Other resource URLs (JSON array)  
- `local_images`: Local file paths (JSON array)

### Change Tracking
- `content_hash`: SHA-256 hash for change detection
- `first_seen`: When first discovered
- `last_updated`: When content last changed
- `last_checked`: When last verified with LOC

### Status Flags
- `images_downloaded`: Whether images are local
- `metadata_complete`: Whether metadata extraction is complete

## 🔧 Usage Examples

### Initial Full Scrape
```bash
# Scrape first 100 items (metadata only)
python cli_enhanced.py scrape --max-items 100 --no-images

# Scrape with image downloads
python cli_enhanced.py scrape --max-items 50 --download-images
```

### Regular Updates
```bash
# Check for changes daily (items not checked in 24 hours)
python cli_enhanced.py update --hours 24

# Weekly comprehensive check
python cli_enhanced.py update --hours 168 --download-images
```

### Monitoring
```bash
# View database statistics
python cli_enhanced.py stats

# Check specific item
python cli_enhanced.py check-item --item-id afc2019048_0001
```

## 🔄 Recommended Workflow

### 1. **Initial Setup**
```bash
# Initialize database
python cli_enhanced.py init-db

# Test API connection
python cli_enhanced.py test-connection

# Start with small batch
python cli_enhanced.py scrape --max-items 50 --no-images
```

### 2. **Regular Maintenance**
```bash
# Daily metadata check (automated via cron/scheduler)
python cli_enhanced.py update --hours 24

# Weekly image sync
python cli_enhanced.py update --hours 168 --download-images

# Monthly full verification
python cli_enhanced.py update --hours 720 --download-images
```

### 3. **Monitoring**
```bash
# Check progress
python cli_enhanced.py stats
```

## 📈 Performance Benefits

### Rate Limiting Respect
- Exponential backoff for 429 errors
- Configurable delays between requests
- Retry logic for transient failures

### Efficient Updates
- Only processes items that need checking
- Avoids redundant API calls
- Smart change detection prevents unnecessary updates

### Scalability
- Database indexes for fast queries
- Batch processing with configurable sizes
- Memory-efficient streaming

## 🛠️ Configuration

Update `config/settings.py` for your needs:

```python
# Request settings
rate_limit_delay: float = 2.0  # Seconds between requests
max_concurrent_downloads: int = 3

# Database location
# Database will be created at: {base_dir}/quilt_records.db
```

## 📊 Current Status

✅ **Successfully tested with 5 AIDS Memorial Quilt records**
- Database initialization: Working
- API integration: Working (with proper rate limiting)  
- Metadata extraction: Working
- Change detection: Working
- Incremental updates: Working

### Test Results
```
📊 Database Statistics:
Total records: 5
Records with images: 0 
Records without images: 5
Recent updates (7 days): 5
```

## 🚀 Next Steps

You can now:

1. **Run larger batches**: `python cli_enhanced.py scrape --max-items 500`
2. **Set up automated daily updates**: `python cli_enhanced.py update`
3. **Download images**: Add `--download-images` to any command
4. **Monitor progress**: Use `python cli_enhanced.py stats`

The system is ready to handle the full AIDS Memorial Quilt collection (~5,164 records) efficiently with proper rate limiting and change tracking!
