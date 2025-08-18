#!/usr/bin/env python3
"""
Simple AIDS Memorial Quilt Records scraper.
Supports metadata-only, images-only, full, and db-sync modes.
In metadata mode, saves the full JSON response from the LOC API for each block,
converting single quotes to double quotes and escaping double quotes inside string values.
Other modes are stubs for future implementation.
"""

import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Dict, Optional
import aiohttp
import aiofiles
import json
from simple_metadata_extractor import insert_block_into_db, extract_and_insert_artifacts

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ScraperModes:
    FULL = "full"
    METADATA = "metadata"
    IMAGES = "images"
    DB_SYNC = "db-sync"

def fix_json_quotes(json_str: str) -> str:
    """
    Convert single quotes to double quotes and escape double quotes inside string values.
    Args:
        json_str: Raw JSON string with single quotes and unescaped double quotes.
    Returns:
        Conformant JSON string with double quotes and escaped double quotes.
    """
    # Replace single quotes around keys: 'key': → "key":
    json_str = re.sub(r"'(\w+)':", r'"\1":', json_str)
    # Replace single quotes around string values: : 'value' → : "value"
    json_str = re.sub(r':\s*\'([^\']*)\'', r': "\1"', json_str)
    # Escape double quotes inside string values
    def escape_inside_strings(match):
        value = match.group(1)
        value = value.replace('"', r'\"')
        return f': "{value}"'
    json_str = re.sub(r':\s*"([^"]*)"', escape_inside_strings, json_str)
    return json_str

async def fetch_block_metadata(block_id: str, output_dir: Path, db_path: str = "quilt_blocks.db") -> bool:
    """
    Fetch block metadata from LOC API, fix quotes, save as JSON, and insert into DB.

    Args:
        block_id: Block identifier (e.g., "0001")
        output_dir: Directory to save metadata files
        db_path: Path to SQLite database

    Returns:
        True if successful, False otherwise
    """
    item_id = f"afc2019048_{block_id}"
    api_url = f"https://www.loc.gov/item/{item_id}/?fo=json"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=30) as resp:
                if resp.status != 200:
                    logger.error("Failed to fetch metadata for block %s: HTTP %d", block_id, resp.status)
                    return False
                raw_data = await resp.text()
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"block_{block_id}_metadata.json"
        fixed_json = fix_json_quotes(raw_data)
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            await f.write(fixed_json)
        logger.info("Saved metadata for block %s to %s", block_id, file_path)
        # Parse the fixed JSON and insert into DB using the imported function
        try:
            block_json = json.loads(fixed_json)
            if "item" in block_json and isinstance(block_json["item"], dict):
                block_json["item"]["block_id"] = block_id  # Ensure block_id is included
                insert_block_into_db(block_json["item"], db_path)
                extract_and_insert_artifacts(block_json, block_id, db_path)
            else:
                logger.warning("Block %s metadata does not contain a valid 'item' object.", block_id)
        except Exception as e:
            logger.error("Error parsing fixed JSON for block %s: %s", block_id, e)
        return True
    except aiohttp.ClientError as e:
        logger.error("Network error fetching metadata for block %s: %s", block_id, e)
        return False
    except Exception as e:
        logger.error("Error fetching metadata for block %s: %s", block_id, e)
        return False

async def scrape_blocks(start_id: int, end_id: int, mode: str, output_dir: Path, delay: float, db_path: str = "quilt_blocks.db") -> None:
    """
    Scrape blocks in the specified mode.

    Args:
        start_id: Starting block ID (inclusive)
        end_id: Ending block ID (inclusive)
        mode: Scraper mode
        output_dir: Output directory for metadata files
        delay: Delay in seconds between fetches
        db_path: Path to SQLite database
    """
    logger.info("Scraping blocks %d to %d in mode: %s with delay %.1f seconds", start_id, end_id, mode, delay)
    if mode == ScraperModes.METADATA:
        for block_num in range(start_id, end_id + 1):
            block_id = f"{block_num:04d}"
            await fetch_block_metadata(block_id, output_dir, db_path)
            logger.info("Sleeping for %.1f seconds to respect rate limits...", delay)
            await asyncio.sleep(delay)
    elif mode == ScraperModes.FULL:
        logger.info("Full mode not implemented yet.")
    elif mode == ScraperModes.IMAGES:
        logger.info("Images-only mode not implemented yet.")
    elif mode == ScraperModes.DB_SYNC:
        logger.info("DB sync mode not implemented yet.")
    else:
        logger.error("Unknown mode: %s", mode)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simple AIDS Memorial Quilt Records Scraper"
    )
    parser.add_argument("--start-id", type=int, required=True, help="Starting block ID (e.g., 1)")
    parser.add_argument("--end-id", type=int, required=True, help="Ending block ID (inclusive)")
    parser.add_argument("--mode", choices=[ScraperModes.FULL, ScraperModes.METADATA, ScraperModes.IMAGES, ScraperModes.DB_SYNC], default=ScraperModes.METADATA, help="Scraper mode")
    parser.add_argument("--output-path", type=Path, default=Path("output/metadata"), help="Directory to save metadata files")
    parser.add_argument("--delay", type=float, default=30.0, help="Delay in seconds between fetches (default: 30)")
    parser.add_argument("--db-path", type=str, default="quilt_record_simple.db", help="Path to SQLite database")
    args = parser.parse_args()

    asyncio.run(scrape_blocks(args.start_id, args.end_id, args.mode, args.output_path, args.delay, args.db_path))

if __name__ == "__main__":
    main()