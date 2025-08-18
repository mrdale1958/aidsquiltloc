"""
Simple metadata extractor for AIDS Memorial Quilt block data.
Verifies input JSON structure and inserts block values into the blocks table.
Follows PEP 8, uses type hints, structured logging, and comprehensive error handling.
"""

import json
import os

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

RESERVED_WORDS = {"group", "index", "type", "date", "format"}
DB_PATH = "quilt_blocks.db"

def sanitize_column_name(key: str) -> str:
    """Rename reserved SQL words by appending '_key'."""
    return f"{key}_key" if key in RESERVED_WORDS else key

def is_block_json(data: Dict[str, Any]) -> bool:
    """Check if the JSON looks like block data (has 'item' key and 'block_id')."""
    if "item" not in data:
        logger.error("JSON missing top-level 'item' key.")
        return False
    item = data["item"]
    if not isinstance(item, dict):
        logger.error("'item' value is not a dictionary.")
        return False
    if "block_id" not in item:
        logger.error("'item' missing 'block_id'.")
        return False
    return True

def insert_block_into_db(block_data: Dict[str, Any], db_path: str = DB_PATH) -> None:
    """Insert block data into the blocks table, serializing complex values as JSON strings."""
    print("Connecting to DB:", os.path.abspath(db_path))
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    conn.execute("PRAGMA foreign_keys = ON;")
    columns = []
    values = []
    for key, value in block_data.items():
        col = sanitize_column_name(key)
        columns.append(col)
        # Store simple types as-is, complex types as JSON strings
        if isinstance(value, (str, int, float, bool)) or value is None:
            values.append(value)
        else:
            values.append(json.dumps(value))
    col_str = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(values))
    sql = f"INSERT OR REPLACE INTO blocks ({col_str}) VALUES ({placeholders})"
    print(sql, values)
    try:
        cursor.execute(sql, values)
        conn.commit()
        logger.info("Inserted block_id %s into blocks table.", block_data.get("block_id"))
    except sqlite3.DatabaseError as e:
        logger.error("Database error inserting block: %s", e)
    finally:
        conn.close()

def extract_and_insert_artifacts(block_json: Dict[str, Any], block_id: str, db_path: str = "quilt_blocks.db") -> None:
    """
    Extract artifact descriptors from block JSON and insert them into the artifacts table.

    Args:
        block_json: Parsed block JSON dictionary (should contain 'resources'/'files')
        block_id: Block identifier
        db_path: Path to SQLite database
    """
    resources = block_json.get("resources")
    if not resources or "files" not in resources:
        logger.info(f"No artifact resources found for block {block_id}")
        return

    files = resources["files"]
    if not isinstance(files, list):
        logger.warning(f"'files' field is not a list for block {block_id}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for file_group in files:
        # file_group is expected to be a list of artifact descriptors
        if not isinstance(file_group, list):
            continue
        for descriptor in file_group:
            # Extract manuscript_id from descriptor['url'] or similar field
            url = descriptor.get("url", "")
            manuscript_id = None
            # Example: url contains .../afc2019048_XXXX_msYYYY/...
            import re
            match = re.search(r'afc2019048_(\d{4})_ms(\w+)', url)
            if match:
                manuscript_id = f"ms{match.group(2)}"
            else:
                # Fallback: try to extract manuscript_id from other fields if available
                manuscript_id = descriptor.get("manuscript_id", "")

            cursor.execute("""
                INSERT INTO artifacts (
                    block_id, manuscript_id, mimetype, height, width, levels, url, info, size, fulltext_service, word_coordinates
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                block_id,
                manuscript_id,
                descriptor.get("mimetype"),
                descriptor.get("height"),
                descriptor.get("width"),
                descriptor.get("levels"),
                descriptor.get("url"),
                str(descriptor.get("info")) if descriptor.get("info") is not None else None,
                descriptor.get("size"),
                descriptor.get("fulltext_service"),
                descriptor.get("word_coordinates")
            ))
    conn.commit()
    conn.close()
    logger.info(f"Inserted artifacts for block {block_id}")

# Usage example (call after block insert):
# extract_and_insert_artifacts(block_json, block_id, db_path)
def process_json_file(json_path: Path, db_path: str = DB_PATH) -> None:
    """Load, validate, and insert block JSON data from a file."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not is_block_json(data):
            logger.error("File %s does not contain valid block data.", json_path)
            return
        block_data = data["item"]
        insert_block_into_db(block_data, db_path)
    except Exception as e:
        logger.error("Error processing file %s: %s", json_path, e)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract and store AIDS Quilt block metadata from JSON.")
    parser.add_argument("json_file", type=Path, help="Path to block metadata JSON file")
    parser.add_argument("--db-path", type=str, default=DB_PATH, help="Path to SQLite database")
    args = parser.parse_args()
    process_json_file(args.json_file, args.db_path)