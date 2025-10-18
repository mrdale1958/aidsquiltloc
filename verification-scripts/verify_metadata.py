import os
import json
import glob
import asyncio
import aiohttp
import logging
from typing import Set, List

LOC_API_URL = "https://www.loc.gov/collections/aids-memorial-quilt-records/?fo=json&c=100&at=results"

# Tune these for polite API usage
RATE_LIMIT_DELAY = 2        # seconds between successful page requests
MAX_RETRIES = 6
RETRY_BACKOFF_BASE = 5      # seconds, exponential backoff base for 429

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def fetch_all_loc_ids() -> Set[int]:
    """
    Fetch all record IDs from the LOC API with polite rate-limiting and retry on 429.
    Returns a set of unique integer IDs (skips non-integer ids).
    """
    ids: Set[int] = set()
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        page = 1
        while True:
            url = f"{LOC_API_URL}&sp={page}"
            retries = 0
            while retries <= MAX_RETRIES:
                logging.info("Fetching LOC page %d", page)
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            results = data.get("results", []) or []
                            if not results:
                                logging.info("No results on page %d, finishing", page)
                                return ids
                            for item in results:
                                # prefer numeric uid fields if present
                                uid = item.get("uid") or item.get("id")
                                if uid is None:
                                    continue
                                try:
                                    ids.add(int(uid))
                                except Exception:
                                    # skip non-integer ids (keep verification conservative)
                                    continue
                            logging.info("Fetched %d records from page %d (total so far: %d)", len(results), page, len(ids))
                            if len(results) < 100:
                                return ids
                            await asyncio.sleep(RATE_LIMIT_DELAY)
                            break  # success, move to next page
                        elif resp.status == 429:
                            backoff = RETRY_BACKOFF_BASE * (2 ** retries)
                            logging.warning("Rate limited on page %d. Backing off %ds (retry %d/%d).", page, backoff, retries + 1, MAX_RETRIES)
                            await asyncio.sleep(backoff)
                            retries += 1
                        else:
                            raise RuntimeError(f"Failed to fetch LOC API page {page}: HTTP {resp.status}")
                except aiohttp.ClientError as e:
                    backoff = RETRY_BACKOFF_BASE * (2 ** retries)
                    logging.warning("Network error fetching page %d: %s. Backing off %ds (retry %d/%d).", page, str(e), backoff, retries + 1, MAX_RETRIES)
                    await asyncio.sleep(backoff)
                    retries += 1
            else:
                raise RuntimeError(f"Exceeded max retries for LOC API page {page} due to repeated 429/network errors.")
            page += 1


def get_local_metadata_ids(directory: str) -> Set[int]:
    """
    Read all local JSON metadata files and return the set of integer uid values found.
    Malformed JSON files are skipped here (they are reported separately).
    """
    ids: Set[int] = set()
    json_files = glob.glob(os.path.join(directory, "*.json"))
    for jf in json_files:
        try:
            with open(jf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            uid = data.get("uid")
            if uid is not None:
                try:
                    ids.add(int(uid))
                except Exception:
                    continue
        except Exception:
            continue
    return ids


def find_malformed_json_files(directory: str) -> List[str]:
    """
    Return filenames for JSON files that fail to parse.
    """
    malformed: List[str] = []
    json_files = glob.glob(os.path.join(directory, "*.json"))
    for jf in json_files:
        try:
            with open(jf, "r", encoding="utf-8") as fh:
                json.load(fh)
        except Exception:
            malformed.append(jf)
    return malformed


async def verify_metadata(directory: str) -> None:
    print("Fetching LOC record IDs...")
    loc_ids = await fetch_all_loc_ids()
    print(f"LOC reports {len(loc_ids)} records.")

    print("Scanning local metadata files...")
    local_ids = get_local_metadata_ids(directory)
    print(f"Found {len(local_ids)} local metadata JSON files with integer uid.")

    missing = loc_ids - local_ids
    extra = local_ids - loc_ids

    if missing:
        print(f"Missing {len(missing)} metadata files. Sample: {sorted(list(missing))[:20]}")
    else:
        print("No missing metadata files detected.")

    if extra:
        print(f"Extra local metadata files not present in LOC: {sorted(list(extra))[:20]}")

    malformed = find_malformed_json_files(directory)
    if malformed:
        print(f"Malformed JSON files ({len(malformed)}):")
        for m in malformed[:50]:
            print("  ", m)
    else:
        print("All local metadata files are well-formed JSON.")


if __name__ == "__main__":
    import sys
    target = "D:/LOCData/metadata"
    if len(sys.argv) > 1:
        target = sys.argv[1]
    asyncio.run(verify_metadata(target))