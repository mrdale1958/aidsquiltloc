import asyncio
import aiohttp
import aiofiles
import logging
import sqlite3
import os
import signal
import threading
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, List, Optional, Dict, Any
import shutil
import csv

MAX_CONSECUTIVE_FAILURES = 10
RETRY_DELAY_SECONDS = 60
SHUTDOWN_MODE_THRESHOLD = 3  # Number of consecutive server errors to trigger shutdown mode
PROGRESSIVE_BACKOFF_BASE = 300  # 5 minutes base for progressive backoff
MAX_PROGRESSIVE_BACKOFF = 3600  # 1 hour max

# Global progress tracking
class ProgressTracker:
    """
    Thread-safe progress tracker used by both async tasks and the dashboard thread.
    Tracks current block for dashboard display.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.total = 0
        self.completed = 0
        self.failed = 0
        self.start_time = datetime.now()
        self.last_update = self.start_time
        self.current_block: Optional[str] = None

    def set_total(self, total: int) -> None:
        with self._lock:
            self.total = int(total)
            self.last_update = datetime.now()

    def set_current_block(self, block_id: Optional[str]) -> None:
        with self._lock:
            self.current_block = block_id
            self.last_update = datetime.now()

    def increment_completed(self, block_id: Optional[str] = None) -> None:
        with self._lock:
            self.completed += 1
            if block_id:
                self.current_block = block_id
            self.last_update = datetime.now()

    def increment_failed(self, block_id: Optional[str] = None) -> None:
        with self._lock:
            self.failed += 1
            if block_id:
                self.current_block = block_id
            self.last_update = datetime.now()

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            elapsed = datetime.now() - self.start_time
            return {
                "total": self.total,
                "completed": self.completed,
                "failed": self.failed,
                "remaining": max(0, self.total - (self.completed + self.failed)),
                "elapsed": elapsed,
                "last_update": self.last_update,
                "current_block": self.current_block,
            }

progress = ProgressTracker()

# Add a stop event the dashboard thread and main can use
import threading
dashboard_stop = threading.Event()

def setup_logging():
    """Setup file logging and return logger."""
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"artifact_download_{timestamp}.log"

    # Setup file logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            # Don't add console handler - we'll use dashboard instead
        ]
    )

    logger = logging.getLogger(__name__)
    print(f"Logging to: {log_file}")
    return logger

logger = setup_logging()

def display_dashboard(poll_interval: float = 1.0) -> None:
    """
    Runs in a background thread and prints a compact ASCII dashboard to stdout.
    Uses a snapshot from progress.get_stats() to avoid locking the main thread.
    Exits cleanly when dashboard_stop is set and prints one final snapshot without clearing.
    """
    try:
        while not dashboard_stop.is_set():
            stats = progress.get_stats()
            total = stats["total"]
            completed = stats["completed"]
            failed = stats["failed"]
            remaining = stats["remaining"]
            elapsed = stats["elapsed"]
            current_block = stats.get("current_block") or "N/A"
            percent = (completed / total * 100) if total else 0.0

            # clear screen for nicer single-frame dashboard if possible
            try:
                if os.name == "nt":
                    os.system("cls")
                else:
                    os.system("clear")
            except Exception:
                pass

            # print dashboard
            print("=" * 60)
            print("      AIDS Quilt Artifact Download Dashboard")
            print("=" * 60)
            print()
            print(f"Current Block: {current_block}")
            print(f"Progress:          {completed} completed, {failed} failed")
            print(f"Total:             {total} artifacts")
            print(f"Completion:        {percent:.1f}%")
            print()
            print("Press Ctrl+C to stop")
            print("=" * 60)
            try:
                import sys
                sys.stdout.flush()
            except Exception:
                pass

            time.sleep(poll_interval)

        # final snapshot (do NOT clear screen) so final main output remains visible
        stats = progress.get_stats()
        total = stats["total"]
        completed = stats["completed"]
        failed = stats["failed"]
        elapsed = stats["elapsed"]
        current_block = stats.get("current_block") or "N/A"
        percent = (completed / total * 100) if total else 0.0

        print("\n" + "=" * 60)
        print("      AIDS Quilt Artifact Download Dashboard (Final)")
        print("=" * 60)
        print()
        print(f"Current Block: {current_block}")
        print(f"Progress:          {completed} completed, {failed} failed")
        print(f"Total:             {total} artifacts")
        print(f"Completion:        {percent:.1f}%")
        print("=" * 60)
        try:
            import sys
            sys.stdout.flush()
        except Exception:
            pass

    except Exception:
        print("Dashboard thread terminated unexpectedly", flush=True)

class GlobalErrorTracker:
    """Track consecutive server errors across all downloads to detect shutdown mode."""
    def __init__(self):
        self.consecutive_server_errors = 0
        self.progressive_backoff_level = 0
    
    def record_server_error(self):
        """Record a server error and check if we've entered shutdown mode."""
        self.consecutive_server_errors += 1
        if self.consecutive_server_errors == SHUTDOWN_MODE_THRESHOLD:
            self.progressive_backoff_level = 1
            logger.warning(
                "Detected server shutdown mode after %d consecutive errors. "
                "Progressive backoff level: %d", 
                self.consecutive_server_errors, self.progressive_backoff_level
            )
        elif self.consecutive_server_errors > SHUTDOWN_MODE_THRESHOLD:
            self.progressive_backoff_level += 1
            logger.warning(
                "Escalating shutdown mode after %d consecutive errors. "
                "Progressive backoff level: %d", 
                self.consecutive_server_errors, self.progressive_backoff_level
            )
    
    def record_success(self):
        """Reset counters on successful download."""
        if self.consecutive_server_errors > 0:
            logger.info("Server recovered. Resetting error counters.")
        self.consecutive_server_errors = 0
        self.progressive_backoff_level = 0
    
    def get_backoff_delay(self) -> int:
        """Get the appropriate backoff delay based on current state."""
        if self.consecutive_server_errors < SHUTDOWN_MODE_THRESHOLD:
            return RETRY_DELAY_SECONDS
        
        # Progressive backoff: 5min, 10min, 20min, 40min, up to 1 hour
        progressive_delay = PROGRESSIVE_BACKOFF_BASE * (2 ** (self.progressive_backoff_level - 1))
        return min(progressive_delay, MAX_PROGRESSIVE_BACKOFF)
    
    def is_shutdown_mode(self) -> bool:
        """Check if we're in server shutdown mode."""
        return self.consecutive_server_errors >= SHUTDOWN_MODE_THRESHOLD

error_tracker = GlobalErrorTracker()

def update_download_status(db_path: str, artifact_id: int, status: int) -> None:
    """
    Update the downloaded status for an artifact.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("UPDATE artifacts SET downloaded = ? WHERE uid = ?", (status, artifact_id))
    conn.commit()
    conn.close()

def get_artifacts(
    db_path: str,
    block_ids: Optional[List[str]] = None,
    limit: Optional[int] = None
) -> List[Tuple[int, str, str, str, str]]:
    """
    Retrieve artifact records from the database.
    Returns a list of tuples: (uid, block_id, manuscript_id, mimetype, url)
    Optionally filter by block_ids and limit number of results.
    Skips artifacts that have already been downloaded (downloaded = 1).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = (
        "SELECT uid, block_id, manuscript_id, mimetype, url "
        "FROM artifacts "
        "WHERE url IS NOT NULL "
        "AND (url NOT LIKE '%iiif%' OR url LIKE '%pct:100%')"
    )
    params = []
    if block_ids:
        query += " AND block_id IN ({})".format(",".join("?" for _ in block_ids))
        params.extend(block_ids)
    query += " ORDER BY block_id, manuscript_id"
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    cursor.execute(query, params)
    artifacts = cursor.fetchall()
    conn.close()
    return artifacts

def get_timeout_for_url(url: str) -> int:
    if url.endswith('.jp2') or 'pct:100' in url or 'pct:50' in url:
        return 180  # seconds
    return 60

def get_filename_from_url(url: str) -> str:
    """
    Extract a filename from the URL.
    """
    return os.path.basename(url.split("?")[0])

async def download_file_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: Path,
    artifact_id: int,
    db_path: str,
    block_id: str
) -> bool:
    """
    Download a file with retry logic and update DB status.
    """
    logger.info("[BLOCK %s] Starting download: %s", block_id, url)
    consecutive_failures = 0
    while consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        try:
            request_timeout = aiohttp.ClientTimeout(total=get_timeout_for_url(url))
            async with session.get(url, timeout=request_timeout) as resp:
                if resp.status == 200:
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    # Add timeout for file operations
                    try:
                        f = await asyncio.wait_for(aiofiles.open(dest_path, "wb"), timeout=30)
                        try:
                            async for chunk in resp.content.iter_chunked(1024 * 64):
                                await asyncio.wait_for(f.write(chunk), timeout=10)
                        finally:
                            await f.close()
                    except asyncio.TimeoutError:
                        logger.warning("[BLOCK %s] File write timeout for %s", block_id, url)
                        if dest_path.exists():
                            dest_path.unlink()  # Remove partial file
                        consecutive_failures += 1
                        continue
                    update_download_status(db_path, artifact_id, 1)
                    error_tracker.record_success()
                    progress.increment_completed(block_id)
                    logger.info("[BLOCK %s] Downloaded %s to %s", block_id, url, dest_path)
                    return True
                elif resp.status in (429, 500, 502, 503, 504):
                    error_tracker.record_server_error()
                    backoff_delay = error_tracker.get_backoff_delay()
                    if error_tracker.is_shutdown_mode():
                        logger.warning(
                            "Server error or rate limit for %s (HTTP %d). "
                            "In shutdown mode - progressive backoff: %d seconds.", 
                            url, resp.status, backoff_delay
                        )
                    else:
                        logger.warning(
                            "Server error or rate limit for %s (HTTP %d). Waiting %d seconds.", 
                            url, resp.status, backoff_delay
                        )
                    await asyncio.sleep(backoff_delay)
                    consecutive_failures += 1
                else:
                    logger.error("Failed to download %s: HTTP %d", url, resp.status)
                    update_download_status(db_path, artifact_id, -1)
                    progress.increment_failed(block_id)
                    return False
        except asyncio.TimeoutError:
            error_tracker.record_server_error()
            backoff_delay = error_tracker.get_backoff_delay()
            logger.warning("Timeout downloading %s. Waiting %d seconds.", url, backoff_delay)
            await asyncio.sleep(backoff_delay)
            consecutive_failures += 1
        except aiohttp.ClientError as e:
            error_tracker.record_server_error()
            backoff_delay = error_tracker.get_backoff_delay()
            logger.warning("Network error downloading %s: %s. Waiting %d seconds.", url, str(e), backoff_delay)
            await asyncio.sleep(backoff_delay)
            consecutive_failures += 1
        except asyncio.CancelledError:
            logger.info("[BLOCK %s] Download cancelled: %s", block_id, url)
            raise
        except Exception as e:
            logger.error("[BLOCK %s] Unexpected error downloading %s: %s", block_id, url, str(e))
            update_download_status(db_path, artifact_id, -1)
            progress.increment_failed(block_id)
            return False
    logger.error("Too many consecutive failures for %s. Exiting for human intervention.", url)
    progress.increment_failed(block_id)
    return False

async def download_artifacts(
    db_path: str,
    output_dir: Path,
    block_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
    max_concurrent: int = 4
) -> None:
    """
    Download artifact files from the database, organizing by block, manuscript, and mimetype.
    """
    artifacts = get_artifacts(db_path, block_ids, limit)
    progress.set_total(len(artifacts))
    logger.info("Found %d artifacts to download.", len(artifacts))
    logger.info("Starting download process with %d max concurrent connections...", max_concurrent)

    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(
        limit=max_concurrent * 2,
        limit_per_host=max_concurrent,
        ttl_dns_cache=300,
        use_dns_cache=True,
        keepalive_timeout=30,
        enable_cleanup_closed=True
    )
    timeout = aiohttp.ClientTimeout(total=300, connect=30, sock_read=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        logger.info("Created HTTP session, preparing tasks...")
        tasks = []
        for uid, block_id, manuscript_id, mimetype, url in artifacts:
            filename = get_filename_from_url(url)
            safe_mimetype = mimetype.replace("/", "_") if mimetype else "unknown"
            dest_path = output_dir / f"block_{block_id}" / f"manuscript_{manuscript_id}" / safe_mimetype / filename

            async def sem_task(uid=uid, url=url, dest_path=dest_path, block_id=block_id):
                try:
                    async with semaphore:
                        await download_file_with_retry(session, url, dest_path, uid, db_path, block_id)
                except asyncio.CancelledError:
                    logger.info("[BLOCK %s] Download cancelled: %s", block_id, url)
                    raise

            # Create actual Task objects, not coroutines
            task = asyncio.create_task(sem_task())
            tasks.append(task)

        logger.info("Created %d download tasks, starting concurrent execution...", len(tasks))
        try:
            await asyncio.gather(*tasks)
            logger.info("All download tasks completed.")
        except asyncio.CancelledError:
            logger.info("Download cancelled by user, cleaning up tasks...")
            # Cancel all remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
            # Wait for all tasks to finish cancellation
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Task cleanup completed.")
            raise

async def check_artifacts(
    db_path: str,
    output_dir: Path,
    block_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> None:
    """
    Verify that artifact files expected by the DB exist on disk and are non-empty.
    Produces a CSV report of missing or zero-length artifact files.
    """
    artifacts = get_artifacts(db_path, block_ids, limit)
    progress.set_total(len(artifacts))
    logger.info("Found %d artifacts to verify.", len(artifacts))

    missing: List[Dict[str, Optional[str]]] = []

    for uid, block_id, manuscript_id, mimetype, url in artifacts:
        filename = get_filename_from_url(url)
        safe_mimetype = mimetype.replace("/", "_") if mimetype else "unknown"
        dest_path = output_dir / f"block_{block_id}" / f"manuscript_{manuscript_id}" / safe_mimetype / filename

        try:
            if dest_path.exists() and dest_path.is_file() and dest_path.stat().st_size > 0:
                progress.increment_completed(block_id)
                logger.debug("[CHECK %s] Present: %s", block_id, dest_path)
            else:
                logger.debug("[CHECK %s] Missing or zero-length: %s", block_id, dest_path)
                missing.append({
                    "uid": str(uid),
                    "block_id": str(block_id),
                    "manuscript_id": str(manuscript_id),
                    "mimetype": mimetype or "",
                    "url": url,
                    "expected_path": str(dest_path),
                })
                progress.increment_failed(block_id)
        except Exception as e:
            logger.warning("[CHECK %s] Error inspecting %s: %s", block_id, dest_path, e)
            missing.append({
                "uid": str(uid),
                "block_id": str(block_id),
                "manuscript_id": str(manuscript_id),
                "mimetype": mimetype or "",
                "url": url,
                "expected_path": str(dest_path),
            })
            progress.increment_failed(block_id)

    logger.info("Verification complete: %d missing/zero-length, %d present", len(missing), progress.completed)

    # write CSV report of missing entries
    report_path = output_dir / "verification_missing_report.csv"
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["uid", "block_id", "manuscript_id", "mimetype", "url", "expected_path"])
            writer.writeheader()
            for row in missing:
                writer.writerow(row)
        logger.info("Wrote verification report: %s", report_path.resolve())
    except Exception as e:
        logger.error("Failed to write verification report %s: %s", report_path, e)

async def main():
    """Main function with proper signal handling."""
    parser = argparse.ArgumentParser(description="Download artifact files from the database.")
    parser.add_argument("--db-path", type=str, default="D:/LOCData/quilt_records_simple.db", help="Path to SQLite database")
    parser.add_argument("--output-dir", type=Path, default=Path("D:/LOCData/artifacts"), help="Directory to save downloaded files")
    parser.add_argument("--max-concurrent", type=int, default=4, help="Max concurrent downloads")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of artifacts to process")
    parser.add_argument("--blocks", type=str, nargs="*", default=None, help="Specific block_id(s) to process")
    parser.add_argument("--verify-downloads", action="store_true", help="Do not download; verify files exist on disk and report missing")
    args = parser.parse_args()

    # Start dashboard in a separate thread
    dashboard_thread = threading.Thread(target=display_dashboard, daemon=False)
    dashboard_thread.start()

    # Give dashboard a moment to start
    await asyncio.sleep(1)

    # Setup signal handling
    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("Received interrupt signal, cancelling downloads/checks...")
        for task in asyncio.all_tasks(loop):
            if task is not asyncio.current_task():
                task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            logger.warning(f"Signal handling not implemented for {sig} on this platform.")

    try:
        if args.verify_downloads:
            await check_artifacts(
                db_path=args.db_path,
                output_dir=args.output_dir,
                block_ids=args.blocks,
                limit=args.limit
            )
        else:
            await download_artifacts(
                db_path=args.db_path,
                output_dir=args.output_dir,
                block_ids=args.blocks,
                limit=args.limit,
                max_concurrent=args.max_concurrent
            )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except asyncio.CancelledError:
        logger.info("Operation cancelled")
    finally:
        # Stop dashboard and wait briefly so it prints final snapshot before main prints summary
        dashboard_stop.set()
        if dashboard_thread.is_alive():
            dashboard_thread.join(timeout=3)
        # Show final stats
        stats = progress.get_stats()
        print(f"\nOperation completed!")
        print(f"Completed: {stats['completed']:,}")
        print(f"Failed: {stats['failed']:,}")
        print(f"Total: {stats['total']:,}")
        if stats['elapsed']:
            print(f"Total time: {str(stats['elapsed']).split('.')[0]}")

if __name__ == "__main__":
    import argparse
    asyncio.run(main())