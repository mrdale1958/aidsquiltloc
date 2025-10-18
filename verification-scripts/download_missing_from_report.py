import csv
import logging
import os
import time
from pathlib import Path
from typing import Dict

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("download_missing")

DEFAULT_REPORT = Path(r"D:/LOCData/artifacts/verification_missing_report.csv")
DEFAULT_FAILED = Path(r"D:/LOCData/artifacts/verification_failed_report.csv")
CHUNK_SIZE = 64 * 1024  # 64KB


def safe_mkdir_for(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def download_url_to_path(url: str, dest: Path, timeout: int = 30) -> None:
    """
    Stream-download `url` to `dest`. Overwrites partial files only on success.
    Raises requests.RequestException on network errors.
    """
    tmp = dest.with_suffix(dest.suffix + ".part")
    safe_mkdir_for(tmp)
    headers = {"User-Agent": "aidsquiltloc-verifier/1.0 (+https://github.com)"}
    with requests.get(url, stream=True, timeout=timeout, headers=headers) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(CHUNK_SIZE):
                if chunk:
                    fh.write(chunk)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception:
                # fsync not critical on all platforms
                pass
    tmp.replace(dest)


def read_report(path: Path) -> Dict[int, Dict[str, str]]:
    """
    Read CSV report and return dict keyed by row index (0-based) -> row dict.
    Expects header with: uid,block_id,manuscript_id,mimetype,url,expected_path
    """
    rows = {}
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for i, r in enumerate(reader):
            rows[i] = r
    return rows


def main(report_path: Path = DEFAULT_REPORT, delay: int = 30, timeout: int = 60, dry_run: bool = False) -> int:
    if not report_path.exists():
        LOGGER.error("Report not found: %s", report_path)
        return 2

    rows = read_report(report_path)
    LOGGER.info("Loaded %d rows from report: %s", len(rows), report_path)

    failed_rows = []
    for idx, row in rows.items():
        uid = row.get("uid", "")
        url = row.get("url", "")
        expected = Path(row.get("expected_path", "") or "")
        LOGGER.info("(%d/%d) uid=%s -> %s", idx + 1, len(rows), uid, expected)

        if not url:
            LOGGER.warning("  skipping uid=%s: no URL", uid)
            failed_rows.append({**row, "error": "no_url"})
            continue

        if expected.exists() and expected.is_file() and expected.stat().st_size > 0:
            LOGGER.info("  already exists and non-empty, skipping: %s", expected)
            # still count as success, continue without delay
            continue

        if dry_run:
            LOGGER.info("  dry-run: would download %s -> %s", url, expected)
            time.sleep(delay)
            continue

        try:
            download_url_to_path(url, expected, timeout=timeout)
            if expected.exists() and expected.stat().st_size > 0:
                LOGGER.info("  downloaded: %s (%d bytes)", expected, expected.stat().st_size)
            else:
                LOGGER.warning("  downloaded file is zero-length: %s", expected)
                failed_rows.append({**row, "error": "zero_length_after_download"})
        except requests.HTTPError as e:
            LOGGER.warning("  HTTP error for uid=%s url=%s: %s", uid, url, e)
            failed_rows.append({**row, "error": f"http:{e.response.status_code if e.response is not None else 'err'}"})
        except requests.RequestException as e:
            LOGGER.warning("  network error for uid=%s url=%s: %s", uid, url, e)
            failed_rows.append({**row, "error": f"network:{e}"})
        except Exception as e:
            LOGGER.exception("  unexpected error for uid=%s url=%s", uid, url)
            failed_rows.append({**row, "error": f"exception:{e}"})

        LOGGER.debug("  sleeping %ds before next download", delay)
        time.sleep(delay)

    # write failures
    try:
        if failed_rows:
            with DEFAULT_FAILED.open("w", encoding="utf-8", newline="") as fh:
                fieldnames = ["uid", "block_id", "manuscript_id", "mimetype", "url", "expected_path", "error"]
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                for fr in failed_rows:
                    writer.writerow({k: fr.get(k, "") for k in fieldnames})
            LOGGER.info("Wrote failed report: %s (%d rows)", DEFAULT_FAILED, len(failed_rows))
        else:
            LOGGER.info("All rows processed successfully; no failures recorded.")
    except Exception:
        LOGGER.exception("Failed to write failed report")

    return 0


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Download missing artifacts from verification report.")
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT, help="CSV report path (verification_missing_report.csv)")
    p.add_argument("--delay", type=int, default=30, help="Seconds to wait between downloads")
    p.add_argument("--timeout", type=int, default=60, help="HTTP request timeout in seconds")
    p.add_argument("--dry-run", action="store_true", help="Do not actually download; just show actions")
    args = p.parse_args()

    raise SystemExit(main(report_path=args.report, delay=args.delay, timeout=args.timeout, dry_run=args.dry_run))