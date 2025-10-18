import argparse
import glob
import json
import logging
import os
import re
import urllib.parse
import csv
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DEFAULT_METADATA_DIR = Path("D:/LOCData/metadata")
DEFAULT_ARTIFACTS_DIR = Path("D:/LOCData/artifacts")


def is_downloadable_url(url: Optional[str]) -> Tuple[bool, str]:
    """
    Mirror downloader filtering:
     - Skip empty URLs
     - Skip iiif URLs unless they include 'pct:100'
    Returns (is_downloadable, reason)
    """
    if not url:
        return False, "empty_url"
    url_lower = url.lower()
    if "iiif" in url_lower and "pct:100" not in url_lower:
        return False, "iiif_no_pct100"
    return True, "ok"


def extract_ids_from_metadata(data: Dict[str, Any], fname: str) -> Tuple[str, str]:
    """
    Extract block_id and manuscript_id from metadata dict or filename.
    Tries multiple common keys and falls back to parsing the metadata filename.
    Numeric block ids are zero-padded to 4 digits to match existing layout (e.g. 0002).
    """
    # try many possible keys
    block_keys = ("block_id", "block", "blockNumber", "block_number", "blockId", "block-num", "item_id", "id")
    manuscript_keys = ("manuscript_id", "manuscript", "msid", "manuscriptId", "manuscript_number")

    def find_first(keys):
        for k in keys:
            v = data.get(k)
            if v is not None:
                return str(v)
        return None

    block = find_first(block_keys)
    manuscript = find_first(manuscript_keys)

    # fallback: parse from metadata filename (e.g. block_0002_metadata.json or manuscript_123.json)
    if not block:
        m = re.search(r"block[_\-]?0*([0-9]+)", fname, re.IGNORECASE)
        if m:
            block = m.group(1)
    if not manuscript:
        m = re.search(r"manuscript[_\-]?0*([0-9]+)", fname, re.IGNORECASE)
        if m:
            manuscript = m.group(1)

    # normalize/pad numeric block id to 4 digits if numeric
    if block and block.isdigit():
        block = block.zfill(4)
    if not block:
        block = "unknown"

    if manuscript and manuscript.isdigit():
        manuscript = manuscript.zfill(4)
    if not manuscript:
        manuscript = "unknown"

    return block, manuscript


def expected_artifact_path(
    artifact_url: str,
    block_id: str,
    manuscript_id: str,
    mimetype: Optional[str],
    artifacts_dir: Path,
    *,
    uid: Optional[str] = None,
    metadata_fname: Optional[str] = None,
) -> Path:
    """
    Reproduce downloader path rules robustly and exactly:
      ARTIFACTS_DIR / block_{block_id} / manuscript_{manuscript_id} / <mimetype with '/'->'_'> / <basename(url) or fallback>
    This mirrors how artifact_downloader.py derives filenames (strip query, use basename).
    If URL has no basename, create a fallback filename using uid or metadata filename and mimetype-derived extension.
    """
    # Mirror artifact_downloader.get_filename_from_url: strip query and take basename
    path_part = (artifact_url.split("?", 1)[0] if artifact_url else "")
    filename = os.path.basename(path_part or "") or ""
    if not filename:
        # build fallback filename
        base = uid or (metadata_fname and os.path.splitext(metadata_fname)[0]) or "artifact"
        # try to infer extension from mimetype
        ext = ""
        if mimetype and "/" in mimetype:
            ext = mimetype.split("/")[-1].split("+")[0]
            # sanitize ext
            ext = re.sub(r"[^a-zA-Z0-9]+", "", ext)
        if ext:
            filename = f"{base}.{ext}"
        else:
            filename = f"{base}.bin"

    safe_mimetype = (mimetype or "unknown").replace("/", "_")
    return artifacts_dir / f"block_{block_id}" / f"manuscript_{manuscript_id}" / safe_mimetype / filename


def scan_metadata_for_missing(
    metadata_dir: Path,
    artifacts_dir: Path,
    *,
    allow_undownloadable: bool = False,
    verbose: bool = False,
) -> Tuple[List[Dict[str, str]], List[Tuple[str, str]], Dict[str, int], Dict[str, List[str]]]:
    """
    Scan metadata files and return:
      - list of dicts representing missing artifact items (queue entries)
      - list of parsing errors as tuples (filename, reason)
      - summary counts dict
      - samples dict with small lists for debugging

    If allow_undownloadable=True the downloader filter is ignored (useful for verifying everything).
    """
    missing: List[Dict[str, str]] = []
    errors: List[Tuple[str, str]] = []

    stats = {
        "total_files": 0,
        "expected_artifacts": 0,
        "skipped_not_downloadable": 0,
        "skipped_no_url": 0,
        "malformed_json": 0,
        "missing_artifacts": 0,
        "found_artifacts": 0,
    }
    samples: Dict[str, List[str]] = {
        "skipped_examples": [],
        "malformed_examples": [],
        "missing_examples": [],
        "found_examples": [],
        "skip_url_samples": [],
    }

    json_files = glob.glob(str(metadata_dir.joinpath("*.json")))
    for idx, jf in enumerate(json_files, start=1):
        fname = os.path.basename(jf)
        stats["total_files"] += 1
        try:
            with open(jf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except json.JSONDecodeError:
            stats["malformed_json"] += 1
            errors.append((fname, "malformed json"))
            if len(samples["malformed_examples"]) < 20:
                samples["malformed_examples"].append(fname)
            if verbose:
                logging.debug("Malformed JSON: %s", jf)
            continue
        except Exception as e:
            errors.append((fname, f"read error: {e}"))
            if len(samples["malformed_examples"]) < 20:
                samples["malformed_examples"].append(f"{fname} ({e})")
            if verbose:
                logging.debug("Error reading %s: %s", jf, e)
            continue

        # Try to find the most appropriate source URL (mirrors downloader logic)
        url = extract_preferred_url(data, fname)
        # fall back to top-level 'url' if extractor returned None
        if not url:
            url = data.get("url") or data.get("original") or None

        if not allow_undownloadable:
            downloadable, reason = is_downloadable_url(url)
        else:
            downloadable, reason = True, "force_allowed"

        # Count entries the verifier will actually attempt to find on disk:
        will_check = False
        if downloadable and url:
            will_check = True
            stats["expected_artifacts"] += 1

        if not downloadable:
            stats["skipped_not_downloadable"] += 1
            if len(samples["skipped_examples"]) < 20:
                samples["skipped_examples"].append(f"{fname}: {reason}")
            if url and len(samples["skip_url_samples"]) < 30:
                samples["skip_url_samples"].append(f"{fname}: {url} ({reason})")
            if verbose:
                logging.debug("Skipping %s (%s) -> %s", fname, reason, url)
            continue

        if not url:
            stats["skipped_no_url"] += 1
            errors.append((fname, "no url"))
            if len(samples["skipped_examples"]) < 20:
                samples["skipped_examples"].append(f"{fname}: no_url")
            if verbose:
                logging.debug("No URL present in %s", jf)
            continue

        block_id, manuscript_id = extract_ids_from_metadata(data, fname)
        mimetype = data.get("mimetype") or data.get("format") or ""

        expected = expected_artifact_path(url, block_id, manuscript_id, mimetype, artifacts_dir, uid=str(data.get("uid") or ""), metadata_fname=fname)
        if not expected.exists():
            stats["missing_artifacts"] += 1
            entry = {
                "uid": str(data.get("uid") or ""),
                "block_id": block_id,
                "manuscript_id": manuscript_id,
                "mimetype": mimetype or "",
                "url": url,
                "expected_path": str(expected),
                "metadata_filename": fname,
            }
            missing.append(entry)
            if len(samples["missing_examples"]) < 50:
                samples["missing_examples"].append(str(expected))
            if verbose:
                logging.debug("Missing artifact for %s -> %s", fname, expected)
        else:
            stats["found_artifacts"] += 1
            if len(samples["found_examples"]) < 20:
                samples["found_examples"].append(str(expected))
            if verbose and idx % 500 == 0:
                logging.debug("Verified %d artifacts so far (sample): %s", idx, expected)

    return missing, errors, stats, samples


def count_artifacts_on_disk(artifacts_dir: Path, sample_limit: int = 50) -> Tuple[int, Dict[str, int], List[str]]:
    """
    Walk artifacts_dir and return:
      - total file count
      - counts per top-level block directory (block_* name -> count)
      - sample file paths (up to sample_limit)
    """
    total = 0
    per_block: Counter = Counter()
    samples: List[str] = []
    if not artifacts_dir.exists():
        return 0, {}, []

    for root, _, files in os.walk(artifacts_dir):
        for f in files:
            total += 1
            rel = Path(root).relative_to(artifacts_dir)
            top = rel.parts[0] if rel.parts else ""
            per_block[top] += 1
            if len(samples) < sample_limit:
                samples.append(str(Path(root) / f))
    return total, dict(per_block), samples


def write_queue_jsonl(queue: List[Dict[str, str]], out_path: Path) -> None:
    """
    Write queue as JSON Lines. One item per line with keys the downloader can use.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        for item in queue:
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")


def write_missing_csv(missing: List[Dict[str, str]], out_path: Path) -> None:
    """Write a CSV report of missing artifacts (one row per missing expected file)."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["metadata_filename", "uid", "block_id", "manuscript_id", "mimetype", "url", "expected_path"]
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for item in missing:
            writer.writerow({k: item.get(k, "") for k in fieldnames})


def extract_preferred_url(data: Dict, fname: Optional[str] = None, debug_rejects: Optional[Dict[str, int]] = None) -> Optional[str]:
    """
    More comprehensive URL extraction that mirrors the downloader's intent:
      - Prefer explicit 'url' / 'original' / 'image_url' string fields when acceptable
      - Inspect common list/dict fields ('files', 'images', 'items', 'resources', 'links')
      - Look inside dict entries for keys ('url','file','href','src','download')
      - Apply same IIIF pct:100 rule used by the downloader
    Records reasons for rejection into debug_rejects dict (if provided) for diagnostics.
    """
    def record(reason: str) -> None:
        if debug_rejects is not None:
            debug_rejects[reason] = debug_rejects.get(reason, 0) + 1

    # helper to test candidate
    def ok_candidate(u: Optional[str]) -> bool:
        if not u or not isinstance(u, str):
            record("empty_candidate")
            return False
        u = u.strip()
        if not u:
            record("blank_candidate")
            return False
        downloadable, reason = is_downloadable_url(u)
        if not downloadable:
            record(f"filtered:{reason}")
            return False
        return True

    # 1) straight string fields commonly used
    for key in ("url", "original", "image_url", "file", "src", "href", "download"):
        cand = data.get(key)
        if isinstance(cand, str) and ok_candidate(cand):
            return cand

    # 2) obvious nested single-object patterns
    for key in ("resource", "resource_url", "primary", "attachment"):
        val = data.get(key)
        if isinstance(val, str) and ok_candidate(val):
            return val
        if isinstance(val, dict):
            for ukey in ("url", "file", "href", "src"):
                u = val.get(ukey)
                if isinstance(u, str) and ok_candidate(u):
                    return u

    # 3) list structures: files, images, items, resources, links
    for key in ("files", "images", "items", "resources", "links"):
        arr = data.get(key)
        if not isinstance(arr, list):
            continue
        for entry in arr:
            if isinstance(entry, str):
                if ok_candidate(entry):
                    return entry
            elif isinstance(entry, dict):
                # check common url-bearing keys inside each entry
                for ukey in ("url", "file", "href", "src", "download"):
                    u = entry.get(ukey)
                    if isinstance(u, str) and ok_candidate(u):
                        return u
                # sometimes the dict contains nested 'links' or 'resources'
                for subkey in ("links", "resources", "files"):
                    sub = entry.get(subkey)
                    if isinstance(sub, list):
                        for sube in sub:
                            if isinstance(sube, str) and ok_candidate(sube):
                                return sube
                            if isinstance(sube, dict):
                                for ukey in ("url", "file", "href", "src"):
                                    u = sube.get(ukey)
                                    if isinstance(u, str) and ok_candidate(u):
                                        return u

    # 4) last-chance: find any url-like string deep in dict values
    def walk_for_strings(obj: Any) -> Optional[str]:
        if isinstance(obj, str):
            if ok_candidate(obj):
                return obj
            return None
        if isinstance(obj, dict):
            for v in obj.values():
                res = walk_for_strings(v)
                if res:
                    return res
        if isinstance(obj, list):
            for it in obj:
                res = walk_for_strings(it)
                if res:
                    return res
        return None

    deep = walk_for_strings(data)
    if deep:
        return deep

    # none acceptable
    record("no_acceptable_url")
    return None


def build_expected_entries(
    metadata_dir: Path,
    artifacts_dir: Path,
    *,
    allow_undownloadable: bool = False,
    sample_limit: Optional[int] = None,
) -> List[Dict[str, str]]:
    """
    Build and return the full list of expected artifact entries derived from metadata files.
    Uses the same selection heuristics as the downloader via extract_preferred_url.
    If sample_limit is provided, return only that many entries (deterministic order).
    """
    expected: List[Dict[str, str]] = []
    json_files = glob.glob(str(metadata_dir.joinpath("*.json")))
    debug_rejects: Dict[str, int] = {}

    for jf in json_files:
        fname = os.path.basename(jf)
        try:
            with open(jf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            debug_rejects["unreadable_file"] = debug_rejects.get("unreadable_file", 0) + 1
            continue

        url = extract_preferred_url(data, fname, debug_rejects)
        if not allow_undownloadable and not url:
            continue

        if not url:
            url = data.get("url") or data.get("original") or ""
            if not url:
                debug_rejects["force_no_url"] = debug_rejects.get("force_no_url", 0) + 1
                continue

        uid = str(data.get("uid") or data.get("id") or "")
        block_id, manuscript_id = extract_ids_from_metadata(data, fname)
        mimetype = data.get("mimetype") or data.get("format") or ""
        expected_path = expected_artifact_path(url, block_id, manuscript_id, mimetype, artifacts_dir, uid=uid, metadata_fname=fname)
        expected.append({
            "metadata_filename": fname,
            "uid": uid,
            "url": url,
            "expected_path": str(expected_path),
            "block_id": block_id,
            "manuscript_id": manuscript_id,
            "mimetype": mimetype,
        })

    expected.sort(key=lambda e: (e["block_id"], e["manuscript_id"], e["expected_path"]))

    if debug_rejects:
        logging.getLogger().info("URL extraction diagnostics (counts): %s", debug_rejects)

    if sample_limit and len(expected) > sample_limit:
        return expected[:sample_limit]
    return expected


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify artifacts and produce download queue from local metadata.")
    parser.add_argument("--metadata-dir", type=Path, default=DEFAULT_METADATA_DIR, help="Directory with metadata JSON files")
    parser.add_argument("--artifacts-dir", type=Path, default=DEFAULT_ARTIFACTS_DIR, help="Root artifacts directory")
    parser.add_argument("--queue-out", type=Path, default=Path("verification-scripts/missing_queue.jsonl"), help="Output JSONL queue path")
    parser.add_argument("--report-out", type=Path, default=Path("verification-scripts/missing_report.csv"), help="CSV report of missing artifacts")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit of queue items to write (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write queue file; just print summary")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    parser.add_argument("--force-check", action="store_true", help="Ignore downloader filters and verify expected files for all metadata")
    parser.add_argument("--scan-artifacts-only", action="store_true", help="Skip metadata scanning; just summarize artifacts directory")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.info("Scanning metadata: %s", args.metadata_dir)
    logging.info("Artifacts root: %s", args.artifacts_dir)

    # Build explicit expected entries (sample) so user can see what verifier will check
    expected_sample = build_expected_entries(args.metadata_dir, args.artifacts_dir, allow_undownloadable=args.force_check, sample_limit=50)
    logging.info("Sample expected artifact entries to check (up to 50 shown): %d", len(expected_sample))
    for e in expected_sample[:20]:
        logging.info("  expect: %s -> %s", e.get("metadata_filename"), e.get("expected_path"))

    if args.scan_artifacts_only:
        total_files, per_block, samples = count_artifacts_on_disk(args.artifacts_dir)
        logging.info("Artifacts on disk: total_files=%d", total_files)
        if per_block:
            top_blocks = sorted(per_block.items(), key=lambda x: x[1], reverse=True)[:20]
            logging.info("Top block dirs (by file count):")
            for b, c in top_blocks:
                logging.info("  %s: %d", b, c)
        if samples:
            logging.info("Sample artifact files (%d):", len(samples))
            for s in samples[:20]:
                logging.info("  %s", s)
        return 0

    missing, errors, stats, samples = scan_metadata_for_missing(
        args.metadata_dir, args.artifacts_dir, allow_undownloadable=args.force_check, verbose=args.verbose
    )

    # Report how many artifacts we expected to find
    logging.info("Expected artifacts to check (downloadable & with URL): %d", stats.get("expected_artifacts", 0))

    # If everything was skipped by downloadability and nothing was checked, show samples so user can inspect
    logging.info(
        "Summary: total=%d, found=%d, missing=%d, skipped_not_downloadable=%d, skipped_no_url=%d, malformed=%d",
        stats["total_files"],
        stats["found_artifacts"],
        stats["missing_artifacts"],
        stats["skipped_not_downloadable"],
        stats["skipped_no_url"],
        stats["malformed_json"],
    )

    # show samples when counts are surprising
    if stats["found_artifacts"] == 0 and stats["total_files"] > 0:
        logging.warning(
            "No artifacts found by verification run. Showing diagnostic samples of skipped/parsed entries to help debug."
        )
        if samples["skip_url_samples"]:
            logging.warning("Sample skipped metadata (url + reason):")
            for s in samples["skip_url_samples"][:50]:
                logging.warning("  %s", s)
        if samples["malformed_examples"]:
            logging.warning("Sample malformed metadata files:")
            for s in samples["malformed_examples"][:50]:
                logging.warning("  %s", s)
        if not args.force_check:
            logging.info("Rerun with --force-check to ignore iiif filtering and verify all expected artifact paths.")

    if samples["missing_examples"]:
        logging.info("Sample missing artifact paths (up to 20):")
        for p in samples["missing_examples"][:20]:
            logging.info("  %s", p)

    if errors:
        logging.warning("Metadata read errors: %d", len(errors))
        for fname, reason in errors[:20]:
            logging.warning("  %s: %s", fname, reason)

    if missing:
        logging.info("Found %d missing artifacts", len(missing))
        to_write = missing[: args.limit] if args.limit and args.limit > 0 else missing
        logging.info("Preparing to write %d items to queue (limit=%d)", len(to_write), args.limit)
        if args.dry_run:
            for item in to_write[:50]:
                logging.info("  QUEUE: %s -> %s", item.get("metadata_filename"), item.get("expected_path"))
            logging.info("Dry-run mode: no queue file written.")
        else:
            write_queue_jsonl(to_write, args.queue_out)
            write_missing_csv(to_write, args.report_out)
            logging.info("Wrote queue file: %s", args.queue_out.resolve())
            logging.info("Wrote missing CSV report: %s", args.report_out.resolve())
            for item in to_write[:10]:
                logging.info("  %s -> %s", item.get("metadata_filename"), item.get("expected_path"))
    else:
        logging.info("No missing downloadable artifacts detected (for the rules applied).")

    # provide a quick disk summary to reassure user
    total_files_on_disk, per_block_counts, disk_samples = count_artifacts_on_disk(args.artifacts_dir)
    logging.info("Artifacts on disk: total_files=%d", total_files_on_disk)
    if per_block_counts:
        top_blocks = sorted(per_block_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        logging.info("Top artifact directories (samples):")
        for b, c in top_blocks:
            logging.info("  %s: %d", b, c)
    if disk_samples:
        logging.debug("Sample files on disk (%d):", len(disk_samples))
        for s in disk_samples[:20]:
            logging.debug("  %s", s)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())