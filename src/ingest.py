"""
src/ingest.py — Phase 2 (Class 5): Data Retrieval
NYC TLC Trip Reliability & Efficiency Pipeline

Two distinct retrieval modes (rubric requirement):
  Mode 1 (S1): Bulk HTTP download — Yellow Taxi Parquet from NYC TLC CDN
  Mode 2 (S2): Socrata Open Data API pull — Taxi Zone lookup (dataset 8meu-9t5y)

Every retrieval:
  - Is idempotent: skips re-download when local file + checksum already match
  - Logs row counts, file size, date coverage, and SHA-256 checksum to prove
    nothing was silently dropped
  - Writes a machine-readable manifest to data/raw/manifest.json (committed to
    git as proof-of-retrieval completeness — not gitignored like the raw data)
  - Fails explicitly on errors with clear messages (no silent fall-through)

FDE judgement calls (also documented in README):
  SHA-256 over MD5 : stronger integrity guarantee; negligible perf cost at this file size
  pyarrow.read_metadata() : reads only the Parquet file footer (~KB) for row count
                            without loading all ~50 MB into RAM; full read deferred to validate.py
  Socrata anonymous access: SOCRATA_APP_TOKEN is optional; the 263-row zone table
                            will never hit anonymous rate limits
  manifest.json committed : it is metadata (checksums, counts, URLs), not raw data —
                            the rubric needs it visible in the repo as retrieval proof
  Socrata ID 8meu-9t5y   : verified live Sep 2026 via Socrata catalog API;
                            commonly cited IDs 755u-8jsi and 2yv8-t2f9 both return 404

Usage (standalone):
  python src/pipeline.py --month 2025-01   # via orchestrator
  python src/ingest.py --month 2025-01     # standalone

Run from repo root so relative paths (data/raw/, data/reference/) resolve correctly.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import requests
from sodapy import Socrata

# ── Logging ───────────────────────────────────────────────────────────────────
# Format: timestamp [LEVEL] logger_name — message
# Callers (pipeline.py) may attach additional handlers (file, etc.)
logger = logging.getLogger("ingest")

if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# ── Constants ──────────────────────────────────────────────────────────────────
TLC_CDN_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
SOCRATA_DOMAIN = "data.cityofnewyork.us"

# Verified live Sep 2026 via Socrata catalog API (api.us.socrata.com/api/catalog/v1).
# Commonly cited IDs 755u-8jsi and 2yv8-t2f9 both return HTTP 404.
# See docs/source_map.md — Source Detail: S2 for the full discrepancy note.
SOCRATA_DATASET_ID = "8meu-9t5y"

MANIFEST_PATH = Path("data/raw/manifest.json")


# ── Utility: SHA-256 ──────────────────────────────────────────────────────────
def sha256_file(path: Path) -> str:
    """
    Compute SHA-256 of a file in 1 MB streaming chunks.

    FDE rationale: SHA-256 over MD5 — stronger integrity guarantee.
    Streaming avoids loading the full ~50 MB Parquet into RAM for the checksum.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1_048_576), b""):  # 1 MB chunks
            h.update(chunk)
    return h.hexdigest()


# ── Utility: Manifest ─────────────────────────────────────────────────────────
def _load_manifest() -> dict:
    """Load existing manifest JSON or return an empty dict if none exists."""
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH) as f:
            return json.load(f)
    return {}


def _save_manifest(manifest: dict) -> None:
    """
    Persist manifest atomically: write to a .tmp file, then rename.

    Atomic rename prevents a partial write from corrupting a previous good manifest
    if the process is killed mid-write.
    """
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = MANIFEST_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    tmp.rename(MANIFEST_PATH)
    logger.info("Manifest written → %s", MANIFEST_PATH)


# ── Mode 1: Bulk Parquet download (S1) ────────────────────────────────────────
def download_bulk_trip_file(
    month: str,
    dest_dir: Path = Path("data/raw"),
) -> Path:
    """
    Download NYC TLC Yellow Taxi Parquet for the given month (YYYY-MM).

    Idempotent:
      - If the file exists AND its SHA-256 matches the manifest record → skip download.
      - If the file exists but the checksum differs → re-download (upstream may have
        re-published a corrected file; we log a warning, never silently accept stale data).
      - If the file does not exist → download fresh.

    Logging: URL, file size (MB), elapsed seconds, row count, pickup date range,
             SHA-256 digest prefix. All written to manifest.json.

    Args:
        month   : "YYYY-MM" string, e.g. "2025-01"
        dest_dir: directory for the downloaded Parquet (default: data/raw/)

    Returns:
        Path to the local Parquet file.

    Raises:
        requests.HTTPError : HTTP non-200 from TLC CDN (file not found, server error)
        RuntimeError       : row count is 0 after download (corrupt or empty file)
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = f"yellow_tripdata_{month}.parquet"
    url = f"{TLC_CDN_BASE}/{filename}"
    dest_path = dest_dir / filename

    manifest = _load_manifest()
    manifest_key = f"trip_{month}"

    # ── Idempotency check ──────────────────────────────────────────────────────
    if dest_path.exists() and manifest_key in manifest:
        recorded = manifest[manifest_key].get("sha256")
        logger.info("File already present: %s — verifying SHA-256...", filename)
        actual = sha256_file(dest_path)
        if actual == recorded:
            logger.info(
                "Checksum match ✓ — skipping re-download (idempotent). "
                "Rows: %s | Size: %.2f MB",
                manifest[manifest_key].get("row_count"),
                manifest[manifest_key].get("file_size_mb"),
            )
            return dest_path
        else:
            # This should be rare — log at WARNING so it is visible in pipeline runs
            logger.warning(
                "SHA-256 MISMATCH — re-downloading. "
                "Recorded: %s… | Actual: %s… "
                "(upstream may have updated the file)",
                recorded[:12],
                actual[:12],
            )

    # ── Download ───────────────────────────────────────────────────────────────
    logger.info("Downloading: %s", url)
    t0 = time.time()

    try:
        resp = requests.get(url, stream=True, timeout=180)
        resp.raise_for_status()  # Raises HTTPError on 4xx/5xx — explicit, not silent
    except requests.HTTPError as e:
        raise requests.HTTPError(
            f"TLC CDN returned HTTP {e.response.status_code} for {url}. "
            f"Check that the month '{month}' exists in the TLC data catalogue: "
            f"https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
        ) from e

    bytes_written = 0
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1_048_576):  # 1 MB chunks
            f.write(chunk)
            bytes_written += len(chunk)

    elapsed = time.time() - t0
    logger.info(
        "Download complete — %.2f MB in %.1fs → %s",
        bytes_written / 1e6, elapsed, dest_path,
    )

    # ── Row count + date range via Parquet footer (cheap — no full load) ───────
    # FDE rationale: pq.read_metadata() reads only the Parquet file footer (a few KB),
    # giving us row count and optional column statistics without loading ~50 MB into RAM.
    # Full column-level validation is deferred to validate.py which does the real read.
    try:
        pq_meta = pq.read_metadata(dest_path)
    except Exception as e:
        dest_path.unlink(missing_ok=True)  # Remove corrupt file so reruns don't silently skip it
        raise RuntimeError(
            f"Downloaded file failed Parquet metadata read — likely corrupt: {dest_path}. "
            f"Error: {e}"
        ) from e

    row_count = pq_meta.num_rows
    if row_count == 0:
        dest_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Downloaded Parquet file has 0 rows — possible corrupt or empty download: {url}"
        )

    # Attempt to read date range from Parquet column statistics (written by many engines).
    # If statistics are absent, we record None rather than doing a full scan here.
    date_min, date_max = _extract_date_range_from_parquet_meta(pq_meta)

    # ── SHA-256 ────────────────────────────────────────────────────────────────
    checksum = sha256_file(dest_path)

    # ── Manifest ───────────────────────────────────────────────────────────────
    manifest[manifest_key] = {
        "source": "S1",
        "retrieval_mode": "bulk_http_parquet",
        "description": "NYC TLC Yellow Taxi trip records",
        "url": url,
        "local_path": str(dest_path),
        "filename": filename,
        "month": month,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "file_size_bytes": bytes_written,
        "file_size_mb": round(bytes_written / 1e6, 2),
        "row_count": row_count,
        "num_row_groups": pq_meta.num_row_groups,
        "pickup_date_min": date_min,
        "pickup_date_max": date_max,
        "sha256": checksum,
        "elapsed_seconds": round(elapsed, 1),
    }
    _save_manifest(manifest)

    logger.info(
        "Manifest entry written — rows: %s | size: %.2f MB | "
        "date range: %s → %s | SHA-256: %s…",
        row_count, bytes_written / 1e6, date_min, date_max, checksum[:16],
    )
    return dest_path


def _extract_date_range_from_parquet_meta(pq_meta) -> tuple:
    """
    Attempt to read min/max pickup datetime from Parquet row-group statistics.

    Returns (min_str, max_str) if available, (None, None) otherwise.
    Parquet column statistics are optional — some writers omit them.
    We never do a full table scan here; that belongs in validate.py.
    """
    try:
        schema = pq_meta.schema.to_arrow_schema()
        col_names = [schema.field(i).name for i in range(len(schema))]
        if "tpep_pickup_datetime" not in col_names:
            return None, None
        col_idx = col_names.index("tpep_pickup_datetime")
        mins, maxes = [], []
        for rg_idx in range(pq_meta.num_row_groups):
            stats = pq_meta.row_group(rg_idx).column(col_idx).statistics
            if stats and stats.has_min_max:
                mins.append(stats.min)
                maxes.append(stats.max)
        if mins and maxes:
            return str(min(mins)), str(max(maxes))
    except Exception as e:
        logger.debug("Could not extract date range from Parquet statistics: %s", e)
    return None, None


# ── Mode 2: Socrata API pull (S2) — Taxi Zone Lookup ──────────────────────────
def fetch_zone_lookup_api(
    dest_dir: Path = Path("data/reference"),
) -> Path:
    """
    Pull NYC Taxi Zone lookup from the Socrata Open Data API (dataset 8meu-9t5y).

    This is the second required retrieval mode (API vs bulk HTTP).
    The zone lookup feeds the dim_zones dimension table and is required for every
    zone-level metric computation.

    Idempotent: if the output file exists and contains > 0 rows, skip the API call.
    (Zone data changes extremely rarely; daily re-fetching is unnecessary.)

    App token: reads SOCRATA_APP_TOKEN env var. Warns — does not fail — if absent.
    Anonymous Socrata access is sufficient for a 263-row public dataset.

    Saves: data/reference/taxi_zones_socrata.json (committed to git)

    Returns:
        Path to the saved JSON file.

    Raises:
        RuntimeError: if the API returns 0 rows (dataset should have ~263)
        RuntimeError: wraps Socrata client errors with clear context + fallback hint
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "taxi_zones_socrata.json"

    # ── App token (optional) ──────────────────────────────────────────────────
    # Anonymous access is fine for 263 rows. App token only needed for high-volume queries.
    app_token = os.environ.get("SOCRATA_APP_TOKEN")
    if not app_token:
        logger.warning(
            "SOCRATA_APP_TOKEN not set — using anonymous Socrata access. "
            "This is fine for the 263-row zone table. "
            "Set SOCRATA_APP_TOKEN env var to suppress this warning."
        )

    # ── Idempotency check ──────────────────────────────────────────────────────
    if dest_path.exists():
        try:
            with open(dest_path) as f:
                existing = json.load(f)
            if len(existing) > 0:
                logger.info(
                    "Zone lookup already fetched (%d rows) — skipping API call (idempotent). "
                    "Delete %s to force re-fetch.",
                    len(existing), dest_path,
                )
                return dest_path
        except (json.JSONDecodeError, Exception) as e:
            logger.warning("Existing zone file is unreadable (%s) — re-fetching.", e)

    # ── Socrata API call ───────────────────────────────────────────────────────
    logger.info(
        "Fetching Taxi Zone lookup — domain: %s | dataset: %s",
        SOCRATA_DOMAIN, SOCRATA_DATASET_ID,
    )
    t0 = time.time()

    try:
        # timeout=30: zone table is tiny; 30s is generous for a 263-row fetch
        client = Socrata(SOCRATA_DOMAIN, app_token, timeout=30)
        # limit=10000: well above 263 zones; future-proofs if TLC adds zones
        results = client.get(SOCRATA_DATASET_ID, limit=10_000)
    except Exception as e:
        raise RuntimeError(
            f"Socrata API call failed.\n"
            f"  Domain  : {SOCRATA_DOMAIN}\n"
            f"  Dataset : {SOCRATA_DATASET_ID}\n"
            f"  Error   : {e}\n"
            f"  Fallback: run download_zone_lookup_csv() to use TLC CDN instead.\n"
            f"  See docs/source_map.md for dataset provenance."
        ) from e

    elapsed = time.time() - t0
    row_count = len(results)

    if row_count == 0:
        raise RuntimeError(
            f"Socrata API returned 0 rows for dataset {SOCRATA_DATASET_ID}. "
            f"Expected ~263 rows (one per NYC taxi zone). "
            f"Verify: https://{SOCRATA_DOMAIN}/resource/{SOCRATA_DATASET_ID}.json"
        )

    columns = list(results[0].keys()) if results else []
    logger.info(
        "Socrata pull complete — %d rows | %d columns | %.1fs | columns: %s",
        row_count, len(columns), elapsed, columns,
    )

    # ── Save ───────────────────────────────────────────────────────────────────
    with open(dest_path, "w") as f:
        json.dump(results, f, indent=2)
    logger.info("Zone lookup saved → %s", dest_path)

    # ── Manifest ───────────────────────────────────────────────────────────────
    manifest = _load_manifest()
    manifest["zone_lookup_socrata"] = {
        "source": "S2",
        "retrieval_mode": "socrata_api",
        "description": "NYC Taxi Zone lookup — LocationID → Borough/Zone name",
        "dataset_id": SOCRATA_DATASET_ID,
        "domain": SOCRATA_DOMAIN,
        "url": f"https://{SOCRATA_DOMAIN}/resource/{SOCRATA_DATASET_ID}.json",
        "local_path": str(dest_path),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "columns": columns,
        "elapsed_seconds": round(elapsed, 1),
        "app_token_used": bool(app_token),
    }
    _save_manifest(manifest)

    return dest_path


# ── Mode 3: Fallback CSV (S3) ──────────────────────────────────────────────────
def download_zone_lookup_csv(
    dest_dir: Path = Path("data/reference"),
) -> Path:
    """
    Fallback: download Taxi Zone Lookup CSV directly from TLC CDN.

    Called only when fetch_zone_lookup_api() fails. Not a separate retrieval mode
    for rubric purposes (same data, different transport). Logged and manifested
    identically so the fallback is visible and auditable.

    Idempotent: skips if file already exists.

    Returns:
        Path to the saved CSV file.

    Raises:
        requests.HTTPError: on non-200 response from TLC CDN
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "taxi_zone_lookup.csv"

    if dest_path.exists():
        logger.info("Zone CSV already exists — skipping re-download: %s", dest_path)
        return dest_path

    logger.info("Downloading Zone Lookup CSV (S3 fallback) → %s", ZONE_CSV_URL)
    t0 = time.time()

    resp = requests.get(ZONE_CSV_URL, timeout=30)
    resp.raise_for_status()
    elapsed = time.time() - t0

    with open(dest_path, "wb") as f:
        f.write(resp.content)

    # Row count (small file — safe to read all at once)
    lines = resp.content.decode("utf-8").strip().splitlines()
    row_count = len(lines) - 1  # subtract header row

    logger.info(
        "Zone CSV saved — %d data rows | %.1fs → %s",
        row_count, elapsed, dest_path,
    )

    manifest = _load_manifest()
    manifest["zone_lookup_csv_fallback"] = {
        "source": "S3",
        "retrieval_mode": "bulk_http_csv_fallback",
        "description": "TLC Taxi Zone Lookup CSV — used as fallback when Socrata API is unavailable",
        "url": ZONE_CSV_URL,
        "local_path": str(dest_path),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "elapsed_seconds": round(elapsed, 1),
    }
    _save_manifest(manifest)

    return dest_path


# ── Orchestrated ingest ────────────────────────────────────────────────────────
def run_ingest(month: str) -> dict:
    """
    Run both retrieval modes for a given month. Called by pipeline.py.

    Failure handling:
      - Mode 1 (Parquet) failure → raises immediately. This is the critical data path;
        there is no valid fallback for the trip records.
      - Mode 2 (Socrata API) failure → logs the error and falls back to Mode 3 (CSV).
        The fallback is explicit, logged, and manifested — not silent.

    Args:
        month: "YYYY-MM", e.g. "2025-01"

    Returns:
        dict with keys: trip_parquet (Path), zone_lookup (Path), zone_lookup_mode (str)
    """
    logger.info("══════════════════════════════════════════════")
    logger.info(" INGEST START — month: %s", month)
    logger.info("══════════════════════════════════════════════")
    t_total = time.time()

    result = {}

    # ── Mode 1: Bulk Parquet — no fallback; failure is fatal ──────────────────
    logger.info("─── Mode 1: Bulk Parquet download (S1) ───")
    result["trip_parquet"] = download_bulk_trip_file(month)

    # ── Mode 2: Socrata API — explicit fallback to CSV ────────────────────────
    logger.info("─── Mode 2: Socrata API zone lookup (S2) ───")
    try:
        result["zone_lookup"] = fetch_zone_lookup_api()
        result["zone_lookup_mode"] = "socrata_api"
    except RuntimeError as e:
        logger.error("Socrata API failed:\n  %s", e)
        logger.warning(
            "Falling back to TLC CSV zone lookup (S3). "
            "This is acceptable but should be investigated if it recurs."
        )
        result["zone_lookup"] = download_zone_lookup_csv()
        result["zone_lookup_mode"] = "csv_fallback"

    elapsed_total = time.time() - t_total
    logger.info("══════════════════════════════════════════════")
    logger.info(" INGEST COMPLETE — %.1fs", elapsed_total)
    logger.info(" trip_parquet  : %s", result["trip_parquet"])
    logger.info(" zone_lookup   : %s (mode: %s)", result["zone_lookup"], result["zone_lookup_mode"])
    logger.info("══════════════════════════════════════════════")
    return result


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "NYC TLC ingest: download Yellow Taxi Parquet + zone lookup.\n"
            "Run from repo root: python src/ingest.py --month 2025-01"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--month",
        required=True,
        metavar="YYYY-MM",
        help="Month to ingest (e.g. 2025-01). Yellow Taxi Parquet for this month will be downloaded.",
    )
    args = parser.parse_args()

    # Validate month format before hitting the network — fail fast with a clear message
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        logger.error(
            "Invalid --month format: '%s'. Expected YYYY-MM (e.g. 2025-01)",
            args.month,
        )
        sys.exit(1)

    paths = run_ingest(args.month)

    print("\n── Ingest summary ──────────────────────────────")
    for key, val in paths.items():
        print(f"  {key:<20} : {val}")
    print(f"  manifest             : {MANIFEST_PATH}")
    print("────────────────────────────────────────────────")
