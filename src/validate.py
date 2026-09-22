"""
src/validate.py — Phase 3 (Class 6): Profiling + Validation
NYC TLC Trip Reliability & Efficiency Pipeline

What this module does:
  1. PROFILE: full read of the raw Parquet, compute null counts / dtype / range /
     cardinality for every column — before touching anything.
  2. VALIDATE: apply explicit, business-justified rules in two tiers:
       Hard rules  — violations exclude the trip from downstream metrics
       Soft rules  — violations are counted and flagged but trip is retained
  3. OUTPUT:
       data/processed/trips_validated_<month>.parquet  (valid rows only; gitignored)
       data/processed/validation_report_<month>.json   (committed; evidence artifact)

Design principles (driven by rubric):
  - Never silently fix or impute data. Flag violations, count them, write them up.
  - Every threshold has a one-line rationale comment AND a KUAL entry in the report.
  - Out-of-month trips are flagged and counted — not silently dropped, not silently kept.
  - Downstream (model.py) reads only the valid-rows Parquet; excluded trips never
    reach metric computation but their volume is visible in the report.

Validation thresholds and business justifications (FDE judgement calls):
  Duration ≤ 360 min (6 hrs): NYC geography makes genuine trips under 6 hrs; beyond
    this almost certainly = driver forgot to close the meter. No TLC published threshold.
  Distance ≤ 100 miles: covers all standard metered NYC metro trips including airports;
    no published TLC cap — FDE judgment based on service area bounds.
  Fare ≤ $500: at TLC rate (~$3.50/mile × 100 miles = $350 + $50 tolls ≈ $400);
    $500 gives headroom. No TLC published maximum — FDE judgment.
  Fare-per-mile [$1.50–$50]: TLC standard rate is $3.50/mile on open road;
    lower bound catches metering failures, upper bound catches fraud/errors.
    Not a TLC-published range — FDE judgment, may need multi-month calibration.

Usage (standalone):
  python src/validate.py --month 2025-01
Run from repo root so relative paths resolve correctly.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("validate")

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

# ── Validation thresholds ─────────────────────────────────────────────────────
# Every threshold here is an FDE judgement call — no TLC-published equivalent
# unless noted. Each has a rationale comment and a KUAL entry in the report.

DURATION_MIN_MINUTES = 1
# Lower: sub-minute "trips" are cancelled meters / system pings, no operational value

DURATION_MAX_MINUTES = 360
# Upper: 6 hours. NYC tip-to-tip is ~13 miles. Any genuine taxi trip fits within 6 hrs.
# Exceeding this = driver forgot to close the meter (known TLC data quality issue).

DISTANCE_MAX_MILES = 100
# Upper: standard TLC service area. JFK ~17 mi, Newark ~15 mi, Montauk ~120 mi
# (special negotiated rate, not metered). 100 miles captures all metered trips.
# Lower: strictly > 0. Zero distance = meter ran without movement (error or cancelled).

FARE_MAX = 500.0
# Upper: $3.50/mile × 100 miles ≈ $350 metered + $50 tolls/surcharges ≈ $400.
# $500 gives comfortable headroom. No TLC maximum published.
# Lower: ≥ 0 generally. Zero fare is valid for No-charge / Voided (payment_type 3/6).
# Zero fare with payment_type 1/2 (Credit/Cash) is flagged as fare_invalid.

FARE_PER_MILE_MIN = 1.50
# Lower: below TLC minimum suggests metering malfunction or data corruption.

FARE_PER_MILE_MAX = 50.00
# Upper: heavy stop-and-go on a very short trip can push rate high but $50/mile is
# the outer bound of plausibility. Above this = likely meter fraud or data error.

# payment_type codes where a $0 fare is NOT legitimate
PAID_PAYMENT_TYPES = {1, 2}  # 1=Credit card, 2=Cash
# payment_type 3=No charge, 6=Voided trip → $0 fare is valid business logic

# LocationIDs that exist in trip data but have no zone entry (Unknown / N/A)
UNKNOWN_LOCATION_IDS = {264, 265}

# Columns the validation rules actually touch — schema drift check
REQUIRED_COLUMNS = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "payment_type",
    "PULocationID", "DOLocationID", "passenger_count",
}

# Columns to profile with full numeric stats
NUMERIC_PROFILE_COLS = [
    "trip_distance", "fare_amount", "total_amount", "tip_amount",
    "extra", "mta_tax", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "passenger_count",
]
# Columns to profile with value_counts (low-cardinality categoricals)
CATEGORICAL_PROFILE_COLS = [
    "VendorID", "RatecodeID", "payment_type", "store_and_fwd_flag",
]
# Datetime columns to profile with min/max
DATETIME_PROFILE_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


# ── Profiling ─────────────────────────────────────────────────────────────────
def profile_dataframe(df: pd.DataFrame) -> dict:
    """
    Compute column-level profile statistics on the raw (unmodified) DataFrame.

    Runs BEFORE any validation — this is the "before you touch it" snapshot
    required by the rubric. Stats computed:
      - dtype, null_count, null_pct for all columns
      - min, max, mean, median, std for numeric columns
      - min, max (temporal) for datetime columns
      - value_counts for low-cardinality categoricals

    Returns a dict keyed by column name.
    """
    profile = {}
    n = len(df)

    for col in df.columns:
        null_count = int(df[col].isna().sum())
        info = {
            "dtype": str(df[col].dtype),
            "null_count": null_count,
            "null_pct": round(null_count / n * 100, 3) if n > 0 else 0.0,
        }

        if col in DATETIME_PROFILE_COLS:
            try:
                info["min"] = str(df[col].min())
                info["max"] = str(df[col].max())
            except Exception:
                pass

        elif col in NUMERIC_PROFILE_COLS or pd.api.types.is_numeric_dtype(df[col]):
            try:
                non_null = df[col].dropna()
                if len(non_null) > 0:
                    info["min"] = round(float(non_null.min()), 4)
                    info["max"] = round(float(non_null.max()), 4)
                    info["mean"] = round(float(non_null.mean()), 4)
                    info["median"] = round(float(non_null.median()), 4)
                    info["std"] = round(float(non_null.std()), 4)
            except Exception:
                pass

        if col in CATEGORICAL_PROFILE_COLS:
            try:
                vc = df[col].value_counts(dropna=False).head(20)
                info["value_counts"] = {str(k): int(v) for k, v in vc.items()}
            except Exception:
                pass

        profile[col] = info

    return profile


# ── Validation rules ──────────────────────────────────────────────────────────
def apply_validation_rules(
    df: pd.DataFrame,
    zone_ids: set,
    month: str,
) -> tuple:
    """
    Apply all validation rules and add per-row flag columns.

    Hard rules (H1–H6): violations mark a trip as excluded from metrics.
    Soft rules (S1–S3): violations are counted but trips are retained.

    Adds to df:
      flag_<name>        : bool columns, one per rule
      validation_flags   : pipe-separated string of violated rule names, '' if clean

    Returns: (df_with_flags, hard_flag_cols, soft_flag_cols)

    The caller decides what to do with flagged rows (run_validate excludes hard-flagged
    rows from the output Parquet; all counts are recorded in the report).
    """
    # ── Schema drift check ────────────────────────────────────────────────────
    # Fail loudly if expected columns are missing — schema drift should not be a
    # silent failure that produces silently wrong metrics downstream.
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Schema drift detected — required columns missing from Parquet: {missing}. "
            f"Check the TLC data dictionary for schema changes. "
            f"This run cannot continue safely."
        )

    # ── Parse month boundaries ────────────────────────────────────────────────
    year, mon = int(month[:4]), int(month[5:])
    month_start = pd.Timestamp(year=year, month=mon, day=1)
    # Month end = first moment of next month minus 1 second
    if mon == 12:
        month_end = pd.Timestamp(year=year + 1, month=1, day=1) - pd.Timedelta(seconds=1)
    else:
        month_end = pd.Timestamp(year=year, month=mon + 1, day=1) - pd.Timedelta(seconds=1)

    # ── Ensure datetimes are parsed ───────────────────────────────────────────
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # ── Derived fields (used across multiple rules) ───────────────────────────
    df["_duration_minutes"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60.0

    # ── H1: Out-of-month ─────────────────────────────────────────────────────
    # Trips outside the target calendar month are not January data and must not
    # contribute to January metrics. Flagged + counted; excluded from valid Parquet.
    # The manifest already confirmed stray Dec 31 and Feb 1 trips in the raw file.
    df["flag_out_of_month"] = (
        (df["tpep_pickup_datetime"] < month_start) |
        (df["tpep_pickup_datetime"] > month_end)
    )

    # ── H2: Temporal invalid ──────────────────────────────────────────────────
    # A trip cannot end before or exactly when it starts. Logically impossible.
    # Indicates vendor-level data corruption (swapped timestamps, clock drift).
    # Cannot be used for any duration, speed, or fare-per-mile calculation.
    df["flag_temporal_invalid"] = (
        df["tpep_pickup_datetime"] >= df["tpep_dropoff_datetime"]
    )

    # ── H3: Duration invalid ──────────────────────────────────────────────────
    # Lower: < 1 min — sub-minute "trips" are cancelled meters or system noise.
    # Upper: > 360 min (6 hrs) — see module docstring for threshold rationale.
    # Note: only meaningful when temporal is valid; we apply it unconditionally
    # and let the flag_counts show the overlap (double-flagged trips) in the report.
    df["flag_duration_invalid"] = (
        (df["_duration_minutes"] < DURATION_MIN_MINUTES) |
        (df["_duration_minutes"] > DURATION_MAX_MINUTES)
    )

    # ── H4: Distance invalid ─────────────────────────────────────────────────
    # Lower: ≤ 0 — zero/negative distance means meter ran without movement.
    # Upper: > 100 miles — see module docstring for threshold rationale.
    df["flag_distance_invalid"] = (
        (df["trip_distance"] <= 0) |
        (df["trip_distance"] > DISTANCE_MAX_MILES)
    )

    # ── H5: Fare invalid ─────────────────────────────────────────────────────
    # Negative: metered fares cannot be negative; signals data corruption.
    # Zero with paid payment type: $0 fare on credit/cash trip is not a valid
    #   completed metered fare. Zero fare IS valid for No-charge (3) or Voided (6).
    # Upper: > $500 — see module docstring for threshold rationale.
    payment_int = df["payment_type"].fillna(-1).astype(float).astype(int)
    fare_negative = df["fare_amount"] < 0
    fare_zero_paid = (df["fare_amount"] == 0) & (payment_int.isin(PAID_PAYMENT_TYPES))
    fare_too_high = df["fare_amount"] > FARE_MAX
    df["flag_fare_invalid"] = fare_negative | fare_zero_paid | fare_too_high

    # ── H6a: Location unknown (264 or 265) ───────────────────────────────────
    # LocationIDs 264 (Unknown) and 265 (N/A) exist in trip data but have no
    # zone entry. They cannot be joined to the dimension table. Separate flag
    # from location_invalid so their volume is visible independently.
    pu = df["PULocationID"].fillna(-1).astype(float).astype(int)
    do_ = df["DOLocationID"].fillna(-1).astype(float).astype(int)
    df["flag_location_unknown"] = (
        pu.isin(UNKNOWN_LOCATION_IDS) | do_.isin(UNKNOWN_LOCATION_IDS)
    )

    # ── H6b: Location invalid (not in lookup and not 264/265) ────────────────
    # LocationIDs that are not in the zone lookup AND not the known unknowns.
    # Indicates a value outside the expected [1–265] range — likely data corruption.
    all_known_ids = zone_ids | UNKNOWN_LOCATION_IDS
    df["flag_location_invalid"] = (
        ~pu.isin(all_known_ids) | ~do_.isin(all_known_ids)
    ) & ~df["flag_location_unknown"]

    # ── S1: Fare-per-mile outlier (soft — trip retained) ────────────────────
    # Only computed where distance > 0 to avoid division by zero.
    # Bounds: $1.50–$50/mile — see module docstring for rationale.
    # Trips with distance ≤ 0 are already flagged by H4; NaN-fill avoids
    # double-counting them as fare-per-mile outliers too.
    with np.errstate(divide="ignore", invalid="ignore"):
        fare_per_mile = np.where(
            df["trip_distance"] > 0,
            df["fare_amount"] / df["trip_distance"],
            np.nan,
        )
    df["flag_fare_per_mile_outlier"] = (
        (fare_per_mile < FARE_PER_MILE_MIN) |
        (fare_per_mile > FARE_PER_MILE_MAX)
    )
    # NaN entries (zero-distance trips) should not be flagged as outliers
    df["flag_fare_per_mile_outlier"] = df["flag_fare_per_mile_outlier"].fillna(False)

    # ── S2: Passenger count suspect (soft — trip retained) ───────────────────
    # TLC max capacity is 4 (standard) or 6 (accessible vehicle).
    # Null is explicitly allowed per TLC data dictionary — not flagged.
    df["flag_passenger_count_suspect"] = (
        df["passenger_count"].notna() & (df["passenger_count"] > 6)
    )

    # ── S3: Zero distance with fare (soft — informational) ───────────────────
    # trip_distance == 0 AND fare_amount > 0: fare charged but no miles recorded.
    # Could be a time-based waiting fare or sensor dropout. Already excluded by H4
    # (distance_invalid) so this flag is purely for the report's anomaly breakdown.
    df["flag_zero_distance_with_fare"] = (
        (df["trip_distance"] == 0) & (df["fare_amount"] > 0)
    )

    # ── Build validation_flags string column (vectorized) ────────────────────
    hard_flag_cols = [
        "flag_out_of_month",
        "flag_temporal_invalid",
        "flag_duration_invalid",
        "flag_distance_invalid",
        "flag_fare_invalid",
        "flag_location_unknown",
        "flag_location_invalid",
    ]
    soft_flag_cols = [
        "flag_fare_per_mile_outlier",
        "flag_passenger_count_suspect",
        "flag_zero_distance_with_fare",
    ]
    all_flag_cols = hard_flag_cols + soft_flag_cols
    flag_names = np.array([c.replace("flag_", "") for c in all_flag_cols])

    # Boolean flag matrix → pipe-joined string per row (vectorized via numpy)
    flag_matrix = df[all_flag_cols].values  # shape: (n_rows, n_flags), bool
    df["validation_flags"] = [
        "|".join(flag_names[row]) for row in flag_matrix
    ]

    return df, hard_flag_cols, soft_flag_cols


# ── KUAL entries ──────────────────────────────────────────────────────────────
def build_kual_entries(flag_counts: dict) -> list:
    """
    Return the Known / Unknown / Assumption / Limitation entries.

    These are the explicit audit log of every non-obvious decision made in the
    validation step. Written into the validation report JSON (not just README)
    so they travel with the evidence artifact.
    """
    return [
        {
            "category": "KNOWN",
            "entry": (
                "TLC Jan 2025 Parquet contains trips with pickup dates outside January "
                f"(Dec 31 2024 and Feb 1 2025 observed in manifest). This is a documented "
                "TLC batching pattern, not a download error. Trips are flagged 'out_of_month', "
                f"counted ({flag_counts.get('out_of_month', '?')} rows), and excluded from "
                "January metrics. They are not silently dropped — their count is visible here."
            ),
        },
        {
            "category": "KNOWN",
            "entry": (
                "'passenger_count' is nullable per TLC data dictionary — vendors are not "
                "required to record it. Null passenger_count is not treated as a validation "
                "error; it is counted in the profile null_count only."
            ),
        },
        {
            "category": "KNOWN",
            "entry": (
                "LocationIDs 264 (Unknown) and 265 (N/A) appear in trip data but have no "
                "entry in the Socrata zone lookup. Flagged separately as 'location_unknown' "
                f"({flag_counts.get('location_unknown', '?')} rows) so their volume is visible "
                "independently from out-of-range IDs ('location_invalid')."
            ),
        },
        {
            "category": "ASSUMPTION",
            "entry": (
                "Maximum trip duration cutoff of 360 minutes (6 hours) is an FDE judgement "
                "call — no formal TLC threshold is published. Rationale: NYC's geographic "
                "bounds (longest metered route < 35 miles) make any genuine taxi trip under "
                "6 hours. Trips exceeding this almost certainly represent drivers who forgot "
                "to close the meter (a known TLC data quality pattern)."
            ),
        },
        {
            "category": "ASSUMPTION",
            "entry": (
                "Maximum trip distance cap of 100 miles is an FDE judgement call, not a "
                "TLC-published threshold. Rationale: the TLC standard metered service covers "
                "NYC metro; the furthest standard airport (JFK ~17 mi, Newark ~15 mi) and "
                "even Montauk (~120 mi, special negotiated rate) fall well below this. "
                "100 miles captures all plausible metered trips while flagging odometer errors."
            ),
        },
        {
            "category": "ASSUMPTION",
            "entry": (
                "Maximum fare_amount cap of $500 is an FDE judgement call. Rationale: "
                "TLC metered rate is ~$3.50/mile; at the 100-mile distance cap that yields "
                "~$350 metered fare; adding ~$50 in tolls/surcharges gives ~$400. "
                "$500 provides headroom for edge cases while catching clearly impossible values. "
                "No TLC maximum published."
            ),
        },
        {
            "category": "ASSUMPTION",
            "entry": (
                "Fare-per-mile outlier bounds ($1.50/mile lower, $50/mile upper) are "
                "FDE-set based on TLC rate card analysis, not derived from a formal "
                "statistical model of the full dataset. Lower bound: below TLC minimum "
                "metered rate suggests metering malfunction. Upper bound: above $50/mile "
                "is outside the plausible range even for very short, congested trips. "
                "May need calibration with multi-month data."
            ),
        },
        {
            "category": "UNKNOWN",
            "entry": (
                "Whether negative 'fare_amount' records represent disputed fares, refunds, "
                "or data corruption at the vendor level. The TLC data dictionary does not "
                "document negative values. All negative fare_amount values are flagged "
                "'fare_invalid' and excluded from metrics."
            ),
        },
        {
            "category": "UNKNOWN",
            "entry": (
                "Whether TLC applies post-publication corrections to historical Parquet files. "
                "If they do, a re-download of the same month could produce a different row "
                "count or checksum. The manifest records the download timestamp and SHA-256 "
                "to make any such discrepancy detectable."
            ),
        },
        {
            "category": "LIMITATION",
            "entry": (
                "No GPS coordinates are available in the public TLC dataset (removed since 2016). "
                "All spatial analysis is limited to zone-ID granularity (263 zones). "
                "Sub-zone patterns are not observable."
            ),
        },
        {
            "category": "LIMITATION",
            "entry": (
                "'tip_amount' captures credit card tips only — cash tips are not recorded "
                "by vendors. Fare-per-mile consistency analysis uses 'fare_amount' (metered "
                "fare) rather than 'total_amount' to avoid this asymmetry distorting the metric."
            ),
        },
    ]


# ── Validation report ─────────────────────────────────────────────────────────
def build_report(
    df: pd.DataFrame,
    profile: dict,
    month: str,
    hard_flag_cols: list,
    soft_flag_cols: list,
    elapsed: float,
) -> dict:
    """Assemble the validation report JSON structure."""
    all_flag_cols = hard_flag_cols + soft_flag_cols
    n = len(df)

    has_hard_flag = df[hard_flag_cols].any(axis=1)
    has_soft_flag = df[soft_flag_cols].any(axis=1)

    rows_hard_flagged = int(has_hard_flag.sum())
    rows_soft_only_flagged = int((has_soft_flag & ~has_hard_flag).sum())
    rows_valid_out = n - rows_hard_flagged

    flag_counts = {
        col.replace("flag_", ""): int(df[col].sum()) for col in all_flag_cols
    }

    return {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "month": month,
        "elapsed_seconds": round(elapsed, 1),
        "counts": {
            "rows_in_raw": n,
            "rows_hard_flagged": rows_hard_flagged,
            "rows_hard_flagged_pct": round(rows_hard_flagged / n * 100, 3) if n else 0,
            "rows_soft_only_flagged": rows_soft_only_flagged,
            "rows_soft_only_flagged_pct": round(rows_soft_only_flagged / n * 100, 3) if n else 0,
            "rows_valid_out": rows_valid_out,
            "rows_valid_pct": round(rows_valid_out / n * 100, 3) if n else 0,
        },
        "flag_counts": flag_counts,
        "profile": profile,
        "known_unknown_assumption_limitation": build_kual_entries(flag_counts),
    }


# ── Main orchestration ────────────────────────────────────────────────────────
def run_validate(
    month: str,
    raw_dir: Path = Path("data/raw"),
    ref_dir: Path = Path("data/reference"),
    processed_dir: Path = Path("data/processed"),
) -> dict:
    """
    Run profiling + validation for a given month.

    Called by pipeline.py or standalone via CLI.

    Failure handling:
      - Missing Parquet or zone lookup → raises FileNotFoundError with clear message
      - Schema drift (missing required column) → raises ValueError with clear message
      - These are not caught here; pipeline.py handles them at the orchestration level

    Args:
        month: "YYYY-MM", e.g. "2025-01"

    Returns:
        The validation report dict (also written to data/processed/validation_report_<month>.json)
    """
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = Path(raw_dir) / f"yellow_tripdata_{month}.parquet"
    zone_path = Path(ref_dir) / "taxi_zones_socrata.json"
    # Fallback to CSV if Socrata JSON not present
    zone_csv_path = Path(ref_dir) / "taxi_zone_lookup.csv"
    validated_path = processed_dir / f"trips_validated_{month}.parquet"
    report_path = processed_dir / f"validation_report_{month}.json"

    logger.info("══════════════════════════════════════════════")
    logger.info(" VALIDATE START — month: %s", month)
    logger.info("══════════════════════════════════════════════")
    t0 = time.time()

    # ── Check inputs — fail explicitly, not silently ──────────────────────────
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Raw Parquet not found: {parquet_path}\n"
            f"Run ingest first: python src/ingest.py --month {month}"
        )
    if not zone_path.exists() and not zone_csv_path.exists():
        raise FileNotFoundError(
            f"Zone lookup not found at {zone_path} or {zone_csv_path}\n"
            f"Run ingest first: python src/ingest.py --month {month}"
        )

    # ── Load zone lookup → extract LocationID set ─────────────────────────────
    if zone_path.exists():
        with open(zone_path) as f:
            zones = json.load(f)
        zone_ids = {int(z["locationid"]) for z in zones if "locationid" in z}
        logger.info("Zone lookup loaded from Socrata JSON: %d IDs", len(zone_ids))
    else:
        zone_df = pd.read_csv(zone_csv_path)
        zone_ids = set(zone_df["LocationID"].dropna().astype(int).tolist())
        logger.info("Zone lookup loaded from CSV fallback: %d IDs", len(zone_ids))

    # ── Load Parquet (full read — profiling requires all columns) ─────────────
    logger.info("─── Loading Parquet ───")
    logger.info("  Path: %s", parquet_path)
    t_read = time.time()
    df = pd.read_parquet(parquet_path)
    logger.info(
        "  Loaded: %d rows × %d columns in %.1fs",
        len(df), len(df.columns), time.time() - t_read,
    )

    # ── Profile (before touching anything) ───────────────────────────────────
    logger.info("─── Profiling (pre-validation snapshot) ───")
    t_profile = time.time()
    profile = profile_dataframe(df)
    logger.info("  Profiled %d columns in %.1fs", len(profile), time.time() - t_profile)

    # Log a few key profile facts that are immediately meaningful
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount"]:
        if col in profile:
            p = profile[col]
            logger.info(
                "  %s: nulls=%d (%.2f%%) | range=[%s, %s]",
                col, p["null_count"], p["null_pct"],
                p.get("min", "?"), p.get("max", "?"),
            )

    # ── Apply validation rules ────────────────────────────────────────────────
    logger.info("─── Applying validation rules ───")
    t_val = time.time()
    df, hard_flag_cols, soft_flag_cols = apply_validation_rules(df, zone_ids, month)
    logger.info("  Rules applied in %.1fs", time.time() - t_val)

    # ── Log flag summary ──────────────────────────────────────────────────────
    n = len(df)
    has_hard = df[hard_flag_cols].any(axis=1)
    rows_excluded = int(has_hard.sum())
    rows_valid = n - rows_excluded

    logger.info(
        "  ROWS IN: %d | EXCLUDED (hard flags): %d (%.2f%%) | VALID OUT: %d (%.2f%%)",
        n, rows_excluded, rows_excluded / n * 100,
        rows_valid, rows_valid / n * 100,
    )
    all_flag_cols = hard_flag_cols + soft_flag_cols
    for col in all_flag_cols:
        cnt = int(df[col].sum())
        if cnt > 0:
            prefix = "[HARD]" if col in hard_flag_cols else "[soft]"
            logger.info(
                "  %s %-35s : %d rows (%.3f%%)",
                prefix, col.replace("flag_", ""), cnt, cnt / n * 100,
            )

    # ── Write valid-rows Parquet ──────────────────────────────────────────────
    logger.info("─── Writing validated Parquet ───")
    valid_mask = ~has_hard
    valid_df = df[valid_mask].copy()

    # Drop internal working columns; keep validation_flags string for audit
    internal_cols = [c for c in valid_df.columns if c.startswith("flag_") or c.startswith("_")]
    valid_df = valid_df.drop(columns=internal_cols)

    valid_df.to_parquet(validated_path, index=False)
    logger.info(
        "  Written: %d rows → %s",
        len(valid_df), validated_path,
    )

    # ── Build and write report ────────────────────────────────────────────────
    logger.info("─── Writing validation report ───")
    elapsed = time.time() - t0
    report = build_report(df, profile, month, hard_flag_cols, soft_flag_cols, elapsed)

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("  Report written → %s", report_path)

    logger.info("══════════════════════════════════════════════")
    logger.info(
        " VALIDATE COMPLETE — %.1fs | valid rows: %d / %d (%.1f%%)",
        elapsed, rows_valid, n, rows_valid / n * 100,
    )
    logger.info("══════════════════════════════════════════════")

    return report


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "NYC TLC validate: profile raw Parquet and apply validation rules.\n"
            "Run from repo root: python src/validate.py --month 2025-01"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--month",
        required=True,
        metavar="YYYY-MM",
        help="Month to validate (e.g. 2025-01).",
    )
    args = parser.parse_args()

    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        logger.error("Invalid --month format: '%s'. Expected YYYY-MM.", args.month)
        sys.exit(1)

    report = run_validate(args.month)

    print("\n── Validation summary ──────────────────────────────")
    c = report["counts"]
    print(f"  Rows in raw       : {c['rows_in_raw']:>10,}")
    print(f"  Hard-flagged out  : {c['rows_hard_flagged']:>10,}  ({c['rows_hard_flagged_pct']:.2f}%)")
    print(f"  Soft-flagged only : {c['rows_soft_only_flagged']:>10,}  ({c['rows_soft_only_flagged_pct']:.2f}%)")
    print(f"  Valid out (→ model): {c['rows_valid_out']:>10,}  ({c['rows_valid_pct']:.2f}%)")
    print("\n  Flag breakdown:")
    for flag, cnt in report["flag_counts"].items():
        if cnt > 0:
            pct = cnt / c["rows_in_raw"] * 100
            print(f"    {flag:<35} : {cnt:>8,}  ({pct:.3f}%)")
    print("────────────────────────────────────────────────────")
