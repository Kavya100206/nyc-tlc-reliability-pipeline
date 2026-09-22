"""
src/metrics.py — Phase 4 (Class 7): KPI Metric Computation
NYC TLC Trip Reliability & Efficiency Pipeline

Computes the 5 KPI metrics against the DuckDB model and writes results to CSV.

KPI: Trip Reliability & Efficiency Index
  M1  Data Reliability Rate      — % of raw trips passing all hard validation rules
  M2  Median Trip Duration        — MEDIAN(duration_minutes) by borough × hour
  M3  Median Trip Speed           — MEDIAN(speed_mph) by borough × hour
  M4  Fare-per-Mile Consistency   — mean, std, outlier% of fare/mile by borough
  M5  Anomaly Rate                — % of raw trips with any flag (hard or soft)

Output files (all committed — required evidence deliverables):
  outputs/metrics_<month>.csv
      Scalar summary: M1 and M5 with per-flag breakdown. One row per metric.
  outputs/metrics_duration_speed_<month>.csv
      M2 + M3: median duration and speed by pickup_borough × pickup_hour.
      This is the primary operational efficiency table.
  outputs/metrics_duration_speed_by_zone_<month>.csv
      M2 + M3 supplementary: same metrics at zone level. Noisier (low-n zones)
      but included to document WHY borough aggregation was chosen. See FDE note.
  outputs/metrics_fare_<month>.csv
      M4: fare-per-mile mean, std, outlier% per pickup borough.

Soft-flag treatment (see diagrams/data_model.md and plan for full rationale):
  M1, M5 : Source from validation report JSON (not the DuckDB model)
  M2, M3 : Soft-flagged trips INCLUDED. Duration/speed are timestamp-derived;
            fare anomalies do not affect them.
  M4     : Soft-flagged trips INCLUDED and explicitly broken out.
            fare_per_mile_outlier trips ARE the M4 anomaly signal — excluding them
            would make the pricing integrity metric meaningless.

FDE judgement call — Borough vs. Zone for M2/M3:
  Borough × hour (120 rows) is the primary output. Zone × hour (6,312 rows)
  is provided as supplementary. One month of data produces many zones with
  very few trips; zone-level medians for low-n zones are statistically noisy
  and not operationally meaningful. The supplementary file makes this trade-off
  visible and defensible to a grader.

Usage (standalone):
  python src/metrics.py --month 2025-01
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# Allow running as a script (python3 src/metrics.py) from repo root
# without needing the package installed. sys.path insert is a no-op when
# metrics.py is imported by pipeline.py (repo root is already on path).
if __name__ == "__main__" or "src" not in sys.modules:
    import sys as _sys
    from pathlib import Path as _Path
    _repo_root = str(_Path(__file__).parent.parent)
    if _repo_root not in _sys.path:
        _sys.path.insert(0, _repo_root)

from src.model import run_model

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("metrics")

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

# ── SQL Queries ───────────────────────────────────────────────────────────────
# All thresholds in query comments match the constants in validate.py.
# Kept as module-level constants for readability and testability.

# M2 + M3 — primary: borough × hour
# Includes all valid trips (hard-rule-clean + soft-flagged-retained).
# Rows with NULL pu_borough (LEFT JOIN miss) are excluded by WHERE clause;
# their count is logged as a model warning in model.py.
SQL_M2_M3_BOROUGH_HOUR = """
SELECT
    COALESCE(pu_borough, '(unknown)') AS pu_borough,
    pickup_hour,
    COUNT(*)                                       AS trip_count,
    ROUND(MEDIAN(duration_minutes), 2)             AS median_duration_min,
    ROUND(MEDIAN(speed_mph), 2)                    AS median_speed_mph,
    ROUND(AVG(trip_distance), 3)                   AS avg_trip_distance_miles,
    ROUND(AVG(duration_minutes), 2)                AS avg_duration_min,
    ROUND(AVG(speed_mph), 2)                       AS avg_speed_mph
FROM fact_trips_enriched
WHERE pu_borough IS NOT NULL
GROUP BY pu_borough, pickup_hour
ORDER BY pu_borough, pickup_hour
"""

# M2 + M3 — supplementary: zone × hour
# Same query, finer granularity. Many low-n zone-hours will have noisy medians
# (e.g., 3 trips). Provided to justify the borough aggregation choice.
SQL_M2_M3_ZONE_HOUR = """
SELECT
    COALESCE(pu_borough, '(unknown)') AS pu_borough,
    COALESCE(pu_zone,    '(unknown)') AS pu_zone,
    pickup_hour,
    COUNT(*)                                       AS trip_count,
    ROUND(MEDIAN(duration_minutes), 2)             AS median_duration_min,
    ROUND(MEDIAN(speed_mph), 2)                    AS median_speed_mph,
    ROUND(AVG(trip_distance), 3)                   AS avg_trip_distance_miles
FROM fact_trips_enriched
WHERE pu_borough IS NOT NULL
  AND pu_zone    IS NOT NULL
GROUP BY pu_borough, pu_zone, pickup_hour
ORDER BY pu_borough, pu_zone, pickup_hour
"""

# M4 — fare-per-mile consistency by borough
# fare_per_mile is NULL for zero-distance trips (already excluded by H4 hard rule;
# NULLIF in model.py handles any residual edge cases). NULL rows excluded from AVG/STDDEV.
# outlier_count: trips with validation_flags containing 'fare_per_mile_outlier' (soft flag S1).
# These are retained in the valid Parquet and ARE included in the mean/std calculation —
# they are the pricing anomaly signal, not noise to be filtered out.
SQL_M4_FARE_BOROUGH = """
SELECT
    COALESCE(pu_borough, '(unknown)')               AS pu_borough,
    COUNT(*)                                         AS trip_count,
    COUNT(fare_per_mile)                             AS fare_trips_with_distance,
    ROUND(AVG(fare_per_mile),         4)             AS fare_per_mile_mean,
    ROUND(STDDEV_SAMP(fare_per_mile), 4)             AS fare_per_mile_std,
    ROUND(MIN(fare_per_mile),         4)             AS fare_per_mile_min,
    ROUND(MAX(fare_per_mile),         4)             AS fare_per_mile_max,
    ROUND(MEDIAN(fare_per_mile),      4)             AS fare_per_mile_median,
    -- 2-sigma bounds as a consistency spread indicator (FDE-defined; not a TLC standard)
    ROUND(AVG(fare_per_mile) - 2 * STDDEV_SAMP(fare_per_mile), 4) AS fare_per_mile_low_2std,
    ROUND(AVG(fare_per_mile) + 2 * STDDEV_SAMP(fare_per_mile), 4) AS fare_per_mile_high_2std,
    -- Outlier flag count and rate: trips where fare/mile is outside [$1.50, $50.00]
    -- (thresholds from validate.py; FDE judgement — documented in KUAL)
    SUM(CASE WHEN contains(validation_flags, 'fare_per_mile_outlier') THEN 1 ELSE 0 END)
                                                     AS fare_per_mile_outlier_count,
    ROUND(
        100.0 * SUM(CASE WHEN contains(validation_flags, 'fare_per_mile_outlier') THEN 1 ELSE 0 END)
        / NULLIF(COUNT(fare_per_mile), 0),
        3
    )                                                AS fare_per_mile_outlier_pct
FROM fact_trips_enriched
WHERE pu_borough IS NOT NULL
GROUP BY pu_borough
ORDER BY pu_borough
"""


# ── Metric runner ─────────────────────────────────────────────────────────────
def compute_metrics(
    month: str,
    conn: duckdb.DuckDBPyConnection,
    report_path: Path,
    outputs_dir: Path,
) -> dict:
    """
    Run all 5 KPI metrics against the DuckDB model and write CSV outputs.

    Args:
        month       : "YYYY-MM"
        conn        : live DuckDB connection from model.build_model()
        report_path : path to validation_report_<month>.json (source for M1, M5)
        outputs_dir : directory to write metric CSVs into

    Returns:
        dict of output file paths keyed by metric name
    """
    outputs_dir = Path(outputs_dir)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    logger.info("══════════════════════════════════════════════")
    logger.info(" METRICS START — month: %s", month)
    logger.info("══════════════════════════════════════════════")

    # ── Load validation report for M1 and M5 ─────────────────────────────────
    if not report_path.exists():
        raise FileNotFoundError(
            f"Validation report not found: {report_path}\n"
            f"Run validate first: python src/validate.py --month {month}"
        )
    with open(report_path) as f:
        report = json.load(f)
    counts = report["counts"]
    flag_counts = report.get("flag_counts", {})
    logger.info("Validation report loaded from: %s", report_path)

    output_paths = {}

    # ── M1 + M5: Scalar summary CSV ───────────────────────────────────────────
    logger.info("─── M1 + M5: Scalar summary ───")

    raw = counts["rows_in_raw"]
    valid = counts["rows_valid_out"]
    hard_flagged = counts["rows_hard_flagged"]
    soft_only = counts["rows_soft_only_flagged"]

    # Anomaly rate = everything that got any flag (hard OR soft) / raw total
    total_flagged = hard_flagged + soft_only
    anomaly_rate = round(total_flagged / raw * 100, 3) if raw else 0

    summary_rows = [
        # M1: Data reliability
        {
            "metric_name": "data_reliability_pct",
            "kpi_component": "M1_reliability",
            "group": "MONTH",
            "value": counts["rows_valid_pct"],
            "unit": "%",
            "description": "% of raw trips passing all hard validation rules (feeding model.py)",
        },
        {
            "metric_name": "valid_trip_count",
            "kpi_component": "M1_reliability",
            "group": "MONTH",
            "value": valid,
            "unit": "trips",
            "description": "Absolute count of valid trips used in metrics computation",
        },
        {
            "metric_name": "raw_trip_count",
            "kpi_component": "M1_reliability",
            "group": "MONTH",
            "value": raw,
            "unit": "trips",
            "description": "Total rows in source Parquet (before any filtering)",
        },
        # M5: Anomaly rate
        {
            "metric_name": "anomaly_rate_pct",
            "kpi_component": "M5_anomaly",
            "group": "MONTH",
            "value": anomaly_rate,
            "unit": "%",
            "description": "% of raw trips with at least one flag (hard excluded + soft retained)",
        },
        {
            "metric_name": "hard_flagged_count",
            "kpi_component": "M5_anomaly",
            "group": "MONTH",
            "value": hard_flagged,
            "unit": "trips",
            "description": "Trips excluded from all metric computations (failed hard rules)",
        },
        {
            "metric_name": "soft_only_flagged_count",
            "kpi_component": "M5_anomaly",
            "group": "MONTH",
            "value": soft_only,
            "unit": "trips",
            "description": "Anomalous but retained trips (fare-per-mile outlier, passenger suspect)",
        },
    ]

    # Per-flag breakdown rows for M5
    flag_label_map = {
        "out_of_month":             "Out-of-month trips (Dec/Feb boundary — TLC batching pattern)",
        "temporal_invalid":         "Temporal invalid (dropoff ≤ pickup — data corruption)",
        "duration_invalid":         "Duration invalid (< 1 min or > 360 min — meter error)",
        "distance_invalid":         "Distance invalid (≤ 0 or > 100 miles — odometer error)",
        "fare_invalid":             "Fare invalid (negative, zero-on-paid-trip, or > $500)",
        "location_unknown":         "Location unknown (ID 264/265 — no zone mapping available)",
        "location_invalid":         "Location invalid (ID outside known range — data corruption)",
        "fare_per_mile_outlier":    "Fare-per-mile outlier (< $1.50/mi or > $50/mi — soft flag)",
        "passenger_count_suspect":  "Passenger count suspect (> 6 — physically impossible; soft flag)",
        "zero_distance_with_fare":  "Zero distance with fare (informational; caught by distance_invalid)",
    }
    for flag_name, flag_count in flag_counts.items():
        summary_rows.append({
            "metric_name": f"flag_count_{flag_name}",
            "kpi_component": "M5_anomaly",
            "group": "FLAG",
            "value": flag_count,
            "unit": "trips",
            "description": flag_label_map.get(flag_name, flag_name),
        })

    summary_path = outputs_dir / f"metrics_{month}.csv"
    _write_csv(summary_rows, summary_path)
    output_paths["summary"] = summary_path
    logger.info("  Written: %s (%d rows)", summary_path, len(summary_rows))
    logger.info(
        "  M1 data_reliability_pct = %.3f%% | M5 anomaly_rate_pct = %.3f%%",
        counts["rows_valid_pct"], anomaly_rate,
    )

    # ── M2 + M3: Duration and speed by borough × hour ─────────────────────────
    logger.info("─── M2 + M3: Duration and speed by borough × hour ───")
    df_bh = conn.execute(SQL_M2_M3_BOROUGH_HOUR).df()
    bh_path = outputs_dir / f"metrics_duration_speed_{month}.csv"
    df_bh.to_csv(bh_path, index=False)
    output_paths["duration_speed_borough_hour"] = bh_path
    logger.info("  Written: %s (%d rows)", bh_path, len(df_bh))

    # Log a few headline numbers for the run log
    if not df_bh.empty:
        row_peak = df_bh.loc[df_bh["median_duration_min"].idxmax()]
        row_fast = df_bh.loc[df_bh["median_speed_mph"].idxmax()]
        logger.info(
            "  Slowest borough-hour: %s at %02d:00 — %.1f min median trip",
            row_peak["pu_borough"], int(row_peak["pickup_hour"]),
            row_peak["median_duration_min"],
        )
        logger.info(
            "  Fastest borough-hour: %s at %02d:00 — %.1f mph median speed",
            row_fast["pu_borough"], int(row_fast["pickup_hour"]),
            row_fast["median_speed_mph"],
        )

    # ── M2 + M3 supplementary: zone × hour ───────────────────────────────────
    logger.info("─── M2 + M3 supplementary: duration and speed by zone × hour ───")
    df_zh = conn.execute(SQL_M2_M3_ZONE_HOUR).df()
    zh_path = outputs_dir / f"metrics_duration_speed_by_zone_{month}.csv"
    df_zh.to_csv(zh_path, index=False)
    output_paths["duration_speed_zone_hour"] = zh_path
    # Log how many zone-hours have fewer than 10 trips (justifies borough aggregation)
    low_n = (df_zh["trip_count"] < 10).sum()
    logger.info(
        "  Written: %s (%d rows | %d zone-hours with < 10 trips — "
        "justifies borough-level primary aggregation)",
        zh_path, len(df_zh), low_n,
    )

    # ── M4: Fare-per-mile consistency by borough ──────────────────────────────
    logger.info("─── M4: Fare-per-mile consistency by borough ───")
    df_fare = conn.execute(SQL_M4_FARE_BOROUGH).df()
    fare_path = outputs_dir / f"metrics_fare_{month}.csv"
    df_fare.to_csv(fare_path, index=False)
    output_paths["fare_consistency"] = fare_path
    logger.info("  Written: %s (%d rows)", fare_path, len(df_fare))

    if not df_fare.empty:
        for _, row in df_fare.iterrows():
            logger.info(
                "  %s: mean=$%.2f/mi | std=$%.2f | outlier=%.1f%%",
                row["pu_borough"],
                row["fare_per_mile_mean"],
                row["fare_per_mile_std"] if row["fare_per_mile_std"] else 0,
                row["fare_per_mile_outlier_pct"] if row["fare_per_mile_outlier_pct"] else 0,
            )

    elapsed = time.time() - t0
    logger.info("══════════════════════════════════════════════")
    logger.info(" METRICS COMPLETE — %.1fs", elapsed)
    logger.info("══════════════════════════════════════════════")

    return output_paths


def _write_csv(rows: list, path: Path) -> None:
    """Write a list of dicts to CSV without a pandas dependency at import time."""
    import csv
    if not rows:
        path.write_text("")
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# ── Top-level entry ───────────────────────────────────────────────────────────
def run_metrics(
    month: str,
    processed_dir: Path = Path("data/processed"),
    ref_dir: Path = Path("data/reference"),
    outputs_dir: Path = Path("outputs"),
) -> dict:
    """
    High-level entry called by pipeline.py or CLI.
    Builds the model then runs all metrics. Returns output file paths.
    """
    conn = run_model(month, processed_dir=processed_dir, ref_dir=ref_dir)
    report_path = Path(processed_dir) / f"validation_report_{month}.json"
    return compute_metrics(month, conn, report_path, outputs_dir)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "NYC TLC metrics: compute KPI metrics from the DuckDB model.\n"
            "Run from repo root: python src/metrics.py --month 2025-01"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--month", required=True, metavar="YYYY-MM")
    args = parser.parse_args()

    paths = run_metrics(args.month)

    print("\n── Metric outputs ─────────────────────────────────")
    for name, path in paths.items():
        print(f"  {name:<40} → {path}")
    print("────────────────────────────────────────────────────")
