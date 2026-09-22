"""
src/model.py — Phase 4 (Class 7): Relational / Event Model
NYC TLC Trip Reliability & Efficiency Pipeline

Builds an in-memory DuckDB database from:
  - data/processed/trips_validated_<month>.parquet  (fact table)
  - data/reference/taxi_zones_socrata.json          (dimension table)

Schema:
  dim_zones          — one row per taxi zone (location_id, zone_name, borough)
  fact_trips         — one row per valid trip with derived fields
  fact_trips_enriched — view joining both zone dimensions onto the fact table

Design decisions (FDE judgement calls — also in diagrams/data_model.md):
  In-memory DuckDB: no .duckdb file persisted. Pipeline output = CSVs, not the DB.
    In-memory is perfectly idempotent; a stale on-disk DB could diverge from the
    pipeline's actual state if run conditions change.
  Surrogate trip_id: TLC provides no natural primary key for trip records.
    ROW_NUMBER() OVER () gives a stable surrogate within one pipeline run.
  LEFT JOIN on dim_zones: defensive — any missed LocationID surfaces as NULL in
    query results rather than silently dropping rows from aggregations.
  Event model: request event is NOT observable in TLC data (no dispatch timestamp
    for yellow taxis). This is logged as a KUAL Limitation in the validation report
    and shown explicitly in diagrams/data_model.md.

Usage (standalone):
  python src/model.py --month 2025-01   # prints row counts and schema
  from src.model import build_model     # for use by metrics.py and pipeline.py
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("model")

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


def _load_zone_dimension(zone_path: Path) -> pd.DataFrame:
    """
    Load taxi zone JSON into a clean pandas DataFrame for DuckDB registration.

    Uses pandas as an intermediate layer rather than DuckDB's read_json_auto()
    because the Socrata JSON contains a nested GeoJSON geometry field ('the_geom')
    that can trip up auto-schema inference. We only need three columns.

    Returns DataFrame with columns: location_id (int), zone_name (str), borough (str)
    """
    with open(zone_path) as f:
        raw = json.load(f)

    zones = []
    for z in raw:
        loc_id = z.get("locationid")
        if loc_id is None:
            continue
        try:
            zones.append({
                "location_id": int(loc_id),
                "zone_name": z.get("zone", "Unknown"),
                "borough": z.get("borough", "Unknown"),
            })
        except (ValueError, TypeError):
            logger.warning("Skipping zone record with non-integer locationid: %s", loc_id)

    df = pd.DataFrame(zones)
    logger.info("Zone dimension loaded: %d rows", len(df))
    return df


def build_model(
    month: str,
    validated_parquet_path: Path,
    zone_json_path: Path,
) -> duckdb.DuckDBPyConnection:
    """
    Build the in-memory DuckDB model for a given month.

    Creates:
      dim_zones           — zone dimension table (location_id, zone_name, borough)
      fact_trips          — trip fact table with all validated trips + derived fields
      fact_trips_enriched — view: fact_trips LEFT JOIN dim_zones × 2 (PU and DO zones)

    Args:
        month                  : "YYYY-MM" string
        validated_parquet_path : path to data/processed/trips_validated_<month>.parquet
        zone_json_path         : path to data/reference/taxi_zones_socrata.json

    Returns:
        Live duckdb.DuckDBPyConnection with all tables/views registered.
        Caller is responsible for closing it (or it auto-closes when GC'd).

    Raises:
        FileNotFoundError : if either input file is missing
        RuntimeError      : if the fact table loads 0 rows (corrupt or wrong month)
    """
    validated_parquet_path = Path(validated_parquet_path)
    zone_json_path = Path(zone_json_path)

    # ── Input existence checks — explicit failure, not silent ─────────────────
    if not validated_parquet_path.exists():
        raise FileNotFoundError(
            f"Validated Parquet not found: {validated_parquet_path}\n"
            f"Run validate first: python src/validate.py --month {month}"
        )
    if not zone_json_path.exists():
        raise FileNotFoundError(
            f"Zone lookup JSON not found: {zone_json_path}\n"
            f"Run ingest first: python src/ingest.py --month {month}"
        )

    logger.info("Building in-memory DuckDB model for month: %s", month)
    t0 = time.time()

    conn = duckdb.connect(database=":memory:")

    # ── dim_zones ─────────────────────────────────────────────────────────────
    zones_df = _load_zone_dimension(zone_json_path)
    # Register pandas DataFrame as a DuckDB relation, then materialise as a table
    conn.register("_zones_staging", zones_df)
    conn.execute("""
        CREATE TABLE dim_zones AS
        SELECT
            location_id,
            zone_name,
            borough
        FROM _zones_staging
        ORDER BY location_id
    """)
    n_zones = conn.execute("SELECT COUNT(*) FROM dim_zones").fetchone()[0]
    logger.info("dim_zones: %d rows", n_zones)

    # ── fact_trips ────────────────────────────────────────────────────────────
    # Read the validated Parquet natively in DuckDB (fast columnar scan).
    # All derived fields (duration, speed, fare_per_mile, hour, date, soft flag)
    # are computed once here so every downstream metric query can use them directly.
    #
    # NOTE on event model:
    #   - pickup event:  pickup_at, pu_location_id, pickup_hour, pickup_date
    #   - dropoff event: dropoff_at, do_location_id, duration_minutes, speed_mph
    #   - payment event: payment_type, fare_amount, fare_per_mile, total_amount
    #   - REQUEST EVENT IS ABSENT: TLC does not record the hail/dispatch moment
    #     for yellow taxis. Wait time and request-to-pickup latency are not computable.
    #     See diagrams/data_model.md and validation_report KUAL.
    parquet_str = str(validated_parquet_path).replace("'", "''")  # escape for SQL
    conn.execute(f"""
        CREATE TABLE fact_trips AS
        SELECT
            -- Surrogate key (no natural PK in TLC data)
            ROW_NUMBER() OVER () AS trip_id,

            -- ── Pickup event ─────────────────────────────────────────────────
            tpep_pickup_datetime                            AS pickup_at,
            PULocationID                                    AS pu_location_id,
            HOUR(tpep_pickup_datetime)                      AS pickup_hour,
            CAST(DATE_TRUNC('day', tpep_pickup_datetime) AS DATE) AS pickup_date,

            -- ── Dropoff event ─────────────────────────────────────────────────
            tpep_dropoff_datetime                           AS dropoff_at,
            DOLocationID                                    AS do_location_id,
            -- duration_minutes: total trip time as a continuous float
            (epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime)) / 60.0
                                                            AS duration_minutes,

            -- ── Trip attributes ───────────────────────────────────────────────
            trip_distance,
            passenger_count,
            RatecodeID                                      AS rate_code_id,
            store_and_fwd_flag,

            -- speed_mph: average speed over the trip (proxy for congestion)
            -- NULLIF prevents division by zero for any edge-case zero-duration rows
            trip_distance / NULLIF(
                (epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime)) / 3600.0,
                0
            )                                               AS speed_mph,

            -- ── Payment event ─────────────────────────────────────────────────
            payment_type,
            fare_amount,
            -- fare_per_mile: pricing integrity metric. NULL when distance = 0
            -- (zero-distance trips are already excluded by hard validation rule H4,
            -- but NULLIF is defensive against any edge case that slipped through)
            fare_amount / NULLIF(trip_distance, 0)          AS fare_per_mile,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            congestion_surcharge,
            total_amount,

            -- ── Audit fields ──────────────────────────────────────────────────
            validation_flags,
            -- has_soft_flag: True for retained-but-anomalous trips (fare-per-mile
            -- outliers, passenger count suspect). Used in M4 and M5 computations.
            (validation_flags != '')                         AS has_soft_flag

        FROM read_parquet('{parquet_str}')
    """)

    n_trips = conn.execute("SELECT COUNT(*) FROM fact_trips").fetchone()[0]
    if n_trips == 0:
        raise RuntimeError(
            f"fact_trips loaded 0 rows from {validated_parquet_path}. "
            "This likely means the validated Parquet is empty or the wrong month. "
            "Re-run validate.py to regenerate it."
        )
    logger.info("fact_trips: %d rows", n_trips)

    # ── fact_trips_enriched (view) ────────────────────────────────────────────
    # Joins both zone dimensions (pickup and dropoff) onto the fact table.
    # LEFT JOIN: defensive — any missed LocationID produces NULL zone columns
    # rather than silently dropping rows from aggregations. If nulls appear in
    # query results, they signal a data model inconsistency to investigate.
    conn.execute("""
        CREATE VIEW fact_trips_enriched AS
        SELECT
            t.*,
            pu.zone_name  AS pu_zone,
            pu.borough    AS pu_borough,
            do_.zone_name AS do_zone,
            do_.borough   AS do_borough
        FROM fact_trips t
        LEFT JOIN dim_zones pu  ON t.pu_location_id = pu.location_id
        LEFT JOIN dim_zones do_ ON t.do_location_id = do_.location_id
    """)
    logger.info("fact_trips_enriched view created")

    # ── Null zone check — surface any LEFT JOIN mismatches ────────────────────
    null_borough = conn.execute(
        "SELECT COUNT(*) FROM fact_trips_enriched WHERE pu_borough IS NULL"
    ).fetchone()[0]
    if null_borough > 0:
        logger.warning(
            "%d trips have NULL pu_borough after zone join. "
            "These LocationIDs are in the validated data but not in dim_zones. "
            "They will be excluded from borough-level metric aggregations.",
            null_borough,
        )

    elapsed = time.time() - t0
    logger.info(
        "Model built — dim_zones: %d | fact_trips: %d | elapsed: %.1fs",
        n_zones, n_trips, elapsed,
    )
    return conn


def run_model(
    month: str,
    processed_dir: Path = Path("data/processed"),
    ref_dir: Path = Path("data/reference"),
) -> duckdb.DuckDBPyConnection:
    """
    Convenience wrapper called by pipeline.py. Resolves paths from month string.
    """
    validated_path = Path(processed_dir) / f"trips_validated_{month}.parquet"
    zone_path = Path(ref_dir) / "taxi_zones_socrata.json"
    return build_model(month, validated_path, zone_path)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "NYC TLC model: build in-memory DuckDB fact+dimension model.\n"
            "Run from repo root: python src/model.py --month 2025-01"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--month", required=True, metavar="YYYY-MM")
    args = parser.parse_args()

    conn = run_model(args.month)

    print("\n── Model summary ───────────────────────────────────")
    for tbl in ["dim_zones", "fact_trips"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl:<25} : {n:>10,} rows")

    print("\n  fact_trips schema:")
    for row in conn.execute("DESCRIBE fact_trips").fetchall():
        col, dtype = row[0], row[1]
        print(f"    {col:<30} {dtype}")

    print("\n  Sample borough distribution (pu_borough):")
    rows = conn.execute("""
        SELECT pu_borough, COUNT(*) AS trips
        FROM fact_trips_enriched
        GROUP BY pu_borough
        ORDER BY trips DESC
    """).fetchall()
    for borough, count in rows:
        print(f"    {str(borough):<20} : {count:>10,}")
    print("────────────────────────────────────────────────────")
