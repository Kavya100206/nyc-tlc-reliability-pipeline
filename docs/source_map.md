# Source Map — NYC TLC Trip Reliability & Efficiency Pipeline

> **Class 4 deliverable.** Maps every business question to the data source that answers it.
> Produced before writing any ingestion code, so retrieval logic is driven by documented need.

---

## Source Map Table

| # | Business Question | Data Needed | Source System | Owner / Publisher | URL | Retrieval Mode | Grain | Update Cadence | Known Gaps / Risks |
|---|---|---|---|---|---|---|---|---|---|
| S1 | How many trips occurred, where, and when? | Trip records: pickup/dropoff datetime, LocationID, passenger count, distance | NYC TLC Trip Record Data — Yellow Taxi Parquet | NYC TLC (city agency) | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet` | **Bulk Parquet download (HTTP)** | One row per trip | Monthly, ~2–3 months lag | No real-time feed; lat/lon dropped since 2016; vendor-reported (not independently verified) |
| S2 | What zone/borough does each LocationID map to? | Zone lookup: LocationID → Zone name, Borough, service zone | NYC Open Data — Taxi Zones dataset (`2yv8-t2f9`) | NYC TLC via NYC Open Data (Socrata) | `https://data.cityofnewyork.us/resource/755u-8jsi.json` | **Socrata Open Data API (sodapy)** | One row per zone (263 zones total) | Rarely changes; treated as static dimension | Unknown how TLC handles zone retirements; 263 zones documented but S2 and S1 may not cover all |
| S3 | Zone lookup fallback / cross-validation | Same as S2 | TLC Taxi Zone Lookup CSV (static reference) | NYC TLC | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` | **Bulk CSV download (HTTP)** | Same grain as S2 | Published alongside Parquet data dictionaries | Used only if Socrata API is unavailable; not a separate retrieval mode for rubric purposes |

---

## Source Detail: S1 — TLC Yellow Taxi Parquet (Bulk Download)

**Business justification:** Yellow Taxi is the largest, most historically complete vehicle
type in TLC data. January 2025 is the most recent complete month as of the analysis date
and produces a single Parquet file (~50 MB), keeping pipeline runtime fast and explainable.

**Schema (expected columns, as of 2024–2025 data dictionary):**

| Column | Type | Business Meaning |
|---|---|---|
| `VendorID` | int | Data provider (1 = Creative Mobile Tech, 2 = VeriFone) |
| `tpep_pickup_datetime` | timestamp | Trip start time |
| `tpep_dropoff_datetime` | timestamp | Trip end time |
| `passenger_count` | float | Self-reported by driver; nullable |
| `trip_distance` | float | Miles, odometer-based |
| `RatecodeID` | float | Fare rate (1=Standard, 2=JFK, 3=Newark, etc.) |
| `store_and_fwd_flag` | string | Y/N — whether record was buffered before transmission |
| `PULocationID` | int | Pickup taxi zone (FK → zone lookup) |
| `DOLocationID` | int | Dropoff taxi zone (FK → zone lookup) |
| `payment_type` | float | 1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided |
| `fare_amount` | float | Metered fare (USD) |
| `extra` | float | Surcharges (rush hour, overnight) |
| `mta_tax` | float | $0.50 MTA tax |
| `tip_amount` | float | Credit card tips (cash tips not captured) |
| `tolls_amount` | float | Bridge/tunnel tolls |
| `improvement_surcharge` | float | $0.30 improvement surcharge |
| `total_amount` | float | Total charged to passenger |
| `congestion_surcharge` | float | NYC congestion pricing surcharge |
| `airport_fee` | float | Airport pickup fee (post-2022) |

**Known gaps:**
- Lat/lon coordinates removed from public data since 2016 — spatial joins require zone IDs only.
- `passenger_count` is nullable (driver doesn't always enter it).
- Vendor-reported: no independent GPS verification of trip_distance.

---

## Source Detail: S2 — Socrata Taxi Zone Lookup (API Pull)

**Business justification:** This is the authoritative dimension table mapping LocationID
integers (the only spatial reference in the trip data) to human-readable zone names and
boroughs. Pulling via Socrata API satisfies the "second distinct retrieval mode" requirement
and ensures the zone table is fetched programmatically rather than hand-copied.

**Socrata dataset:** `755u-8jsi` (NYC Taxi Zones)
**API endpoint:** `https://data.cityofnewyork.us/resource/755u-8jsi.json`

**Expected columns:**

| Column | Type | Business Meaning |
|---|---|---|
| `objectid` | string/int | Socrata row ID |
| `shape_area` | string | Zone polygon area (not used in pipeline) |
| `zone` | string | Zone name (e.g., "JFK Airport") |
| `locationid` | string | Maps to PULocationID/DOLocationID in trip data |
| `borough` | string | Borough name (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR) |
| `shape_leng` | string | Zone perimeter (not used) |

**Known gaps:**
- LocationIDs 264 (Unknown) and 265 (N/A) exist in trip data but have no zone entry — must be handled explicitly.
- Socrata API has rate limits without an app token; pipeline should handle 429 gracefully.
- Zone names/shapes could theoretically change — treated as static for this analysis period.

---

## Retrieval Mode Summary

| Mode | Script Function | What it fetches | Output file |
|---|---|---|---|
| **Bulk HTTP (Parquet)** | `ingest.py::download_bulk_trip_file()` | Yellow Taxi Jan 2025 trips | `data/raw/yellow_tripdata_2025-01.parquet` |
| **Socrata API** | `ingest.py::fetch_zone_lookup_api()` | 263 taxi zones → zone name + borough | `data/reference/taxi_zones_socrata.json` |
| **Bulk HTTP (CSV, fallback)** | `ingest.py::download_zone_lookup_csv()` | Same zone data via TLC CDN | `data/reference/taxi_zone_lookup.csv` |

---

## Data Flow Diagram (Text)

```
[NYC TLC CDN]                    [NYC Open Data / Socrata]
     |                                      |
     | HTTP GET (Parquet ~50MB)             | Socrata API (JSON, 263 rows)
     v                                      v
data/raw/yellow_tripdata_2025-01.parquet   data/reference/taxi_zones_socrata.json
     |                                      |
     +------------------+-------------------+
                        |
                   src/validate.py
                        |
              data/processed/ (flagged trips, validation report)
                        |
                   src/model.py (DuckDB)
                        |
              fact_trips JOIN dim_zones
                        |
                   src/metrics.py
                        |
              outputs/metrics_2025-01.csv
```

---

*Produced: Phase 1 (Class 4 — Source Understanding)*
*Author: FDE pipeline — NYC TLC Reliability & Efficiency Project*
