# NYC TLC Trip Reliability & Efficiency Pipeline

## Problem Statement

NYC TLC (Taxi & Limousine Commission) leadership lacks a trustworthy, repeatable view of
monthly trip operations. One-off analyses are hard to audit, reproduce, or hand off. This
project delivers a small, dependable data pipeline that goes from raw TLC Parquet files
to a validated, documented metrics output — every month, with the same script, with full
logs and a clear record of what was dropped and why.

## Stakeholders

| Role | Interest |
|---|---|
| **TLC Operations Analyst** | Monthly operational health: how many trips, how long, how fast? |
| **Dispatch / Allocation Planner** | Zone-hour demand patterns for driver positioning |
| **Data Quality Lead** | What % of raw data was invalid? What are the failure modes? |

## Project KPI — Trip Reliability & Efficiency Index

One composite index built from four sub-metrics:

| Sub-metric | Business meaning |
|---|---|
| **Data Reliability %** | What share of raw trips survived all validation checks? Low = upstream data problem. |
| **Median Trip Duration & Speed by Zone/Hour** | Operational efficiency signal; detects congestion or data anomalies. |
| **Fare-per-Mile Consistency** | Pricing integrity check; outliers flag metering errors or fraud. |
| **Anomaly Rate %** | % of trips flagged as invalid/anomalous; the leading indicator of data quality drift. |

## Sources

| # | Source | Retrieval Mode | What we pull |
|---|---|---|---|
| 1 | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Bulk Parquet download (HTTP) | Yellow Taxi trips — January 2025 |
| 2 | [NYC Open Data — Socrata](https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/2yv8-t2f9) | Socrata Open Data API | Taxi Zone lookup: LocationID → Borough/Zone |
| 3 | [TLC Taxi Zone Lookup CSV](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) | Bulk CSV download (HTTP) | Fallback/reference zone table |

See [`docs/source_map.md`](docs/source_map.md) for the full source map.

## Repo Structure

```
nyc-tlc-reliability-pipeline/
├── README.md
├── data/
│   ├── raw/              # Untouched downloaded files (gitignored — use ingest.py to fetch)
│   ├── reference/        # Taxi zone lookup CSV and Socrata pull
│   └── processed/        # Validated/cleaned outputs, validation report JSON
├── src/
│   ├── ingest.py         # Retrieval: bulk Parquet download + Socrata API pull
│   ├── validate.py       # Profiling + business-oriented validation rules
│   ├── model.py          # Entity/event modeling, fact+dimension joins (DuckDB)
│   ├── metrics.py        # KPI calculations (5 metrics tied to the index)
│   └── pipeline.py       # Orchestrates ingest→validate→model→metrics with logging
├── notebooks/
│   └── exploration.ipynb # Scratch profiling (not the source of truth)
├── diagrams/
│   ├── source_map.md     # Source map table (PNG rendered in polish phase)
│   └── data_model.md     # Entity-relationship / event model (PNG in polish phase)
├── docs/
│   └── source_map.md     # Full source map with grain, gaps, and business justification
├── logs/                 # Auto-generated per run (gitignored)
├── tests/
│   └── test_validate.py  # Unit tests for validation rules
├── outputs/              # metrics_<month>.csv per processed month
├── requirements.txt
└── .gitignore
```

## Setup & Run

```bash
# 1. Clone
git clone <repo-url>
cd nyc-tlc-reliability-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the full pipeline (fetches data, validates, models, outputs metrics)
python src/pipeline.py --month 2025-01

# 4. Outputs land in outputs/metrics_2025-01.csv
#    Logs land in logs/pipeline_run_<timestamp>.log
#    Validation report in data/processed/validation_report_2025-01.json
```

> **Note:** `data/raw/` is gitignored. The pipeline's ingest step downloads the raw
> Parquet file on first run and skips re-download if the file already exists and the
> checksum matches (idempotent).

## What Business Decision Does This Support?

A TLC operations analyst can open `outputs/metrics_2025-01.csv` and immediately see:
- Which zones and hours have the longest/shortest trips (allocation signal)
- Whether fare-per-mile is consistent across the fleet (pricing integrity)
- What % of raw records were anomalous (data quality gate — if > X%, escalate upstream)

The pipeline is re-runnable every month with a single command. Logs + validation reports
are saved alongside outputs so any analyst can audit exactly what was dropped and why.

## FDE Judgement Calls

*(Populated as each phase is built — see inline code comments for detail)*

| Decision | Rationale |
|---|---|
| Yellow Taxi, January 2025 | Largest, most complete TLC dataset; Jan 2025 is recent and a single compact Parquet file (~50 MB). |
| Socrata source = Taxi Zone Lookup (`2yv8-t2f9`) | Most natural "second retrieval mode" — zone data feeds the dimension table directly, unlike a duplicate trip subset. |
| Markdown table for source map (Phase 1) | Reproducible, diffable, no tooling required; PNG rendered in final polish phase. |
| `data/raw/` gitignored | Raw Parquet files are large binaries; manifests/checksums are committed instead. |
| *(more added each phase)* | |

## Known / Unknown / Assumption / Limitation

*(Populated in Phase 3 — Class 6 — after profiling and validation)*

| Category | Entry |
|---|---|
| **Assumption** | Yellow Taxi Jan 2025 Parquet is complete and authoritative as published by TLC. |
| **Assumption** | Taxi Zone Lookup LocationIDs are stable across the analysis period. |
| **Unknown** | Whether TLC applies any post-publication corrections to historical Parquet files. |
| **Limitation** | Pipeline covers one vehicle type (Yellow Taxi) and one month; multi-month/type extension is out of scope. |
| *(more added after profiling)* | |

---
*Pipeline version: Phase 1 (Class 4 — Source Understanding). See `build.md` for the full build sequence.*
