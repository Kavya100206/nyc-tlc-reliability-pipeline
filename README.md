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
| 2 | [NYC Open Data — Socrata](https://data.cityofnewyork.us/resource/8meu-9t5y.json) | Socrata Open Data API | Taxi Zone lookup: LocationID → Borough/Zone |
| 3 | [TLC Taxi Zone Lookup CSV](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) | Bulk CSV download (HTTP) | Fallback/reference zone table |

See [`docs/source_map.md`](docs/source_map.md) for the full source map.

## Repo Structure

```
nyc-tlc-reliability-pipeline/
├── README.md
├── data/
│   ├── raw/              # Untouched downloaded files (gitignored — use ingest.py to fetch)
│   ├── raw/manifest.json # Retrieval proof: checksums, row counts, date coverage, URLs
│   ├── reference/        # Taxi zone lookup CSV and Socrata pull (committed)
│   └── processed/        # validation_report_<month>.json (committed); validated Parquet (gitignored)
├── diagrams/
│   ├── source_map.md     # Source map table (PNG rendered in polish phase)
│   └── data_model.md     # Entity-relationship / event model (PNG in polish phase)
├── docs/
│   └── source_map.md     # Full source map with grain, gaps, and business justification
├── logs/                 # Auto-generated per run (gitignored); one file per execution
├── outputs/              # Committed metric CSVs (one set per processed month)
│   ├── metrics_<month>.csv                       # M1 + M5 scalar summary
│   ├── metrics_duration_speed_<month>.csv        # M2 + M3 by borough × hour
│   ├── metrics_duration_speed_by_zone_<month>.csv # M2 + M3 supplementary by zone × hour
│   └── metrics_fare_<month>.csv                  # M4 fare-per-mile consistency by borough
├── src/
│   ├── ingest.py         # Retrieval: bulk Parquet download + Socrata API pull + CSV fallback
│   ├── validate.py       # Profiling + 7 hard + 3 soft validation rules + KUAL report
│   ├── model.py          # Entity/event model, fact+dimension tables in DuckDB (in-memory)
│   ├── metrics.py        # 5 KPI metric queries (M1–M5), writes 4 output CSVs
│   └── pipeline.py       # Orchestrates all stages with dual logging + idempotency
├── tests/
│   ├── test_validate.py  # 49 unit tests for all validation rules (synthetic data)
│   └── test_pipeline.py  # Pipeline unit tests + @integration end-to-end smoke tests
├── requirements.txt
└── .gitignore
```

## Setup & Run

### Prerequisites

```bash
git clone https://github.com/Kavya100206/nyc-tlc-reliability-pipeline.git
cd nyc-tlc-reliability-pipeline
pip install -r requirements.txt
```

### One-line full pipeline run

```bash
python src/pipeline.py --month 2025-01
```

Cold run (~22s): downloads 59 MB Parquet + zone lookup, profiles + validates 3.5M rows,
builds DuckDB model, computes 5 KPI metrics, writes 4 CSV files.  
Warm run (~0.8s): checksum match skips download; existing outputs skip validate + metrics.

### Individual stages

```bash
python src/ingest.py   --month 2025-01   # download raw Parquet + zone lookup
python src/validate.py --month 2025-01   # profile + validate → report JSON
python src/model.py    --month 2025-01   # build DuckDB model, print schema
python src/metrics.py  --month 2025-01   # compute KPI metrics → output CSVs
```

Or use the `--step` flag in the orchestrator:

```bash
python src/pipeline.py --month 2025-01 --step ingest    # ingest only
python src/pipeline.py --month 2025-01 --step validate  # validate only
python src/pipeline.py --month 2025-01 --step metrics   # metrics only
```

### Force re-run (ignore existing outputs)

```bash
python src/pipeline.py --month 2025-01 --force
```

### Tests

```bash
pytest tests/                              # all fast unit tests (no real data needed)
pytest tests/ -m integration              # full end-to-end integration test
pytest tests/test_validate.py -v          # 49 validation rule unit tests
pytest tests/test_pipeline.py -v          # pipeline orchestration unit tests
```

### Output files

| File | What it contains |
|---|---|
| `data/raw/manifest.json` | Retrieval proof: checksums, row counts, URLs, date coverage |
| `data/processed/validation_report_2025-01.json` | Profiling snapshot + flag counts + KUAL entries |
| `outputs/metrics_2025-01.csv` | M1 (data reliability %) + M5 (anomaly rate %) — scalar summary |
| `outputs/metrics_duration_speed_2025-01.csv` | M2 + M3: median duration + speed by borough × hour |
| `outputs/metrics_duration_speed_by_zone_2025-01.csv` | M2 + M3 supplementary: zone × hour (1,668 low-n zone-hours visible) |
| `outputs/metrics_fare_2025-01.csv` | M4: fare-per-mile mean, std, outlier% by borough |
| `logs/pipeline_2025-01_<timestamp>.log` | Full pipeline log, one file per run (gitignored) |

> **Note:** `data/raw/` is gitignored. The ingest step downloads the raw Parquet on first run
> and skips re-download on subsequent runs if the checksum matches (idempotent by design).

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
| Socrata source = Taxi Zone Lookup (`8meu-9t5y`) | Most natural "second retrieval mode" — zone data feeds the dimension table directly. ID `8meu-9t5y` verified live Sep 2026 via Socrata catalog API; previously referenced IDs `755u-8jsi`/`2yv8-t2f9` both return 404. |
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
*Pipeline version: Phase 5 (Class 8 — Dependable Pipeline). All 5 phases complete. See `build.md` for the full build sequence.*
