0. Project framing (keep this pinned in your head throughout)

Business problem: NYC TLC (Taxi & Limousine Commission) leadership wants a trustworthy, repeatable view of monthly trip operations — not a one-off analysis — to support decisions like driver allocation, pricing sanity checks, and service-quality monitoring.

Project KPI (pick one, don't hedge): Recommended — "Trip Reliability & Efficiency Index", built from:

% of trips with valid/complete data (data reliability)
Median trip duration & speed by zone/hour (operational efficiency)
Fare-per-mile consistency (pricing integrity)
% of anomalous/rejected trips (data quality signal)

Stakeholders: TLC operations analyst, dispatch/allocation planner, data quality lead.

What "done" looks like: A GitHub repo where someone can clone it, run one script, and get today's (this month's) metrics table/dashboard from raw TLC files — with logs, validation reports, and a clear Known/Unknown/Assumption/Limitation section.

1. Repo structure to have Antigravity scaffold first
nyc-tlc-pipeline/
├── README.md
├── data/
│   ├── raw/              # untouched downloaded files (gitignored, but folder tracked)
│   ├── reference/        # taxi zone lookup CSV, etc.
│   └── processed/        # validated/cleaned outputs
├── src/
│   ├── ingest.py         # retrieval: bulk parquet download + API pull
│   ├── validate.py       # profiling + validation rules
│   ├── model.py          # entity/event modeling, joins
│   ├── metrics.py        # KPI calculations
│   └── pipeline.py       # orchestrates ingest→validate→model→metrics, logging
├── notebooks/
│   └── exploration.ipynb # profiling & scratch work (not the source of truth)
├── diagrams/
│   ├── source_map.png / .drawio
│   └── data_model.png / .drawio
├── logs/
│   └── pipeline_run_<timestamp>.log
├── tests/
│   └── test_validate.py
├── outputs/
│   └── metrics_<month>.csv / dashboard.html
├── requirements.txt
└── .gitignore
2. Class-by-class build sequence (what Antigravity should do, in order)
Class 4 — Understand sources (do this BEFORE writing code)
Produce a source map table: business question → data needed → source system → owner → grain → known gaps.
Sources to use:
NYC TLC Trip Record Data (bulk Parquet files, monthly) — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
NYC Open Data Socrata API — for a second retrieval mode (e.g., pulling a subset live, or taxi zone data) — https://data.cityofnewyork.us
Taxi Zone Lookup CSV (reference/dimension data — maps LocationID → Borough/Zone)
Output: diagrams/source_map.png + a docs/source_map.md table.
Class 5 — Retrieve data (two distinct retrieval modes)
Mode 1: Bulk file download (Parquet/CSV) via HTTP — src/ingest.py::download_bulk_trip_file()
Mode 2: API pull via Socrata (sodapy or raw requests) — src/ingest.py::fetch_from_api()
Preserve raw inputs untouched in data/raw/ with a checksum/manifest log proving retrieval completeness (row counts, file hash, date range coverage).
Class 6 — Profile & validate
Profile: nulls, dtype mismatches, distribution of trip_distance/fare_amount/duration, cardinality of LocationIDs.
Define business-oriented validation rules, e.g.:
trip duration > 0 and < 6 hours
pickup/dropoff LocationID must exist in zone lookup
fare_amount >= 0, fare-per-mile within plausible bounds
pickup_datetime <= dropoff_datetime
Do NOT silently drop/fix — flag and log into data/processed/validation_report.json, and write a Known/Unknown/Assumption/Limitation section addressing what you chose not to fix and why.
Class 7 — Model the workflow
Entities: Trip, Vehicle (if available), Zone, Payment.
Events/states: request → pickup → dropoff → payment.
Build simple relational model (trips fact table + zone dimension).
Calculate 3–5 metrics tied to the KPI (see below).
Class 8 — Dependable pipeline
src/pipeline.py runs ingest → validate → model → metrics as one script.
Include: logging (timestamps, row counts at each stage), rerun-safety (idempotent — reprocessing same month doesn't duplicate/corrupt), and failure handling (e.g., missing file, schema drift → clear error + non-zero exit code, not a silent crash).
Output: outputs/metrics_<month>.csv and optionally a simple HTML dashboard.
3. Suggested 3–5 metrics (pick and justify in README)
Median trip duration by hour-of-day and borough
% of trips flagged invalid/anomalous (data reliability signal)
Fare-per-mile consistency (mean + std dev, flag outliers)
Average speed by zone (proxy for congestion/delay)
Trip volume trend day-over-day (operational load)