# Source Map — Diagram View

> Concise version for `diagrams/`. Full detail in [`docs/source_map.md`](../docs/source_map.md).

## Sources → Pipeline Flow

| Source ID | System | Retrieval Mode | Grain | → Feeds |
|---|---|---|---|---|
| **S1** | NYC TLC Yellow Taxi Parquet (Jan 2025) | Bulk HTTP download | 1 row / trip | `fact_trips` table |
| **S2** | NYC Open Data Socrata — Taxi Zones (`8meu-9t5y`) | Socrata Open Data API | 1 row / zone (263 zones) | `dim_zones` table |
| **S3** | TLC Taxi Zone Lookup CSV (fallback) | Bulk HTTP download | 1 row / zone | `dim_zones` (if S2 unavailable) |

## Text Flow Diagram

```
┌─────────────────────────────┐     ┌────────────────────────────────┐
│  NYC TLC CDN (S1)           │     │  NYC Open Data / Socrata (S2)  │
│  yellow_tripdata_2025-01    │     │  dataset: 755u-8jsi            │
│  .parquet  (~50 MB)         │     │  263 taxi zones (JSON)         │
└──────────────┬──────────────┘     └────────────────┬───────────────┘
               │ HTTP GET                             │ Socrata API
               ▼                                      ▼
  data/raw/yellow_tripdata_2025-01.parquet   data/reference/taxi_zones_socrata.json
               │                                      │
               └──────────────┬───────────────────────┘
                              │
                         src/validate.py
                     (profile → flag → report)
                              │
                    data/processed/
                    ├── trips_validated_2025-01.parquet
                    └── validation_report_2025-01.json
                              │
                         src/model.py
                    (DuckDB: fact_trips JOIN dim_zones)
                              │
                         src/metrics.py
                    (5 KPI sub-metrics computed)
                              │
                    outputs/metrics_2025-01.csv
```

## Business Question → Source Mapping

| Business Question | Source(s) | Metric it enables |
|---|---|---|
| How many trips, where, when? | S1 | Trip volume, duration, speed |
| Are fares consistent per mile? | S1 | Fare-per-mile mean/std |
| What zone/borough for each LocationID? | S2 (+ S3 fallback) | Zone-level aggregation |
| What % of raw trips had data problems? | S1 (post-validation) | Anomaly rate % |
| Is data coverage complete for the month? | S1 manifest + S2 | Data reliability % |

---
*Phase 1 (Class 4) — rendered as PNG in final polish phase*
