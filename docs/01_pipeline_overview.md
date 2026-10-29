# Pipeline overview

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        POS["POS exports<br/>TB_Daily_TLD_MM-DD-YYYY-m.csv<br/>1 file / business day"]
        CITY["city.xlsx<br/>store -> city"]
        APIS["ESPN · MLB Stats API · NHL API<br/>Open-Meteo geocoding"]
    end

    subgraph UC["Unity Catalog volumes (workspace.default)"]
        RAW[("taco-bueno-raw<br/>597 CSV, read-only")]
        LAND[("taco-bueno-landing<br/>store_locations.csv<br/>sports_events/*.jsonl")]
    end

    ING["ingest_external_data<br/>(job task)"]
    POS --> RAW
    CITY --> ING
    APIS --> ING
    ING --> LAND

    subgraph LDP["Lakeflow pipeline taco_bueno_lakeflow (serverless)"]
        direction LR
        subgraph Bronze
            B1[st_pos_lines_raw]
            B2[st_sports_events_raw]
            B3[mv_store_reference_raw]
        end
        subgraph Silver
            S1[st_orders_cleaned]
            SQ[st_orders_quarantine]
            S2[mv_tickets]
            S3[st_sports_events<br/>AUTO CDC]
            D1[mv_dim_store]
            D2[mv_dim_product]
            D3[mv_dim_calendar]
            D4[mv_dim_team]
            X1[mv_store_event_exposure]
        end
        subgraph Gold
            G1[mv_daily_store_sales]
            G2[mv_daily_product_demand]
            G3[mv_product_price_history]
            G4[mv_event_impact]
            G5[mv_executive_kpis]
            G6[mv_data_quality_daily]
            G7[st_daily_revenue]
        end
    end

    RAW -->|Auto Loader| B1
    LAND -->|Auto Loader| B2
    LAND -->|read_files| B3
    B1 --> S1
    B1 --> SQ
    S1 --> S2
    S1 --> D2
    S2 --> D1
    B3 --> D1
    B2 --> S3
    S3 --> D4
    D1 --> X1
    S3 --> X1
    D4 --> X1
    S2 --> G1
    D1 --> G1
    D3 --> G1
    X1 --> G1
    S1 --> G2
    S1 --> G3
    G1 --> G4
    G1 --> G5
    S2 --> G6
    S1 --> G7

    subgraph ML["ML notebooks (job tasks, MLflow + UC models)"]
        M1[01_store_segmentation]
        M2[02_demand_forecast]
    end
    G1 --> M1
    G2 --> M1
    G1 --> M2
    G2 --> M2
    X1 --> M2
    M1 --> O1[(ml_store_segments<br/>ml_product_segments)]
    M2 --> O2[(ml_demand_forecast<br/>ml_market_forecast<br/>ml_strong_dates<br/>ml_price_sensitivity)]

    G6 --> GATE{data_quality_gate}
```

Everything is in the `workspace` catalog: pipeline tables in schema `taco_bueno` (prod) or `dev_<user>_taco_bueno` (dev), source volumes in `default`.

## How data flows from source to output

| Step | Input | What happens | Output |
|---|---|---|---|
| 1. Land | Daily POS export | Uploaded unmodified to the raw volume (the file-arrival trigger starts the job) | `/Volumes/workspace/default/taco-bueno-raw/` |
| 2. External data | `city.xlsx`, sports APIs | Geocode store cities; fetch schedules and results (last 14 days + next 120) | landing volume: CSV + JSON Lines batch |
| 3. Bronze | Raw files | Auto Loader reads only new files with an all-STRING positional schema; lineage columns added | `st_pos_lines_raw` (85.3M lines), `st_sports_events_raw`, `mv_store_reference_raw` |
| 4. Silver: clean | Bronze lines | Parse types, harmonize the two POS systems, channel, daypart, product key, ticket key; blocking rules route bad rows to quarantine | `st_orders_cleaned`, `st_orders_quarantine` |
| 5. Silver: model | Clean lines, reference, games | Tickets (13.8M) with gross/net/collected sales; store, product, calendar and team dimensions; deduplicated game calendar (AUTO CDC); store-game exposure by distance | `mv_tickets`, `mv_dim_*`, `st_sports_events`, `mv_store_event_exposure` |
| 6. Gold | Silver | Store-day facts with calendar and sports context and baselines; product-day demand; price history; event impact; KPIs; quality scorecard; streaming ledger | `mv_daily_store_sales` (63,868 store-days) and the other gold tables |
| 7. Gate | Gold + event log | Error-level checks stop the run before anything is published downstream | `dq_gate_results` |
| 8. ML | Gold | Store and product segments (weekly); product, combo and market forecasts, strong dates and price sensitivity (daily) | `ml_*` tables, MLflow runs, UC models |
| 9. Consume | Gold + ML | AI/BI dashboard deployed by the bundle (`taco_bueno_overview`), SQL editor, pricing and operations decisions | [06_analysis_and_business_value.md](06_analysis_and_business_value.md) |

## A daily run, end to end

1. The POS export for business day *D* lands in the raw volume.
2. 15 minutes after the upload settles, the file-arrival trigger starts `taco_bueno_daily_orchestration`.
3. `ingest_external_data` writes a new sports batch (about 3,000 games for the rolling window) to the landing volume.
4. The pipeline update ingests the one new POS file and the new sports batch incrementally, then refreshes the materialized views. Day *D* appears in every gold table, and day *D-1* is finalized in the streaming ledger.
5. The quality gate checks reconciliation, quarantine share, ticket-key integrity, freshness and coverage.
6. On Mondays (or on demand) the segments are retrained; every day, forecasts and the strong-date calendar are refreshed for the next 95 days.

## Design choices

* **Medallion layers with clear contracts.** Bronze never fails on format changes (everything is a string). Silver owns parsing, harmonization and quality rules. Gold owns business-ready aggregates and comparisons.
* **Streaming where data is append-only, materialized views where results must be exact and recomputable.** See [05_technical_best_practices.md](05_technical_best_practices.md).
* **External data is decoupled from sales.** Sports ingestion is a separate fail-soft task writing to its own volume, so an API outage never blocks sales reporting.
* **Everything is code.** The bundle (`databricks.yml` + `resources/`) deploys the schema, the pipeline and the job to dev and prod.
