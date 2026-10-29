# Taco Bueno lakehouse: sales, sports events and demand forecasting on Databricks

End-to-end Databricks project built on 21 months of Taco Bueno point-of-sale data: 597 daily exports, 85.3 million order lines, 13.8 million tickets and 119 stores in Texas and Oklahoma. It does four things:

1. **Ingests and cleans** the raw POS exports with a Lakeflow Declarative Pipeline (Auto Loader, expectations, quarantine). It reconciles two POS systems with different semantics and fixes a ticket-key defect found in the data.
2. **Enriches** sales with store locations and **25,546 games** from ESPN, MLB Stats API and NHL API (NFL, NBA, MLB, MiLB, NHL, MLS, NCAA football), matched to each store by distance.
3. **Models** store and product segments (k-means), and forecasts product, combo and market demand plus **strong dates** (gradient boosting, MLflow, Unity Catalog).
4. **Orchestrates** everything with a file-triggered daily job and a data-quality gate, deployed as code with Declarative Automation Bundles.

## Results at a glance

| | |
|---|---|
| Data processed | 597 files · 85,273,331 order lines · 13,788,668 tickets · $181.7M net sales · 119 stores · 25,546 games |
| Accuracy | 91 of 91 reconciliation metrics match an independent pandas implementation to the cent |
| Data quality | 0 quarantined rows; 41 missing export days, 72 reused check numbers, 18,467 non-reconciling tickets, 11 un-geocoded stores, all detected and flagged |
| Business findings | traffic -4.5% to -8.7% YoY since March 2026, offset by two price waves (+6% in July 2026); delivery up to 14% of sales; DFW stores at $2,469/day vs $3,749 in Tulsa; weekend big-game days -4.5% to -9.8% |
| Forecasts | chain tickets 3.3% WAPE (seasonal naive 3.9%); combos 7.5% (10.4%); strong/weak-date calendar to Dec 31, 2026 |
| Run | full orchestration job (ingestion from the APIs inside Databricks -> pipeline -> quality gate -> ML) completed on serverless in the dev target |

## Deliverables

| Deliverable | Where |
|---|---|
| GitHub repository | this repo (layout below) |
| Pipeline job | [docs/02_pipeline_job.md](docs/02_pipeline_job.md) · [`resources/jobs/daily_orchestration_job.yml`](resources/jobs/daily_orchestration_job.yml) |
| Pipeline overview | [docs/01_pipeline_overview.md](docs/01_pipeline_overview.md) |
| Transformations and tables | [docs/03_transformations_and_tables.md](docs/03_transformations_and_tables.md) · [`src/transformations/`](src/transformations) |
| Data quality checks | [docs/04_data_quality.md](docs/04_data_quality.md) |
| Technical best practices | [docs/05_technical_best_practices.md](docs/05_technical_best_practices.md) |
| Analysis and business value | [docs/06_analysis_and_business_value.md](docs/06_analysis_and_business_value.md) |
| ML models (segmentation, forecasting) | [docs/07_ml_models.md](docs/07_ml_models.md) · [`src/ml/`](src/ml) |
| Sports-event sources and matching | [docs/08_sports_events.md](docs/08_sports_events.md) · [`src/ingestion/`](src/ingestion) |
| Where to see everything in Databricks | [docs/09_databricks_walkthrough.md](docs/09_databricks_walkthrough.md) |

## Repository layout

```
├── databricks.yml                       bundle: variables, dev/prod targets
├── resources/
│   ├── jobs/daily_orchestration_job.yml        ingestion -> pipeline -> quality gate -> ML
│   └── pipelines/lakeflow_pipeline.yml         serverless Lakeflow pipeline + target schema
├── src/
│   ├── transformations/                 Lakeflow pipeline sources (st_ = streaming table, mv_ = materialized view)
│   │   ├── bronze/   st_pos_lines_raw.py · st_sports_events_raw.py · mv_store_reference_raw.sql
│   │   ├── silver/   st_orders_cleaned.py · st_orders_quarantine.py · st_sports_events.py (AUTO CDC)
│   │   │             mv_tickets.sql · mv_dim_store.sql · mv_dim_product.sql · mv_dim_calendar.sql
│   │   │             mv_dim_team.sql · mv_store_event_exposure.sql
│   │   └── gold/     st_daily_revenue.py · mv_daily_store_sales.sql · mv_daily_product_demand.sql
│   │                 mv_product_price_history.sql · mv_event_impact.sql · mv_data_quality_daily.sql
│   │                 mv_executive_kpis.sql
│   ├── ingestion/                       sports calendars + store geocoding (job task, also runs locally)
│   ├── quality/data_quality_gate.py     notebook: error/warn checks, history in dq_gate_results
│   ├── ml/                              notebooks: 01_store_segmentation, 02_demand_forecast
│   └── analysis/business_insights.sql   queries behind the analysis deliverable
├── setup/                               one-time volume creation and landing backfill
├── tests/                               unit tests (pytest) + validation/validate_pipeline_vs_eda.sql
├── tools/
│   ├── eda/eda_taco_bueno.py            independent pandas EDA used to reconcile the pipeline
│   └── analysis/make_charts.py          builds docs/images from the gold and ML tables
├── docs/                                the written deliverables and charts
└── .github/workflows/ci.yml             lint, tests, bundle validation
```

The requested template (`silver/st_orders_cleaned.py`, `silver/mv_customer_summary.sql`, `gold/st_daily_revenue.py`, `gold/mv_executive_kpis.sql`) is followed with the project's own tables. `st_orders_cleaned`, `st_daily_revenue` and `mv_executive_kpis` keep their names. The POS data has no customer identifier, so the summary-view role of `mv_customer_summary` is filled by store- and product-level views (`mv_dim_store`, `mv_daily_store_sales`, `mv_dim_product`). A `bronze/` folder holds the raw ingestion tables.

## Run it

Requirements: Databricks CLI v1.20+ (Declarative Automation Bundles; tested with 1.20.0), a Unity Catalog workspace with serverless compute (Free Edition works), Python 3.10+ for local tools.

```powershell
# 1. Authenticate the CLI (opens the browser)
databricks auth login --host https://<workspace-host> --profile DEFAULT

# 2. One-time setup: create the raw and landing volumes (SQL editor)
#    setup/00_create_volumes.sql
#    then upload the raw exports, unmodified:
databricks fs cp -r ..\data\raw dbfs:/Volumes/workspace/default/taco-bueno-raw/

# 3. Backfill sports calendars and store locations into the landing volume
.\setup\01_backfill_landing.ps1 -CityFile ..\data\reference\city.xlsx

# 4. Deploy and run
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run taco_bueno_lakeflow -t dev            # pipeline only
databricks bundle run daily_orchestration_job -t dev --params force_retrain_segments=true   # everything

# Production (file-arrival trigger active)
databricks bundle deploy -t prod
```

Local checks:

```bash
pip install -e ".[dev]"
pytest -q                     # ingestion unit tests (no network)
ruff check src/ingestion tests
```

After a pipeline update, run [`tests/validation/validate_pipeline_vs_eda.sql`](tests/validation/validate_pipeline_vs_eda.sql) in the SQL editor to reconcile the pipeline with the independent EDA:

```bash
python tools/eda/eda_taco_bueno.py --datos ../data/raw --salida ../outputs/eda   # ~3 min, cached per file
python tools/analysis/make_charts.py --schema taco_bueno                         # refresh docs/images
```

## Data handling and local layout

The raw exports, the city file and the landed API data are **not** in this repository (`.gitignore`). They live in Unity Catalog volumes; the pipeline never modifies the raw volume. Docs contain aggregated figures only.

Locally, the repository sits next to the data it works with:

```
<project>/
├── taco-bueno-lakehouse/   this repository
├── data/
│   ├── raw/                597 TB_Daily_TLD_*.csv, unmodified (copy of the raw volume)
│   ├── reference/          city.xlsx (store -> city, from the business)
│   └── landing/            local copy of the landing zone (store_locations.csv, sports_events/, _cache/)
├── outputs/eda/            EDA report, tables and charts (tools/eda output)
└── archive/                superseded files
```
