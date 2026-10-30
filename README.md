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
| AI/BI dashboard (5 pages: overview, calendar & sports, forecasts, stores & products, data quality) | [`resources/dashboards/`](resources/dashboards) · [`src/dashboards/`](src/dashboards) · generator [`tools/dashboard/build_dashboard.py`](tools/dashboard/build_dashboard.py) |
| Where to see everything in Databricks | [docs/09_databricks_walkthrough.md](docs/09_databricks_walkthrough.md) |

## Repository layout

```
├── databricks.yml                       bundle: variables, dev/prod targets
├── resources/
│   ├── jobs/daily_orchestration_job.yml        ingestion -> pipeline -> quality gate -> ML
│   ├── pipelines/lakeflow_pipeline.yml         serverless Lakeflow pipeline + target schema
│   └── dashboards/taco_bueno_dashboard.yml     AI/BI dashboard on the gold and ML tables
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
│   ├── analysis/make_charts.py          builds docs/images from the gold and ML tables
│   └── dashboard/build_dashboard.py     generates src/dashboards/taco_bueno_overview.lvdash.json
├── docs/                                the written deliverables and charts
└── .github/workflows/ci.yml             lint, tests, bundle validation
```

The requested template (`silver/st_orders_cleaned.py`, `silver/mv_customer_summary.sql`, `gold/st_daily_revenue.py`, `gold/mv_executive_kpis.sql`) is followed with the project's own tables. `st_orders_cleaned`, `st_daily_revenue` and `mv_executive_kpis` keep their names. The POS data has no customer identifier, so the summary-view role of `mv_customer_summary` is filled by store- and product-level views (`mv_dim_store`, `mv_daily_store_sales`, `mv_dim_product`). A `bronze/` folder holds the raw ingestion tables.

## Run it

Requirements:

* **Databricks CLI v1.20+** (Declarative Automation Bundles; tested with 1.20.0). Windows: `winget install Databricks.DatabricksCLI`; macOS: `brew tap databricks/tap && brew install databricks`.
* **A Unity Catalog workspace with serverless compute.** Free Edition works as is; other workspaces need the settings in [Other workspaces](#other-workspaces).
* **Python 3.10+.**
* **The data, which is not in this repository:** the 597 raw exports (`TB_Daily_TLD_MM-DD-YYYY-m.csv`, unmodified) and the business city file (`city.xlsx`, or a `.csv` with columns `Store #` and `City`), placed as in [Data handling and local layout](#data-handling-and-local-layout).

`databricks.yml` sets no workspace host: the bundle deploys to the workspace of the CLI profile you log in with, so the commands below reproduce the project in any workspace without editing a file.

On Windows (PowerShell), starting in the folder that holds `data/`:

```powershell
# 0. Clone next to the data folder
git clone https://github.com/luis-fdzr/taco-bueno-lakehouse.git
cd taco-bueno-lakehouse

# 1. Authenticate the CLI (opens the browser); the bundle deploys to this workspace
databricks auth login --host https://<workspace-host> --profile DEFAULT

# 2. One-time setup: create the raw and landing volumes (SQL editor)
#    setup/00_create_volumes.sql
#    then upload the raw exports, unmodified:
databricks fs cp -r ..\data\raw dbfs:/Volumes/workspace/default/taco-bueno-raw/

# 3. Backfill sports calendars and store locations into the landing volume
#    (-ExecutionPolicy Bypass lets the script run on a default Windows setup; pass your city file to -CityFile)
python -m pip install -e .
powershell -ExecutionPolicy Bypass -File .\setup\01_backfill_landing.ps1 -CityFile ..\data\reference\city.xlsx

# 4. Deploy and run
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run taco_bueno_lakeflow -t dev            # pipeline only (10-20 min the first time)
databricks bundle run daily_orchestration_job -t dev --params force_retrain_segments=true   # everything (15-25 min)
databricks bundle open taco_bueno_overview -t dev          # the dashboard

# Production (file-arrival trigger active)
databricks bundle deploy -t prod
```

On macOS or Linux, steps 0, 1 and 4 are the same. Steps 2 and 3 become (step 3 runs what the PowerShell script wraps):

```bash
databricks fs cp -r ../data/raw dbfs:/Volumes/workspace/default/taco-bueno-raw/

python3 -m venv .venv && source .venv/bin/activate && pip install -e .
python src/ingestion/run_ingestion.py --mode backfill --start 2025-01-01 --end 2026-12-31 \
  --landing ../data/landing --city-file ../data/reference/city.xlsx
V=dbfs:/Volumes/workspace/default/taco-bueno-landing
databricks fs mkdir $V/reference/raw
databricks fs cp ../data/reference/city.xlsx $V/reference/raw/city.xlsx --overwrite
databricks fs cp -r ../data/landing/reference/store_locations $V/reference/store_locations --overwrite
databricks fs cp -r ../data/landing/sports_events $V/sports_events
databricks fs mkdir $V/_cache
databricks fs cp ../data/landing/_cache/geocode.json $V/_cache/geocode.json --overwrite
```

### Check the result

* Run [`tests/validation/validate_pipeline_vs_eda.sql`](tests/validation/validate_pipeline_vs_eda.sql) in the SQL editor: every row should say `MATCH`. It only checks sales data (files, rows, coverage, tickets, money, months), so the same raw files give the same numbers.
* The SQL files point to the production schema. In `dev`, replace their `USE CATALOG` and `USE SCHEMA` lines with `USE workspace.dev_<user>_taco_bueno;` (`databricks bundle summary -t dev` prints the schema). The same applies to [`src/analysis/business_insights.sql`](src/analysis/business_insights.sql).
* Sports schedules come from live APIs, so a new backfill returns a slightly different set of games than the documented 25,546 (rescheduled games, newly published schedules). The sports-related figures (event impact, forecasts, strong dates) can then differ a little from the docs; sales figures do not.

### Other workspaces

Free Edition provides the `workspace` catalog and the `Serverless Starter Warehouse`. In any other workspace:

1. In `setup/00_create_volumes.sql`, replace `workspace.default` with a catalog and schema where you can create volumes and schemas. Use the same path in the upload command and pass it to the backfill script (`-Volume dbfs:/Volumes/<catalog>/<schema>/taco-bueno-landing`).
2. Set the bundle variables before the bundle commands (PowerShell shown; in bash, `export BUNDLE_VAR_catalog=...`):

```powershell
$env:BUNDLE_VAR_catalog = "<catalog>"
$env:BUNDLE_VAR_raw_volume_path = "/Volumes/<catalog>/<schema>/taco-bueno-raw"
$env:BUNDLE_VAR_landing_volume_path = "/Volumes/<catalog>/<schema>/taco-bueno-landing"
$env:BUNDLE_VAR_warehouse_id = "<warehouse id>"   # from: databricks warehouses list
```

### Local checks and tools

```bash
pip install -e ".[dev]"
pytest -q                     # ingestion unit tests (no network)
ruff check src/ingestion tests
# The two tools below also need pandas, numpy and matplotlib
python tools/eda/eda_taco_bueno.py --datos ../data/raw --salida ../outputs/eda   # independent pandas EDA (~3 min, cached per file)
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
│   ├── reference/          city.xlsx or city.csv (store -> city, from the business)
│   └── landing/            local copy of the landing zone (store_locations.csv, sports_events/, _cache/)
├── outputs/eda/            EDA report, tables and charts (tools/eda output)
└── archive/                superseded files
```
