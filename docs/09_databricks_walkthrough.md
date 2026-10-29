# Databricks walkthrough: where to see everything

Where each part of the project lives in the Databricks UI after `databricks bundle deploy`. Names below are for the `dev` target, which prefixes resources with `[dev <user>]` and uses schema `dev_<user>_taco_bueno`. In `prod` the names have no prefix and the schema is `taco_bueno`.

Shortcut from the terminal:

```bash
databricks bundle summary -t dev                     # prints the URL of the pipeline, job and schema
databricks bundle open taco_bueno_lakeflow -t dev    # opens the pipeline in the browser
databricks bundle open daily_orchestration_job -t dev
```

## 0. The dashboard: *Dashboards > [dev <user>] Taco Bueno - Sales, Sports and Forecasts*

The fastest way to see the results. Five pages (tabs at the top):

| Page | What it shows |
|---|---|
| Overview | totals, net sales per store-day by month, YoY change of sales, traffic and ticket, channel mix, markets |
| Calendar & sports | weekday and daypart patterns, holiday lift, sports lift (all days and weekday vs weekend) |
| Forecasts | expected tickets and net sales for the next 95 days, actual vs forecast, strong and weak dates, top-product forecasts, backtest accuracy, price-sensitivity scenario |
| Stores & products | store segments, segments by market, top products, price-increase waves |
| Data quality | reconciliation, quarantine, missing days, expectations of the latest update, quality-gate results |

It is deployed by the bundle (`resources/dashboards/taco_bueno_dashboard.yml`) and generated from `tools/dashboard/build_dashboard.py`. To change it, edit the generator, run it, and deploy again. Edits made in the UI are lost on the next deploy unless you export them with `databricks bundle generate dashboard --resource taco_bueno_overview`. Queries run on the SQL warehouse; the first load after a pause takes longer while the warehouse starts.

## 1. The code

* **Deployed copy (read-only):** *Workspace > Users > <you> > .bundle > taco_bueno_lakehouse > dev > files*. The pipeline runs the files under `src/transformations`. `src/ml/*.py` and `src/quality/data_quality_gate.py` open as notebooks.
* **Git folder (browse, edit, pull):** *Workspace > Users > <you> > Create > Git folder*, provider GitHub, URL `https://github.com/luis-fdzr/taco-bueno-lakehouse.git`. The repository is private, so first link GitHub under *Settings > Linked accounts > Add Git credential* (GitHub app, or a personal access token with `repo` scope). From the Git folder you can open every file and notebook, and pull new commits.

## 2. The pipeline: *Jobs & Pipelines > [dev <user>] taco_bueno_lakeflow*

* **Graph:** the 19 datasets from bronze to gold with their dependencies. Click a node for row counts, duration and incremental vs full refresh.
* **Data quality:** select a table to see its expectations with passed/failed records. Good examples: `st_orders_cleaned` (drop rules), `mv_tickets` (`ticket_reconciles`, `single_header_per_ticket`), `st_sports_events`.
* **Updates and event log:** history of every update (initializing, setting up tables, running, completed). The log shows warnings, errors and refresh plans (`GENERIC_AGGREGATE` = incremental, `COMPLETE_RECOMPUTE`).
* **Source:** *Edit pipeline* opens the pipeline editor with every file under `src/transformations` (bronze, silver, gold).
* **Settings:** target catalog/schema, serverless, the `taco.*` configuration (volume paths, local/regional radius), event log table, notifications.

## 3. The job: *Jobs & Pipelines > [dev <user>] taco_bueno_daily_orchestration*

* **Tasks:** the DAG `ingest_external_data -> refresh_lakeflow_pipeline -> data_quality_gate -> (is_monday | force_retrain) -> store_segmentation -> demand_forecast`, with retries, timeouts and `run_if` per task.
* **Runs:** the matrix of task results per run, including the verified run and its *repair* attempts. Click a task to see its output: stdout of the ingestion script, the notebook output of the quality gate and the ML notebooks.
* **Trigger:** file arrival on the raw volume (paused in dev). *Run now with different parameters* > `force_retrain_segments = true` retrains the segments.

## 4. The data: *Catalog > workspace*

* **Schema `dev_<user>_taco_bueno`:** every table with its comment, columns and sample data.
  * *Lineage* shows the graph from the volumes to bronze, silver, gold and the `ml_*` tables.
  * *History* lists the Delta versions.
  * *Details* shows the liquid-clustering columns and the `quality` property.
  * `pipeline_event_log` holds the expectation metrics and `dq_gate_results` the history of the quality gate.
* **Volumes (schema `default`):** `taco-bueno-raw` holds the 597 POS exports, unmodified. `taco-bueno-landing` holds `reference/` (city file, geocoded stores) and `sports_events/` (one folder per ingestion batch).
* **Models:** the schema's *Models* tab lists `store_segmentation`, `product_segmentation`, `product_demand_forecaster` and `market_traffic_forecaster`. Each version links to the MLflow run that produced it.

## 5. The experiments: *Experiments*

`taco_bueno_segmentation` and `taco_bueno_demand_forecast` (under your user folder). Each run has its parameters, metrics (silhouette by k; WAPE of model, baseline, blend and the selected method), artifacts (`segment_names.json`, `method_by_series.json`) and the logged model with its signature. Use the *Compare* view to see how the product model evolved across iterations.

## 6. The results: *SQL Editor*

Open a new query on the SQL warehouse, run `USE workspace.dev_<user>_taco_bueno;` (or `taco_bueno` in prod), then paste:

* [`tests/validation/validate_pipeline_vs_eda.sql`](../tests/validation/validate_pipeline_vs_eda.sql): reconciliation against the independent EDA. Every row should say `MATCH` (remove its `USE` lines first if you are in dev).
* [`src/analysis/business_insights.sql`](../src/analysis/business_insights.sql): the 13 queries behind [06_analysis_and_business_value.md](06_analysis_and_business_value.md). On any result, *+ > Visualization* draws the chart (e.g. `mv_executive_kpis` as a line chart by month).
* Useful one-liners:

```sql
SELECT * FROM mv_executive_kpis ORDER BY month;                          -- monthly KPIs with YoY
SELECT * FROM mv_data_quality_daily WHERE day_status <> 'Complete';      -- missing / partial days
SELECT * FROM mv_event_impact WHERE market = 'All markets';              -- sports lift
SELECT * FROM ml_strong_dates WHERE scope = 'Chain' AND source = 'forecast' AND classification <> 'Normal';
SELECT * FROM ml_store_segment_profiles;
SELECT * FROM ml_forecast_metrics ORDER BY series_type, wape_selected;
```

## 7. The notebooks, interactively

Open `src/ml/01_store_segmentation.py` or `src/ml/02_demand_forecast.py` (from the bundle files or the Git folder), attach serverless compute and *Run all*. The widgets at the top take the catalog and schema; the notebooks display the segment profiles, the backtest and the strong-date table at the end.
