# Technical best practices

How the solution uses (or deliberately does not use) streaming, materialized views, liquid clustering and expectations, and the engineering practices that keep it reliable.

## Streaming tables: where data is append-only

| Table | Why streaming | How |
|---|---|---|
| `st_pos_lines_raw` | One immutable CSV per business day lands in a volume. Each file must be read exactly once, even if the job reruns. | Auto Loader (`cloudFiles`) with a checkpoint, explicit schema, `pathGlobFilter`, `_rescued_data` column. Only new files are read on each update. |
| `st_orders_cleaned`, `st_orders_quarantine` | Row-by-row parsing and routing of the bronze stream; nothing needs to be recomputed when a new day arrives. | Streaming read of bronze; a shared temporary view routes rows to the clean table or to quarantine. |
| `st_sports_events_raw` | Each ingestion run lands a new JSON Lines batch. | Auto Loader on the landing volume. |
| `st_sports_events` | Games change after they are first seen (time set, postponed, final score). | **AUTO CDC** (`create_auto_cdc_flow`, SCD type 1) keyed by `event_id`, sequenced by `fetched_at`: one current row per game, with no duplicates from overlapping fetch windows. |
| `st_daily_revenue` | Demonstrates a stateful streaming aggregation that produces an immutable daily ledger. | Event-time watermark of 1 day + 1-day windows: a day is appended once later data moves the watermark past it. |

**Where streaming was not used, on purpose.** Aggregations that must be exact and include the latest day (tickets, daily store sales, KPIs) are materialized views. A streaming aggregation in append mode can only emit a group after the watermark passes it, so the latest day would always lag, and it cannot do exact `COUNT DISTINCT`. `st_daily_revenue` documents this trade-off in its docstring: it uses HLL-approximate tickets and line-level gross sales, while `mv_daily_store_sales` carries the exact numbers.

## Materialized views: for joins, aggregations and corrections

Every silver dimension and every gold table is a materialized view (`mv_*.sql`):

* They are declarative: the refresh recomputes whatever changed upstream, including corrections. A fix in `st_orders_cleaned` (a full refresh after a logic change) flows to tickets, daily sales and KPIs without manual backfills.
* On serverless pipelines, MVs are refreshed **incrementally** when the query allows it (append-only sources, aggregations, joins), so a daily update does not rescan two years of history.
* Window functions (baselines, price changes), FULL OUTER JOINs (store dimension) and GROUPING SETS (event impact) are expressed in plain SQL, which is easy to review.

## Liquid clustering: keys chosen by query patterns

| Table | `CLUSTER BY` | Typical filter |
|---|---|---|
| `st_orders_cleaned` | `business_date, store_id` | a period and/or a store |
| `mv_tickets` | `business_date, store_id` | same |
| `mv_daily_store_sales` | `business_date, store_id` | dashboards and the ML feature reads |
| `mv_daily_product_demand` | `business_date, product_key` | forecasts read per product over time |
| `mv_product_price_history` | `month, product_key` | pricing analysis per product |
| `mv_store_event_exposure` | `event_date, store_id` | joins to store-days |
| `st_sports_events` | `event_date_ct, league` | calendar lookups |
| `st_daily_revenue` | `business_date, store_id` | ledger reads |

Materialized views are refreshed incrementally when possible: the event log shows plans such as `GENERIC_AGGREGATE` for `mv_daily_product_demand` (incremental) and `COMPLETE_RECOMPUTE` only when a definition changed.

Liquid clustering replaces Hive-style partitions (no small-file problem from 119 stores x 573 days) and the keys can change later without rewriting the table. Lesson learned and encoded in the code: Delta keeps statistics only for the first 32 columns. The AUTO CDC target originally listed `event_date_ct` as column 40, and the update failed with `DELTA_CLUSTERING_COLUMN_MISSING_STATS`. `st_sports_events.py` now selects the clustering columns first.

## Expectations: quality rules where the data is produced

Three policies, used deliberately:

* **`expect_or_drop` + quarantine** for records that cannot be attributed to a ticket (no valid business date, store, check number, or unparseable amounts). Instead of silently dropping them, the same rules are evaluated in a shared view and the failing rows land in `st_orders_quarantine` with `dq_failed_rules`.
* **`expect` (warn)** for anomalies that must stay in the data but be counted: tickets that do not reconcile, unknown channels, negative quantities, date vs file-name mismatch, ambiguous store cities, games without coordinates.
* **Fail the run** is implemented in the `data_quality_gate` task rather than with `expect_or_fail`, so a single bad file stops publication (forecasts, KPIs) without leaving the pipeline in a half-updated state.

Expectation metrics are written to the pipeline **event log**, which the bundle publishes as a Unity Catalog table (`pipeline_event_log`) so it can be queried and charted. See [04_data_quality.md](04_data_quality.md).

## Engineering practices

* **Everything as code (Declarative Automation Bundles).** Pipeline, job, schema, dashboard, variables and targets are in YAML (the dashboard's datasets take the target's catalog and schema, so the same definition serves dev and prod); `databricks bundle validate/deploy` is the only way resources reach the workspace. `dev` (development mode: prefixed names, paused trigger, `development: true` pipeline) and `prod` targets share the same definitions.
* **Raw data is immutable and outside the bundle.** The raw and landing volumes are created once by `setup/00_create_volumes.sql`, not by the bundle, so `bundle destroy` can never delete source data. The pipeline only reads the raw volume.
* **Schema-on-read in bronze, contracts in silver.** Bronze stores every column as STRING (positional schema across the 19/21/22-column export layouts), so a format change can never break ingestion. Parsing uses `try_cast` / `try_to_timestamp` in silver, and expectations make the failures visible.
* **Idempotent and re-runnable.** Checkpointed Auto Loader, AUTO CDC, declarative MVs and overwrite-mode ML outputs mean a Repair run or a full refresh gives the same result. See [02_pipeline_job.md](02_pipeline_job.md).
* **Fail-soft for optional data, fail-hard for core data.** An outage of a sports API never blocks sales reporting (`run_if: ALL_DONE`), while a sales-quality breach stops the ML and KPI publication.
* **Independent reconciliation.** A separate pandas implementation (`tools/eda/eda_taco_bueno.py`) recomputes the core metrics from the raw files; `tests/validation/validate_pipeline_vs_eda.sql` compares both. That comparison surfaced the reused-check-number bug that both implementations originally shared.
* **Unit tests and CI.** Pure-Python ingestion logic (geocoding rules, API parsers, venue fixes) is covered by `pytest` with fixtures, no network. GitHub Actions runs lint and tests on every push and validates the bundle when workspace secrets are configured.
* **ML lifecycle.** Every training run is an MLflow run (parameters, backtest metrics, model artifact with signature); models are registered in Unity Catalog. Forecasts carry the `mlflow_run_id` that produced them.
* **Observability.** Pipeline and job e-mail alerts, a job health rule on duration, the queryable event log and the `dq_gate_results` history table.
* **Security and governance.** Unity Catalog for every table, volume and model; no credentials in the repo (CLI OAuth locally, secrets in CI); business data is excluded by `.gitignore`.
* **Cost.** Serverless compute in triggered mode (the pipeline is not continuous) and incremental processing everywhere: a daily run touches one new file.
* **Naming.** `st_` = streaming table, `mv_` = materialized view, `ml_` = model output, `dq_` = quality history. Tables carry a `quality` property (`bronze`/`silver`/`gold`) and comments that explain their grain and purpose.

## Lessons learned while deploying (encoded in the code)

| Symptom | Cause | Fix |
|---|---|---|
| `DELTA_CLUSTERING_COLUMN_MISSING_STATS` on the first update | AUTO CDC target had the clustering column at position 40; Delta keeps stats for the first 32 columns | clustering columns selected first in `st_sports_events.py` |
| 7,874 games dropped by `has_start_time` | ESPN timestamps have no seconds; a plain cast returns NULL | explicit `try_to_timestamp` patterns |
| ~$334 difference vs the EDA in two months | legacy POS reused 72 check numbers per store-day | ticket key includes the start time; `single_header_per_ticket` expectation |
| 4-ticket difference in reconciliation counts | floating-point noise at the 2-cent threshold | compare in whole cents |
| Python job task fails with `NameError: __file__` / `SystemExit: 0` | serverless runs scripts through `exec()` inside IPython | path from the code object; exit explicitly only on errors |
| Inflated event lifts (+25% on no-game days) | mean of ratios dominated by tiny baselines (new stores, partial days) | ratio of sums plus median |
