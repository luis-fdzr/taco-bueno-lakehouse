# Transformations and tables

Every table lives in `workspace.taco_bueno` (prod) or `workspace.dev_<user>_taco_bueno` (dev). Row counts are from the full run over the 597 raw files. Prefixes: `st_` streaming table, `mv_` materialized view, `ml_` model output.

## Table catalog

| Layer | Table | Grain | Rows | Purpose and value |
|---|---|---|---:|---|
| Bronze | `st_pos_lines_raw` | one raw POS line | 85,273,331 | Exact copy of the exports (all STRING) plus file lineage. Replays and audits start here. |
| Bronze | `st_sports_events_raw` | one fetched version of a game | 25,546 | Every API batch as landed; history of schedule changes. |
| Bronze | `mv_store_reference_raw` | store in the business city file | 140 | Geocoded reference file as delivered. |
| Silver | `st_orders_cleaned` | one valid POS line | 85,273,331 | Typed, harmonized line items: the single source for anything at item level. |
| Silver | `st_orders_quarantine` | one rejected line | 0 | Lines failing a blocking rule, with the failed rules and raw values. |
| Silver | `st_sports_events` | one game (current version) | 25,546 | Deduplicated calendar via AUTO CDC, with Central-time date and marquee flag. |
| Silver | `mv_tickets` | one ticket | 13,788,668 | Gross/net/collected sales per ticket, channel, daypart, reconciliation flag. |
| Silver | `mv_dim_store` | store | 151 | City, state, coordinates, market, activity period, POS migration date, status. |
| Silver | `mv_dim_product` | normalized product | 596 | Category, role (menu item, bundle, modifier, non-menu), median price, sales rank. |
| Silver | `mv_dim_calendar` | date 2024-2027 | 1,461 | Holidays, retail/food moments, school periods, pay-day window, Taco Tuesday. |
| Silver | `mv_dim_team` | team | 457 | Home venue and coordinates of every team seen in the schedules. |
| Silver | `mv_store_event_exposure` | store x relevant game | 221,838 | Distances and exposure flags (local venue, regional team, national marquee). |
| Gold | `mv_daily_store_sales` | store x day | 63,868 | Main analytical and ML table: sales, mix, calendar, sports, baseline and lift. |
| Gold | `mv_daily_product_demand` | day x market x product | 678,452 | Demand history for product and combo forecasting. |
| Gold | `mv_product_price_history` | store x product x month | 369,058 | Modal menu price and month-over-month change: pricing analysis and price feature. |
| Gold | `mv_event_impact` | exposure type x market x day type | 114 | Sales lift of game days against no-game days. |
| Gold | `mv_data_quality_daily` | calendar day | 614 | Data-quality scorecard; missing exports appear as rows. |
| Gold | `mv_executive_kpis` | month | 21 | KPIs with YoY on a per-store-day basis. |
| Gold | `st_daily_revenue` | store x day x channel | 156,091 | Append-only streaming ledger, finalized by watermark. |
| ML | `ml_store_segments`, `ml_store_segment_profiles` | store / segment | see [07](07_ml_models.md) | K-means store segments and their profiles. |
| ML | `ml_product_segments` | product | | Demand-pattern clusters. |
| ML | `ml_demand_forecast`, `ml_market_forecast` | day x product / day x market | | Forecasts with prediction bands. |
| ML | `ml_strong_dates` | day x scope | | Strong/weak days, historical and forecast, with drivers. |
| ML | `ml_forecast_backtest`, `ml_forecast_metrics`, `ml_feature_importance` | | | Model evaluation. |
| Quality | `dq_gate_results`, `pipeline_event_log` | check run / pipeline event | | History of quality checks and expectation metrics. |

## Main transformations

### 1. Ingestion that cannot break (bronze)

* **Positional, all-STRING schema.** The exports come in three layouts (19, 21 and 22 columns: `Report Group`, `Tender Type` and `Total Rounding` were added over time) that share the same column order. Reading with `header=false` and an explicit 22-column schema maps every file correctly; shorter files get NULL in the trailing columns. The header row of each file is filtered out.
* **RFC 4180 quoting.** Product names such as `6"" Flour Tortillas - Side""` are escaped with doubled quotes; `escape='"'` is required or those rows shift columns.
* **Lineage.** `source_file`, file size, modification time and `ingested_at` on every row.

### 2. Cleaning and harmonization (`st_orders_cleaned`)

* **Types**: `try_cast` / `try_to_timestamp` turn bad values into NULLs that expectations catch, instead of failing the stream.
* **Business date vs file date**: the file name is the export date and the data is for the previous day (verified on 100% of lines by the `business_date_is_day_before_file` expectation).
* **Two POS systems.** The stores migrated from a legacy POS to a new one between 2025-11-04 and 2026-01-19, and the two systems use the same columns differently:

  | | Legacy POS (`pop` in To Go / Inside / 3) | New POS |
  |---|---|---|
  | `Quantity` | real quantity (0..n) | always 1: one row per unit |
  | `Menu Price` | unit price | unit price |
  | `Transaction Type = Discount` | a sale of the item with an item discount | not used |
  | voids | `Transaction Type = Void` rows | `Item Voided` = price of the voided line |
  | `Total` (ticket) | gross sales, before discount and tax | net sales + tax |

  Both are reduced to the same line measures: `units` and `line_amount` (gross item sales).
* **Channels**: `Drive-thru` (Drive Thru; legacy To Go), `In-store` (Dine-In, Carry Out; legacy Inside and code 3), `Delivery & digital` (DoorDash, UberEats, GrubHub, Online, Mobile). The legacy mapping is inferred from behavior: legacy *To Go* has the same share (68.8% vs 67.3%) and ticket size (about $12.3 to $12.5) as new-POS drive-thru, while delivery tickets average $22 or more.
* **Dayparts** from the ticket start time: breakfast (<10:30), lunch (10:30-14:00), afternoon (14-17), dinner (17-21), late night (21-05).
* **Product key**: product names are normalized across systems and menu versions by removing combo/platter context suffixes (`(combo)`, `(platter)`, `(Wholotta Box)`, `(CB)`) and year suffixes (`Beef Wholotta Platter 2025` -> `beef wholotta platter`). 647 raw names collapse to 596 product keys.
* **Ticket key**: `business_date | store | check number | check start time`. Check numbers repeat across stores, **and the legacy POS reused 72 check numbers on the same day in the same store** (July 14-15 and October 25-26, 2025). Without the start time those pairs merged into single tickets with two different totals. The defect was found by reconciling two independent implementations and is now guarded by the `single_header_per_ticket` expectation (0 violations).

### 3. Ticket economics (`mv_tickets`)

`gross_sales` (before discount and tax), `net_sales` = gross - discount (the revenue metric everywhere in the project) and `collected` = net + tax, computed with the POS-specific formula. `reconciles` checks the ticket header against the sum of its lines, comparing in whole cents so floating-point noise cannot flip a 2-cent difference; 99.87% of tickets reconcile.

### 4. Enrichment (silver dimensions)

* **Stores**: FULL OUTER JOIN of the business city file and the sales, so both gaps stay visible: 11 stores sell but are missing from the city file, and 32 listed stores have no sales in the period. Markets are assigned by distance to metro anchors (DFW, Oklahoma City, Tulsa, Houston; others by state).
* **Calendar**: rule-based holidays (e.g. Thanksgiving = 4th Thursday of November, Super Bowl Sunday = 2nd Sunday of February), so future dates are covered without reference files.
* **Teams and exposure**: a team's home is derived from its own home games. Each store is paired with every game within the local radius (venue <= 50 km) or involving a regional team (home within 320 km), plus national marquee events. Details in [08_sports_events.md](08_sports_events.md).

### 5. Aggregation and modeling inputs (gold)

* `mv_daily_store_sales` aggregates tickets per store-day and adds channel and daypart mix, calendar context and sports exposure. A **same-weekday baseline** (average of the 4 previous same weekdays) gives `net_sales_lift_vs_baseline`, the basis of holiday and event impact analysis.
* `mv_event_impact` tags every non-holiday store-day with its exposure types and compares their average lift with the lift of no-game days (`incremental_lift_vs_no_game`), by market and weekday/weekend.
* `mv_daily_product_demand` and `mv_product_price_history` feed the forecasts (units, price) and the pricing analysis (modal price per store and month, change vs previous month).
* `mv_executive_kpis` normalizes productivity per store-day so store openings/closures and missing export days do not distort trends, and compares each month with the same month a year earlier.

### 6. Modeling (ML notebooks)

Segmentation and forecasting run as job tasks on top of the gold tables; see [07_ml_models.md](07_ml_models.md).
