# ML models: segmentation and demand forecasting

Two notebooks run as tasks of the daily job on serverless compute, read the gold tables, log every run to MLflow and register the models in Unity Catalog:

| Notebook | Schedule | Models (Unity Catalog) | Output tables |
|---|---|---|---|
| [`src/ml/01_store_segmentation.py`](../src/ml/01_store_segmentation.py) | Mondays or `force_retrain_segments=true` | `store_segmentation`, `product_segmentation` | `ml_store_segments`, `ml_store_segment_profiles`, `ml_product_segments` |
| [`src/ml/02_demand_forecast.py`](../src/ml/02_demand_forecast.py) | daily | `product_demand_forecaster`, `market_traffic_forecaster` | `ml_demand_forecast`, `ml_market_forecast`, `ml_strong_dates`, `ml_price_sensitivity`, `ml_forecast_backtest`, `ml_forecast_metrics`, `ml_feature_importance` |

## 1. Store segmentation (k-means)

**Question:** which stores sell the same way, beyond how much they sell?

**Features per store** (113 stores with 60+ days of sales, standardized): average daily net sales, average ticket, units per ticket, discount rate, drive-thru and delivery share (new-POS days only, because the legacy POS did not record delivery), lunch, dinner and late-night shares, weekend index, game-day sensitivity (lift on regional game days minus lift on no-game days), year-over-year growth, combo share and value-menu share.

**Method:** `StandardScaler` + `KMeans`. k is chosen by silhouette among 3..7, taking the smallest k within 0.02 of the best score: k = 4 (silhouette 0.136; k=3: 0.103, k=5: 0.125, k=6: 0.090). Segment names come from each centroid's most distinctive standardized features.

| Segment | Stores | Avg daily net sales | Avg ticket | Delivery share | Where |
|---|---:|---:|---:|---:|---|
| High-volume · more items per order, bigger baskets | 32 | $3,653 | $13.94 | 10% | 12 in Tulsa, 7 in DFW, 6 among the new stores without city |
| Mid-volume · fewer items per order, lunch-driven | 52 | $2,643 | $12.84 | 12% | 33 in DFW |
| Mid-volume · delivery-heavy, low drive-thru | 17 | $2,643 | $13.57 | 18% | 10 in DFW |
| Low-volume · light dinner, lunch-driven | 12 | $1,939 | $11.72 | 10% | 8 in DFW |

The silhouette is modest: stores form a continuum rather than separate islands. The segments are a practical grouping for targeting and for comparing like with like, not a claim of natural clusters.

**Product demand-pattern clusters** (143 menu items and bundles with 5,000+ units): weekday profile, weekend lift, daily and monthly variability, 90-day trend, share sold inside combos and volume. k = 3 (silhouette 0.23): a high-volume steady core (95 products), a Friday-peaking group (37) and a small volatile group (11, mostly limited-time items).

## 2. Demand forecasting

### Targets and features

* **Product & combo model:** daily units sold at menu price, chain level, for the top 25 menu items and 18 bundles (combos, platters, boxes): 43 series. Priced units are the target because combo components are recorded differently by the legacy and the new POS, so total units have a structural break at the migration. Kitchen demand for components follows from the bundle forecasts.
* **Market traffic model:** tickets per store and net sales per store, per market and day.
* **Features known in advance for every future day:**
  * calendar: weekday, month, week, day of month, holiday, school period, pay-day window, Taco Tuesday
  * sports exposure: share of the market's stores with a regional NFL, college football, NBA, MLB/MiLB, NHL or MLS game, games at local venues, postseason, national marquee
  * commercial plan: menu price and price index vs the product's median price, stores selling the product
  * context: active stores, POS migration share
  * level: product level lagged 98 days (both the 28-day level and the same-weekday level), longer than the forecast horizon so it is available identically in training, backtest and future
* **Model:** scikit-learn `HistGradientBoostingRegressor` on log1p(target), one global model per family, with categorical features (product, market, holiday) converted inside the pipeline. This lets the registered model take plain strings and carry an MLflow signature.

### Evaluation (backtest)

Train up to 2026-06-30, forecast 2026-07-01..09-27 (89 days), compare with a **seasonal-naive baseline** (weekday average of the 8 weeks before the cutoff). For products, a **validation window** (2026-04-01..06-30, trained up to March) picks per series between the model, the baseline and a 50/50 blend before the backtest is scored.

| Series | WAPE model | WAPE baseline | Notes |
|---|---:|---:|---|
| Chain daily tickets (sum of markets) | **3.3%** | 3.9% | |
| Tickets per store, by market (6 markets) | **4.3%** | 5.1% | beats the baseline in 5 of 6 markets, ties in 1 |
| Net sales per store, by market | **4.3%** | 4.5% | |
| Bundles (combos, platters, boxes), 18 series | **7.5%** | 10.4% | e.g. Party Combo 11% vs 83%: the stores-selling feature captures its delisting from 114 to 97 stores in July-August |
| Single menu items, 25 series | 10.0% | **7.9%** | the baseline wins |
| All 43 product series | 9.8% (selected) | **8.1%** | model alone 11.7%, blend 9.1% |

**Reading:** the models add value for traffic (the strong-date calendar) and for bundles. For single menu items, a simple weekday average of recent weeks remains the better 3-month forecast. The validation choice made on April-June did not hold in July-September, after the July 2026 price increase changed the regime. The job keeps scoring all three methods every day, so the selection can be revisited as more post-migration history accumulates.

**What drives the forecasts** (permutation importance on the backtest): market and weekday dominate traffic. Product identity and stores selling dominate product demand, followed by the lagged same-weekday level. Sports and holiday features carry little weight at the daily chain or market level. This is consistent with the measured event effects of a few percent (see [06](06_analysis_and_business_value.md)).

### Prediction bands

P10-P90 bands come from the backtest residuals of each series in log space, so volatile series get wider bands.

### Strong and weak dates

For every day, historical or forecast, chain and market tickets are compared with the median of the same weekday in the 3 weeks before and after (excluding holidays). A day is **strong** at +7% or more and **weak** at -10% or less. `drivers` lists what coincides with the day (holiday, regional or marquee games, Taco Tuesday, pay-day window); it is a list of co-occurring factors, not a causal attribution.

Forecast window 2026-09-28 to 2026-12-31: 2.21M tickets and $31.1M net sales (P10-P90 $30.1M-$33.4M):

| Date | Expected vs baseline | Class | Coinciding factors |
|---|---:|---|---|
| Sun Oct 25 | -14.6% | Weak | NFL Sunday (Giants at Texans) |
| Thu Nov 26 | -14.4% | Weak | Thanksgiving Day, Eagles at Cowboys, TCU at Texas Tech |
| Sun Dec 20 | +11.0% | Strong | Cowboys at Rams, Jaguars at Texans, last Sunday before Christmas |
| Sun Dec 27 | +7.6% | Strong | Jaguars at Cowboys |
| Thu Dec 31 | -11.9% | Weak | New Year's Eve |

Known limitation: the Christmas-week 2025 exports are missing (Dec 22-30), so the models never saw a Christmas. Christmas Eve and Day 2026 forecasts should be overridden with the stores' operating hours.

### Price sensitivity

`ml_price_sensitivity` re-scores the forecast horizon with every menu price 5% higher. Value items respond as expected:

| Product | Current price | Units change at +5% | Implied elasticity |
|---|---:|---:|---:|
| Party Quesadilla | $1.95 | -3.1% | -0.61 |
| Party Burrito | $1.93 | -2.7% | -0.54 |
| Regular Chips and Queso | $2.62 | -2.5% | -0.50 |
| Cheesecake Chimi (2) | $4.13 | -1.8% | -0.37 |

For many core items the model shows no response or a positive one, which is not plausible economically. Historical price changes came in chain-wide waves (April-May 2025, July 2026) that coincided with seasonal demand changes, so observational data cannot separate the price effect. **Recommendation:** run controlled price tests (matched store groups, using the store segments) before relying on elasticities for non-value items.

## Reproducibility

* Every run logs parameters, metrics (silhouette by k, WAPE by method), artifacts (segment names, method by series) and the model with signature and input example to MLflow.
* Models are registered in Unity Catalog under the pipeline schema (`store_segmentation`, `product_segmentation`, `product_demand_forecaster`, `market_traffic_forecaster`).
* Output rows carry `mlflow_run_id` and `created_at`; the notebooks are parameterized by catalog, schema, horizon and backtest dates.
