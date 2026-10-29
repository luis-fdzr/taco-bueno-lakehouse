# Databricks notebook source
# MAGIC %md
# MAGIC # Demand forecasts: products, combos, market traffic and strong dates
# MAGIC
# MAGIC Two global gradient-boosting models (scikit-learn `HistGradientBoostingRegressor`):
# MAGIC
# MAGIC 1. **Product & combo demand**: daily units sold at menu price per product, chain level. One model
# MAGIC    across the top menu items and the bundles (combos, platters, boxes), with the product as a
# MAGIC    categorical feature. Per series, a validation window picks the model, the seasonal-naive
# MAGIC    baseline or their 50/50 blend; a +5% price scenario gives each product's price sensitivity.
# MAGIC 2. **Market traffic**: daily tickets and net sales per store, by market (DFW, OKC, Tulsa, Houston...).
# MAGIC
# MAGIC Features known in advance for every future day: weekday, month, holidays and retail moments,
# MAGIC school period, pay-day window, Taco Tuesday, **sports exposure** (share of stores with a regional
# MAGIC NFL / college football / NBA / MLB / NHL / MLS game, local-venue games, postseason, national
# MAGIC marquee events), menu price, active stores and POS migration.
# MAGIC
# MAGIC Evaluation: backtest from `backtest_start` to the last day with data, against a seasonal-naive
# MAGIC baseline (weekday average of the 8 weeks before the cutoff). Prediction bands come from the
# MAGIC backtest residuals of each series. Then both models are refit on all data and forecast
# MAGIC `horizon_days` ahead. Strong (and weak) dates are days whose expected traffic deviates from the
# MAGIC same-weekday baseline of the surrounding weeks.
# MAGIC
# MAGIC Outputs: `ml_demand_forecast`, `ml_market_forecast`, `ml_strong_dates`, `ml_forecast_backtest`,
# MAGIC `ml_forecast_metrics`, `ml_feature_importance`, `ml_price_sensitivity`; models `product_demand_forecaster` and
# MAGIC `market_traffic_forecaster` in Unity Catalog.

# COMMAND ----------

# MAGIC %pip install -q "scikit-learn==1.5.2"
# MAGIC %restart_python

# COMMAND ----------

from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "taco_bueno")
dbutils.widgets.text("horizon_days", "95")
dbutils.widgets.text("backtest_start", "2026-07-01")
dbutils.widgets.text("top_menu_items", "25")
dbutils.widgets.text("min_bundle_units_180d", "15000")
dbutils.widgets.text("strong_date_threshold", "0.07")

catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
fq = f"{catalog}.{schema}"
HORIZON = int(dbutils.widgets.get("horizon_days"))
BACKTEST_START = pd.Timestamp(dbutils.widgets.get("backtest_start"))
TOP_ITEMS = int(dbutils.widgets.get("top_menu_items"))
MIN_BUNDLE_UNITS = float(dbutils.widgets.get("min_bundle_units_180d"))
STRONG = float(dbutils.widgets.get("strong_date_threshold"))
ORIGIN = pd.Timestamp("2025-01-01")
created_at = datetime.now(timezone.utc)

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/taco_bueno_demand_forecast")


def save(df: pd.DataFrame, name: str) -> None:
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].where(df[c].notna(), None)
    spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.{name}")


def wape(actual, pred) -> float:
    actual, pred = np.asarray(actual, float), np.asarray(pred, float)
    return float(np.abs(actual - pred).sum() / max(np.abs(actual).sum(), 1e-9))

# COMMAND ----------

# MAGIC %md ## 1. Calendar, sports exposure and history

# COMMAND ----------

cal = spark.sql(f"""
  SELECT calendar_date, day_of_week, month, week_of_year, day_of_month, is_weekend, holiday_name,
         school_period, is_payday_window, is_taco_tuesday
  FROM {fq}.mv_dim_calendar
""").toPandas()
cal["calendar_date"] = pd.to_datetime(cal["calendar_date"])
HOLIDAYS = sorted(cal["holiday_name"].dropna().unique())
SCHOOL = sorted(cal["school_period"].dropna().unique())

# Share of each market's stores exposed to each kind of game, per day (history and future schedules).
sports_mkt = spark.sql(f"""
  WITH stores AS (
    SELECT market, count(*) AS n FROM {fq}.mv_dim_store WHERE has_sales AND latitude IS NOT NULL GROUP BY market
  ),
  x AS (
    SELECT event_date, market,
      count(DISTINCT CASE WHEN involves_regional_team AND league = 'NFL' THEN store_id END)               AS nfl,
      count(DISTINCT CASE WHEN involves_regional_team AND league = 'NCAAF' THEN store_id END)             AS ncaaf,
      count(DISTINCT CASE WHEN involves_regional_team AND league = 'NBA' THEN store_id END)               AS nba,
      count(DISTINCT CASE WHEN involves_regional_team AND league IN ('MLB', 'MiLB') THEN store_id END)    AS baseball,
      count(DISTINCT CASE WHEN involves_regional_team AND league = 'NHL' THEN store_id END)               AS nhl,
      count(DISTINCT CASE WHEN involves_regional_team AND league = 'MLS' THEN store_id END)               AS mls,
      count(DISTINCT CASE WHEN is_local_venue THEN store_id END)                                          AS local_venue,
      count(DISTINCT CASE WHEN involves_regional_team AND season_type = 'postseason' THEN store_id END)   AS postseason,
      max(CASE WHEN is_national_marquee THEN 1 ELSE 0 END)                                                AS marquee
    FROM {fq}.mv_store_event_exposure
    GROUP BY event_date, market
  )
  SELECT x.*, s.n AS stores_with_location FROM x JOIN stores s USING (market)
""").toPandas()
sports_mkt["event_date"] = pd.to_datetime(sports_mkt["event_date"])
SPORT_COLS = ["nfl", "ncaaf", "nba", "baseball", "nhl", "mls", "local_venue", "postseason"]

def sports_shares(df: pd.DataFrame, keys: list) -> pd.DataFrame:
    g = df.groupby(keys)[SPORT_COLS + ["stores_with_location"]].sum()
    out = g[SPORT_COLS].div(g["stores_with_location"], axis=0).add_prefix("share_")
    out["marquee"] = df.groupby(keys)["marquee"].max()
    return out.reset_index()

sports_chain = sports_shares(sports_mkt, ["event_date"])
sports_market = sports_shares(sports_mkt, ["event_date", "market"])
SPORT_FEATURES = [f"share_{c}" for c in SPORT_COLS] + ["marquee"]

complete_days = set(pd.to_datetime(spark.sql(
    f"SELECT business_date FROM {fq}.mv_data_quality_daily WHERE day_status = 'Complete'").toPandas()["business_date"]))
LAST_DAY = max(complete_days)
FUTURE = pd.date_range(LAST_DAY + pd.Timedelta(days=1), periods=HORIZON, freq="D")
print(f"Last complete day: {LAST_DAY.date()}, forecasting {FUTURE[0].date()} -> {FUTURE[-1].date()}")


def add_calendar(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    out = df.merge(cal, left_on=date_col, right_on="calendar_date", how="left").drop(columns="calendar_date")
    out["holiday"] = out["holiday_name"].fillna("None").astype(str)
    out["school_period"] = out["school_period"].fillna("Unknown").astype(str)
    out["trend_days"] = (out[date_col] - ORIGIN).dt.days
    for c in ["is_weekend", "is_payday_window", "is_taco_tuesday"]:
        out[c] = out[c].astype(int)
    return out.drop(columns="holiday_name")


CAL_FEATURES = ["day_of_week", "month", "week_of_year", "day_of_month", "is_weekend", "holiday", "school_period",
                "is_payday_window", "is_taco_tuesday", "trend_days"]


def categorizer(levels: dict) -> FunctionTransformer:
    """Text columns -> pandas categoricals with fixed levels, inside the model pipeline.

    Keeping this step in the pipeline lets the logged model take plain strings,
    which MLflow can describe in a signature (required by Unity Catalog).
    """
    def to_categories(X):
        X = X.copy()
        for col, cats in levels.items():
            X[col] = pd.Categorical(X[col].astype(str), categories=cats)
        return X
    return FunctionTransformer(to_categories, validate=False)


def hgb(**kw) -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(learning_rate=0.05, max_leaf_nodes=31, l2_regularization=1.0,
                                         categorical_features="from_dtype", random_state=42, **kw)

# COMMAND ----------

# MAGIC %md ## 2. Product & combo demand model

# COMMAND ----------

prod = spark.sql(f"""
  SELECT business_date AS date, product_key,
         any_value(product_name) AS product_name, any_value(category) AS category, any_value(product_role) AS product_role,
         sum(units) AS units, sum(priced_units) AS priced_units,
         sum(avg_menu_price * priced_units) / nullif(sum(priced_units), 0) AS avg_price,
         sum(stores_selling) AS stores_selling
  FROM {fq}.mv_daily_product_demand
  WHERE product_role IN ('Menu item', 'Bundle')
  GROUP BY business_date, product_key
""").toPandas()
prod["date"] = pd.to_datetime(prod["date"])
prod = prod[prod["date"].isin(complete_days)]
# Target: units sold at a menu price. Combo components are recorded differently by the
# legacy and the new POS, so total units break at the migration; priced units do not.
# Component (kitchen) demand follows from the bundle forecasts.
prod["y"] = prod["priced_units"]

recent = prod[prod["date"] > LAST_DAY - pd.Timedelta(days=180)]
active = prod[prod["date"] > LAST_DAY - pd.Timedelta(days=14)]["product_key"].unique()
vol = recent[recent.product_key.isin(active)].groupby(["product_key", "product_role"])["y"].sum().reset_index()
items = vol[vol.product_role == "Menu item"].nlargest(TOP_ITEMS, "y")["product_key"]
bundles = vol[(vol.product_role == "Bundle") & (vol.y >= MIN_BUNDLE_UNITS)]["product_key"]
SERIES = sorted(set(items) | set(bundles))
prod = prod[prod.product_key.isin(SERIES)].copy()
meta = prod.sort_values("date").groupby("product_key")[["product_name", "category", "product_role"]].last()
print(f"{len(items)} menu items + {len(bundles)} bundles")

day_ctx = spark.sql(f"""
  SELECT business_date AS date, count(*) AS stores_reporting, avg(CASE WHEN pos_system = 'New' THEN 1.0 ELSE 0 END) AS new_pos_share
  FROM {fq}.mv_daily_store_sales GROUP BY business_date
""").toPandas()
day_ctx["date"] = pd.to_datetime(day_ctx["date"])

def product_frame(base: pd.DataFrame) -> pd.DataFrame:
    df = base.merge(day_ctx, on="date", how="left")
    df = df.merge(sports_chain.rename(columns={"event_date": "date"}), on="date", how="left")
    df[SPORT_FEATURES] = df[SPORT_FEATURES].fillna(0.0)
    df = add_calendar(df)
    df["product"] = df["product_key"].astype(str)
    df["log_price"] = np.log(df["avg_price"].clip(lower=0.01))
    return df

# Price and assortment (stores selling) are commercial decisions known in advance, so the
# forecast is conditional on them: the backtest uses the actual price list and assortment,
# the future forecast the current ones, and scenarios change them (see price sensitivity).
P_FEATURES = (["product", "log_price", "price_index", "stores_selling", "level", "level_lag", "dow_level_lag",
               "stores_reporting", "new_pos_share"] + CAL_FEATURES + SPORT_FEATURES)

LAG_DAYS = 98   # >= the 95-day horizon, so the feature exists the same way in training, backtest and future


def lag_levels(df: pd.DataFrame, until: pd.Timestamp) -> pd.DataFrame:
    """Recent level of each product, observed LAG_DAYS before the target date.

    level_lag:     mean log units over 28 days ending LAG_DAYS before the date
    dow_level_lag: mean log units of the same weekday over 8 weeks ending LAG_DAYS before the date
    Missing export days stay missing (they are not zeros).
    """
    out = []
    days = pd.date_range(df.date.min(), until, freq="D")
    for key, g in df.groupby("product_key"):
        s = np.log1p(g.set_index("date")["y"]).reindex(days)
        lvl = s.rolling("28D", min_periods=7).mean().shift(LAG_DAYS)
        frame = pd.DataFrame({"date": days, "product_key": key, "level_lag": lvl.values})
        dow = pd.Series(s.values, index=days)
        frame["dow_level_lag"] = (dow.groupby(dow.index.dayofweek)
                                  .transform(lambda x: x.rolling(8, min_periods=3).mean().shift(LAG_DAYS // 7)).values)
        out.append(frame)
    return pd.concat(out, ignore_index=True)

def fit_products(train: pd.DataFrame):
    stats = train.groupby("product_key").agg(level=("y", lambda s: np.log1p(s).mean()),
                                             median_price=("avg_price", "median"))
    def prep(df):
        df = df.drop(columns=["level", "median_price"], errors="ignore").merge(stats, left_on="product_key", right_index=True, how="left")
        df["price_index"] = df["avg_price"] / df["median_price"]
        return df
    levels = {"product": SERIES, "holiday": ["None"] + HOLIDAYS, "school_period": SCHOOL + ["Unknown"]}
    model = make_pipeline(categorizer(levels), hgb(max_iter=600, min_samples_leaf=20))
    tr = prep(train)
    model.fit(tr[P_FEATURES], np.log1p(tr["y"]))
    return model, prep


def seasonal_naive(df: pd.DataFrame, origin: pd.Timestamp, weeks: int = 8) -> pd.DataFrame:
    """Weekday average of the `weeks` weeks before the forecast origin."""
    win = df[(df.date < origin) & (df.date >= origin - pd.Timedelta(weeks=weeks))]
    return win.groupby(["product_key", "day_of_week"])["y"].mean().rename("baseline").reset_index()


METHODS = ["model", "baseline", "blend"]

def predict_window(train_df: pd.DataFrame, target_df: pd.DataFrame, origin: pd.Timestamp):
    model, prep = fit_products(train_df)
    out = prep(target_df)
    out["model"] = np.clip(np.expm1(model.predict(out[P_FEATURES])), 0, None)
    out = out.merge(seasonal_naive(train_df, origin), on=["product_key", "day_of_week"], how="left")
    out["baseline"] = out["baseline"].fillna(out["model"])
    out["blend"] = 0.5 * out["model"] + 0.5 * out["baseline"]
    return model, prep, out

hist = product_frame(prod)
LAGS = lag_levels(hist, LAST_DAY + pd.Timedelta(days=HORIZON))
hist = hist.merge(LAGS, on=["date", "product_key"], how="left")

# a) Validation window (3 months before the backtest) chooses the method per series.
VAL_START = BACKTEST_START - pd.DateOffset(months=3)
_, _, val = predict_window(hist[hist.date < VAL_START],
                           hist[(hist.date >= VAL_START) & (hist.date < BACKTEST_START)], VAL_START)
choice = (val.groupby("product_key")
          .apply(lambda g: min(METHODS, key=lambda m: wape(g.y, g[m])), include_groups=False).rename("method"))

# b) Backtest on the following window with the methods chosen before it.
model, prep, te = predict_window(hist[hist.date < BACKTEST_START], hist[hist.date >= BACKTEST_START], BACKTEST_START)
te = te.merge(choice, left_on="product_key", right_index=True, how="left")
te["method"] = te["method"].fillna("blend")
te["forecast"] = np.select([te.method == m for m in METHODS], [te[m] for m in METHODS])

p_metrics = te.groupby("product_key").apply(lambda g: pd.Series({
    "method": g["method"].iloc[0],
    "wape_selected": wape(g.y, g.forecast), "wape_model": wape(g.y, g.model), "wape_baseline": wape(g.y, g.baseline),
    "wape_blend": wape(g.y, g.blend), "bias_selected": float(g.forecast.sum() / g.y.sum() - 1), "days": len(g),
    "actual_units": float(g.y.sum())}), include_groups=False).reset_index()
p_metrics = p_metrics.merge(meta, left_on="product_key", right_index=True)
p_overall = {f"wape_{m}": wape(te.y, te[m]) for m in METHODS} | {
    "wape_selected": wape(te.y, te.forecast), "bias_selected": float(te.forecast.sum() / te.y.sum() - 1)}
print("Products backtest:", {k: round(v, 4) for k, v in p_overall.items()})
print("Methods chosen:", choice.value_counts().to_dict())

# Residual bands (log space) per product from the backtest of the selected method
te["log_resid"] = np.log1p(te["y"]) - np.log1p(te["forecast"])
bands = te.groupby("product_key")["log_resid"].quantile([0.1, 0.9]).unstack().rename(columns={0.1: "q10", 0.9: "q90"})

imp = permutation_importance(model, te[P_FEATURES], np.log1p(te["y"]), n_repeats=5, random_state=42)
p_importance = pd.DataFrame({"model": "product_demand", "feature": P_FEATURES, "importance": imp.importances_mean})

# c) Refit on all history and forecast with the current price list and assortment.
model_all, prep_all = fit_products(hist)
recent14 = hist[hist.date > LAST_DAY - pd.Timedelta(days=14)]
last_price = recent14.groupby("product_key")["avg_price"].mean()
last_stores = recent14.groupby("product_key")["stores_selling"].median()
last_ctx = day_ctx[day_ctx.date > LAST_DAY - pd.Timedelta(days=14)][["stores_reporting", "new_pos_share"]].median()

def future_frame(price_factor: float = 1.0) -> pd.DataFrame:
    f = pd.MultiIndex.from_product([FUTURE, SERIES], names=["date", "product_key"]).to_frame(index=False)
    f["avg_price"] = f["product_key"].map(last_price) * price_factor
    f["stores_selling"] = f["product_key"].map(last_stores)
    f = f.merge(sports_chain.rename(columns={"event_date": "date"}), on="date", how="left")
    f[SPORT_FEATURES] = f[SPORT_FEATURES].fillna(0.0)
    f["stores_reporting"], f["new_pos_share"] = last_ctx["stores_reporting"], last_ctx["new_pos_share"]
    f = add_calendar(f)
    f["product"] = f["product_key"].astype(str)
    f["log_price"] = np.log(f["avg_price"].clip(lower=0.01))
    f = f.merge(LAGS, on=["date", "product_key"], how="left")
    return prep_all(f)

fut = future_frame()
fut["model"] = np.clip(np.expm1(model_all.predict(fut[P_FEATURES])), 0, None)
fut = fut.merge(seasonal_naive(hist, LAST_DAY + pd.Timedelta(days=1)), on=["product_key", "day_of_week"], how="left")
fut["baseline"] = fut["baseline"].fillna(fut["model"])
fut["blend"] = 0.5 * fut["model"] + 0.5 * fut["baseline"]
fut = fut.merge(choice, left_on="product_key", right_index=True, how="left")
fut["method"] = fut["method"].fillna("blend")
fut["units_forecast"] = np.select([fut.method == m for m in METHODS], [fut[m] for m in METHODS])
fut = fut.merge(bands, left_on="product_key", right_index=True, how="left")
log_fc = np.log1p(fut["units_forecast"])
fut["units_lower"] = np.clip(np.expm1(log_fc + fut["q10"].fillna(-0.15)), 0, None)
fut["units_upper"] = np.clip(np.expm1(log_fc + fut["q90"].fillna(0.15)), 0, None)

# d) Price sensitivity: same horizon with every menu price 5% higher (model component).
plus5 = future_frame(1.05)
plus5["model_plus5"] = np.expm1(model_all.predict(plus5[P_FEATURES]))
sens = (fut[["product_key", "model"]].assign(model_plus5=plus5["model_plus5"].values)
        .groupby("product_key")[["model", "model_plus5"]].sum())
sens["units_change_pct"] = sens["model_plus5"] / sens["model"] - 1
sens["implied_elasticity"] = sens["units_change_pct"] / 0.05
sens = sens.join(meta).join(last_price.rename("current_price")).reset_index()

with mlflow.start_run(run_name="product_demand_forecaster") as run:
    mlflow.log_params({"series": len(SERIES), "menu_items": len(items), "bundles": len(bundles), "target": "priced_units",
                       "backtest_start": str(BACKTEST_START.date()), "validation_start": str(VAL_START.date()),
                       "horizon_days": HORIZON})
    mlflow.log_metrics(p_overall)
    mlflow.log_dict(choice.to_dict(), "method_by_series.json")
    sig_X = fut[P_FEATURES].head(50)
    info = mlflow.sklearn.log_model(model_all, "model", input_example=sig_X.head(3),
                                    signature=infer_signature(sig_X, model_all.predict(sig_X)))
    try:
        mlflow.register_model(info.model_uri, f"{fq}.product_demand_forecaster")
    except Exception as e:
        print(f"Model registry skipped: {e}")
    product_run = run.info.run_id

# COMMAND ----------

# MAGIC %md ## 3. Market traffic model (tickets and net sales per store)

# COMMAND ----------

mkt = spark.sql(f"""
  SELECT business_date AS date, market, count(*) AS stores, sum(tickets) AS tickets, sum(net_sales) AS net_sales,
         avg(CASE WHEN pos_system = 'New' THEN 1.0 ELSE 0 END) AS new_pos_share
  FROM {fq}.mv_daily_store_sales GROUP BY business_date, market
""").toPandas()
mkt["date"] = pd.to_datetime(mkt["date"])
mkt = mkt[mkt["date"].isin(complete_days)].copy()
MARKETS = sorted(mkt["market"].unique())
mkt["tickets_per_store"] = mkt["tickets"] / mkt["stores"]
mkt["net_sales_per_store"] = mkt["net_sales"] / mkt["stores"]

def market_frame(base: pd.DataFrame) -> pd.DataFrame:
    df = base.merge(sports_market.rename(columns={"event_date": "date"}), on=["date", "market"], how="left")
    df[SPORT_FEATURES] = df[SPORT_FEATURES].fillna(0.0)
    df = add_calendar(df)
    df["market_cat"] = df["market"].astype(str)
    return df

M_FEATURES = ["market_cat", "stores", "new_pos_share"] + CAL_FEATURES + SPORT_FEATURES
mhist = market_frame(mkt)

def fit_market(train, target):
    levels = {"market_cat": MARKETS, "holiday": ["None"] + HOLIDAYS, "school_period": SCHOOL + ["Unknown"]}
    m = make_pipeline(categorizer(levels), hgb(max_iter=500, min_samples_leaf=10))
    return m.fit(train[M_FEATURES], np.log1p(train[target]))

mtrain, mtest = mhist[mhist.date < BACKTEST_START].copy(), mhist[mhist.date >= BACKTEST_START].copy()
m_models, m_metrics, m_importance, m_bands = {}, [], [], {}
for target in ["tickets_per_store", "net_sales_per_store"]:
    m = fit_market(mtrain, target)
    mtest[f"{target}_fc"] = np.expm1(m.predict(mtest[M_FEATURES]))
    base = (mtrain[mtrain.date >= BACKTEST_START - pd.Timedelta(weeks=8)]
            .groupby(["market", "day_of_week"])[target].mean().rename(f"{target}_base").reset_index())
    mtest = mtest.merge(base, on=["market", "day_of_week"], how="left")
    for mk, g in mtest.groupby("market"):
        m_metrics.append({"series_type": target, "series_key": mk, "wape_model": wape(g[target], g[f"{target}_fc"]),
                          "wape_baseline": wape(g[target], g[f"{target}_base"]), "days": len(g),
                          "bias_model": float(g[f"{target}_fc"].sum() / g[target].sum() - 1)})
    resid = np.log1p(mtest[target]) - np.log1p(mtest[f"{target}_fc"])
    m_bands[target] = mtest.assign(r=resid).groupby("market")["r"].quantile([0.1, 0.9]).unstack()
    imp = permutation_importance(m, mtest[M_FEATURES], np.log1p(mtest[target]), n_repeats=5, random_state=42)
    m_importance.append(pd.DataFrame({"model": f"market_{target}", "feature": M_FEATURES, "importance": imp.importances_mean}))
    m_models[target] = fit_market(mhist, target)           # refit on all history
m_metrics = pd.DataFrame(m_metrics)
print(m_metrics.groupby("series_type")[["wape_model", "wape_baseline"]].mean())

last_stores = mkt[mkt.date > LAST_DAY - pd.Timedelta(days=14)].groupby("market")["stores"].median()
mfut = pd.MultiIndex.from_product([FUTURE, MARKETS], names=["date", "market"]).to_frame(index=False)
mfut["stores"] = mfut["market"].map(last_stores).fillna(0)
mfut["new_pos_share"] = 1.0
mfut = market_frame(mfut)
for target in ["tickets_per_store", "net_sales_per_store"]:
    lp = m_models[target].predict(mfut[M_FEATURES])
    b = m_bands[target].reindex(mfut["market"]).reset_index(drop=True)
    mfut[f"{target}_forecast"] = np.expm1(lp)
    mfut[f"{target}_lower"] = np.expm1(lp + b[0.1].fillna(-0.1).values)
    mfut[f"{target}_upper"] = np.expm1(lp + b[0.9].fillna(0.1).values)
for c in ["forecast", "lower", "upper"]:
    mfut[f"tickets_{c}"] = mfut[f"tickets_per_store_{c}"] * mfut["stores"]
    mfut[f"net_sales_{c}"] = mfut[f"net_sales_per_store_{c}"] * mfut["stores"]

with mlflow.start_run(run_name="market_traffic_forecaster") as run:
    mlflow.log_params({"markets": len(MARKETS), "backtest_start": str(BACKTEST_START.date()), "horizon_days": HORIZON})
    for t, g in m_metrics.groupby("series_type"):
        mlflow.log_metric(f"{t}_wape_model", float(g.wape_model.mean()))
        mlflow.log_metric(f"{t}_wape_baseline", float(g.wape_baseline.mean()))
    info = mlflow.sklearn.log_model(m_models["tickets_per_store"], "model", input_example=mfut[M_FEATURES].head(3),
                                    signature=infer_signature(mfut[M_FEATURES].head(50),
                                                              m_models["tickets_per_store"].predict(mfut[M_FEATURES].head(50))))
    try:
        mlflow.register_model(info.model_uri, f"{fq}.market_traffic_forecaster")
    except Exception as e:
        print(f"Model registry skipped: {e}")
    market_run = run.info.run_id

# COMMAND ----------

# MAGIC %md ## 4. Strong and weak dates (history and forecast)

# COMMAND ----------

def classify(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Uplift vs the median of the same weekday in the 3 weeks before and after (holidays excluded)."""
    df = df.sort_values("date").copy()
    base = []
    for i, r in df.iterrows():
        win = df[(df.day_of_week == r.day_of_week) & (df.date != r.date)
                 & (df.date.between(r.date - pd.Timedelta(weeks=3), r.date + pd.Timedelta(weeks=3)))
                 & (df.holiday == "None")]
        base.append(win[value_col].median() if len(win) else np.nan)
    df["baseline"] = base
    df["uplift"] = df[value_col] / df["baseline"] - 1
    df["classification"] = np.select([df.uplift >= STRONG, df.uplift <= -0.10], ["Strong", "Weak"], "Normal")
    return df

events_by_day = spark.sql(f"""
  SELECT event_date AS date, market,
         array_join(slice(array_sort(collect_set(CASE WHEN involves_regional_team OR is_national_marquee
                                                       THEN coalesce(headline, short_name) END)), 1, 4), ' | ') AS top_events
  FROM {fq}.mv_store_event_exposure
  WHERE involves_regional_team AND (season_type = 'postseason' OR league IN ('NFL', 'NCAAF')) OR is_national_marquee
  GROUP BY event_date, market
""").toPandas()
events_by_day["date"] = pd.to_datetime(events_by_day["date"])
events_chain = events_by_day.groupby("date")["top_events"].apply(lambda s: " | ".join(sorted(set(" | ".join(s).split(" | ")))[:5])).reset_index()

actual_chain = mhist.groupby("date").agg(tickets=("tickets", "sum"), net_sales=("net_sales", "sum")).reset_index()
forecast_chain = mfut.groupby("date").agg(tickets=("tickets_forecast", "sum"), net_sales=("net_sales_forecast", "sum")).reset_index()
chain = pd.concat([actual_chain.assign(source="actual"), forecast_chain.assign(source="forecast")], ignore_index=True)
chain = add_calendar(chain)
chain = classify(chain, "tickets").assign(scope="Chain")

market_series = pd.concat([
    mhist[["date", "market", "tickets", "net_sales"]].assign(source="actual"),
    mfut[["date", "market"]].assign(tickets=mfut["tickets_forecast"], net_sales=mfut["net_sales_forecast"], source="forecast"),
], ignore_index=True)
market_series = add_calendar(market_series)
market_strong = pd.concat([classify(g, "tickets").assign(scope=mk) for mk, g in market_series.groupby("market")], ignore_index=True)

strong = pd.concat([chain.merge(events_chain, on="date", how="left"),
                    market_strong.merge(events_by_day, on=["date", "market"], how="left")], ignore_index=True)
strong["holiday_name"] = strong["holiday"].where(strong["holiday"] != "None")
def drivers(r) -> str | None:
    parts = [r["holiday_name"], r["top_events"],
             "Taco Tuesday" if r["is_taco_tuesday"] else None,
             "pay-day window" if r["is_payday_window"] else None]
    parts = [p for p in parts if isinstance(p, str) and p]       # NaN is truthy; keep real text only
    return " + ".join(parts) if parts else None


strong["drivers"] = strong.apply(drivers, axis=1)
strong = strong[["date", "scope", "source", "tickets", "net_sales", "baseline", "uplift", "classification",
                 "holiday_name", "top_events", "drivers", "day_of_week"]]
strong = strong.rename(columns={"baseline": "baseline_tickets", "uplift": "tickets_uplift"})
strong["created_at"] = created_at

# COMMAND ----------

# MAGIC %md ## 5. Publish

# COMMAND ----------

chain_flags = strong[(strong.scope == "Chain") & (strong.source == "forecast")][["date", "classification", "drivers"]]
out = fut.merge(chain_flags, on="date", how="left").merge(meta, left_on="product_key", right_index=True, how="left")
out = out.rename(columns={"date": "forecast_date", "classification": "date_classification", "drivers": "date_drivers"})
save(out[["forecast_date", "product_key", "product_name", "category", "product_role", "method", "units_forecast",
          "units_lower", "units_upper", "avg_price", "stores_selling", "date_classification", "date_drivers"]]
     .assign(mlflow_run_id=product_run, created_at=created_at), "ml_demand_forecast")

save(mfut[["date", "market", "stores", "tickets_forecast", "tickets_lower", "tickets_upper", "net_sales_forecast",
           "net_sales_lower", "net_sales_upper"]].rename(columns={"date": "forecast_date"})
     .assign(mlflow_run_id=market_run, created_at=created_at), "ml_market_forecast")

save(strong, "ml_strong_dates")

backtest = pd.concat([
    te[["date", "product_key", "y", "forecast", "baseline"]].rename(columns={"product_key": "series_key", "y": "actual"})
      .assign(series_type="product_units"),
    *[mtest[["date", "market", t, f"{t}_fc", f"{t}_base"]].rename(columns={"market": "series_key", t: "actual",
       f"{t}_fc": "forecast", f"{t}_base": "baseline"}).assign(series_type=t) for t in ["tickets_per_store", "net_sales_per_store"]],
], ignore_index=True)
save(backtest.assign(created_at=created_at), "ml_forecast_backtest")

metrics = pd.concat([
    p_metrics.rename(columns={"product_key": "series_key", "bias_selected": "bias_model"}).assign(series_type="product_units")
      [["series_type", "series_key", "product_name", "category", "product_role", "method", "wape_selected", "wape_model",
        "wape_baseline", "wape_blend", "bias_model", "days"]],
    m_metrics.assign(product_name=None, category=None, product_role=None, method="model",
                     wape_selected=m_metrics["wape_model"], wape_blend=np.nan),
], ignore_index=True)
save(metrics.assign(backtest_start=str(BACKTEST_START.date()), created_at=created_at), "ml_forecast_metrics")
save(pd.concat([p_importance, *m_importance], ignore_index=True).assign(created_at=created_at), "ml_feature_importance")
save(sens.assign(mlflow_run_id=product_run, created_at=created_at), "ml_price_sensitivity")

display(strong[(strong.scope == "Chain") & (strong.source == "forecast") & (strong.classification != "Normal")]
        .sort_values("tickets_uplift", ascending=False))
