# Databricks notebook source
# MAGIC %md
# MAGIC # Store and product segmentation (k-means)
# MAGIC
# MAGIC **Stores**: clusters stores by how they sell, not just how much: volume, ticket size, channel and
# MAGIC daypart mix, weekend dependence, product mix, sensitivity to regional game days and growth.
# MAGIC The segments feed the forecasts and give operations/pricing a shared language
# MAGIC ("delivery-heavy", "game-day responsive"...).
# MAGIC
# MAGIC **Products**: clusters menu items and bundles by demand pattern (weekday profile, volatility,
# MAGIC trend, combo-component share), which tells planners which items are steady staples and which
# MAGIC swing with weekends, promotions or seasons.
# MAGIC
# MAGIC Inputs: `mv_daily_store_sales`, `mv_daily_product_demand`, `st_orders_cleaned`, `mv_dim_product`.
# MAGIC Outputs: `ml_store_segments`, `ml_store_segment_profiles`, `ml_product_segments`, plus MLflow runs
# MAGIC and registered models `store_segmentation` and `product_segmentation`.

# COMMAND ----------

# MAGIC %pip install -q "scikit-learn==1.5.2"
# MAGIC %restart_python

# COMMAND ----------

from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "taco_bueno")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
fq = f"{catalog}.{schema}"
scored_at = datetime.now(timezone.utc)

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/taco_bueno_segmentation")

# COMMAND ----------

# MAGIC %md ## 1. Store features

# COMMAND ----------

store_features = spark.sql(f"""
WITH d AS (
  SELECT * FROM {fq}.mv_daily_store_sales
  WHERE store_status IN ('Active', 'Opened during period')
),
base AS (
  SELECT
    store_id,
    any_value(market) AS market, any_value(city) AS city, any_value(state) AS state,
    count(*)                                                     AS days,
    avg(net_sales)                                               AS avg_daily_net_sales,
    avg(tickets)                                                 AS avg_daily_tickets,
    sum(net_sales) / sum(tickets)                                AS avg_ticket,
    sum(units) / sum(tickets)                                    AS units_per_ticket,
    sum(discounts) / sum(gross_sales)                            AS discount_rate,
    sum(net_sales_lunch) / sum(net_sales)                        AS lunch_share,
    sum(net_sales_dinner) / sum(net_sales)                       AS dinner_share,
    sum(net_sales_late_night) / sum(net_sales)                   AS late_night_share,
    avg(CASE WHEN is_weekend THEN net_sales END)
      / avg(CASE WHEN NOT is_weekend THEN net_sales END)         AS weekend_index
  FROM d GROUP BY store_id
),
-- Channel mix only on new-POS days: the legacy POS did not record delivery separately.
channels AS (
  SELECT store_id,
    sum(net_sales_drive_thru) / sum(net_sales) AS drive_thru_share,
    sum(net_sales_delivery) / sum(net_sales)   AS delivery_share
  FROM d WHERE pos_system = 'New' GROUP BY store_id
),
events AS (
  SELECT store_id,
    avg(CASE WHEN regional_team_games > 0 THEN net_sales_lift_vs_baseline END)
      - avg(CASE WHEN regional_team_games = 0 THEN net_sales_lift_vs_baseline END) AS game_day_sensitivity
  FROM d WHERE holiday_name IS NULL AND baseline_net_sales_4w IS NOT NULL GROUP BY store_id
),
growth AS (
  SELECT store_id,
    avg(CASE WHEN business_date BETWEEN DATE'2026-02-01' AND DATE'2026-09-30' THEN net_sales END)
      / avg(CASE WHEN business_date BETWEEN DATE'2025-02-01' AND DATE'2025-09-30' THEN net_sales END) - 1 AS yoy_growth
  FROM d GROUP BY store_id
),
mix AS (
  SELECT o.store_id,
    sum(CASE WHEN p.category = 'Combos & Platters' THEN o.line_amount ELSE 0 END) / sum(o.line_amount) AS combo_share,
    sum(CASE WHEN p.category = 'Value menu' THEN o.line_amount ELSE 0 END) / sum(o.line_amount)        AS value_menu_share
  FROM {fq}.st_orders_cleaned o JOIN {fq}.mv_dim_product p USING (product_key)
  WHERE o.is_sale_line GROUP BY o.store_id
)
SELECT * FROM base
LEFT JOIN channels USING (store_id)
LEFT JOIN events USING (store_id)
LEFT JOIN growth USING (store_id)
LEFT JOIN mix USING (store_id)
WHERE days >= 60
""").toPandas()

FEATURES = ["avg_daily_net_sales", "avg_ticket", "units_per_ticket", "discount_rate", "drive_thru_share",
            "delivery_share", "lunch_share", "dinner_share", "late_night_share", "weekend_index",
            "game_day_sensitivity", "yoy_growth", "combo_share", "value_menu_share"]
X = store_features[FEATURES].astype(float)
X = X.fillna(X.median())          # e.g. yoy_growth is missing for stores opened in 2025-26
print(f"{len(X)} stores x {len(FEATURES)} features")
display(store_features.describe().T)

# COMMAND ----------

# MAGIC %md ## 2. Choose k by silhouette and fit

# COMMAND ----------

def choose_k(X: pd.DataFrame, k_range=range(3, 8), seed=42):
    Z = StandardScaler().fit_transform(X)
    scores = {k: silhouette_score(Z, KMeans(k, n_init=20, random_state=seed).fit_predict(Z)) for k in k_range}
    best = max(scores.values())
    # Smallest k within 0.02 of the best score: easier to explain, nearly as separated.
    return min(k for k, s in scores.items() if s >= best - 0.02), scores


def name_segments(centers_z: pd.DataFrame, names: dict) -> dict:
    """Readable names from the most distinctive standardized centroid features."""
    labels = {}
    for seg, row in centers_z.iterrows():
        tier = "High-volume" if row["avg_daily_net_sales"] > 0.5 else "Low-volume" if row["avg_daily_net_sales"] < -0.5 else "Mid-volume"
        traits = row.drop("avg_daily_net_sales").abs().sort_values(ascending=False).index[:2]
        words = [names[f][0] if row[f] > 0 else names[f][1] for f in traits]
        labels[seg] = f"{tier} · {', '.join(words)}"
    return labels


STORE_TRAIT_NAMES = {
    "avg_ticket": ("bigger baskets", "smaller baskets"), "units_per_ticket": ("more items per order", "fewer items per order"),
    "discount_rate": ("promo-heavy", "few discounts"), "drive_thru_share": ("drive-thru led", "low drive-thru"),
    "delivery_share": ("delivery-heavy", "little delivery"), "lunch_share": ("lunch-driven", "light lunch"),
    "dinner_share": ("dinner-driven", "light dinner"), "late_night_share": ("late-night", "early-closing profile"),
    "weekend_index": ("weekend-driven", "weekday-driven"), "game_day_sensitivity": ("game-day responsive", "game-day neutral"),
    "yoy_growth": ("growing", "declining"), "combo_share": ("combo-heavy", "few combos"),
    "value_menu_share": ("value-menu heavy", "premium mix"),
}

k, sil = choose_k(X)
model = Pipeline([("scale", StandardScaler()), ("kmeans", KMeans(k, n_init=50, random_state=42))])
labels = model.fit_predict(X)
Z = model.named_steps["scale"].transform(X)
pca = PCA(2, random_state=42).fit(Z)
centers_z = pd.DataFrame(model.named_steps["kmeans"].cluster_centers_, columns=FEATURES)
segment_names = name_segments(centers_z, STORE_TRAIT_NAMES)
print("silhouette by k:", {kk: round(v, 3) for kk, v in sil.items()}, "-> k =", k)
print(segment_names)

# COMMAND ----------

with mlflow.start_run(run_name="store_segmentation") as run:
    mlflow.log_params({"k": k, "features": ",".join(FEATURES), "n_stores": len(X)})
    mlflow.log_metrics({f"silhouette_k{kk}": v for kk, v in sil.items()} | {"silhouette": sil[k]})
    mlflow.log_dict(segment_names, "segment_names.json")
    info = mlflow.sklearn.log_model(model, "model", input_example=X.head(3), signature=infer_signature(X, labels))
    try:
        mlflow.register_model(info.model_uri, f"{fq}.store_segmentation")
    except Exception as e:  # registry is optional for scoring
        print(f"Model registry skipped: {e}")
    run_id = run.info.run_id

out = store_features[["store_id", "market", "city", "state"] + FEATURES].copy()
out["segment_id"] = labels
out["segment_name"] = out["segment_id"].map(segment_names)
out[["pca_1", "pca_2"]] = pca.transform(Z)
out["mlflow_run_id"], out["scored_at"] = run_id, scored_at
spark.createDataFrame(out).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.ml_store_segments")

profiles = out.groupby(["segment_id", "segment_name"])[FEATURES].mean().reset_index()
profiles["stores"] = out.groupby("segment_id").size().values
profiles["scored_at"] = scored_at
spark.createDataFrame(profiles).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.ml_store_segment_profiles")
display(profiles)

# COMMAND ----------

# MAGIC %md ## 3. Product demand-pattern clusters

# COMMAND ----------

daily = spark.sql(f"""
  SELECT business_date, product_key, any_value(product_name) AS product_name, any_value(category) AS category,
         any_value(product_role) AS product_role, sum(units) AS units, sum(priced_units) AS priced_units,
         sum(sales_amount) AS sales_amount
  FROM {fq}.mv_daily_product_demand
  WHERE product_role IN ('Menu item', 'Bundle')
  GROUP BY business_date, product_key
""").toPandas()
daily["business_date"] = pd.to_datetime(daily["business_date"])

totals = daily.groupby("product_key").agg(units=("units", "sum"), days=("business_date", "nunique"))
keep = totals[(totals.units >= 5000) & (totals.days >= 120)].index
d = daily[daily.product_key.isin(keep)].copy()
d["dow"] = d.business_date.dt.dayofweek
last = d.business_date.max()

def product_features(g: pd.DataFrame) -> pd.Series:
    by_dow = g.groupby("dow")["units"].mean()
    prof = (by_dow / by_dow.mean()).reindex(range(7)).fillna(1.0)
    recent = g[g.business_date > last - pd.Timedelta(days=90)]["units"].mean()
    prior = g[(g.business_date <= last - pd.Timedelta(days=90)) & (g.business_date > last - pd.Timedelta(days=180))]["units"].mean()
    monthly = g.set_index("business_date")["units"].resample("MS").mean()
    return pd.Series({
        **{f"dow_{i}": prof[i] for i in range(7)},
        "weekend_lift": prof[[5, 6]].mean() - prof[[0, 1, 2, 3]].mean(),
        "daily_cv": g["units"].std() / g["units"].mean(),
        "monthly_cv": monthly.std() / monthly.mean() if len(monthly) > 2 else 0.0,
        "trend_90d": (recent / prior - 1) if prior and prior > 0 else 0.0,
        "component_share": 1 - g["priced_units"].sum() / max(g["units"].sum(), 1),
        "log_volume": np.log1p(g["units"].mean()),
    })

pf = d.groupby("product_key").apply(product_features, include_groups=False)
meta = d.groupby("product_key")[["product_name", "category", "product_role"]].first()
P_FEATURES = list(pf.columns)
P = pf.fillna(0.0)

PRODUCT_TRAIT_NAMES = {
    "weekend_lift": ("weekend peaks", "weekday staple"), "daily_cv": ("volatile", "steady"),
    "monthly_cv": ("seasonal", "flat across months"), "trend_90d": ("rising", "fading"),
    "component_share": ("mostly inside combos", "sold on its own"), "log_volume": ("high volume", "niche"),
    **{f"dow_{i}": (f"{'Mon Tue Wed Thu Fri Sat Sun'.split()[i]} peak", f"soft {'Mon Tue Wed Thu Fri Sat Sun'.split()[i]}") for i in range(7)},
}
pk, psil = choose_k(P, range(3, 7))
pmodel = Pipeline([("scale", StandardScaler()), ("kmeans", KMeans(pk, n_init=50, random_state=42))])
plabels = pmodel.fit_predict(P)
pcenters = pd.DataFrame(pmodel.named_steps["kmeans"].cluster_centers_, columns=P_FEATURES)
pnames = {}
for seg, row in pcenters.iterrows():
    traits = row.abs().sort_values(ascending=False).index[:2]
    pnames[seg] = ", ".join(PRODUCT_TRAIT_NAMES[f][0] if row[f] > 0 else PRODUCT_TRAIT_NAMES[f][1] for f in traits).capitalize()
print("product silhouette by k:", {kk: round(v, 3) for kk, v in psil.items()}, "-> k =", pk, pnames)

with mlflow.start_run(run_name="product_segmentation") as run:
    mlflow.log_params({"k": pk, "features": ",".join(P_FEATURES), "n_products": len(P)})
    mlflow.log_metric("silhouette", psil[pk])
    info = mlflow.sklearn.log_model(pmodel, "model", input_example=P.head(3), signature=infer_signature(P, plabels))
    try:
        mlflow.register_model(info.model_uri, f"{fq}.product_segmentation")
    except Exception as e:
        print(f"Model registry skipped: {e}")
    prun = run.info.run_id

pout = meta.join(pf).reset_index()
pout["segment_id"] = plabels
pout["segment_name"] = pout["segment_id"].map(pnames)
pout["units_total"] = pout["product_key"].map(totals["units"])
pout["mlflow_run_id"], pout["scored_at"] = prun, scored_at
spark.createDataFrame(pout).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.ml_product_segments")
display(pout.groupby(["segment_id", "segment_name"]).agg(products=("product_key", "count"), units=("units_total", "sum")).reset_index())
