# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality gate
# MAGIC
# MAGIC Runs after every pipeline update. Each check has a severity:
# MAGIC
# MAGIC * **error**: the task fails, the job stops before the ML tasks and the on-failure alert fires.
# MAGIC * **warn**: logged only.
# MAGIC
# MAGIC Every result is appended to `dq_gate_results`, so the history of the checks can be charted.

# COMMAND ----------

from datetime import datetime, timezone

import pyspark.sql.functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "taco_bueno")
dbutils.widgets.text("max_staleness_days", "3")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
max_staleness = int(dbutils.widgets.get("max_staleness_days"))
fq = f"{catalog}.{schema}"
run_ts = datetime.now(timezone.utc)

# COMMAND ----------

def scalar(sql: str):
    row = spark.sql(sql).first()
    return None if row is None else row[0]


latest_day = scalar(f"SELECT max(business_date) FROM {fq}.mv_data_quality_daily WHERE tickets > 0")
window = f"business_date > date_sub(DATE'{latest_day}', 7)"

checks = []


def check(name, severity, value, threshold, passed, detail=""):
    checks.append({
        "check_name": name, "severity": severity,
        "value": None if value is None else float(value), "threshold": str(threshold),
        "status": "PASS" if passed else ("FAIL" if severity == "error" else "WARN"), "detail": detail,
    })


# 1. Ticket headers reconcile with their lines (last 7 days of data).
rate = scalar(f"SELECT sum(reconciliation_rate * tickets) / sum(tickets) FROM {fq}.mv_data_quality_daily WHERE {window}")
check("reconciliation_rate_7d", "error", rate, ">= 0.995", rate is not None and rate >= 0.995)

# 2. Quarantined rows stay marginal.
q = spark.sql(f"SELECT sum(quarantined_rows) q, sum(raw_rows) r FROM {fq}.mv_data_quality_daily WHERE {window}").first()
share = (q.q or 0) / q.r if q.r else None
check("quarantine_share_7d", "error", share, "<= 0.005", share is not None and share <= 0.005)

# 3. One header per ticket (guards the ticket key against reused check numbers).
bad = scalar(f"SELECT coalesce(sum(tickets_with_inconsistent_header), 0) FROM {fq}.mv_data_quality_daily")
check("tickets_with_inconsistent_header", "error", bad, "= 0", bad == 0)

# 4. Latest day is complete and recent.
status = scalar(f"SELECT day_status FROM {fq}.mv_data_quality_daily WHERE business_date = DATE'{latest_day}'")
check("latest_day_complete", "warn", None, "Complete", status == "Complete", f"{latest_day}: {status}")
staleness = (run_ts.date() - latest_day).days if latest_day else None
check("data_freshness_days", "warn", staleness, f"<= {max_staleness}",
      staleness is not None and staleness <= max_staleness, f"latest business date {latest_day}")

# 5. Missing export days in the last 30 days.
missing = scalar(f"""SELECT count(*) FROM {fq}.mv_data_quality_daily
                     WHERE day_status = 'Missing file' AND business_date > date_sub(DATE'{latest_day}', 30)""")
check("missing_days_30d", "warn", missing, "= 0", missing == 0)

# 6. Every store with sales has coordinates (needed for the sports features).
no_loc = scalar(f"SELECT count(*) FROM {fq}.mv_dim_store WHERE has_sales AND latitude IS NULL")
check("stores_without_location", "warn", no_loc, "= 0", no_loc == 0,
      "add the missing stores to the business city file")

# 7. Sports calendar is current.
fetched = scalar(f"SELECT max(fetched_ts) FROM {fq}.st_sports_events")
age_h = (run_ts - fetched.replace(tzinfo=timezone.utc)).total_seconds() / 3600 if fetched else None
check("sports_events_age_hours", "warn", age_h, "<= 48", age_h is not None and age_h <= 48)

# 8. Expectations of the last pipeline update (from the event log).
try:
    failed = spark.sql(f"""
        WITH last_update AS (
          SELECT origin.update_id AS update_id FROM {fq}.pipeline_event_log
          WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1
        ),
        exp AS (
          SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                         'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
          FROM {fq}.pipeline_event_log
          WHERE event_type = 'flow_progress' AND origin.update_id = (SELECT update_id FROM last_update)
        )
        SELECT e.dataset, e.name, sum(e.failed_records) AS failed, sum(e.passed_records) AS passed
        FROM exp GROUP BY 1, 2 HAVING sum(e.failed_records) > 0 ORDER BY failed DESC
    """).collect()
    detail = "; ".join(f"{r.dataset}.{r.name}: {r.failed:,} failed" for r in failed[:10])
    check("expectations_with_failures_last_update", "warn", len(failed), "= 0", len(failed) == 0, detail)
except Exception as e:  # the event log table appears after the first update
    check("expectations_with_failures_last_update", "warn", None, "= 0", False, f"event log not readable: {e}")

# COMMAND ----------

results = spark.createDataFrame(checks).withColumn("run_ts", F.lit(run_ts)).withColumn("latest_business_date", F.lit(latest_day))
results.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{fq}.dq_gate_results")
display(results)

errors = [c for c in checks if c["status"] == "FAIL"]
if errors:
    raise RuntimeError("Data quality gate failed: " + ", ".join(f"{c['check_name']}={c['value']}" for c in errors))
print(f"Data quality gate passed ({sum(c['status'] == 'WARN' for c in checks)} warnings).")
