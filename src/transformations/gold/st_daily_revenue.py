"""Gold: streaming daily revenue per store and channel (append-only).

A stateful streaming aggregation over st_orders_cleaned with an event-time
watermark. A day is finalized and appended once the watermark passes it, which
happens when the following days' exports arrive (watermark delay = 1 day). The
result is an immutable daily ledger: once a day is written it never changes,
which suits downstream consumers that read incrementally (CDC, alerts, exports).

Trade-offs, documented on purpose:
* Latency: the most recent business day stays in state until a later file
  arrives. mv_daily_store_sales always has every day, including the latest.
* Late data: an export older than the watermark is ignored here (it is still in
  the materialized views). Late days are visible in mv_data_quality_daily.
* Amounts are line-level gross item sales (before ticket discount and tax),
  the measure that can be aggregated row by row in a stream; ticket-level net
  sales live in the materialized views. Tickets are an HLL approximation
  because exact COUNT DISTINCT is not supported in streaming aggregations.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()


@dp.table(
    name="st_daily_revenue",
    comment="Gold streaming ledger. Store x business day x channel: gross item sales, units, lines and "
            "approximate tickets, appended once the event-time watermark (1 day) passes the day.",
    cluster_by=["business_date", "store_id"],
    table_properties={"quality": "gold"},
)
@dp.expect("positive_lines", "lines > 0")
def st_daily_revenue():
    return (
        spark.readStream.table("st_orders_cleaned")
        .withColumn("event_ts", F.to_timestamp("business_date"))
        .withWatermark("event_ts", "1 day")
        .groupBy(
            F.window("event_ts", "1 day").alias("day_window"),
            "business_date",
            "store_id",
            "channel_group",
        )
        .agg(
            F.sum("line_amount").alias("gross_item_sales"),
            F.sum("units").alias("units"),
            F.count(F.lit(1)).alias("lines"),
            F.approx_count_distinct("ticket_key", 0.01).alias("tickets_approx"),
            F.max("ingested_at").alias("last_ingested_at"),
        )
        .select(
            "business_date",
            "store_id",
            "channel_group",
            F.round("gross_item_sales", 2).alias("gross_item_sales"),
            "units",
            "lines",
            "tickets_approx",
            "last_ingested_at",
            F.current_timestamp().alias("finalized_at"),
        )
    )
