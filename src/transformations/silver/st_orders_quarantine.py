"""Silver: POS lines that failed a blocking quality rule (quarantine).

Same stream as st_orders_cleaned, opposite filter. Keeps the raw strings and
the list of failed rules so the record can be investigated and replayed.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()


@dp.table(
    name="st_orders_quarantine",
    comment="Silver quarantine. POS lines that failed a blocking rule in st_orders_cleaned, with the raw "
            "values and the names of the failed rules (dq_failed_rules).",
    table_properties={"quality": "quarantine"},
)
def st_orders_quarantine():
    return (
        spark.readStream.table("v_orders_parsed")
        .where("size(dq_failed_rules) > 0")
        .selectExpr(
            "dq_failed_rules",
            "business_date", "file_date", "store_id", "check_number", "product_name_raw",
            "raw_date", "raw_store_num", "raw_check_number", "raw_total", "raw_tax", "raw_total_discount",
            "raw_quantity", "raw_menu_price", "raw_item_voided", "raw_begin_time", "raw_pop",
            "source_file", "ingested_at",
        )
    )
