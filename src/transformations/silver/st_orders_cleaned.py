"""Silver: typed, harmonized POS order lines (streaming).

What happens here
* Types: dates, times and amounts are parsed from the raw strings with try_* so a
  bad value becomes NULL and is caught by an expectation instead of failing.
* Harmonization of the two POS systems found in the data:
    Legacy (pop in To Go / Inside / 3): menu_price is a unit price, quantity is
      real, 'Discount' rows are sales of the item and 'Void' rows are not.
    New (any other pop): one row per unit; item_voided carries the price of a
      voided line.
  Both are reduced to the same line measures: units and line_amount (gross
  item sales before ticket discounts and tax).
* Ticket key: business_date + store + check number + check start time. Check
  numbers repeat across stores, and the legacy POS reused 72 numbers on the
  same day in the same store; the start time separates those sales.
* Data quality: rows failing a blocking rule are dropped here and kept, with
  the failed rule names, in st_orders_quarantine. Non-blocking rules are
  tracked as warnings in the pipeline event log.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()

LEGACY_POP = "('To Go', 'Inside', '3')"

# Blocking rules: a row that fails any of these cannot be attributed to a ticket.
DROP_RULES = {
    "valid_business_date": "business_date IS NOT NULL",
    "valid_store_id": "store_id RLIKE '^[0-9]+$'",
    "has_check_number": "check_number IS NOT NULL AND trim(check_number) <> ''",
    "amounts_parse": "amounts_parse_ok",
}

# Warning rules: kept in the table, counted in the event log.
WARN_RULES = {
    "business_date_is_day_before_file": "datediff(file_date, business_date) = 1",
    "known_transaction_type": "transaction_type IN ('Regular Sale', 'Discount', 'Void')",
    "known_channel": "channel_group <> 'Unknown'",
    "quantity_not_negative": "coalesce(quantity, 0) >= 0",
    "menu_price_plausible": "menu_price IS NULL OR menu_price BETWEEN -100 AND 500",
    "begin_time_parses": "begin_minute IS NOT NULL",
    "named_product_or_modifier": "product_name_raw IS NOT NULL OR pos_system = 'New'",
}

NUMERIC_RAW = ["total", "tax", "total_discount", "quantity", "menu_price", "item_discount", "item_voided"]

PARSED = [
    "source_file",
    "ingested_at",
    "to_date(try_to_timestamp(regexp_extract(source_file, '([0-9]{2}-[0-9]{2}-[0-9]{4})', 1), 'MM-dd-yyyy')) AS file_date",
    "to_date(try_to_timestamp(date, 'M/d/yyyy')) AS business_date",
    "store_num AS store_id",
    "check_number",
    "begin_time AS check_begin_time",
    "end_time AS check_end_time",
    "hour(try_to_timestamp(begin_time, 'h:mm a')) * 60 + minute(try_to_timestamp(begin_time, 'h:mm a')) AS begin_minute",
    "hour(try_to_timestamp(end_time, 'h:mm a')) * 60 + minute(try_to_timestamp(end_time, 'h:mm a')) AS end_minute",
    "pop AS channel_raw",
    "transaction_type",
    "menu_item AS product_name_raw",
    "pos_code",
    "report_group",
    "tender_type",
    "try_cast(total AS DOUBLE) AS check_total",
    "try_cast(tax AS DOUBLE) AS check_tax",
    "try_cast(total_discount AS DOUBLE) AS check_discount",
    "try_cast(quantity AS DOUBLE) AS quantity",
    "try_cast(menu_price AS DOUBLE) AS menu_price",
    "try_cast(item_discount AS DOUBLE) AS item_discount",
    "try_cast(item_voided AS DOUBLE) AS item_voided",
    "try_cast(total_rounding AS DOUBLE) AS total_rounding",
    " AND ".join(f"({c} IS NULL OR try_cast({c} AS DOUBLE) IS NOT NULL)" for c in NUMERIC_RAW) + " AS amounts_parse_ok",
] + [f"{c} AS raw_{c}" for c in ["date", "store_num", "check_number", "total", "tax", "total_discount",
                                  "quantity", "menu_price", "item_voided", "begin_time", "pop"]]

DERIVED = [
    "*",
    f"CASE WHEN channel_raw IN {LEGACY_POP} THEN 'Legacy' ELSE 'New' END AS pos_system",
    # Legacy channel codes mapped by behavior: 'To Go' matches drive-thru (share and
    # ticket size); 'Inside' and '3' behave like counter orders (inferred mapping).
    """CASE
         WHEN channel_raw IN ('Drive Thru', 'To Go') THEN 'Drive-thru'
         WHEN channel_raw IN ('Dine-In', 'Carry Out', 'Inside', '3') THEN 'In-store'
         WHEN channel_raw LIKE 'Online%' OR channel_raw = 'Mobile' THEN 'Delivery & digital'
         ELSE 'Unknown'
       END AS channel_group""",
    "concat_ws('|', date_format(business_date, 'yyyyMMdd'), store_id, check_number, coalesce(check_begin_time, '')) AS ticket_key",
    "coalesce(product_name_raw, '(unnamed modifier)') AS product_name",
    # Same product across POS systems and menu versions: drop combo/platter context
    # suffixes ('(combo)', '(platter)', '(Wholotta Box)', '(CB)') and year suffixes ('2025').
    """lower(trim(regexp_replace(
         regexp_replace(regexp_replace(coalesce(product_name_raw, '(unnamed modifier)'), ' +', ' '),
                        '(?i) *[(](combo|platter|wholotta box|cb)[)] *$', ''),
         ' +20[0-9]{2}$', ''))) AS product_key""",
    """CASE
         WHEN begin_minute IS NULL THEN 'Unknown'
         WHEN begin_minute < 300 THEN 'Late night'
         WHEN begin_minute < 630 THEN 'Breakfast'
         WHEN begin_minute < 840 THEN 'Lunch'
         WHEN begin_minute < 1020 THEN 'Afternoon'
         WHEN begin_minute < 1260 THEN 'Dinner'
         ELSE 'Late night'
       END AS daypart""",
]

MEASURES = [
    "*",
    "CASE WHEN is_sale_line THEN coalesce(quantity, 0) ELSE 0 END AS units",
    """CASE WHEN pos_system = 'Legacy'
            THEN CASE WHEN is_sale_line THEN coalesce(menu_price, 0) * coalesce(quantity, 0) ELSE 0 END
            ELSE coalesce(menu_price, 0) * coalesce(quantity, 0) - coalesce(item_voided, 0)
       END AS line_amount""",
    "is_sale_line AND coalesce(menu_price, 0) > 0 AS is_priced_sale",
]


def _failed_rules(rules: dict) -> F.Column:
    checks = ", ".join(f"CASE WHEN NOT coalesce(({cond}), false) THEN '{name}' END" for name, cond in rules.items())
    return F.expr(f"filter(array({checks}), x -> x IS NOT NULL)")


@dp.temporary_view(name="v_orders_parsed", comment="Bronze lines parsed and harmonized, before quality routing.")
def v_orders_parsed():
    return (
        spark.readStream.table("st_pos_lines_raw")
        .selectExpr(*PARSED)
        .selectExpr(*DERIVED)
        .withColumn(
            "is_sale_line",
            F.expr(f"""CASE WHEN pos_system = 'Legacy'
                            THEN coalesce(transaction_type IN ('Regular Sale', 'Discount'), false)
                            ELSE coalesce(item_voided, 0) <= 0 END"""),
        )
        .selectExpr(*MEASURES)
        .withColumn("dq_failed_rules", _failed_rules(DROP_RULES))
    )


CLEAN_COLUMNS = [
    "business_date", "file_date", "store_id", "check_number", "check_begin_time", "check_end_time",
    "begin_minute", "end_minute", "ticket_key", "pos_system", "channel_raw", "channel_group", "daypart",
    "transaction_type", "product_name_raw", "product_name", "product_key", "pos_code", "report_group",
    "quantity", "menu_price", "item_discount", "item_voided", "is_sale_line", "is_priced_sale", "units",
    "line_amount", "check_total", "check_tax", "check_discount", "total_rounding", "tender_type",
    "source_file", "ingested_at", "amounts_parse_ok",
]


@dp.table(
    name="st_orders_cleaned",
    comment="Silver. One row per valid POS order line with types, harmonized POS semantics, channel, "
            "daypart, product key and ticket key. Invalid rows are routed to st_orders_quarantine.",
    cluster_by=["business_date", "store_id"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(DROP_RULES)
@dp.expect_all(WARN_RULES)
def st_orders_cleaned():
    return spark.readStream.table("v_orders_parsed").select(*CLEAN_COLUMNS)
