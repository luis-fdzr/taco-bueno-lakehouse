"""Generates the AI/BI dashboard definition (src/dashboards/taco_bueno_overview.lvdash.json).

The dashboard is deployed by the bundle (resources/dashboards/). Dataset queries use
unqualified table names; the bundle sets the catalog and schema per target
(`dataset_catalog` / `dataset_schema`), so the same file works in dev and prod.

    python tools/dashboard/build_dashboard.py
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[2] / "src" / "dashboards" / "taco_bueno_overview.lvdash.json"
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]

# --------------------------------------------------------------------------- #
# Datasets (one SQL query each; table names resolve to the bundle schema)
# --------------------------------------------------------------------------- #
DATASETS = {
    "kpi_totals": """
SELECT sum(net_sales) AS net_sales, sum(tickets) AS tickets, sum(net_sales) / sum(tickets) AS avg_ticket,
       count(DISTINCT store_id) AS stores, count(DISTINCT business_date) AS days,
       sum(net_sales) / count(*) AS net_sales_per_store_day
FROM mv_daily_store_sales""",
    "kpis_monthly": """
SELECT month, net_sales_per_store_day, tickets_per_store_day, avg_ticket
FROM mv_executive_kpis""",
    "yoy_long": """
SELECT month, 'Net sales per store-day' AS metric, round(net_sales_per_store_day_yoy * 100, 1) AS yoy_pct FROM mv_executive_kpis WHERE yoy_comparable
UNION ALL SELECT month, 'Tickets per store-day', round(tickets_per_store_day_yoy * 100, 1) FROM mv_executive_kpis WHERE yoy_comparable
UNION ALL SELECT month, 'Average ticket', round(avg_ticket_yoy * 100, 1) FROM mv_executive_kpis WHERE yoy_comparable""",
    "channel_month": """
SELECT month, 'Drive-thru' AS channel, round(drive_thru_share * 100, 1) AS share_pct FROM mv_executive_kpis WHERE new_pos_share > 0.99
UNION ALL SELECT month, 'In-store', round(in_store_share * 100, 1) FROM mv_executive_kpis WHERE new_pos_share > 0.99
UNION ALL SELECT month, 'Delivery & digital', round(delivery_share * 100, 1) FROM mv_executive_kpis WHERE new_pos_share > 0.99""",
    "markets": """
SELECT market, count(DISTINCT store_id) AS stores, round(avg(net_sales), 0) AS net_sales_per_store_day,
       round(sum(net_sales) / sum(tickets), 2) AS avg_ticket, round(sum(net_sales_delivery) / sum(net_sales) * 100, 1) AS delivery_pct
FROM mv_daily_store_sales WHERE pos_system = 'New' GROUP BY market""",
    "weekday": """
SELECT concat(CAST((day_of_week + 5) % 7 + 1 AS STRING), '. ', substr(day_name, 1, 3)) AS weekday,
       round(avg(net_sales), 0) AS net_sales_per_store, round(avg(tickets), 1) AS tickets_per_store
FROM mv_daily_store_sales GROUP BY 1""",
    "daypart": """
SELECT '1. Breakfast' AS daypart, round(sum(net_sales_breakfast) / sum(net_sales) * 100, 1) AS share_pct FROM mv_daily_store_sales
UNION ALL SELECT '2. Lunch', round(sum(net_sales_lunch) / sum(net_sales) * 100, 1) FROM mv_daily_store_sales
UNION ALL SELECT '3. Afternoon', round(sum(net_sales_afternoon) / sum(net_sales) * 100, 1) FROM mv_daily_store_sales
UNION ALL SELECT '4. Dinner', round(sum(net_sales_dinner) / sum(net_sales) * 100, 1) FROM mv_daily_store_sales
UNION ALL SELECT '5. Late night', round(sum(net_sales_late_night) / sum(net_sales) * 100, 1) FROM mv_daily_store_sales""",
    "holidays": """
SELECT holiday_name, round((sum(net_sales) / sum(baseline_net_sales_4w) - 1) * 100, 1) AS lift_pct,
       count(DISTINCT business_date) AS dates
FROM mv_daily_store_sales
WHERE holiday_name IS NOT NULL AND baseline_net_sales_4w IS NOT NULL
GROUP BY holiday_name""",
    "sports_all": """
SELECT exposure_type, round(incremental_lift_vs_no_game * 100, 2) AS lift_pct, store_days
FROM mv_event_impact
WHERE market = 'All markets' AND day_type = 'All days' AND exposure_type <> 'No relevant game'""",
    "sports_split": """
SELECT exposure_type AS `Exposure`,
       round(max(CASE WHEN day_type = 'Weekday' THEN incremental_lift_vs_no_game END) * 100, 1) AS `Weekday lift %`,
       round(max(CASE WHEN day_type = 'Weekend' THEN incremental_lift_vs_no_game END) * 100, 1) AS `Weekend lift %`,
       max(CASE WHEN day_type = 'All days' THEN store_days END) AS `Store-days`
FROM mv_event_impact
WHERE market = 'All markets' AND exposure_type <> 'No relevant game'
GROUP BY exposure_type
ORDER BY `Weekend lift %`""",
    "forecast_totals": """
SELECT sum(tickets_forecast) AS tickets, sum(net_sales_forecast) AS net_sales
FROM ml_market_forecast""",
    "chain_actual_forecast": """
SELECT date, CASE source WHEN 'actual' THEN 'Actual' ELSE 'Forecast' END AS series, tickets
FROM ml_strong_dates
WHERE scope = 'Chain' AND date >= DATE'2026-05-01'""",
    "strong_dates": """
SELECT date_format(date, 'yyyy-MM-dd (EEE)') AS `Date`, classification AS `Class`,
       round(tickets_uplift * 100, 1) AS `Expected vs baseline %`, round(tickets, 0) AS `Expected tickets`,
       coalesce(drivers, '') AS `Coinciding factors`
FROM ml_strong_dates
WHERE scope = 'Chain' AND source = 'forecast' AND classification <> 'Normal'
ORDER BY date""",
    "product_forecast": """
WITH top AS (
  SELECT product_key, any_value(product_name) AS product
  FROM ml_demand_forecast GROUP BY product_key ORDER BY sum(units_forecast) DESC LIMIT 6
)
SELECT d.business_date AS date, t.product, sum(d.priced_units) AS units
FROM mv_daily_product_demand d JOIN top t USING (product_key)
WHERE d.business_date >= DATE'2026-08-01'
GROUP BY d.business_date, t.product
UNION ALL
SELECT f.forecast_date, t.product, f.units_forecast
FROM ml_demand_forecast f JOIN top t USING (product_key)""",
    "accuracy": """
SELECT CASE series_type WHEN 'product_units' THEN 'Products and combos (units)'
                        WHEN 'tickets_per_store' THEN 'Tickets per store, by market'
                        ELSE 'Net sales per store, by market' END AS `Forecast`,
       count(DISTINCT series_key) AS `Series`,
       round(sum(abs(actual - forecast)) / sum(actual) * 100, 1) AS `Forecast WAPE %`,
       round(sum(abs(actual - baseline)) / sum(actual) * 100, 1) AS `Seasonal naive WAPE %`
FROM ml_forecast_backtest
GROUP BY series_type""",
    "price_sensitivity": """
SELECT product_name AS `Product`, product_role AS `Type`, round(current_price, 2) AS `Current price`,
       round(units_change_pct * 100, 1) AS `Units change at +5% price, %`, round(implied_elasticity, 2) AS `Implied elasticity`
FROM ml_price_sensitivity
ORDER BY implied_elasticity""",
    "segments": """
SELECT segment_name AS `Segment`, stores AS `Stores`, round(avg_daily_net_sales, 0) AS `Net sales per day`,
       round(avg_ticket, 2) AS `Avg ticket`, round(delivery_share * 100, 1) AS `Delivery %`,
       round(drive_thru_share * 100, 1) AS `Drive-thru %`, round(lunch_share * 100, 1) AS `Lunch %`,
       round(weekend_index, 2) AS `Weekend index`, round(yoy_growth * 100, 1) AS `YoY growth %`
FROM ml_store_segment_profiles
ORDER BY avg_daily_net_sales DESC""",
    "segments_market": """
SELECT market, segment_name AS segment, count(*) AS stores
FROM ml_store_segments GROUP BY market, segment_name""",
    "top_products": """
SELECT product_name, round(sales_amount, 0) AS sales
FROM mv_dim_product
WHERE product_role IN ('Menu item', 'Bundle')
ORDER BY sales_amount DESC LIMIT 15""",
    "price_waves": """
SELECT month, count_if(price_change_pct > 0) AS increases
FROM mv_product_price_history
WHERE prev_modal_price IS NOT NULL
GROUP BY month""",
    "dq_totals": """
SELECT round(sum(reconciliation_rate * tickets) / sum(tickets) * 100, 2) AS reconciled_pct,
       sum(quarantined_rows) AS quarantined_rows,
       count_if(day_status = 'Missing file') AS missing_days,
       sum(reused_check_numbers) AS reused_check_numbers,
       (SELECT count(*) FROM mv_dim_store WHERE has_sales AND latitude IS NULL) AS stores_without_location
FROM mv_data_quality_daily""",
    "dq_daily": """
SELECT business_date, day_status,
       CASE WHEN day_status = 'Complete' THEN tickets ELSE 30000 END AS bar_height
FROM mv_data_quality_daily""",
    "dq_issues": """
SELECT date_format(business_date, 'yyyy-MM-dd (EEE)') AS `Business date`, day_status AS `Status`,
       coalesce(holiday_name, '') AS `Holiday`, files AS `Files`, tickets AS `Tickets`,
       stores_reporting AS `Stores reporting`, stores_expected AS `Stores expected`
FROM mv_data_quality_daily
WHERE day_status <> 'Complete'
ORDER BY business_date""",
    "expectations": """
WITH exp AS (
  SELECT origin.update_id AS update_id, timestamp,
         explode(from_json(details:flow_progress.data_quality.expectations,
                 'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM pipeline_event_log
  WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
),
per_update AS (
  SELECT e.dataset AS dataset, e.name AS expectation, update_id, max(timestamp) AS ts,
         sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
  FROM exp GROUP BY e.dataset, e.name, update_id
),
latest AS (
  SELECT *, row_number() OVER (PARTITION BY dataset, expectation ORDER BY ts DESC) AS rn FROM per_update
)
SELECT regexp_extract(dataset, '[^.]+$', 0) AS `Table`, expectation AS `Expectation`,
       passed AS `Passed`, failed AS `Failed`, round(failed / nullif(passed + failed, 0) * 100, 3) AS `Failed %`
FROM latest WHERE rn = 1
ORDER BY failed DESC, `Table`""",
    "dq_gate": """
SELECT date_format(run_ts, 'yyyy-MM-dd HH:mm') AS `Run (UTC)`, check_name AS `Check`, severity AS `Severity`,
       status AS `Status`, round(value, 4) AS `Value`, threshold AS `Threshold`
FROM dq_gate_results
QUALIFY dense_rank() OVER (ORDER BY run_ts DESC) = 1
ORDER BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END, check_name""",
}


# --------------------------------------------------------------------------- #
# Widget helpers (formats follow dashboards exported from Databricks)
# --------------------------------------------------------------------------- #
def _id(*parts: str) -> str:
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:8]


def _field(name: str, expr: str | None = None) -> dict:
    return {"name": name, "expression": expr or f"`{name}`"}


def _query(ds: str, fields: list, disaggregated: bool) -> list:
    return [{"name": "main_query", "query": {"datasetName": _id("ds", ds), "fields": fields, "disaggregated": disaggregated}}]


def text(markdown: str) -> dict:
    return {"name": _id("text", markdown), "textbox_spec": markdown}


def counter(ds: str, column: str, title: str) -> dict:
    fname = f"sum({column})"
    return {
        "name": _id("counter", ds, column),
        "queries": _query(ds, [_field(fname, f"SUM(`{column}`)")], False),
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": fname, "displayName": title}},
                 "frame": {"showTitle": True, "title": title}},
    }


def chart(kind: str, ds: str, category: str, value: str, title: str, category_title: str, value_title: str,
          color: str | None = None, color_title: str = "", category_type: str = "temporal", horizontal: bool = False,
          description: str = "") -> dict:
    """Bar or line chart. `value` is summed per category (and color); datasets are pre-aggregated."""
    vname = f"sum({value})"
    fields = [_field(category), _field(vname, f"SUM(`{value}`)")]
    cat_enc = {"fieldName": category, "scale": {"type": category_type}, "displayName": category_title,
               "axis": {"title": category_title}}
    val_enc = {"fieldName": vname, "scale": {"type": "quantitative"}, "displayName": value_title,
               "axis": {"title": value_title}}
    enc = {"x": val_enc, "y": cat_enc} if horizontal else {"x": cat_enc, "y": val_enc}
    if color:
        fields.append(_field(color))
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color_title}
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update({"showDescription": True, "description": description})
    return {
        "name": _id(kind, ds, category, value, title),
        "queries": _query(ds, fields, False),
        "spec": {"version": 3, "widgetType": kind, "encodings": enc, "frame": frame,
                 "mark": {"colors": PALETTE}},
    }


_COLUMN_DEFAULTS = {
    "allowHTML": False, "allowSearch": False, "booleanValues": ["false", "true"], "highlightLinks": False,
    "imageHeight": "", "imageTitleTemplate": "{{ @ }}", "imageUrlTemplate": "{{ @ }}", "imageWidth": "",
    "linkOpenInNewTab": True, "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}",
    "linkUrlTemplate": "{{ @ }}", "preserveWhitespace": False, "useMonospaceFont": False, "visible": True,
}
_NUMBER_FORMATS = {"integer": "0,0", "float": "0,0.0", "money": "0,0.00"}


def table(ds: str, columns: list[tuple[str, str]], title: str, description: str = "") -> dict:
    """columns: (name, kind) with kind in string | integer | float | money."""
    cols = []
    for i, (name, kind) in enumerate(columns):
        numeric = kind != "string"
        col = {**_COLUMN_DEFAULTS, "fieldName": name, "displayName": name, "title": name, "order": 100000 + i,
               "type": "string" if not numeric else ("integer" if kind == "integer" else "float"),
               "displayAs": "number" if numeric else "string", "alignContent": "right" if numeric else "left"}
        if numeric:
            col["numberFormat"] = _NUMBER_FORMATS[kind]
        cols.append(col)
    frame = {"showTitle": True, "title": title}
    if description:
        frame.update({"showDescription": True, "description": description})
    return {
        "name": _id("table", ds, title),
        "queries": _query(ds, [_field(name) for name, _ in columns], True),
        "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": cols}, "frame": frame,
                 "allowHTMLByDefault": False, "condensed": True, "invisibleColumns": [], "itemsPerPage": 25,
                 "paginationSize": "default", "withRowNumber": False},
    }


def place(widget: dict, x: int, y: int, width: int, height: int) -> dict:
    return {"widget": widget, "position": {"x": x, "y": y, "width": width, "height": height}}


# --------------------------------------------------------------------------- #
# Pages (grid is 6 columns wide)
# --------------------------------------------------------------------------- #
def overview() -> list:
    return [
        place(text("# Taco Bueno: sales overview\n"
                   "Net sales = after discounts, before tax · business days Jan 22, 2025 to Sep 27, 2026 · "
                   "source: Lakeflow pipeline gold tables"), 0, 0, 6, 2),
        place(counter("kpi_totals", "net_sales", "Net sales ($)"), 0, 2, 1, 2),
        place(counter("kpi_totals", "tickets", "Tickets"), 1, 2, 1, 2),
        place(counter("kpi_totals", "avg_ticket", "Average ticket ($)"), 2, 2, 1, 2),
        place(counter("kpi_totals", "net_sales_per_store_day", "Net sales per store-day ($)"), 3, 2, 1, 2),
        place(counter("kpi_totals", "stores", "Stores"), 4, 2, 1, 2),
        place(counter("kpi_totals", "days", "Days with data"), 5, 2, 1, 2),
        place(chart("line", "kpis_monthly", "month", "net_sales_per_store_day", "Net sales per store-day, by month",
                    "Month", "Net sales per store-day ($)"), 0, 4, 3, 6),
        place(chart("line", "yoy_long", "month", "yoy_pct", "Year-over-year change (comparable months)",
                    "Month", "Change vs same month last year (%)", color="metric", color_title="Metric",
                    description="Traffic falls every month since March 2026; price increases lift the average ticket."),
              3, 4, 3, 6),
        place(chart("bar", "channel_month", "month", "share_pct", "Channel mix of net sales (new POS)",
                    "Month", "Share of net sales (%)", color="channel", color_title="Channel"), 0, 10, 3, 6),
        place(chart("bar", "markets", "market", "net_sales_per_store_day", "Net sales per store-day by market (new POS)",
                    "Market", "Net sales per store-day ($)", category_type="categorical"), 3, 10, 3, 6),
    ]


def calendar_and_sports() -> list:
    return [
        place(text("# Calendar and sports\n"
                   "Lift = net sales vs the same weekday of the 4 previous weeks. Sports lift is net of the lift on "
                   "days without a relevant game."), 0, 0, 6, 2),
        place(chart("bar", "weekday", "weekday", "net_sales_per_store", "Net sales per store by weekday",
                    "Weekday", "Net sales per store ($)", category_type="categorical"), 0, 2, 3, 6),
        place(chart("bar", "daypart", "daypart", "share_pct", "Share of net sales by daypart",
                    "Daypart", "Share (%)", category_type="categorical"), 3, 2, 3, 6),
        place(chart("bar", "holidays", "holiday_name", "lift_pct", "Holiday lift vs baseline",
                    "Holiday", "Lift (%)", category_type="categorical", horizontal=True,
                    description="Each holiday appears once or twice in the data; read small differences as indicative."),
              0, 8, 3, 9),
        place(chart("bar", "sports_all", "exposure_type", "lift_pct", "Sports: incremental lift on game days",
                    "Exposure", "Incremental lift (%)", category_type="categorical", horizontal=True), 3, 8, 3, 9),
        place(table("sports_split", [("Exposure", "string"), ("Weekday lift %", "float"), ("Weekend lift %", "float"),
                                     ("Store-days", "integer")],
                    "Sports lift: weekdays vs weekends",
                    description="Big weekend games pull customers away from restaurants; weekday games add a little."),
              0, 17, 6, 5),
    ]


def forecasts() -> list:
    return [
        place(text("# Forecasts and strong dates\n"
                   "Models trained on data to Sep 27, 2026; forecast horizon Sep 28 to Dec 31, 2026. "
                   "'Coinciding factors' are what falls on that day, not proven causes. "
                   "Christmas week 2025 is missing from the data, so Dec 24-25 forecasts need a manual check."), 0, 0, 6, 2),
        place(counter("forecast_totals", "tickets", "Expected tickets (next 95 days)"), 0, 2, 3, 2),
        place(counter("forecast_totals", "net_sales", "Expected net sales, $ (next 95 days)"), 3, 2, 3, 2),
        place(chart("line", "chain_actual_forecast", "date", "tickets", "Chain daily tickets: actual and forecast",
                    "Date", "Tickets", color="series", color_title="Series"), 0, 4, 6, 7),
        place(table("strong_dates", [("Date", "string"), ("Class", "string"), ("Expected vs baseline %", "float"),
                                     ("Expected tickets", "integer"), ("Coinciding factors", "string")],
                    "Strong and weak dates ahead (chain)"), 0, 11, 6, 5),
        place(chart("line", "product_forecast", "date", "units", "Top 6 products: daily units at menu price "
                    "(actual to Sep 27, forecast after)", "Date", "Units", color="product", color_title="Product"),
              0, 16, 6, 7),
        place(table("accuracy", [("Forecast", "string"), ("Series", "integer"), ("Forecast WAPE %", "float"),
                                 ("Seasonal naive WAPE %", "float")],
                    "Backtest accuracy, Jul 1 to Sep 27, 2026 (lower is better)"), 0, 23, 3, 5),
        place(table("price_sensitivity", [("Product", "string"), ("Type", "string"), ("Current price", "money"),
                                          ("Units change at +5% price, %", "float"), ("Implied elasticity", "money")],
                    "Price sensitivity scenario (+5% on every menu price)",
                    description="Only value items show the expected response; core items need a controlled price test."),
              3, 23, 3, 5),
    ]


def stores_and_products() -> list:
    return [
        place(text("# Stores and products\n"
                   "Store segments come from k-means on how each store sells (volume, basket, channels, dayparts, "
                   "product mix, game-day sensitivity, growth)."), 0, 0, 6, 2),
        place(table("segments", [("Segment", "string"), ("Stores", "integer"), ("Net sales per day", "integer"),
                                 ("Avg ticket", "money"), ("Delivery %", "float"), ("Drive-thru %", "float"),
                                 ("Lunch %", "float"), ("Weekend index", "money"), ("YoY growth %", "float")],
                    "Store segments"), 0, 2, 6, 4),
        place(chart("bar", "segments_market", "market", "stores", "Stores by market and segment",
                    "Market", "Stores", color="segment", color_title="Segment", category_type="categorical"), 0, 6, 3, 8),
        place(chart("bar", "top_products", "product_name", "sales", "Top 15 products by sales",
                    "Product", "Sales ($)", category_type="categorical", horizontal=True), 3, 6, 3, 8),
        place(chart("bar", "price_waves", "month", "increases", "Store-product menu price increases per month",
                    "Month", "Price increases", description="Two chain-wide waves: Apr-May 2025 and Jul 2026."),
              0, 14, 6, 5),
    ]


def data_quality() -> list:
    return [
        place(text("# Data quality\n"
                   "Pipeline expectations, the daily scorecard (mv_data_quality_daily) and the quality gate of the "
                   "daily job."), 0, 0, 6, 2),
        place(counter("dq_totals", "reconciled_pct", "Tickets reconciled (%)"), 0, 2, 1, 2),
        place(counter("dq_totals", "quarantined_rows", "Quarantined rows"), 1, 2, 1, 2),
        place(counter("dq_totals", "missing_days", "Missing export days"), 2, 2, 1, 2),
        place(counter("dq_totals", "reused_check_numbers", "Reused check numbers (fixed)"), 3, 2, 2, 2),
        place(counter("dq_totals", "stores_without_location", "Stores without location"), 5, 2, 1, 2),
        place(chart("bar", "dq_daily", "business_date", "bar_height", "Tickets per business day",
                    "Business date", "Tickets", color="day_status", color_title="Day status",
                    description="Missing and partial days are drawn as full-height bars so the gaps are visible."),
              0, 4, 6, 6),
        place(table("dq_issues", [("Business date", "string"), ("Status", "string"), ("Holiday", "string"),
                                  ("Files", "integer"), ("Tickets", "integer"), ("Stores reporting", "integer"),
                                  ("Stores expected", "integer")], "Days that are missing or partial"), 0, 10, 3, 7),
        place(table("expectations", [("Table", "string"), ("Expectation", "string"), ("Passed", "integer"),
                                     ("Failed", "integer"), ("Failed %", "float")],
                    "Expectations, latest pipeline update per rule"), 3, 10, 3, 7),
        place(table("dq_gate", [("Run (UTC)", "string"), ("Check", "string"), ("Severity", "string"),
                                ("Status", "string"), ("Value", "float"), ("Threshold", "string")],
                    "Quality gate, latest job run"), 0, 17, 6, 5),
    ]


def build() -> dict:
    pages = [("Overview", overview()), ("Calendar & sports", calendar_and_sports()), ("Forecasts", forecasts()),
             ("Stores & products", stores_and_products()), ("Data quality", data_quality())]
    return {
        "datasets": [{"name": _id("ds", key), "displayName": key, "queryLines": [line + "\n" for line in sql.strip().splitlines()]}
                     for key, sql in DATASETS.items()],
        "pages": [{"name": _id("page", title), "displayName": title, "layout": layout, "pageType": "PAGE_TYPE_CANVAS"}
                  for title, layout in pages],
    }


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")
