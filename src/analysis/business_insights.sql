-- =============================================================================
-- Business insight queries behind docs/06_analysis_and_business_value.md.
-- Run in the SQL editor against the target schema (prod: workspace.taco_bueno).
-- Each query is independent; tools/analysis/make_charts.py runs the same ones.
-- =============================================================================
USE CATALOG workspace;
USE SCHEMA taco_bueno;

-- Q1. Headline totals
SELECT
  min(business_date) AS first_day, max(business_date) AS last_day, count(DISTINCT business_date) AS days,
  count(DISTINCT store_id) AS stores, sum(tickets) AS tickets, sum(net_sales) AS net_sales,
  sum(net_sales) / sum(tickets) AS avg_ticket, sum(net_sales) / count(*) AS net_sales_per_store_day
FROM mv_daily_store_sales;

-- Q2. Monthly KPIs and year-over-year (per store-day, comparable months only)
SELECT month, days_with_data, active_stores, net_sales, tickets, avg_ticket, net_sales_per_store_day,
       tickets_per_store_day, discount_rate, delivery_share, net_sales_per_store_day_yoy, tickets_per_store_day_yoy,
       avg_ticket_yoy, yoy_comparable
FROM mv_executive_kpis ORDER BY month;

-- Q3. Markets: productivity and mix (new POS period, when channels are comparable)
SELECT market, count(DISTINCT store_id) AS stores, avg(net_sales) AS net_sales_per_store_day,
       sum(net_sales) / sum(tickets) AS avg_ticket, avg(tickets) AS tickets_per_store_day,
       sum(net_sales_drive_thru) / sum(net_sales) AS drive_thru_share,
       sum(net_sales_delivery) / sum(net_sales) AS delivery_share
FROM mv_daily_store_sales WHERE pos_system = 'New'
GROUP BY market ORDER BY net_sales_per_store_day DESC;

-- Q4. Channel and daypart mix (new POS period)
SELECT 'channel' AS dimension, 'Drive-thru' AS value, sum(net_sales_drive_thru) / sum(net_sales) AS share FROM mv_daily_store_sales WHERE pos_system = 'New'
UNION ALL SELECT 'channel', 'In-store', sum(net_sales_in_store) / sum(net_sales) FROM mv_daily_store_sales WHERE pos_system = 'New'
UNION ALL SELECT 'channel', 'Delivery & digital', sum(net_sales_delivery) / sum(net_sales) FROM mv_daily_store_sales WHERE pos_system = 'New'
UNION ALL SELECT 'daypart', 'Breakfast', sum(net_sales_breakfast) / sum(net_sales) FROM mv_daily_store_sales
UNION ALL SELECT 'daypart', 'Lunch', sum(net_sales_lunch) / sum(net_sales) FROM mv_daily_store_sales
UNION ALL SELECT 'daypart', 'Afternoon', sum(net_sales_afternoon) / sum(net_sales) FROM mv_daily_store_sales
UNION ALL SELECT 'daypart', 'Dinner', sum(net_sales_dinner) / sum(net_sales) FROM mv_daily_store_sales
UNION ALL SELECT 'daypart', 'Late night', sum(net_sales_late_night) / sum(net_sales) FROM mv_daily_store_sales;

-- Q5. Weekday pattern (net sales per store, all stores)
SELECT day_of_week, day_name, avg(net_sales) AS net_sales_per_store_day, avg(tickets) AS tickets_per_store_day
FROM mv_daily_store_sales GROUP BY ALL ORDER BY day_of_week;

-- Q6. Holidays vs their baseline (same weekday, 4 previous weeks)
SELECT holiday_name, count(DISTINCT business_date) AS dates,
       sum(net_sales) / sum(baseline_net_sales_4w) - 1 AS net_sales_lift,
       sum(tickets) / sum(baseline_tickets_4w) - 1     AS tickets_lift
FROM mv_daily_store_sales WHERE holiday_name IS NOT NULL AND baseline_net_sales_4w IS NOT NULL
GROUP BY holiday_name ORDER BY net_sales_lift DESC;

-- Q7. Sports: incremental lift vs no-game days (all markets, all days)
SELECT exposure_type, store_days, dates, net_sales_lift, median_net_sales_lift, incremental_lift_vs_no_game,
       incremental_tickets_lift_vs_no_game
FROM mv_event_impact WHERE market = 'All markets' AND day_type = 'All days'
ORDER BY incremental_lift_vs_no_game DESC;

-- Q8. Sports by market (regional postseason and NFL/college game days)
SELECT market, exposure_type, day_type, store_days, incremental_lift_vs_no_game
FROM mv_event_impact
WHERE market <> 'All markets' AND day_type <> 'All days' AND store_days >= 100
  AND exposure_type IN ('Regional team in postseason', 'Regional NFL game', 'Regional college football game', 'Regional NBA game')
ORDER BY exposure_type, market, day_type;

-- Q9. Price change waves (store-product modal price changes per month)
SELECT month, count_if(price_change_pct > 0) AS increases, count_if(price_change_pct < 0) AS decreases,
       percentile_approx(CASE WHEN price_change_pct > 0 THEN price_change_pct END, 0.5) AS median_increase_pct
FROM mv_product_price_history WHERE prev_modal_price IS NOT NULL
GROUP BY month ORDER BY month;

-- Q10. Product concentration and categories
SELECT category, product_role, count(*) AS products, sum(sales_amount) AS sales_amount,
       sum(sales_amount) / sum(sum(sales_amount)) OVER () AS share
FROM mv_dim_product GROUP BY ALL ORDER BY sales_amount DESC;

-- Q11. Store segments
SELECT * FROM ml_store_segment_profiles ORDER BY avg_daily_net_sales DESC;

-- Q12. Forecast accuracy (backtest)
SELECT series_type, count(*) AS series, avg(wape_model) AS wape_model, avg(wape_baseline) AS wape_baseline
FROM ml_forecast_metrics GROUP BY series_type;

-- Q13. Upcoming strong and weak dates (chain)
SELECT date, tickets, baseline_tickets, tickets_uplift, classification, drivers
FROM ml_strong_dates WHERE scope = 'Chain' AND source = 'forecast' AND classification <> 'Normal'
ORDER BY date;
