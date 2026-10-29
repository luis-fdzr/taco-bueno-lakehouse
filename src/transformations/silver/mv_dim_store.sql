-- Silver: store dimension. Combines the business city file (geocoded) with what
-- the sales data says about each store: activity period, POS migration date and
-- status. A FULL OUTER JOIN keeps stores missing from either side visible.
CREATE OR REFRESH MATERIALIZED VIEW mv_dim_store (
  CONSTRAINT store_with_sales_has_location EXPECT (NOT has_sales OR latitude IS NOT NULL),
  CONSTRAINT city_state_is_unambiguous EXPECT (NOT coalesce(geocode_ambiguous, false))
)
COMMENT 'Silver. One row per store: city, state, coordinates, market, activity period, POS migration date and status.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH sales AS (
  SELECT
    store_id,
    min(business_date)                                          AS first_sale_date,
    max(business_date)                                          AS last_sale_date,
    count(DISTINCT business_date)                               AS days_with_sales,
    min(CASE WHEN pos_system = 'New' THEN business_date END)    AS new_pos_first_date,
    max(CASE WHEN pos_system = 'Legacy' THEN business_date END) AS legacy_pos_last_date,
    count(*)                                                    AS tickets,
    sum(net_sales)                                              AS net_sales
  FROM mv_tickets
  GROUP BY store_id
),
period AS (
  SELECT min(first_sale_date) AS period_start, max(last_sale_date) AS period_end FROM sales
),
joined AS (
  SELECT
    coalesce(s.store_id, r.store_id) AS store_id,
    r.city_raw, r.city, r.state, r.latitude, r.longitude, r.geocode_ambiguous, r.geocode_candidates,
    s.first_sale_date, s.last_sale_date, s.days_with_sales, s.new_pos_first_date, s.legacy_pos_last_date,
    s.tickets, s.net_sales,
    s.store_id IS NOT NULL AS has_sales
  FROM sales s
  FULL OUTER JOIN mv_store_reference_raw r ON s.store_id = r.store_id
)
SELECT
  j.*,
  -- Markets: distance (km, equirectangular) to each metro anchor.
  CASE
    WHEN latitude IS NULL THEN 'Unknown'
    WHEN 111.32 * sqrt(pow(latitude - 32.8998, 2) + pow((longitude + 97.0403) * cos(radians(32.8998)), 2)) <= 90 THEN 'Dallas-Fort Worth'
    WHEN 111.32 * sqrt(pow(latitude - 35.4676, 2) + pow((longitude + 97.5164) * cos(radians(35.4676)), 2)) <= 70 THEN 'Oklahoma City'
    WHEN 111.32 * sqrt(pow(latitude - 36.1540, 2) + pow((longitude + 95.9928) * cos(radians(36.1540)), 2)) <= 70 THEN 'Tulsa'
    WHEN 111.32 * sqrt(pow(latitude - 29.7604, 2) + pow((longitude + 95.3698) * cos(radians(29.7604)), 2)) <= 100 THEN 'Houston'
    ELSE concat('Other ', state)
  END                                                      AS market,
  CASE
    WHEN NOT has_sales THEN 'No sales in period'
    WHEN last_sale_date < date_sub(p.period_end, 14) THEN 'Closed / stopped reporting'
    WHEN first_sale_date > date_add(p.period_start, 14) THEN 'Opened during period'
    ELSE 'Active'
  END                                                      AS store_status,
  net_sales / nullif(days_with_sales, 0)                   AS avg_daily_net_sales
FROM joined j
CROSS JOIN period p;
