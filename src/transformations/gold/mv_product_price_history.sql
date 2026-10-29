-- Gold: menu price history per store, product and month. The modal price is the
-- menu price with the most units sold that month; price changes are detected
-- against the previous month the store sold the product.
CREATE OR REFRESH MATERIALIZED VIEW mv_product_price_history (
  CONSTRAINT modal_price_positive EXPECT (modal_price > 0)
)
COMMENT 'Gold. Store x product x month: modal menu price, units, previous modal price and % change. Feeds pricing analysis and the forecast price feature.'
CLUSTER BY (month, product_key)
TBLPROPERTIES ('quality' = 'gold')
AS
WITH price_points AS (
  SELECT
    date_trunc('MONTH', business_date) AS month,
    store_id,
    product_key,
    menu_price,
    sum(units)                         AS units
  FROM st_orders_cleaned
  WHERE is_priced_sale
  GROUP BY ALL
),
monthly AS (
  SELECT
    month,
    store_id,
    product_key,
    max_by(menu_price, units)          AS modal_price,
    min(menu_price)                    AS min_price,
    max(menu_price)                    AS max_price,
    count(*)                           AS price_points,
    sum(units)                         AS units
  FROM price_points
  GROUP BY ALL
)
SELECT
  m.*,
  p.product_name,
  p.category,
  lag(modal_price) OVER (PARTITION BY store_id, product_key ORDER BY month)                    AS prev_modal_price,
  round((modal_price / lag(modal_price) OVER (PARTITION BY store_id, product_key ORDER BY month) - 1) * 100, 2)
                                                                                                 AS price_change_pct
FROM monthly m
LEFT JOIN mv_dim_product p USING (product_key);
