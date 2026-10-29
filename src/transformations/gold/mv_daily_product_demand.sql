-- Gold: daily demand per product and market. Input of the product/combo demand
-- forecasts. `units` counts every unit served (including combo components,
-- which drive kitchen and inventory needs); `priced_units` counts units sold at
-- a menu price (what pricing decisions act on).
CREATE OR REFRESH MATERIALIZED VIEW mv_daily_product_demand (
  CONSTRAINT units_not_negative EXPECT (units >= 0)
)
COMMENT 'Gold. Business day x market x product: units served, priced units, sales amount, average menu price, stores selling and tickets containing the product.'
CLUSTER BY (business_date, product_key)
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  l.business_date,
  coalesce(s.market, 'Unknown')                                                  AS market,
  l.product_key,
  p.product_name,
  p.category,
  p.product_role,
  sum(l.units)                                                                   AS units,
  sum(CASE WHEN l.is_priced_sale THEN l.units ELSE 0 END)                        AS priced_units,
  sum(l.line_amount)                                                             AS sales_amount,
  sum(CASE WHEN l.is_priced_sale THEN l.menu_price * l.units END)
    / nullif(sum(CASE WHEN l.is_priced_sale THEN l.units END), 0)                AS avg_menu_price,
  count(DISTINCT l.store_id)                                                     AS stores_selling,
  count(DISTINCT l.ticket_key)                                                   AS tickets_with_product
FROM st_orders_cleaned l
LEFT JOIN mv_dim_store s   ON s.store_id = l.store_id
LEFT JOIN mv_dim_product p ON p.product_key = l.product_key
WHERE l.is_sale_line
GROUP BY ALL;
