-- Silver: product dimension keyed by product_key (name normalized across POS
-- systems and menu versions). Category comes from the new POS Report Group when
-- the product was sold there; legacy-only products are classified by name.
CREATE OR REFRESH MATERIALIZED VIEW mv_dim_product (
  CONSTRAINT has_category EXPECT (category <> 'Unclassified')
)
COMMENT 'Silver. One row per normalized product: display name, category, role (menu item, bundle, modifier, non-menu), price and sales summary.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH agg AS (
  SELECT
    product_key,
    mode(product_name)                                                  AS product_name,
    mode(report_group)                                                  AS report_group,
    count(DISTINCT product_name)                                        AS name_variants,
    array_join(array_sort(collect_set(pos_system)), ', ')               AS pos_systems,
    sum(units)                                                          AS units_total,
    sum(CASE WHEN is_priced_sale THEN units ELSE 0 END)                 AS units_priced,
    sum(line_amount)                                                    AS sales_amount,
    percentile_approx(CASE WHEN is_priced_sale THEN menu_price END, 0.5) AS median_menu_price,
    min(business_date)                                                  AS first_sale_date,
    max(business_date)                                                  AS last_sale_date
  FROM st_orders_cleaned
  WHERE is_sale_line
  GROUP BY product_key
),
classified AS (
  SELECT
    *,
    CASE
      WHEN report_group IN ('(M) Combos', '(M) Platters') THEN 'Combos & Platters'
      WHEN report_group IN ('(I) Paper', '(M) Beverages') THEN 'Beverages'
      WHEN report_group = '(M) Bueno Under a Buck' THEN 'Value menu'
      WHEN report_group = '(M) LTO/Promo' THEN 'LTO & promo'
      WHEN report_group IS NOT NULL THEN regexp_replace(report_group, '^[(][A-Z][)] ', '')
      WHEN product_key RLIKE 'combo|platter|wholotta box|party box|pack|box - [0-9]+' THEN 'Combos & Platters'
      WHEN product_key RLIKE 'donation|customer name|gift card|coupon' THEN 'Non-menu'
      WHEN product_key RLIKE 'taco' THEN 'Tacos'
      WHEN product_key RLIKE 'burrito' THEN 'Burritos'
      WHEN product_key RLIKE 'nacho|chips|queso' THEN 'Nachos & Chips'
      WHEN product_key RLIKE 'quesadilla' THEN 'Quesadillas'
      WHEN product_key RLIKE 'salad|bowl' THEN 'Salads & Bowls'
      WHEN product_key RLIKE 'drink|tea|coke|pepper|sprite|lemonade|fanta|water|coffee|shake|punch|soda|bubbler' THEN 'Beverages'
      WHEN product_key RLIKE 'cheesecake|churro|sopapilla|dessert|cookie|brownie' THEN 'Sweets'
      WHEN product_key RLIKE 'muchaco|chilada|enchilada|tamale|bob|burger' THEN 'Specialties'
      WHEN product_key RLIKE 'sauce|salsa|sour cream|guac|jalape|onion|tomato|lettuce|cheese|modifier|unnamed' THEN 'Condiments'
      ELSE 'Unclassified'
    END AS category
  FROM agg
)
SELECT
  *,
  CASE
    WHEN category = 'Non-menu' THEN 'Non-menu'
    WHEN category IN ('Combos & Platters', 'Catering')
      OR product_key RLIKE '^combo [0-9]|wholotta box|party box' THEN 'Bundle'
    WHEN category IN ('Condiments', 'Substitution')
      OR product_key RLIKE '[(](w/o|on side|easy|cond[.]|n/r)[)]|^(no|xt|add|extra|sub|lite|light) |^[*+-]|modifier'
      OR median_menu_price IS NULL THEN 'Modifier'
    ELSE 'Menu item'
  END                                                   AS product_role,
  units_priced / nullif(units_total, 0)                 AS priced_unit_share,
  dense_rank() OVER (ORDER BY sales_amount DESC)        AS sales_rank
FROM classified;
