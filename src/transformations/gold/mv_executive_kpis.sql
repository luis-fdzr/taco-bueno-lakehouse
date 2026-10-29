-- Gold: monthly executive KPIs with year-over-year comparison. Productivity is
-- normalized per store-day so store openings/closures and missing export days do
-- not distort the trend.
CREATE OR REFRESH MATERIALIZED VIEW mv_executive_kpis
COMMENT 'Gold. Month: net sales, tickets, average ticket, sales and tickets per store-day, discount rate, channel mix, data coverage and YoY change.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH monthly AS (
  SELECT
    date_trunc('MONTH', business_date)                         AS month,
    count(DISTINCT business_date)                              AS days_with_data,
    count(DISTINCT store_id)                                   AS active_stores,
    count(*)                                                   AS store_days,
    sum(tickets)                                               AS tickets,
    sum(net_sales)                                             AS net_sales,
    sum(gross_sales)                                           AS gross_sales,
    sum(discounts)                                             AS discounts,
    sum(units)                                                 AS units,
    sum(net_sales_drive_thru)                                  AS net_sales_drive_thru,
    sum(net_sales_in_store)                                    AS net_sales_in_store,
    sum(net_sales_delivery)                                    AS net_sales_delivery,
    sum(tickets_reconciled)                                    AS tickets_reconciled,
    count_if(regional_team_games > 0)                          AS store_days_with_regional_game,
    count_if(pos_system = 'New')                               AS store_days_new_pos
  FROM mv_daily_store_sales
  GROUP BY 1
),
kpis AS (
  SELECT
    *,
    net_sales / tickets                       AS avg_ticket,
    units / tickets                           AS units_per_ticket,
    net_sales / store_days                    AS net_sales_per_store_day,
    tickets / store_days                      AS tickets_per_store_day,
    discounts / gross_sales                   AS discount_rate,
    net_sales_drive_thru / net_sales          AS drive_thru_share,
    net_sales_in_store / net_sales            AS in_store_share,
    net_sales_delivery / net_sales            AS delivery_share,
    tickets_reconciled / tickets              AS reconciliation_rate,
    store_days_new_pos / store_days           AS new_pos_share
  FROM monthly
)
SELECT
  k.*,
  ly.net_sales_per_store_day                                            AS net_sales_per_store_day_ly,
  k.net_sales_per_store_day / ly.net_sales_per_store_day - 1            AS net_sales_per_store_day_yoy,
  k.tickets_per_store_day / ly.tickets_per_store_day - 1                AS tickets_per_store_day_yoy,
  k.avg_ticket / ly.avg_ticket - 1                                      AS avg_ticket_yoy,
  k.days_with_data >= 25 AND ly.days_with_data >= 25                    AS yoy_comparable
FROM kpis k
LEFT JOIN kpis ly ON ly.month = add_months(k.month, -12);
