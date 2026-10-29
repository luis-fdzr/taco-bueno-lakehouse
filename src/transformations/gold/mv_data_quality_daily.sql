-- Gold: data quality scorecard per business day. One row per calendar day in the
-- data range, so missing exports show up as rows instead of disappearing.
--   day_status   Complete | Partial (stores reporting < 80% of expected) | Missing file
--   expected stores are those active (first <= day <= last sale date) that day.
CREATE OR REFRESH MATERIALIZED VIEW mv_data_quality_daily
COMMENT 'Gold. Business day: files, raw/clean/quarantined rows, tickets, stores reporting vs expected, reconciliation rate, zero-total tickets, reused check numbers and day status.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH bounds AS (
  SELECT min(business_date) AS d0, max(business_date) AS d1 FROM mv_tickets
),
cal AS (
  SELECT c.calendar_date AS business_date, c.day_name, c.holiday_name
  FROM mv_dim_calendar c CROSS JOIN bounds b
  WHERE c.calendar_date BETWEEN b.d0 AND b.d1
),
raw AS (
  SELECT
    to_date(try_to_timestamp(date, 'M/d/yyyy'))   AS business_date,
    count(*)                                      AS raw_rows,
    count(DISTINCT source_file)                   AS files,
    count_if(_rescued_data IS NOT NULL)           AS rescued_rows
  FROM st_pos_lines_raw
  GROUP BY 1
),
clean AS (
  SELECT business_date, count(*) AS clean_rows, count_if(NOT is_sale_line) AS non_sale_rows
  FROM st_orders_cleaned
  GROUP BY 1
),
quarantine AS (
  SELECT business_date, count(*) AS quarantined_rows
  FROM st_orders_quarantine
  GROUP BY 1
),
tickets AS (
  SELECT
    business_date,
    count(*)                         AS tickets,
    count(DISTINCT store_id)         AS stores_reporting,
    count_if(reconciles)             AS tickets_reconciled,
    count_if(is_zero_total)          AS zero_total_tickets,
    count_if(header_variants > 1)    AS tickets_with_inconsistent_header,
    sum(net_sales)                   AS net_sales
  FROM mv_tickets
  GROUP BY 1
),
reused AS (
  SELECT business_date, count(*) AS reused_check_numbers
  FROM (
    SELECT business_date, store_id, check_number
    FROM mv_tickets
    GROUP BY ALL
    HAVING count(*) > 1
  )
  GROUP BY 1
),
expected AS (
  SELECT cal.business_date, count(s.store_id) AS stores_expected
  FROM cal
  LEFT JOIN mv_dim_store s
    ON s.has_sales AND cal.business_date BETWEEN s.first_sale_date AND s.last_sale_date
  GROUP BY 1
)
SELECT
  cal.business_date,
  cal.day_name,
  cal.holiday_name,
  coalesce(r.files, 0)                                       AS files,
  coalesce(r.raw_rows, 0)                                    AS raw_rows,
  coalesce(r.rescued_rows, 0)                                AS rescued_rows,
  coalesce(c.clean_rows, 0)                                  AS clean_rows,
  coalesce(q.quarantined_rows, 0)                            AS quarantined_rows,
  coalesce(c.non_sale_rows, 0)                               AS non_sale_rows,
  coalesce(t.tickets, 0)                                     AS tickets,
  coalesce(t.stores_reporting, 0)                            AS stores_reporting,
  e.stores_expected,
  t.tickets_reconciled / nullif(t.tickets, 0)                AS reconciliation_rate,
  coalesce(t.zero_total_tickets, 0)                          AS zero_total_tickets,
  coalesce(t.tickets_with_inconsistent_header, 0)            AS tickets_with_inconsistent_header,
  coalesce(u.reused_check_numbers, 0)                        AS reused_check_numbers,
  t.net_sales,
  CASE
    WHEN r.raw_rows IS NULL THEN 'Missing file'
    WHEN coalesce(t.stores_reporting, 0) < 0.8 * e.stores_expected THEN 'Partial'
    ELSE 'Complete'
  END                                                        AS day_status
FROM cal
LEFT JOIN raw r        ON r.business_date = cal.business_date
LEFT JOIN clean c      ON c.business_date = cal.business_date
LEFT JOIN quarantine q ON q.business_date = cal.business_date
LEFT JOIN tickets t    ON t.business_date = cal.business_date
LEFT JOIN reused u     ON u.business_date = cal.business_date
LEFT JOIN expected e   ON e.business_date = cal.business_date;
