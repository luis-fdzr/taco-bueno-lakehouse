-- Silver: one row per ticket (check) with harmonized money columns.
--
--   gross_sales = sales before discount and tax
--   net_sales   = gross_sales - discount        (the revenue metric used everywhere)
--   collected   = net_sales + tax
--
-- Legacy POS: Total = gross sales. New POS: Total = net sales + tax.
-- `reconciles` checks the ticket header against the sum of its lines (within 1 cent).
CREATE OR REFRESH MATERIALIZED VIEW mv_tickets (
  CONSTRAINT ticket_reconciles EXPECT (reconciles),
  CONSTRAINT single_header_per_ticket EXPECT (header_variants = 1),
  CONSTRAINT net_sales_not_negative EXPECT (net_sales >= 0)
)
COMMENT 'Silver. One row per ticket: channel, daypart, POS system, gross/net/collected sales, units, discount, tax and a line-vs-header reconciliation flag.'
CLUSTER BY (business_date, store_id)
TBLPROPERTIES ('quality' = 'silver')
AS
WITH by_ticket AS (
  SELECT
    ticket_key,
    business_date,
    store_id,
    check_number,
    check_begin_time,
    max(pos_system)                                       AS pos_system,
    max(channel_raw)                                      AS channel_raw,
    max(channel_group)                                    AS channel_group,
    max(daypart)                                          AS daypart,
    min(begin_minute)                                     AS begin_minute,
    max(end_minute)                                       AS end_minute,
    max(tender_type)                                      AS tender_type,
    max(coalesce(check_total, 0))                         AS check_total,
    max(coalesce(check_tax, 0))                           AS check_tax,
    max(coalesce(check_discount, 0))                      AS check_discount,
    count(DISTINCT coalesce(check_total, 0))              AS header_variants,
    count(*)                                              AS lines,
    sum(units)                                            AS units,
    sum(CASE WHEN is_priced_sale THEN units ELSE 0 END)   AS priced_units,
    sum(line_amount)                                      AS line_amount,
    sum(coalesce(item_voided, 0))                         AS voided_amount,
    max(source_file)                                      AS source_file
  FROM st_orders_cleaned
  GROUP BY ticket_key, business_date, store_id, check_number, check_begin_time
)
SELECT
  *,
  CASE WHEN pos_system = 'Legacy' THEN check_total
       ELSE check_total - check_tax + check_discount END                      AS gross_sales,
  CASE WHEN pos_system = 'Legacy' THEN check_total - check_discount
       ELSE check_total - check_tax END                                       AS net_sales,
  CASE WHEN pos_system = 'Legacy' THEN check_total - check_discount + check_tax
       ELSE check_total END                                                   AS collected,
  -- Compared in whole cents: float noise must not decide a 2-cent difference.
  round(abs(check_total - CASE WHEN pos_system = 'Legacy' THEN line_amount
                               ELSE line_amount - check_discount + check_tax END), 2) < 0.02 AS reconciles,
  abs(check_total) < 0.005                                                    AS is_zero_total,
  check_discount > 0                                                          AS has_discount,
  CASE WHEN begin_minute IS NOT NULL AND end_minute IS NOT NULL
       THEN (end_minute - begin_minute + 1440) % 1440 END                     AS duration_minutes
FROM by_ticket;
