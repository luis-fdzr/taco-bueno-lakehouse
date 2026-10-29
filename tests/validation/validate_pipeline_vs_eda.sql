-- =============================================================================
-- Pipeline vs independent EDA reconciliation
--
-- The expected values come from tools/eda/eda_taco_bueno.py, a separate pandas
-- implementation that reads the same raw CSV files on a laptop. Both use the same
-- ticket key (date + store + check number + start time), so every count must match
-- exactly and money must match to the cent (tolerance 0.05 for float sums).
--
-- Run in the Databricks SQL editor after a pipeline update. Point it at the
-- target schema first (dev: dev_<user>_taco_bueno, prod: taco_bueno).
-- =============================================================================
USE CATALOG workspace;
USE SCHEMA taco_bueno;

WITH
expected AS (
  SELECT section, metric, dimension,
         CAST(expected_value AS DOUBLE) AS expected_value,
         CAST(tolerance AS DOUBLE)      AS tolerance
  FROM VALUES
    ('1. Files', 'files_in_raw_volume', 'Total', 597, 0),
    ('1. Files', 'bytes_in_raw_volume', 'Total', 11433435502, 0),
    ('1. Files', 'files_with_rows_in_bronze', 'Total', 573, 0),
    ('1. Files', 'header_only_files', 'Total', 24, 0),
    ('2. Rows', 'bronze_rows', 'Total', 85273331, 0),
    ('2. Rows', 'silver_clean_plus_quarantine_rows', 'Total', 85273331, 0),
    ('2. Rows', 'quarantined_rows', 'Total', 0, 0),
    ('3. Coverage', 'business_days', 'Total', 573, 0),
    ('3. Coverage', 'first_business_date', 'Total', 20250122, 0),
    ('3. Coverage', 'last_business_date', 'Total', 20260927, 0),
    ('3. Coverage', 'stores_with_sales', 'Total', 119, 0),
    ('3. Coverage', 'distinct_product_names', 'Total', 647, 0),
    ('3. Coverage', 'first_new_pos_date', 'Total', 20251104, 0),
    ('3. Coverage', 'last_legacy_pos_date', 'Total', 20260119, 0),
    ('4. Tickets', 'lines', 'Legacy', 41976500, 0),
    ('4. Tickets', 'tickets', 'Legacy', 7579047, 0),
    ('4. Tickets', 'units', 'Legacy', 49132753, 0),
    ('4. Tickets', 'gross_sales', 'Legacy', 101458285.01, 0.05),
    ('4. Tickets', 'discounts', 'Legacy', 3003345.72, 0.05),
    ('4. Tickets', 'net_sales', 'Legacy', 98454939.29, 0.05),
    ('4. Tickets', 'tax', 'Legacy', 8353311.1, 0.05),
    ('4. Tickets', 'collected', 'Legacy', 106808250.39, 0.05),
    ('4. Tickets', 'voided_amount', 'Legacy', 834288.08, 0.05),
    ('4. Tickets', 'tickets_reconciled', 'Legacy', 7567490, 0),
    ('4. Tickets', 'zero_total_tickets', 'Legacy', 83434, 0),
    ('4. Tickets', 'tickets_with_discount', 'Legacy', 767897, 0),
    ('4. Tickets', 'lines', 'New', 43296831, 0),
    ('4. Tickets', 'tickets', 'New', 6209621, 0),
    ('4. Tickets', 'units', 'New', 43134010, 0),
    ('4. Tickets', 'gross_sales', 'New', 84866510.13, 0.05),
    ('4. Tickets', 'discounts', 'New', 1614034.36, 0.05),
    ('4. Tickets', 'net_sales', 'New', 83252475.77, 0.05),
    ('4. Tickets', 'tax', 'New', 6966599.18, 0.05),
    ('4. Tickets', 'collected', 'New', 90219074.95, 0.05),
    ('4. Tickets', 'voided_amount', 'New', 620042.63, 0.05),
    ('4. Tickets', 'tickets_reconciled', 'New', 6202711, 0),
    ('4. Tickets', 'zero_total_tickets', 'New', 128107, 0),
    ('4. Tickets', 'tickets_with_discount', 'New', 443505, 0),
    ('4. Tickets', 'lines', 'Total', 85273331, 0),
    ('4. Tickets', 'tickets', 'Total', 13788668, 0),
    ('4. Tickets', 'units', 'Total', 92266763, 0),
    ('4. Tickets', 'gross_sales', 'Total', 186324795.14, 0.05),
    ('4. Tickets', 'discounts', 'Total', 4617380.08, 0.05),
    ('4. Tickets', 'net_sales', 'Total', 181707415.06, 0.05),
    ('4. Tickets', 'tax', 'Total', 15319910.28, 0.05),
    ('4. Tickets', 'collected', 'Total', 197027325.34, 0.05),
    ('4. Tickets', 'voided_amount', 'Total', 1454330.71, 0.05),
    ('4. Tickets', 'tickets_reconciled', 'Total', 13770201, 0),
    ('4. Tickets', 'zero_total_tickets', 'Total', 211541, 0),
    ('4. Tickets', 'tickets_with_discount', 'Total', 1211402, 0),
    ('5. Monthly', 'tickets', '2025-01', 157235, 0),
    ('5. Monthly', 'net_sales', '2025-01', 1909203.5, 0.05),
    ('5. Monthly', 'tickets', '2025-02', 599944, 0),
    ('5. Monthly', 'net_sales', '2025-02', 7339325.33, 0.05),
    ('5. Monthly', 'tickets', '2025-03', 742628, 0),
    ('5. Monthly', 'net_sales', '2025-03', 9495114.35, 0.05),
    ('5. Monthly', 'tickets', '2025-04', 701538, 0),
    ('5. Monthly', 'net_sales', '2025-04', 9130481.69, 0.05),
    ('5. Monthly', 'tickets', '2025-05', 702609, 0),
    ('5. Monthly', 'net_sales', '2025-05', 9468763.63, 0.05),
    ('5. Monthly', 'tickets', '2025-06', 722213, 0),
    ('5. Monthly', 'net_sales', '2025-06', 9498239.79, 0.05),
    ('5. Monthly', 'tickets', '2025-07', 686234, 0),
    ('5. Monthly', 'net_sales', '2025-07', 9105670.13, 0.05),
    ('5. Monthly', 'tickets', '2025-08', 724949, 0),
    ('5. Monthly', 'net_sales', '2025-08', 9407033.24, 0.05),
    ('5. Monthly', 'tickets', '2025-09', 682944, 0),
    ('5. Monthly', 'net_sales', '2025-09', 8707915.66, 0.05),
    ('5. Monthly', 'tickets', '2025-10', 754825, 0),
    ('5. Monthly', 'net_sales', '2025-10', 9750926.59, 0.05),
    ('5. Monthly', 'tickets', '2025-11', 711281, 0),
    ('5. Monthly', 'net_sales', '2025-11', 9378631.16, 0.05),
    ('5. Monthly', 'tickets', '2025-12', 551524, 0),
    ('5. Monthly', 'net_sales', '2025-12', 7364253.73, 0.05),
    ('5. Monthly', 'tickets', '2026-01', 563829, 0),
    ('5. Monthly', 'net_sales', '2026-01', 7524956.46, 0.05),
    ('5. Monthly', 'tickets', '2026-02', 618062, 0),
    ('5. Monthly', 'net_sales', '2026-02', 8039400.77, 0.05),
    ('5. Monthly', 'tickets', '2026-03', 730491, 0),
    ('5. Monthly', 'net_sales', '2026-03', 9543836.57, 0.05),
    ('5. Monthly', 'tickets', '2026-04', 729525, 0),
    ('5. Monthly', 'net_sales', '2026-04', 9579782.0, 0.05),
    ('5. Monthly', 'tickets', '2026-05', 746988, 0),
    ('5. Monthly', 'net_sales', '2026-05', 9877909.25, 0.05),
    ('5. Monthly', 'tickets', '2026-06', 715871, 0),
    ('5. Monthly', 'net_sales', '2026-06', 9564762.12, 0.05),
    ('5. Monthly', 'tickets', '2026-07', 656396, 0),
    ('5. Monthly', 'net_sales', '2026-07', 9183902.82, 0.05),
    ('5. Monthly', 'tickets', '2026-08', 665367, 0),
    ('5. Monthly', 'net_sales', '2026-08', 9241449.0, 0.05),
    ('5. Monthly', 'tickets', '2026-09', 624215, 0),
    ('5. Monthly', 'net_sales', '2026-09', 8595857.27, 0.05)
  AS t(section, metric, dimension, expected_value, tolerance)
),
raw_files AS (
  SELECT count(*) AS files, sum(length) AS bytes
  FROM read_files('/Volumes/workspace/default/taco-bueno-raw/TB_Daily_TLD_*-m.csv', format => 'binaryFile')
),
bronze AS (
  SELECT count(*) AS row_count, count(DISTINCT source_file) AS files FROM st_pos_lines_raw
),
silver AS (
  SELECT
    (SELECT count(*) FROM st_orders_cleaned) + (SELECT count(*) FROM st_orders_quarantine) AS row_count,
    (SELECT count(*) FROM st_orders_quarantine)                                            AS quarantined
),
coverage AS (
  SELECT
    count(DISTINCT business_date)                                AS days,
    min(business_date)                                           AS first_day,
    max(business_date)                                           AS last_day,
    count(DISTINCT store_id)                                     AS stores,
    min(CASE WHEN pos_system = 'New' THEN business_date END)     AS first_new,
    max(CASE WHEN pos_system = 'Legacy' THEN business_date END)  AS last_legacy
  FROM mv_tickets
),
products AS (
  SELECT count(DISTINCT product_name) AS n FROM st_orders_cleaned WHERE is_sale_line
),
tickets AS (
  SELECT
    coalesce(pos_system, 'Total') AS dimension,
    sum(lines) AS lines, count(*) AS tickets, sum(units) AS units, sum(gross_sales) AS gross_sales,
    sum(check_discount) AS discounts, sum(net_sales) AS net_sales, sum(check_tax) AS tax, sum(collected) AS collected,
    sum(voided_amount) AS voided_amount, count_if(reconciles) AS tickets_reconciled,
    count_if(is_zero_total) AS zero_total_tickets, count_if(has_discount) AS tickets_with_discount
  FROM mv_tickets
  GROUP BY ROLLUP (pos_system)
),
monthly AS (
  SELECT date_format(business_date, 'yyyy-MM') AS month, count(*) AS tickets, sum(net_sales) AS net_sales
  FROM mv_tickets GROUP BY 1
),
actual AS (
  SELECT '1. Files' AS section, 'files_in_raw_volume' AS metric, 'Total' AS dimension, CAST(files AS DOUBLE) AS actual_value FROM raw_files
  UNION ALL SELECT '1. Files', 'bytes_in_raw_volume', 'Total', bytes FROM raw_files
  UNION ALL SELECT '1. Files', 'files_with_rows_in_bronze', 'Total', files FROM bronze
  UNION ALL SELECT '1. Files', 'header_only_files', 'Total', (SELECT files FROM raw_files) - (SELECT files FROM bronze)
  UNION ALL SELECT '2. Rows', 'bronze_rows', 'Total', row_count FROM bronze
  UNION ALL SELECT '2. Rows', 'silver_clean_plus_quarantine_rows', 'Total', row_count FROM silver
  UNION ALL SELECT '2. Rows', 'quarantined_rows', 'Total', quarantined FROM silver
  UNION ALL SELECT '3. Coverage', 'business_days', 'Total', days FROM coverage
  UNION ALL SELECT '3. Coverage', 'first_business_date', 'Total', CAST(date_format(first_day, 'yyyyMMdd') AS DOUBLE) FROM coverage
  UNION ALL SELECT '3. Coverage', 'last_business_date', 'Total', CAST(date_format(last_day, 'yyyyMMdd') AS DOUBLE) FROM coverage
  UNION ALL SELECT '3. Coverage', 'stores_with_sales', 'Total', stores FROM coverage
  UNION ALL SELECT '3. Coverage', 'distinct_product_names', 'Total', n FROM products
  UNION ALL SELECT '3. Coverage', 'first_new_pos_date', 'Total', CAST(date_format(first_new, 'yyyyMMdd') AS DOUBLE) FROM coverage
  UNION ALL SELECT '3. Coverage', 'last_legacy_pos_date', 'Total', CAST(date_format(last_legacy, 'yyyyMMdd') AS DOUBLE) FROM coverage
  UNION ALL
  SELECT '4. Tickets', s.metric, t.dimension, s.value
  FROM tickets t
  LATERAL VIEW stack(12,
    'lines', CAST(lines AS DOUBLE), 'tickets', CAST(tickets AS DOUBLE), 'units', CAST(units AS DOUBLE),
    'gross_sales', gross_sales, 'discounts', discounts, 'net_sales', net_sales, 'tax', tax, 'collected', collected,
    'voided_amount', voided_amount, 'tickets_reconciled', CAST(tickets_reconciled AS DOUBLE),
    'zero_total_tickets', CAST(zero_total_tickets AS DOUBLE), 'tickets_with_discount', CAST(tickets_with_discount AS DOUBLE)
  ) s AS metric, value
  UNION ALL
  SELECT '5. Monthly', s.metric, m.month, s.value
  FROM monthly m
  LATERAL VIEW stack(2, 'tickets', CAST(tickets AS DOUBLE), 'net_sales', net_sales) s AS metric, value
),
comparison AS (
  SELECT
    coalesce(e.section, a.section)                                   AS section,
    coalesce(e.metric, a.metric)                                     AS metric,
    coalesce(e.dimension, a.dimension)                               AS dimension,
    CAST(a.actual_value AS DECIMAL(22, 2))                           AS pipeline_value,
    CAST(e.expected_value AS DECIMAL(22, 2))                         AS eda_value,
    CAST(a.actual_value - e.expected_value AS DECIMAL(22, 2))        AS difference,
    CASE
      WHEN a.actual_value IS NULL OR e.expected_value IS NULL       THEN 'MISSING'
      WHEN abs(a.actual_value - e.expected_value) <= e.tolerance    THEN 'MATCH'
      ELSE 'MISMATCH'
    END                                                              AS status
  FROM expected e
  FULL OUTER JOIN actual a
    ON e.section = a.section AND e.metric = a.metric AND e.dimension = a.dimension
)
SELECT *
FROM comparison
ORDER BY CASE status WHEN 'MISMATCH' THEN 0 WHEN 'MISSING' THEN 1 ELSE 2 END, section, dimension, metric;
