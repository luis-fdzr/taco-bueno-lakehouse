# Data quality checks

Quality is enforced at four levels:

1. **Expectations** inside the pipeline: drop, warn, quarantine.
2. **A quality scorecard table**, `mv_data_quality_daily`.
3. **A gate task** that can stop the job.
4. **An independent reconciliation** against a separate implementation.

Every number below comes from the full run over the 597 raw files.

## 1. Issues found in the raw data and how the pipeline handles them

| Issue | Size | Detection | Handling |
|---|---|---|---|
| **Missing export days** | 41 business days with no data, plus 1 partial day (2025-11-26: 2 stores, 36 tickets) out of 614 calendar days | `mv_data_quality_daily.day_status` (one row per calendar day, so gaps are rows) | Flagged `Missing file` / `Partial`; excluded from model training; the ingestion picks the day up if the file is delivered later |
| **Header-only files** | 24 of 597 files (all of 2025-01-01..22, 2025-12-25, 2026-01-25) | bronze vs volume file count (validation query) | Produce no rows; their business days show as missing |
| **Reused check numbers** | 72 check numbers reused on the same day and store by the legacy POS (Jul 14-15 and Oct 25-26, 2025) | Found by reconciling two implementations; tracked as `reused_check_numbers` | Ticket key includes the check start time; expectation `single_header_per_ticket`: **0 violations** |
| **Two POS systems with different semantics** | Legacy until 2026-01-19, new from 2025-11-04 | `pos_system` derived from the channel code | Harmonized formulas for units, line amounts and ticket totals (see [03](03_transformations_and_tables.md)) |
| **Ticket header vs lines mismatch** | 18,467 of 13,788,668 tickets (0.13%): 10,376 zero-total tickets (comped or fully voided orders whose lines still carry prices), 4,551 off by less than $1, 3,540 off by $1 or more (mostly new POS) | expectation `ticket_reconciles` (warn), store-day rule `day_reconciles` (1,429 store-days below 99%) | Kept and flagged: the header total is what the store collected, so it remains the revenue figure |
| **Floating-point noise** | 4 tickets flipped between implementations at the 2-cent threshold | Pipeline vs EDA reconciliation | Reconciliation compares in whole cents |
| **Three column layouts** (19/21/22 columns) | 167 / 345 / 85 files | Header inspection during EDA; `no_rescued_data` expectation (0 violations) | Positional all-STRING schema in bronze |
| **Quoted text with doubled quotes** | e.g. `6"" Flour Tortillas - Side""` | Row counts per file matched exactly only with RFC 4180 escaping | `escape='"'` in Auto Loader |
| **Invalid dates, stores, check numbers or amounts** | 0 rows | `expect_all_or_drop` on 4 rules + `st_orders_quarantine` | Dropped from silver, kept in quarantine with `dq_failed_rules` (empty today; the mechanism is tested by the rule definitions) |
| **Date in file name is the export date** | 100% of lines | expectation `business_date_is_day_before_file` (0 violations) | `business_date` comes from the data; `file_date` kept for lineage |
| **Lines without a product name** | 62,387 legacy lines (modifiers) | expectation `named_product_or_modifier` (warn) | Labeled `(unnamed modifier)` and classified as modifiers |
| **Negative net sales** | 714 tickets, -$3,125 in total (711 legacy refunds) | expectation `net_sales_not_negative` (warn) | Kept: they are real refunds and must net out of revenue |
| **Implausible menu prices** | 3 lines outside [-$100, $500] | expectation `menu_price_plausible` (warn) | Kept and flagged |
| **Empty columns** | `Time Card`, `Order Voided`, `Menu Item Ext Ref`, `Total Rounding`: 100% null or zero | EDA column profile | Kept in bronze, not carried to silver |
| **Stores missing from the city file** | 11 stores with sales (4.75% of net sales), mostly opened after Oct 2025 | expectation `store_with_sales_has_location` (11), store-day rule `store_has_market` (3,432 store-days) | Market `Unknown`; no sports features; gate warning `stores_without_location` |
| **Ambiguous city names** | 3 stores (Kansas City, Shawnee, Weatherford) | geocoder candidates; expectation `city_state_is_unambiguous` | State chosen by population x market weight, flagged for review |
| **Listed stores without sales** | 32 stores in the city file | FULL OUTER JOIN in `mv_dim_store` | `store_status = 'No sales in period'` |
| **Unclassified products** | 8 rare product keys | expectation `has_category` | Category `Unclassified` |
| **Unparseable timestamps in a feed** | 7,874 ESPN games (minutes without seconds) were dropped in the first run | expectation `has_start_time` on `st_sports_events` | Parser fixed (explicit patterns); after a full refresh of the table: 0 violations |
| **Duplicate games across feeds / fetch windows** | FBS-vs-FCS games listed twice; overlapping daily windows | `event_id` uniqueness | De-duplicated at ingestion; AUTO CDC keeps the latest version per game |
| **Games without venue coordinates** | 103 games (international venues, venue not assigned yet) | expectation `us_venue_has_coordinates` (warn) | Excluded from distance matching only |

## 2. Expectations

Defined next to the transformation they protect (`@dp.expect*` in Python, `CONSTRAINT ... EXPECT` in SQL). Results of the latest full run:

| Table | Expectation | Policy | Passed | Failed |
|---|---|---|---:|---:|
| `st_pos_lines_raw` | `file_name_matches_contract`, `no_rescued_data` | warn | 85,273,331 | 0 |
| `st_orders_cleaned` | `valid_business_date`, `valid_store_id`, `has_check_number`, `amounts_parse` | **drop + quarantine** | 85,273,331 | 0 |
| `st_orders_cleaned` | `business_date_is_day_before_file`, `known_transaction_type`, `known_channel`, `quantity_not_negative`, `begin_time_parses` | warn | 85,273,331 | 0 |
| `st_orders_cleaned` | `menu_price_plausible` | warn | 85,273,328 | 3 |
| `st_orders_cleaned` | `named_product_or_modifier` | warn | 85,210,944 | 62,387 |
| `st_sports_events` | `has_event_id`, `has_start_time`, `has_event_date` | **drop** | 25,546 | 0 |
| `st_sports_events` | `us_venue_has_coordinates` | warn | 25,443 | 103 |
| `mv_tickets` | `single_header_per_ticket` | warn | 13,788,668 | 0 |
| `mv_tickets` | `ticket_reconciles` | warn | 13,770,201 | 18,467 |
| `mv_tickets` | `net_sales_not_negative` | warn | 13,787,954 | 714 |
| `mv_dim_store` | `store_with_sales_has_location` / `city_state_is_unambiguous` | warn | 140 / 148 | 11 / 3 |
| `mv_dim_product` | `has_category` | warn | 588 | 8 |
| `mv_daily_store_sales` | `tickets_positive` / `day_reconciles` / `store_has_market` | warn | 63,868 / 62,439 / 60,436 | 0 / 1,429 / 3,432 |
| `mv_daily_product_demand`, `mv_product_price_history`, `mv_dim_team` | non-negative units, positive prices, team coordinates | warn | all | 0 |

The metrics are in the pipeline event log, published as the table `pipeline_event_log`:

```sql
WITH exp AS (
  SELECT explode(from_json(details:flow_progress.data_quality.expectations,
         'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM pipeline_event_log
  WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
)
SELECT e.dataset, e.name, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM exp GROUP BY 1, 2 ORDER BY failed DESC;
```

## 3. Quality scorecard: `mv_data_quality_daily`

One row per calendar day between the first and last business date, so a missing export is a visible row, not an absence. Columns: files, raw / clean / quarantined / non-sale rows, tickets, stores reporting vs expected (stores active that day), reconciliation rate, zero-total tickets, inconsistent headers, reused check numbers, net sales and `day_status`.

| Day status | Days | Tickets |
|---|---:|---:|
| Complete | 572 | 13,788,632 |
| Missing file | 41 | 0 |
| Partial | 1 | 36 |

Missing days cluster around holidays (Christmas week 2025: Dec 22-30) and in pairs (e.g. Mar 27-28 2025, Jul 7-8 2025, Aug 25-26 2026), which points to the export process rather than store closures.

## 4. Quality gate task

`src/quality/data_quality_gate.py` runs after every pipeline update. Error-level checks fail the job before the ML tasks publish anything; warnings are only logged. Every result is appended to `dq_gate_results`.

| Check | Severity | Rule |
|---|---|---|
| `reconciliation_rate_7d` | error | >= 99.5% of tickets reconcile in the last 7 days of data |
| `quarantine_share_7d` | error | <= 0.5% of raw rows quarantined |
| `tickets_with_inconsistent_header` | error | = 0 (protects the ticket key) |
| `latest_day_complete` | warn | latest day has `day_status = Complete` |
| `data_freshness_days` | warn | latest business date <= 3 days old |
| `missing_days_30d` | warn | no missing export day in the last 30 days |
| `stores_without_location` | warn | every store with sales is geocoded |
| `sports_events_age_hours` | warn | sports calendar refreshed in the last 48 h |
| `expectations_with_failures_last_update` | warn | lists expectations with failures in the latest pipeline update |

Result of the verified job run (`dq_gate_results`):

| Check | Value | Status |
|---|---|---|
| `reconciliation_rate_7d` | 99.92% | PASS |
| `quarantine_share_7d` | 0 | PASS |
| `tickets_with_inconsistent_header` | 0 | PASS |
| `latest_day_complete` | 2026-09-27 complete | PASS |
| `missing_days_30d` | 0 | PASS |
| `sports_events_age_hours` | 0.1 h | PASS |
| `data_freshness_days` | 31 (historical dataset ends 2026-09-27) | WARN |
| `stores_without_location` | 11 | WARN |
| `expectations_with_failures_last_update` | 8 (warn-level rules listed above) | WARN |

## 5. Independent reconciliation

[`tests/validation/validate_pipeline_vs_eda.sql`](../tests/validation/validate_pipeline_vs_eda.sql) compares the pipeline with a separate pandas implementation ([`tools/eda/eda_taco_bueno.py`](../tools/eda/eda_taco_bueno.py)) that reads the same raw files on a laptop. It checks files and bytes, rows per layer, coverage dates, stores, products, POS migration dates, tickets, units, gross/net/collected sales, tax, discounts, voids and reconciliation counts by POS system, plus tickets and net sales for each of the 21 months.

**Result: 91 of 91 metrics match exactly** (money to the cent): 13,788,668 tickets and $181,707,415.06 of net sales.

The reconciliation itself found two defects that both implementations shared or disagreed on:
1. The reused check numbers. The first comparison differed by about $334 in two months, and the investigation traced it to the 72 pairs.
2. The 2-cent rounding threshold.

Both are now fixed in both implementations.
