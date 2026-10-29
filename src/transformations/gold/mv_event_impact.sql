-- Gold: how much sports exposure moves sales. Each store-day is tagged with the
-- exposure types it had and compared with the same weekday of the 4 previous
-- weeks. Lifts are ratios of sums (actual / baseline - 1 over all store-days of
-- a group), so small baselines (new stores, partial days) cannot dominate the
-- result; the median lift is kept as a robustness check. Days with no relevant
-- game are the control: `incremental_lift_vs_no_game` isolates the game effect
-- from seasonality.
CREATE OR REFRESH MATERIALIZED VIEW mv_event_impact
COMMENT 'Gold. Net-sales and ticket lift (ratio of sums and median) by sports exposure type, market and day type, against same-weekday baselines, with the no-game control.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH tagged AS (
  SELECT
    store_id,
    business_date,
    market,
    is_weekend,
    net_sales,
    baseline_net_sales_4w,
    tickets,
    baseline_tickets_4w,
    net_sales_lift_vs_baseline AS lift,
    filter(array(
      CASE WHEN national_marquee_event THEN 'National marquee (NFL playoffs, Super Bowl, finals)' END,
      CASE WHEN regional_postseason_game THEN 'Regional team in postseason' END,
      CASE WHEN regional_nfl_games > 0 THEN 'Regional NFL game' END,
      CASE WHEN regional_ncaaf_games > 0 THEN 'Regional college football game' END,
      CASE WHEN regional_nba_games > 0 THEN 'Regional NBA game' END,
      CASE WHEN regional_nhl_games > 0 THEN 'Regional NHL game' END,
      CASE WHEN regional_baseball_games > 0 THEN 'Regional MLB / MiLB game' END,
      CASE WHEN regional_mls_games > 0 THEN 'Regional MLS game' END,
      CASE WHEN local_venue_games > 0 THEN 'Game at a venue within the local radius' END
    ), x -> x IS NOT NULL) AS exposures,
    holiday_name IS NOT NULL AS is_holiday
  FROM mv_daily_store_sales
  WHERE baseline_net_sales_4w IS NOT NULL
),
exploded AS (
  SELECT
    *,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    explode(CASE WHEN size(exposures) = 0 THEN array('No relevant game') ELSE exposures END) AS exposure_type
  FROM tagged
  WHERE NOT is_holiday   -- holidays are measured separately; keep the game effect clean
),
summary AS (
  SELECT
    exposure_type,
    market,
    day_type,
    count(*)                                                AS store_days,
    count(DISTINCT business_date)                           AS dates,
    sum(net_sales) / sum(baseline_net_sales_4w) - 1         AS net_sales_lift,
    percentile_approx(lift, 0.5)                            AS median_net_sales_lift,
    sum(tickets) / sum(baseline_tickets_4w) - 1             AS tickets_lift
  FROM exploded
  GROUP BY GROUPING SETS ((exposure_type, market, day_type), (exposure_type, day_type), (exposure_type))
)
SELECT
  s.exposure_type,
  coalesce(s.market, 'All markets')                        AS market,
  coalesce(s.day_type, 'All days')                         AS day_type,
  s.store_days,
  s.dates,
  s.net_sales_lift,
  s.median_net_sales_lift,
  s.tickets_lift,
  s.net_sales_lift - c.net_sales_lift                      AS incremental_lift_vs_no_game,
  s.tickets_lift - c.tickets_lift                          AS incremental_tickets_lift_vs_no_game
FROM summary s
LEFT JOIN summary c
  ON c.exposure_type = 'No relevant game'
 AND c.market <=> s.market
 AND c.day_type <=> s.day_type;
