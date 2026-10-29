-- Gold: one row per store and business day. The main analytical and ML table:
-- sales, traffic, channel and daypart mix, calendar context, sports exposure,
-- and a same-weekday baseline to measure lift.
CREATE OR REFRESH MATERIALIZED VIEW mv_daily_store_sales (
  CONSTRAINT store_has_market EXPECT (market <> 'Unknown'),
  CONSTRAINT tickets_positive EXPECT (tickets > 0),
  CONSTRAINT day_reconciles EXPECT (tickets_reconciled >= 0.99 * tickets)
)
COMMENT 'Gold. Store x business day: tickets, net sales, average ticket, channel and daypart mix, holiday/school/pay-day context, sports exposure and lift vs the same weekday of the 4 previous weeks.'
CLUSTER BY (business_date, store_id)
TBLPROPERTIES ('quality' = 'gold')
AS
WITH sales AS (
  SELECT
    business_date,
    store_id,
    max(pos_system)                                                             AS pos_system,
    count(*)                                                                    AS tickets,
    sum(net_sales)                                                              AS net_sales,
    sum(gross_sales)                                                            AS gross_sales,
    sum(check_discount)                                                         AS discounts,
    sum(check_tax)                                                              AS tax,
    sum(collected)                                                              AS collected,
    sum(units)                                                                  AS units,
    count_if(has_discount)                                                      AS tickets_with_discount,
    count_if(reconciles)                                                        AS tickets_reconciled,
    sum(CASE WHEN channel_group = 'Drive-thru' THEN net_sales ELSE 0 END)         AS net_sales_drive_thru,
    sum(CASE WHEN channel_group = 'In-store' THEN net_sales ELSE 0 END)           AS net_sales_in_store,
    sum(CASE WHEN channel_group = 'Delivery & digital' THEN net_sales ELSE 0 END) AS net_sales_delivery,
    count_if(channel_group = 'Drive-thru')                                       AS tickets_drive_thru,
    count_if(channel_group = 'In-store')                                         AS tickets_in_store,
    count_if(channel_group = 'Delivery & digital')                               AS tickets_delivery,
    sum(CASE WHEN daypart = 'Breakfast' THEN net_sales ELSE 0 END)                AS net_sales_breakfast,
    sum(CASE WHEN daypart = 'Lunch' THEN net_sales ELSE 0 END)                    AS net_sales_lunch,
    sum(CASE WHEN daypart = 'Afternoon' THEN net_sales ELSE 0 END)                AS net_sales_afternoon,
    sum(CASE WHEN daypart = 'Dinner' THEN net_sales ELSE 0 END)                   AS net_sales_dinner,
    sum(CASE WHEN daypart = 'Late night' THEN net_sales ELSE 0 END)               AS net_sales_late_night
  FROM mv_tickets
  GROUP BY business_date, store_id
),
events AS (
  SELECT
    store_id,
    event_date,
    count_if(is_local_venue)                                                     AS local_venue_games,
    count_if(involves_regional_team)                                             AS regional_team_games,
    count_if(involves_regional_team AND league = 'NFL')                          AS regional_nfl_games,
    count_if(involves_regional_team AND league = 'NCAAF')                        AS regional_ncaaf_games,
    count_if(involves_regional_team AND league = 'NBA')                          AS regional_nba_games,
    count_if(involves_regional_team AND league IN ('MLB', 'MiLB'))               AS regional_baseball_games,
    count_if(involves_regional_team AND league = 'NHL')                          AS regional_nhl_games,
    count_if(involves_regional_team AND league = 'MLS')                          AS regional_mls_games,
    max(involves_regional_team AND season_type = 'postseason')                   AS regional_postseason_game,
    max(is_national_marquee)                                                     AS national_marquee_event,
    min(CASE WHEN involves_regional_team THEN start_hour_ct END)                 AS first_regional_game_hour_ct,
    array_join(slice(array_sort(collect_set(
      CASE WHEN involves_regional_team OR is_national_marquee THEN short_name END)), 1, 6), ', ') AS event_labels
  FROM mv_store_event_exposure
  GROUP BY store_id, event_date
),
joined AS (
  SELECT
    s.*,
    coalesce(d.market, 'Unknown')                    AS market,
    d.city,
    d.state,
    d.store_status,
    c.day_name,
    c.day_of_week,
    c.is_weekend,
    c.week_of_year,
    c.month,
    c.holiday_name,
    c.holiday_type,
    c.school_period,
    c.is_payday_window,
    c.is_taco_tuesday,
    coalesce(e.local_venue_games, 0)                 AS local_venue_games,
    coalesce(e.regional_team_games, 0)               AS regional_team_games,
    coalesce(e.regional_nfl_games, 0)                AS regional_nfl_games,
    coalesce(e.regional_ncaaf_games, 0)              AS regional_ncaaf_games,
    coalesce(e.regional_nba_games, 0)                AS regional_nba_games,
    coalesce(e.regional_baseball_games, 0)           AS regional_baseball_games,
    coalesce(e.regional_nhl_games, 0)                AS regional_nhl_games,
    coalesce(e.regional_mls_games, 0)                AS regional_mls_games,
    coalesce(e.regional_postseason_game, false)      AS regional_postseason_game,
    coalesce(e.national_marquee_event, false)        AS national_marquee_event,
    e.first_regional_game_hour_ct,
    e.event_labels
  FROM sales s
  LEFT JOIN mv_dim_store d    ON d.store_id = s.store_id
  LEFT JOIN mv_dim_calendar c ON c.calendar_date = s.business_date
  LEFT JOIN events e          ON e.store_id = s.store_id AND e.event_date = s.business_date
)
SELECT
  *,
  net_sales / nullif(tickets, 0)                                         AS avg_ticket,
  units / nullif(tickets, 0)                                             AS units_per_ticket,
  discounts / nullif(gross_sales, 0)                                     AS discount_rate,
  net_sales_delivery / nullif(net_sales, 0)                              AS delivery_share,
  avg(net_sales) OVER w4                                                 AS baseline_net_sales_4w,
  avg(tickets) OVER w4                                                   AS baseline_tickets_4w,
  net_sales / nullif(avg(net_sales) OVER w4, 0) - 1                      AS net_sales_lift_vs_baseline,
  tickets / nullif(avg(tickets) OVER w4, 0) - 1                          AS tickets_lift_vs_baseline
FROM joined
WINDOW w4 AS (PARTITION BY store_id, day_of_week ORDER BY business_date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING);
