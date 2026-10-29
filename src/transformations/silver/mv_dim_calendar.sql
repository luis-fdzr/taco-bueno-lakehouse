-- Silver: calendar dimension 2024-2027 with US holidays, retail and food
-- moments, school periods and pay-day windows. Rule-based so it extends to any
-- future date (needed by the forecasts). Easter is the only listed date.
CREATE OR REFRESH MATERIALIZED VIEW mv_dim_calendar
COMMENT 'Silver. One row per date (2024-2027): weekday, holiday name and type, school period, pay-day window and Taco Tuesday.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH days AS (
  SELECT explode(sequence(DATE'2024-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS calendar_date
),
base AS (
  SELECT
    calendar_date,
    year(calendar_date)                         AS year,
    quarter(calendar_date)                      AS quarter,
    month(calendar_date)                        AS month,
    date_format(calendar_date, 'MMMM')          AS month_name,
    weekofyear(calendar_date)                   AS week_of_year,
    day(calendar_date)                          AS day_of_month,
    dayofweek(calendar_date)                    AS day_of_week,      -- 1 = Sunday ... 7 = Saturday
    date_format(calendar_date, 'EEEE')          AS day_name,
    dayofweek(calendar_date) IN (1, 7)          AS is_weekend
  FROM days
),
labeled AS (
  SELECT
    *,
    CASE
      WHEN month = 1  AND day_of_month = 1 THEN 'New Year''s Day'
      WHEN month = 1  AND day_of_week = 2 AND day_of_month BETWEEN 15 AND 21 THEN 'Martin Luther King Jr. Day'
      WHEN month = 2  AND day_of_week = 1 AND day_of_month BETWEEN 8 AND 14 THEN 'Super Bowl Sunday'
      WHEN month = 2  AND day_of_month = 14 THEN 'Valentine''s Day'
      WHEN month = 2  AND day_of_week = 2 AND day_of_month BETWEEN 15 AND 21 THEN 'Presidents'' Day'
      WHEN month = 3  AND day_of_month = 17 THEN 'St. Patrick''s Day'
      WHEN calendar_date IN (DATE'2024-03-31', DATE'2025-04-20', DATE'2026-04-05', DATE'2027-03-28') THEN 'Easter Sunday'
      WHEN month = 5  AND day_of_month = 5 THEN 'Cinco de Mayo'
      WHEN month = 5  AND day_of_week = 1 AND day_of_month BETWEEN 8 AND 14 THEN 'Mother''s Day'
      WHEN month = 5  AND day_of_week = 2 AND day_of_month >= 25 THEN 'Memorial Day'
      WHEN month = 6  AND day_of_week = 1 AND day_of_month BETWEEN 15 AND 21 THEN 'Father''s Day'
      WHEN month = 6  AND day_of_month = 19 THEN 'Juneteenth'
      WHEN month = 7  AND day_of_month = 4 THEN 'Independence Day'
      WHEN month = 9  AND day_of_week = 2 AND day_of_month <= 7 THEN 'Labor Day'
      WHEN month = 10 AND day_of_month = 4 THEN 'National Taco Day'
      WHEN month = 10 AND day_of_month = 31 THEN 'Halloween'
      WHEN month = 11 AND day_of_month = 11 THEN 'Veterans Day'
      WHEN month = 11 AND day_of_week = 5 AND day_of_month BETWEEN 22 AND 28 THEN 'Thanksgiving Day'
      WHEN month = 11 AND day_of_week = 6 AND day_of_month BETWEEN 23 AND 29 THEN 'Black Friday'
      WHEN month = 12 AND day_of_month = 24 THEN 'Christmas Eve'
      WHEN month = 12 AND day_of_month = 25 THEN 'Christmas Day'
      WHEN month = 12 AND day_of_month = 31 THEN 'New Year''s Eve'
    END AS holiday_name
  FROM base
)
SELECT
  *,
  CASE
    WHEN holiday_name IN ('New Year''s Day', 'Martin Luther King Jr. Day', 'Presidents'' Day', 'Memorial Day',
                          'Juneteenth', 'Independence Day', 'Labor Day', 'Veterans Day', 'Thanksgiving Day',
                          'Christmas Day') THEN 'Federal holiday'
    WHEN holiday_name IN ('Super Bowl Sunday') THEN 'Sports moment'
    WHEN holiday_name IN ('Cinco de Mayo', 'National Taco Day') THEN 'Food moment'
    WHEN holiday_name IS NOT NULL THEN 'Retail / family moment'
  END                                                             AS holiday_type,
  CASE
    WHEN (month = 12 AND day_of_month >= 20) OR (month = 1 AND day_of_month <= 3) THEN 'Winter break'
    WHEN month = 3 AND day_of_month BETWEEN 9 AND 22 THEN 'Spring break window'
    WHEN month IN (6, 7) OR (month = 8 AND day_of_month <= 15) THEN 'Summer break'
    ELSE 'School in session'
  END                                                             AS school_period,
  day_of_month IN (1, 2, 3, 15, 16, 17)                           AS is_payday_window,
  day_of_week = 3                                                 AS is_taco_tuesday
FROM labeled;
