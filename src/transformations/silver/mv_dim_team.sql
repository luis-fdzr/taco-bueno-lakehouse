-- Silver: team dimension derived from the games themselves. A team's home is the
-- venue where it played most of its regular-season, non-neutral home games, so
-- stadium moves and temporary venues are handled without reference files.
CREATE OR REFRESH MATERIALIZED VIEW mv_dim_team (
  CONSTRAINT home_has_coordinates EXPECT (home_latitude IS NOT NULL)
)
COMMENT 'Silver. One row per team (league + team id) with its home venue, city, state and coordinates.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH home_games AS (
  SELECT
    league,
    home_team_id                 AS team_id,
    max(home_team_name)          AS team_name,
    max(home_team_abbrev)        AS team_abbrev,
    venue_name,
    venue_city,
    venue_state,
    avg(venue_latitude)          AS venue_latitude,
    avg(venue_longitude)         AS venue_longitude,
    count(*)                     AS home_games
  FROM st_sports_events
  WHERE NOT coalesce(neutral_site, false)
    AND season_type = 'regular'
    AND venue_latitude IS NOT NULL
  GROUP BY league, home_team_id, venue_name, venue_city, venue_state
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY league, team_id ORDER BY home_games DESC, venue_name) AS rn
  FROM home_games
)
SELECT
  league,
  team_id,
  team_name,
  team_abbrev,
  venue_name       AS home_venue,
  venue_city       AS home_city,
  venue_state      AS home_state,
  venue_latitude   AS home_latitude,
  venue_longitude  AS home_longitude,
  home_games       AS home_games_observed
FROM ranked
WHERE rn = 1;
