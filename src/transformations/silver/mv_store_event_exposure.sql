-- Silver: which games each store is exposed to, and how.
--
--   is_local_venue          the game is played within taco.local_radius_km of the store
--                           (stadium traffic, local buzz)
--   involves_regional_team  the home or away team is based within taco.regional_radius_km
--                           (fans watching at home or at a bar, wherever the game is played)
--   is_national_marquee     NFL playoffs, Super Bowl, World Series, NBA Finals, Stanley Cup
--                           Final, national championships (every store is exposed)
--
-- Distances use the haversine formula on store and venue/team-home coordinates.
CREATE OR REFRESH MATERIALIZED VIEW mv_store_event_exposure
COMMENT 'Silver. One row per store and relevant game, with distances to the venue and to each team home, and exposure flags.'
CLUSTER BY (event_date, store_id)
TBLPROPERTIES ('quality' = 'silver')
AS
WITH stores AS (
  SELECT store_id, market, latitude, longitude
  FROM mv_dim_store
  WHERE has_sales AND latitude IS NOT NULL
),
games AS (
  SELECT
    e.event_id, e.event_date_ct AS event_date, e.league, e.division, e.sport, e.level, e.season_type,
    e.event_name, e.short_name, e.headline, e.start_hour_ct, e.national_tv, e.completed, e.winner,
    e.is_national_marquee, e.home_team_name, e.away_team_name,
    e.venue_name, e.venue_city, e.venue_state, e.venue_latitude, e.venue_longitude,
    h.home_latitude AS home_team_latitude, h.home_longitude AS home_team_longitude,
    a.home_latitude AS away_team_latitude, a.home_longitude AS away_team_longitude
  FROM st_sports_events e
  LEFT JOIN mv_dim_team h ON e.league = h.league AND e.home_team_id = h.team_id
  LEFT JOIN mv_dim_team a ON e.league = a.league AND e.away_team_id = a.team_id
  WHERE coalesce(e.status, '') NOT IN ('STATUS_POSTPONED', 'STATUS_CANCELED', 'Postponed', 'Cancelled')
),
pairs AS (
  SELECT
    s.store_id,
    s.market,
    g.*,
    2 * 6371.0088 * asin(sqrt(
      pow(sin(radians(g.venue_latitude - s.latitude) / 2), 2)
      + cos(radians(s.latitude)) * cos(radians(g.venue_latitude))
        * pow(sin(radians(g.venue_longitude - s.longitude) / 2), 2)))          AS venue_distance_km,
    2 * 6371.0088 * asin(sqrt(
      pow(sin(radians(g.home_team_latitude - s.latitude) / 2), 2)
      + cos(radians(s.latitude)) * cos(radians(g.home_team_latitude))
        * pow(sin(radians(g.home_team_longitude - s.longitude) / 2), 2)))      AS home_team_distance_km,
    2 * 6371.0088 * asin(sqrt(
      pow(sin(radians(g.away_team_latitude - s.latitude) / 2), 2)
      + cos(radians(s.latitude)) * cos(radians(g.away_team_latitude))
        * pow(sin(radians(g.away_team_longitude - s.longitude) / 2), 2)))      AS away_team_distance_km
  FROM stores s
  CROSS JOIN games g
),
flagged AS (
  SELECT
    *,
    coalesce(venue_distance_km <= ${taco.local_radius_km}, false)                AS is_local_venue,
    coalesce(home_team_distance_km <= ${taco.regional_radius_km}, false)
      OR coalesce(away_team_distance_km <= ${taco.regional_radius_km}, false)    AS involves_regional_team,
    CASE
      WHEN home_team_distance_km <= ${taco.regional_radius_km} THEN 'home'
      WHEN away_team_distance_km <= ${taco.regional_radius_km} THEN 'away'
    END                                                                          AS regional_team_side
  FROM pairs
)
SELECT *
FROM flagged
WHERE is_local_venue OR involves_regional_team OR is_national_marquee;
