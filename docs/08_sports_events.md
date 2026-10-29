# Sports events: sources and store matching

Sports calendars are joined to the sales data to measure, and forecast, how games move traffic. The code is in [`src/ingestion/sports_events.py`](../src/ingestion/sports_events.py); matching happens in the pipeline (`mv_dim_team`, `mv_store_event_exposure`).

## Sources

| League | Source | Access | Granularity fetched | Notes |
|---|---|---|---|---|
| NFL | ESPN site API (`site.api.espn.com`) | unofficial, free | per week and season type (pre, regular, post) | Super Bowl and playoff rounds are in `headline` |
| NBA | ESPN site API | unofficial, free | per day (Oct-Jun) | playoff series and NBA Finals in `headline` |
| MLS | ESPN site API | unofficial, free | per day (Feb-Dec) | ESPN writes "City, State" in the city field; fixed in the parser |
| NCAA football (FBS + FCS) | ESPN site API | unofficial, free | per week, `groups=80` (FBS) and `81` (FCS) | **CollegeFootballData** needs an API key; ESPN covers the same games without one. FBS-vs-FCS games appear in both lists and are de-duplicated |
| MLB, Triple-A, Double-A | MLB Stats API (`statsapi.mlb.com`) | public | one request per year per level | includes venue coordinates; minor leagues add local teams in store cities (Tulsa Drillers, OKC Comets, Frisco RoughRiders...) |
| NHL | NHL API (`api-web.nhle.com`) | public | per week | only the arena name is published; home cities come from a 32-team reference table in the code |

ESPN no longer accepts date ranges on the scoreboard endpoint (`dates=YYYYMMDD-YYYYMMDD` returns HTTP 400), so the client iterates days or weeks with 4 parallel workers, retries with back-off and an on-disk cache for windows that ended more than 10 days ago.

**Coverage of the backfill (2025-01-01 to 2026-12-31): 25,546 games.**

| League | Games |
|---|---:|
| MLB | 5,923 |
| MiLB (AAA + AA) | 8,697 |
| NHL | 3,052 |
| NBA | 2,793 |
| NCAAF (FBS + FCS) | 3,370 |
| MLS | 1,055 |
| NFL | 656 |

131 games (0.5%) have no venue coordinates: international venues (London, Mexico City, Dublin...), games without an assigned venue yet, and a few far-away FCS games. They are flagged by the `us_venue_has_coordinates` expectation.

## Normalized schema

One record per game, whatever the source: `event_id` (source-scoped), league, division, sport, level, season, season type, start time (UTC) and official local date, status, completed flag, neutral site, venue name/city/state/country/coordinates, home and away team id/name/abbreviation, scores, winner, national TV flag, broadcasts, attendance, headline (e.g. "Super Bowl LX", "West Finals - Game 7") and `fetched_at`.

## Geocoding

* **Stores**: the business file (`city.xlsx`) has only `Store #` and `City`. Cities are geocoded with the Open-Meteo geocoding API (free, no key), restricted to the Taco Bueno footprint states. When the same city name exists in several of them, the candidate is ranked by population x state weight (TX/OK = 1.0, other states = 0.4). Only 3 of 140 stores remain ambiguous and are flagged: Kansas City (MO chosen), Shawnee (OK chosen; the store closed in April 2025) and Weatherford (TX chosen).
* **Venues**: MLB provides coordinates; ESPN and NHL venues are geocoded by city + state (US and Canada).

## Matching games to stores

| Concept | Definition | Variable |
|---|---|---|
| Team home | venue where the team played most of its regular-season, non-neutral home games (`mv_dim_team`) | none |
| Local venue game | venue within **50 km** of the store (stadium traffic, local buzz) | `local_radius_km` |
| Regional team game | home or away team based within **320 km** of the store, played anywhere (fans watching from home or a bar) | `regional_radius_km` |
| National marquee | NFL playoffs, Super Bowl, World Series, NBA Finals, Stanley Cup Final, national championships; every store is exposed | in `st_sports_events` |

Distances use the haversine formula. With these radii, Dallas-Fort Worth stores follow the Cowboys, Rangers, Mavericks, Stars, FC Dallas, TCU, SMU and North Texas. Oklahoma stores follow the Thunder, the Sooners and the Cowboys of Oklahoma State, plus Tulsa and the local minor-league teams. Houston-area stores follow the Texans, Astros, Rockets and Dynamo.

`mv_daily_store_sales` aggregates the exposure per store and day (games by league, postseason, marquee, first game hour, labels). `mv_event_impact` measures lift against a same-weekday baseline with no-game days as the control. The forecasting models use the share of stores exposed to each kind of game as features, so known future schedules (NFL, college football, NBA, NHL) shape the forecast.

## Refresh

* **Backfill** (once, from a laptop or a job run): `run_ingestion.py --mode backfill --start 2025-01-01 --end 2026-12-31`.
* **Daily** (job task): `--mode incremental` re-fetches the last 14 days (results, postponements) and the next 120 days (schedules). Each run writes a new batch folder; AUTO CDC keeps the latest version of each game.
