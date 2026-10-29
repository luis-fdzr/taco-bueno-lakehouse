import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.sports_events import (  # noqa: E402
    LEAGUES, _winner, espn_requests, parse_espn_event, parse_mlb_game, parse_nhl_game,
)


def espn_event(city="Arlington", state="TX", home_score="31", away_score="24", completed=True, national=True):
    return {
        "id": "401772510",
        "date": "2025-11-27T21:30Z",
        "name": "Kansas City Chiefs at Dallas Cowboys",
        "shortName": "KC @ DAL",
        "season": {"year": 2025, "type": 2},
        "status": {"type": {"name": "STATUS_FINAL", "completed": completed}},
        "competitions": [{
            "neutralSite": False,
            "attendance": 93000,
            "venue": {"id": "3687", "fullName": "AT&T Stadium", "address": {"city": city, "state": state}},
            "competitors": [
                {"homeAway": "home", "score": home_score, "team": {"id": "6", "abbreviation": "DAL", "displayName": "Dallas Cowboys"}},
                {"homeAway": "away", "score": away_score, "team": {"id": "12", "abbreviation": "KC", "displayName": "Kansas City Chiefs"}},
            ],
            "broadcasts": [{"market": "national", "names": ["CBS"]}] if national else [{"market": "home", "names": ["KTVT"]}],
            "notes": [{"headline": "Thanksgiving Day"}],
        }],
    }


def test_parse_espn_event_core_fields():
    rec = parse_espn_event(espn_event(), LEAGUES["NFL"])
    assert rec["event_id"] == "espn:football/nfl:401772510"
    assert rec["league"] == "NFL" and rec["season_type"] == "regular"
    assert (rec["venue_city"], rec["venue_state"], rec["venue_country"]) == ("Arlington", "TX", "US")
    assert rec["home_team_abbrev"] == "DAL" and rec["away_team_abbrev"] == "KC"
    assert rec["winner"] == "home" and rec["national_tv"] is True
    assert rec["headline"] == "Thanksgiving Day"


def test_parse_espn_soccer_city_with_state_name():
    rec = parse_espn_event(espn_event(city="Chicago, Illinois", state=None), LEAGUES["MLS"])
    assert (rec["venue_city"], rec["venue_state"], rec["venue_country"]) == ("Chicago", "IL", "US")


def test_parse_espn_stadium_name_in_city_field():
    rec = parse_espn_event(espn_event(city="Toyota Stadium", state=None), LEAGUES["MLS"])
    assert (rec["venue_city"], rec["venue_state"]) == ("Frisco", "TX")


def test_parse_espn_canadian_venue():
    rec = parse_espn_event(espn_event(city="Toronto", state="ON"), LEAGUES["NBA"])
    assert rec["venue_country"] == "CA"


def test_fbs_and_fcs_copies_share_one_event_id():
    ev = espn_event()
    assert parse_espn_event(ev, LEAGUES["NCAAF_FBS"])["event_id"] == parse_espn_event(ev, LEAGUES["NCAAF_FCS"])["event_id"]
    assert parse_espn_event(ev, LEAGUES["NCAAF_FCS"])["division"] == "FCS"


def test_winner_requires_completed_game():
    assert _winner(3, 1, True) == "home"
    assert _winner(1, 3, True) == "away"
    assert _winner(2, 2, True) == "tie"
    assert _winner(3, 1, False) is None


def test_parse_mlb_game_uses_official_date_and_coordinates():
    g = {
        "gamePk": 776543, "season": "2025", "gameType": "W", "gameDate": "2025-10-25T00:00:00Z",
        "officialDate": "2025-10-24", "seriesDescription": "World Series",
        "status": {"abstractGameState": "Final", "detailedState": "Final"},
        "venue": {"id": 14, "name": "Rogers Centre", "location": {"city": "Toronto", "stateAbbrev": "ON", "country": "Canada",
                                                                   "defaultCoordinates": {"latitude": 43.64, "longitude": -79.39}}},
        "teams": {"home": {"score": 11, "team": {"id": 141, "name": "Toronto Blue Jays", "abbreviation": "TOR"}},
                  "away": {"score": 4, "team": {"id": 119, "name": "Los Angeles Dodgers", "abbreviation": "LAD"}}},
    }
    rec = parse_mlb_game(g, LEAGUES["MLB"])
    assert rec["season_type"] == "postseason" and rec["headline"] == "World Series"
    assert rec["official_date"] == "2025-10-24"
    assert rec["venue_country"] == "CA" and rec["venue_latitude"] == 43.64
    assert rec["winner"] == "home"


def test_parse_nhl_neutral_site_has_no_home_city():
    g = {"id": 2025020001, "season": 20252026, "gameType": 2, "neutralSite": True, "startTimeUTC": "2025-11-14T18:00:00Z",
         "venue": {"default": "Avicii Arena"}, "gameState": "FUT",
         "homeTeam": {"id": 25, "abbrev": "DAL", "placeName": {"default": "Dallas"}, "commonName": {"default": "Stars"}},
         "awayTeam": {"id": 9, "abbrev": "OTT", "placeName": {"default": "Ottawa"}, "commonName": {"default": "Senators"}}}
    rec = parse_nhl_game(g, LEAGUES["NHL"])
    assert rec["venue_city"] is None and rec["neutral_site"] is True
    assert rec["home_team_name"] == "Dallas Stars" and rec["season"] == 2025


def test_day_mode_skips_off_season_months():
    reqs = espn_requests(LEAGUES["NBA"], date(2025, 7, 1), date(2025, 10, 31))
    months = {r[1]["dates"][4:6] for r in reqs}
    assert months == {"10"}
