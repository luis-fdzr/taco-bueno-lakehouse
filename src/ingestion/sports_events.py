"""Sports schedules and results from free public APIs, normalized to one schema.

Sources
    ESPN site API (unofficial, no key) .. NFL, NBA, MLS, NCAA football (FBS + FCS)
    MLB Stats API (public) ............... MLB, Triple-A, Double-A
    NHL API (public) ..................... NHL

CollegeFootballData needs an API key; ESPN covers the same games without one.
Every league keeps all of its games: the Lakeflow pipeline decides which ones
matter for each store by distance, so the ingestion stays store-agnostic.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from .geo import CA_PROVINCES, US_STATES, Geocoder, normalize_state

USER_AGENT = "Mozilla/5.0 (taco-bueno-lakehouse; sports calendar research)"
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
MLB_BASE = "https://statsapi.mlb.com/api/v1"
NHL_BASE = "https://api-web.nhle.com/v1"

ESPN_SEASON_TYPES = {1: "preseason", 2: "regular", 3: "postseason"}
MLB_GAME_TYPES = {"R": "regular", "F": "postseason", "D": "postseason", "L": "postseason", "W": "postseason",
                  "S": "preseason", "E": "other", "A": "other"}
NHL_GAME_TYPES = {1: "preseason", 2: "regular", 3: "postseason"}

# NHL schedules only give the arena name, so home cities are fixed reference data.
NHL_HOME = {
    "ANA": ("Anaheim", "CA", "US"), "BOS": ("Boston", "MA", "US"), "BUF": ("Buffalo", "NY", "US"),
    "CGY": ("Calgary", "AB", "CA"), "CAR": ("Raleigh", "NC", "US"), "CHI": ("Chicago", "IL", "US"),
    "COL": ("Denver", "CO", "US"), "CBJ": ("Columbus", "OH", "US"), "DAL": ("Dallas", "TX", "US"),
    "DET": ("Detroit", "MI", "US"), "EDM": ("Edmonton", "AB", "CA"), "FLA": ("Sunrise", "FL", "US"),
    "LAK": ("Los Angeles", "CA", "US"), "MIN": ("Saint Paul", "MN", "US"), "MTL": ("Montreal", "QC", "CA"),
    "NSH": ("Nashville", "TN", "US"), "NJD": ("Newark", "NJ", "US"), "NYI": ("Elmont", "NY", "US"),
    "NYR": ("New York", "NY", "US"), "OTT": ("Ottawa", "ON", "CA"), "PHI": ("Philadelphia", "PA", "US"),
    "PIT": ("Pittsburgh", "PA", "US"), "SJS": ("San Jose", "CA", "US"), "SEA": ("Seattle", "WA", "US"),
    "STL": ("St. Louis", "MO", "US"), "TBL": ("Tampa", "FL", "US"), "TOR": ("Toronto", "ON", "CA"),
    "UTA": ("Salt Lake City", "UT", "US"), "VAN": ("Vancouver", "BC", "CA"), "VGK": ("Las Vegas", "NV", "US"),
    "WSH": ("Washington", "DC", "US"), "WPG": ("Winnipeg", "MB", "CA"), "ARI": ("Tempe", "AZ", "US"),
}


COUNTRY_CODES = {"USA": "US", "United States": "US", "Canada": "CA"}
VENUE_CITY_FIXES = {"Toyota Stadium": ("Frisco", "TX"), "New York City": ("New York", "NY")}


@dataclass(frozen=True)
class League:
    key: str            # fetch unit: NFL, NBA, MLS, NCAAF_FBS, NCAAF_FCS, MLB, MILB_AAA, MILB_AA, NHL
    source: str         # espn | mlb_statsapi | nhl_api
    sport: str
    level: str          # pro | college | minor
    name: str           # league written to the data: NFL, NBA, MLS, NCAAF, MLB, MiLB, NHL
    division: str | None = None   # FBS/FCS, AAA/AA
    espn_path: str = ""  # e.g. football/nfl
    mode: str = ""      # week | day (ESPN only)
    groups: int | None = None
    months: tuple[int, ...] = ()
    week_plan: dict = field(default_factory=dict)
    mlb_sport_id: int | None = None


LEAGUES = {
    "NFL": League("NFL", "espn", "football", "pro", "NFL", None, "football/nfl", "week",
                  week_plan={1: range(1, 5), 2: range(1, 19), 3: range(1, 6)}),
    "NCAAF_FBS": League("NCAAF_FBS", "espn", "football", "college", "NCAAF", "FBS", "football/college-football", "week", groups=80,
                        week_plan={2: range(1, 17), 3: range(1, 3)}),
    "NCAAF_FCS": League("NCAAF_FCS", "espn", "football", "college", "NCAAF", "FCS", "football/college-football", "week", groups=81,
                        week_plan={2: range(1, 17), 3: range(1, 3)}),
    "NBA": League("NBA", "espn", "basketball", "pro", "NBA", None, "basketball/nba", "day",
                  months=(1, 2, 3, 4, 5, 6, 10, 11, 12)),
    "MLS": League("MLS", "espn", "soccer", "pro", "MLS", None, "soccer/usa.1", "day", months=tuple(range(2, 13))),
    "MLB": League("MLB", "mlb_statsapi", "baseball", "pro", "MLB", None, mlb_sport_id=1),
    "MILB_AAA": League("MILB_AAA", "mlb_statsapi", "baseball", "minor", "MiLB", "AAA", mlb_sport_id=11),
    "MILB_AA": League("MILB_AA", "mlb_statsapi", "baseball", "minor", "MiLB", "AA", mlb_sport_id=12),
    "NHL": League("NHL", "nhl_api", "hockey", "pro", "NHL"),
}


class HttpClient:
    """GET with retries, per-thread sessions and an optional on-disk cache.

    Responses are cached only when `cacheable` is true (windows that ended long
    ago), so upcoming games are always re-fetched and stay current.
    """

    def __init__(self, cache_dir: Path | None = None, pause_s: float = 0.1):
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.pause_s = pause_s
        self._local = threading.local()
        self.calls = 0
        self.cache_hits = 0

    def _session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            s = requests.Session()
            s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
            self._local.session = s
        return self._local.session

    def get_json(self, url: str, params: dict | None = None, cacheable: bool = False) -> dict:
        full = requests.Request("GET", url, params=params).prepare().url
        cache_file = None
        if self.cache_dir and cacheable:
            cache_file = self.cache_dir / (hashlib.sha1(full.encode()).hexdigest() + ".json")
            if cache_file.exists():
                self.cache_hits += 1
                return json.loads(cache_file.read_text(encoding="utf-8"))
        for intento in range(5):
            try:
                resp = self._session().get(full, timeout=40)
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
                if resp.status_code == 400:   # ESPN answers 400 for weeks/days that do not exist
                    data = {}
                else:
                    resp.raise_for_status()
                    data = resp.json()
                break
            except (requests.RequestException, ValueError):
                if intento == 4:
                    raise
                time.sleep(1.5 * 2 ** intento)
        self.calls += 1
        time.sleep(self.pause_s)
        if cache_file is not None:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(data), encoding="utf-8")
        return data


# --------------------------------------------------------------------------- #
# Parsers (one normalized record per game)
# --------------------------------------------------------------------------- #
def _as_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _winner(home_score, away_score, completed):
    if not completed or home_score is None or away_score is None:
        return None
    return "home" if home_score > away_score else "away" if away_score > home_score else "tie"


def parse_espn_event(ev: dict, league: League) -> dict | None:
    comps = ev.get("competitions") or []
    if not comps:
        return None
    comp = comps[0]
    teams = {c.get("homeAway"): c for c in comp.get("competitors", [])}
    home, away = teams.get("home", {}), teams.get("away", {})
    venue = comp.get("venue") or {}
    addr = venue.get("address") or {}
    status = (ev.get("status") or comp.get("status") or {}).get("type", {})
    season = ev.get("season") or {}
    hs, as_ = _as_int(home.get("score")), _as_int(away.get("score"))
    national = sorted({n for b in comp.get("broadcasts", []) if b.get("market") == "national" for n in b.get("names", [])})
    all_tv = sorted({n for b in comp.get("broadcasts", []) for n in b.get("names", [])})
    headlines = [n.get("headline") for n in comp.get("notes", []) if n.get("headline")]
    city, state_raw = addr.get("city"), addr.get("state")
    if city and not state_raw and "," in city:      # soccer feeds write "Chicago, Illinois" in city
        city, state_raw = (x.strip() for x in city.rsplit(",", 1))
    if city in VENUE_CITY_FIXES:                     # feeds that put the stadium name in `city`
        city, state_raw = VENUE_CITY_FIXES[city]
    state = normalize_state(state_raw)
    country = ("CA" if state in CA_PROVINCES else "US" if state in US_STATES
               else COUNTRY_CODES.get(addr.get("country"), addr.get("country")))
    return {
        "event_id": f"espn:{league.espn_path}:{ev.get('id')}",
        "source": "espn",
        "source_event_id": str(ev.get("id")),
        "league": league.name,
        "division": league.division,
        "sport": league.sport,
        "level": league.level,
        "season": season.get("year"),
        "season_type": ESPN_SEASON_TYPES.get(season.get("type"), "other"),
        "event_name": ev.get("name"),
        "short_name": ev.get("shortName"),
        "headline": "; ".join(headlines) or None,
        "start_time_utc": ev.get("date"),
        "official_date": None,
        "time_tbd": comp.get("timeValid") is False,
        "status": status.get("name"),
        "completed": bool(status.get("completed")),
        "neutral_site": bool(comp.get("neutralSite")),
        "venue_id": venue.get("id"),
        "venue_name": venue.get("fullName"),
        "venue_city": city,
        "venue_state": state,
        "venue_country": country,
        "venue_latitude": None,
        "venue_longitude": None,
        "home_team_id": (home.get("team") or {}).get("id"),
        "home_team_name": (home.get("team") or {}).get("displayName"),
        "home_team_abbrev": (home.get("team") or {}).get("abbreviation"),
        "away_team_id": (away.get("team") or {}).get("id"),
        "away_team_name": (away.get("team") or {}).get("displayName"),
        "away_team_abbrev": (away.get("team") or {}).get("abbreviation"),
        "home_score": hs,
        "away_score": as_,
        "winner": _winner(hs, as_, status.get("completed")),
        "national_tv": bool(national),
        "broadcasts": ", ".join(all_tv) or None,
        "attendance": _as_int(comp.get("attendance")) or None,
    }


def parse_mlb_game(g: dict, league: League) -> dict:
    venue = g.get("venue") or {}
    loc = venue.get("location") or {}
    coords = loc.get("defaultCoordinates") or {}
    home, away = g["teams"]["home"], g["teams"]["away"]
    status = g.get("status", {})
    completed = status.get("abstractGameState") == "Final" and status.get("detailedState") not in ("Postponed", "Cancelled")
    hs, as_ = home.get("score"), away.get("score")
    country = loc.get("country")
    return {
        "event_id": f"mlb:{g['gamePk']}",
        "source": "mlb_statsapi",
        "source_event_id": str(g["gamePk"]),
        "league": league.name,
        "division": league.division,
        "sport": league.sport,
        "level": league.level,
        "season": _as_int(g.get("season")),
        "season_type": MLB_GAME_TYPES.get(g.get("gameType"), "other"),
        "event_name": f"{away['team'].get('name')} at {home['team'].get('name')}",
        "short_name": f"{away['team'].get('abbreviation', '')} @ {home['team'].get('abbreviation', '')}".strip(),
        "headline": g.get("seriesDescription") if g.get("gameType") not in ("R", "S") else None,
        "start_time_utc": g.get("gameDate"),
        "official_date": g.get("officialDate"),
        "time_tbd": status.get("startTimeTBD", False),
        "status": status.get("detailedState"),
        "completed": completed,
        "neutral_site": False,
        "venue_id": str(venue.get("id")) if venue.get("id") else None,
        "venue_name": venue.get("name"),
        "venue_city": loc.get("city"),
        "venue_state": normalize_state(loc.get("stateAbbrev") or loc.get("state")),
        "venue_country": {"USA": "US", "Canada": "CA"}.get(country, country),
        "venue_latitude": coords.get("latitude"),
        "venue_longitude": coords.get("longitude"),
        "home_team_id": str(home["team"].get("id")),
        "home_team_name": home["team"].get("name"),
        "home_team_abbrev": home["team"].get("abbreviation"),
        "away_team_id": str(away["team"].get("id")),
        "away_team_name": away["team"].get("name"),
        "away_team_abbrev": away["team"].get("abbreviation"),
        "home_score": hs,
        "away_score": as_,
        "winner": _winner(hs, as_, completed),
        "national_tv": None,
        "broadcasts": None,
        "attendance": None,
    }


def parse_nhl_game(g: dict, league: League) -> dict:
    home, away = g.get("homeTeam", {}), g.get("awayTeam", {})
    neutral = bool(g.get("neutralSite"))
    city, state, country = (None, None, None) if neutral else NHL_HOME.get(home.get("abbrev"), (None, None, None))
    state_txt = g.get("gameState")
    completed = state_txt in ("OFF", "FINAL")
    hs, as_ = home.get("score"), away.get("score")

    def name(t: dict) -> str:
        return f"{(t.get('placeName') or {}).get('default', '')} {(t.get('commonName') or {}).get('default', '')}".strip()

    national = [b.get("network") for b in g.get("tvBroadcasts", []) if b.get("market") == "N"]
    return {
        "event_id": f"nhl:{g['id']}",
        "source": "nhl_api",
        "source_event_id": str(g["id"]),
        "league": league.name,
        "division": league.division,
        "sport": league.sport,
        "level": league.level,
        "season": _as_int(str(g.get("season", ""))[:4]),
        "season_type": NHL_GAME_TYPES.get(g.get("gameType"), "other"),
        "event_name": f"{name(away)} at {name(home)}",
        "short_name": f"{away.get('abbrev')} @ {home.get('abbrev')}",
        "headline": None,
        "start_time_utc": g.get("startTimeUTC"),
        "official_date": None,
        "time_tbd": g.get("gameScheduleState") == "TBD",
        "status": state_txt,
        "completed": completed,
        "neutral_site": neutral,
        "venue_id": None,
        "venue_name": (g.get("venue") or {}).get("default"),
        "venue_city": city,
        "venue_state": state,
        "venue_country": country,
        "venue_latitude": None,
        "venue_longitude": None,
        "home_team_id": str(home.get("id")),
        "home_team_name": name(home),
        "home_team_abbrev": home.get("abbrev"),
        "away_team_id": str(away.get("id")),
        "away_team_name": name(away),
        "away_team_abbrev": away.get("abbrev"),
        "home_score": hs,
        "away_score": as_,
        "winner": _winner(hs, as_, completed),
        "national_tv": bool(national),
        "broadcasts": ", ".join(sorted({b.get("network") for b in g.get("tvBroadcasts", []) if b.get("network")})) or None,
        "attendance": None,
    }


# --------------------------------------------------------------------------- #
# Fetchers
# --------------------------------------------------------------------------- #
def _event_day(rec: dict) -> date | None:
    if rec.get("official_date"):
        return date.fromisoformat(rec["official_date"])
    ts = rec.get("start_time_utc")
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).date()


def _in_window(rec: dict, start: date, end: date) -> bool:
    d = _event_day(rec)
    return d is not None and start - timedelta(days=1) <= d <= end + timedelta(days=1)


def _stable(end_of_window: date) -> bool:
    """Results for windows that ended more than 10 days ago no longer change."""
    return end_of_window < date.today() - timedelta(days=10)


def espn_requests(league: League, start: date, end: date) -> list[tuple[str, dict, bool]]:
    url = f"{ESPN_BASE}/{league.espn_path}/scoreboard"
    reqs = []
    if league.mode == "day":
        d = start
        while d <= end:
            if d.month in league.months:
                params = {"dates": d.strftime("%Y%m%d"), "limit": 500}
                reqs.append((url, params, _stable(d)))
            d += timedelta(days=1)
    else:
        for season in range(start.year - 1, end.year + 1):
            for season_type, weeks in league.week_plan.items():
                for week in weeks:
                    params = {"dates": season, "seasontype": season_type, "week": week, "limit": 500}
                    if league.groups:
                        params["groups"] = league.groups
                    # A season's games end by February of the following year.
                    reqs.append((url, params, _stable(date(season + 1, 3, 1))))
    return reqs


def fetch_espn(client: HttpClient, league: League, start: date, end: date, workers: int = 4) -> list[dict]:
    reqs = espn_requests(league, start, end)
    with ThreadPoolExecutor(workers) as ex:
        payloads = list(ex.map(lambda r: client.get_json(r[0], r[1], cacheable=r[2]), reqs))
    out = {}
    for data in payloads:
        for ev in data.get("events", []) or []:
            rec = parse_espn_event(ev, league)
            if rec and _in_window(rec, start, end):
                out[rec["event_id"]] = rec
    return list(out.values())


def fetch_mlb(client: HttpClient, league: League, start: date, end: date) -> list[dict]:
    out = {}
    for year in range(start.year, end.year + 1):
        s, e = max(start, date(year, 1, 1)), min(end, date(year, 12, 31))
        params = {"sportId": league.mlb_sport_id, "startDate": s.isoformat(), "endDate": e.isoformat(),
                  "hydrate": "venue(location),team"}
        data = client.get_json(f"{MLB_BASE}/schedule", params, cacheable=_stable(e))
        for d in data.get("dates", []):
            for g in d.get("games", []):
                rec = parse_mlb_game(g, league)
                out[rec["event_id"]] = rec       # resumed/rescheduled games keep the last entry
    return list(out.values())


def fetch_nhl(client: HttpClient, league: League, start: date, end: date) -> list[dict]:
    out = {}
    d = start
    while d <= end:
        data = client.get_json(f"{NHL_BASE}/schedule/{d.isoformat()}", cacheable=_stable(d + timedelta(days=7)))
        for day in data.get("gameWeek", []):
            for g in day.get("games", []):
                rec = parse_nhl_game(g, league)
                if _in_window(rec, start, end):
                    out[rec["event_id"]] = rec
        nxt = data.get("nextStartDate")
        d = date.fromisoformat(nxt) if nxt and date.fromisoformat(nxt) > d else d + timedelta(days=7)
    return list(out.values())


def fetch_league(client: HttpClient, league: League, start: date, end: date) -> list[dict]:
    if league.source == "espn":
        return fetch_espn(client, league, start, end)
    if league.source == "mlb_statsapi":
        return fetch_mlb(client, league, start, end)
    if league.source == "nhl_api":
        return fetch_nhl(client, league, start, end)
    raise ValueError(f"Unknown source {league.source}")


def geocode_venues(events: list[dict], geocoder: Geocoder) -> None:
    """Fills venue coordinates in place for US/Canada venues that lack them."""
    for rec in events:
        if rec.get("venue_latitude") is not None or not rec.get("venue_city"):
            continue
        country = rec.get("venue_country")
        if country not in ("US", "CA"):
            continue
        loc = geocoder.locate(rec["venue_city"], rec.get("venue_state"), country=country)
        if loc:
            rec["venue_latitude"], rec["venue_longitude"] = loc["latitude"], loc["longitude"]
    geocoder.save()


def write_jsonl(events: list[dict], path: Path, fetched_at: str) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in sorted(events, key=lambda r: (r.get("start_time_utc") or "", r["event_id"])):
            f.write(json.dumps({**rec, "fetched_at": fetched_at}, ensure_ascii=False) + "\n")
    return len(events)


def run(start: date, end: date, out_dir: Path, leagues: list[str] | None = None,
        cache_dir: Path | None = None, geocoder: Geocoder | None = None, log=print) -> dict[str, int]:
    """Fetches every league for [start, end] and writes one JSONL file per league.

    Files land in <out_dir>/<YYYYMMDDHHMMSS>/<league>.jsonl so each run is a new,
    immutable batch for Auto Loader; the pipeline keeps the latest version of
    each game (AUTO CDC on event_id ordered by fetched_at).
    """
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    batch_dir = Path(out_dir) / datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    client = HttpClient(cache_dir)
    counts = {}
    seen: set[str] = set()
    for key in leagues or list(LEAGUES):
        league = LEAGUES[key]
        t0 = time.time()
        # An FBS vs FCS game is listed in both ESPN groups; keep the first copy only.
        events = [e for e in fetch_league(client, league, start, end) if e["event_id"] not in seen]
        seen.update(e["event_id"] for e in events)
        if geocoder is not None:
            geocode_venues(events, geocoder)
        counts[key] = write_jsonl(events, batch_dir / f"{key.lower()}.jsonl", fetched_at)
        log(f"  {key:10s} {counts[key]:6,d} games  ({time.time() - t0:5.1f}s, {client.calls} API calls, {client.cache_hits} cached)")
    return counts
