"""Geocoding and distance helpers.

Uses the Open-Meteo geocoding API (free, no key) to turn city names into
coordinates. Results are cached on disk so repeated runs do not call the API
again.
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path

import requests

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
USER_AGENT = "taco-bueno-lakehouse/1.0 (store and venue geocoding)"

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon",
    "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia",
    "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "PR": "Puerto Rico",
}
CA_PROVINCES = {
    "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba", "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador", "NS": "Nova Scotia", "ON": "Ontario",
    "PE": "Prince Edward Island", "QC": "Quebec", "SK": "Saskatchewan",
}
STATE_ABBREV = {v.lower(): k for k, v in {**US_STATES, **CA_PROVINCES}.items()}

# Cities written differently in the source files.
CITY_ALIASES = {"okc": "Oklahoma City", "desoto": "DeSoto"}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometers."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def normalize_state(state: str | None) -> str | None:
    """'Texas' or 'TX' -> 'TX'. Returns None when unknown."""
    if not state:
        return None
    s = state.strip()
    if s.upper() in US_STATES or s.upper() in CA_PROVINCES:
        return s.upper()
    return STATE_ABBREV.get(s.lower())


def clean_city(city: str | None) -> str | None:
    if city is None:
        return None
    c = " ".join(str(city).split())
    return CITY_ALIASES.get(c.lower(), c) or None


class Geocoder:
    """Small cached client for the Open-Meteo geocoding API."""

    def __init__(self, cache_path: Path | None = None, pause_s: float = 0.15, session: requests.Session | None = None):
        self.cache_path = Path(cache_path) if cache_path else None
        self.pause_s = pause_s
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._cache: dict[str, list[dict]] = {}
        if self.cache_path and self.cache_path.exists():
            self._cache = json.loads(self.cache_path.read_text(encoding="utf-8"))

    def save(self) -> None:
        if self.cache_path:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(json.dumps(self._cache, indent=0, sort_keys=True), encoding="utf-8")

    def search(self, name: str, country: str = "US") -> list[dict]:
        key = f"{country}|{name.lower()}"
        if key not in self._cache:
            params = {"name": name, "count": 20, "language": "en", "format": "json", "countryCode": country}
            for intento in range(4):
                try:
                    resp = self.session.get(GEOCODE_URL, params=params, timeout=30)
                    resp.raise_for_status()
                    self._cache[key] = resp.json().get("results", []) or []
                    break
                except requests.RequestException:
                    if intento == 3:
                        raise
                    time.sleep(2 ** intento)
            time.sleep(self.pause_s)
        return self._cache[key]

    def locate(
        self,
        city: str | None,
        state: str | None = None,
        country: str = "US",
        state_weights: dict[str, float] | None = None,
    ) -> dict | None:
        """Best match for a city.

        If `state` is given, only candidates in that state are considered. If not,
        candidates are restricted to the keys of `state_weights` and ranked by
        population x weight (lets the main market win ties such as Shawnee OK/KS).
        """
        city = clean_city(city)
        if not city:
            return None
        state_abbrev = normalize_state(state)
        results = self.search(city, country)
        for r in results:
            r["_state"] = normalize_state(r.get("admin1"))

        if state_abbrev:
            candidates = [r for r in results if r["_state"] == state_abbrev]
        elif state_weights:
            candidates = [r for r in results if r["_state"] in state_weights]
        else:
            candidates = results
        # Prefer populated places (PPL*) over parks, airports and administrative areas.
        candidates = [r for r in candidates if (r.get("feature_code") or "").startswith("PPL")] or candidates
        same_name = [r for r in candidates if (r.get("name") or "").lower() == city.lower()]
        candidates = same_name or candidates
        if not candidates:
            return None

        def score(r: dict) -> float:
            weight = (state_weights or {}).get(r["_state"], 1.0)
            return (r.get("population") or 0) * weight

        ranked = sorted(candidates, key=score, reverse=True)
        best = ranked[0]
        # Ambiguous = a same-named city of comparable size exists in another state.
        rivals = [r for r in ranked[1:] if r["_state"] != best["_state"]
                  and (r.get("population") or 0) >= 5000 and score(r) >= 0.25 * score(best)]
        return {
            "city": best.get("name"),
            "state": best["_state"],
            "country": best.get("country_code", country),
            "latitude": best.get("latitude"),
            "longitude": best.get("longitude"),
            "population": best.get("population"),
            "geocode_ambiguous": bool(rivals),
            "geocode_candidates": ", ".join(
                f"{r.get('name')}, {r['_state']} (pop {r.get('population') or 'n/a'})" for r in ranked[:3]
            ),
        }
