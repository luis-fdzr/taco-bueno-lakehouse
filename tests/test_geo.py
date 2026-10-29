import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.geo import Geocoder, clean_city, haversine_km, normalize_state  # noqa: E402


class OfflineGeocoder(Geocoder):
    """Geocoder that answers from a dict instead of the API."""

    def __init__(self, results: dict):
        super().__init__(cache_path=None)
        self._results = results

    def search(self, name, country="US"):
        return [dict(r) for r in self._results.get(name.lower(), [])]


def place(name, state, pop, lat=0.0, lon=0.0, code="PPL"):
    return {"name": name, "admin1": state, "population": pop, "latitude": lat, "longitude": lon, "feature_code": code}


def test_haversine_dallas_to_oklahoma_city():
    assert 295 < haversine_km(32.7767, -96.7970, 35.4676, -97.5164) < 310


def test_haversine_same_point_is_zero():
    assert haversine_km(36.15, -95.99, 36.15, -95.99) == 0


def test_normalize_state_accepts_names_and_codes():
    assert normalize_state("Texas") == "TX"
    assert normalize_state("ok") == "OK"
    assert normalize_state("Ontario") == "ON"
    assert normalize_state(None) is None
    assert normalize_state("Narnia") is None


def test_clean_city_trims_and_resolves_aliases():
    assert clean_city("OKC ") == "Oklahoma City"
    assert clean_city("  Fort   Worth ") == "Fort Worth"
    assert clean_city(None) is None


def test_core_market_wins_a_same_name_tie():
    g = OfflineGeocoder({"shawnee": [place("Shawnee", "Kansas", 65046), place("Shawnee", "Oklahoma", 31286)]})
    loc = g.locate("Shawnee", state_weights={"TX": 1.0, "OK": 1.0, "KS": 0.4})
    assert loc["state"] == "OK"
    assert loc["geocode_ambiguous"]


def test_small_namesake_is_not_ambiguous():
    g = OfflineGeocoder({"abilene": [place("Abilene", "Texas", 125182), place("Abilene", "Kansas", 6558)]})
    loc = g.locate("Abilene", state_weights={"TX": 1.0, "OK": 1.0, "KS": 0.4})
    assert loc["state"] == "TX"
    assert not loc["geocode_ambiguous"]


def test_explicit_state_filters_candidates():
    g = OfflineGeocoder({"arlington": [place("Arlington", "Virginia", 238643), place("Arlington", "Texas", 394266)]})
    assert g.locate("Arlington", "VA")["state"] == "VA"


def test_populated_place_preferred_over_admin_area():
    g = OfflineGeocoder({"fairfield": [place("Fairfield", "Connecticut", 61512, code="ADM3"),
                                       place("Fairfield", "Connecticut", 0, code="PPL")]})
    assert g.locate("Fairfield", "CT")["population"] == 0


def test_unknown_city_returns_none():
    assert OfflineGeocoder({}).locate("Atlantis", "TX") is None
