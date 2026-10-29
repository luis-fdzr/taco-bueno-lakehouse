"""Store reference data: store number -> city -> coordinates.

Input is the city file maintained by the business (city.xlsx, columns
`Store #` and `City`, no state). The output CSV adds state, coordinates and a
flag for cities whose state had to be inferred among several candidates.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from .geo import Geocoder, clean_city

# Taco Bueno footprint. Weights rank candidates when a city name exists in more
# than one of these states (population x weight); TX and OK are the core market.
FOOTPRINT_STATE_WEIGHTS = {"TX": 1.0, "OK": 1.0, "KS": 0.4, "MO": 0.4, "AR": 0.4, "LA": 0.4, "NM": 0.4}

OUTPUT_COLUMNS = [
    "store_id", "city_raw", "city", "state", "latitude", "longitude", "population",
    "geocode_ambiguous", "geocode_candidates", "geocoded_at",
]


def read_city_file(path: Path) -> list[dict]:
    """Reads the business city file (.xlsx or .csv) without altering it."""
    path = Path(path)
    if path.suffix.lower() in (".xlsx", ".xls"):
        import openpyxl  # optional dependency, only needed for Excel input

        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        rows = list(wb.worksheets[0].iter_rows(values_only=True))
        header = [str(h).strip() for h in rows[0]]
        records = [dict(zip(header, r)) for r in rows[1:] if any(v is not None for v in r)]
    else:
        with path.open(encoding="utf-8-sig", newline="") as f:
            records = list(csv.DictReader(f))
    out = []
    for r in records:
        store = r.get("Store #") or r.get("store_id")
        if store is None:
            continue
        out.append({"store_id": str(store).strip().split(".")[0], "city_raw": r.get("City") or r.get("city")})
    return out


def build_store_locations(city_file: Path, geocoder: Geocoder) -> list[dict]:
    geocoded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = []
    for rec in read_city_file(city_file):
        loc = geocoder.locate(rec["city_raw"], state_weights=FOOTPRINT_STATE_WEIGHTS) or {}
        rows.append({
            "store_id": rec["store_id"],
            "city_raw": rec["city_raw"],
            "city": loc.get("city") or clean_city(rec["city_raw"]),
            "state": loc.get("state"),
            "latitude": loc.get("latitude"),
            "longitude": loc.get("longitude"),
            "population": loc.get("population"),
            "geocode_ambiguous": loc.get("geocode_ambiguous", False),
            "geocode_candidates": loc.get("geocode_candidates", ""),
            "geocoded_at": geocoded_at,
        })
    geocoder.save()
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        w.writeheader()
        w.writerows(rows)
