"""Entry point for the external-data ingestion task.

Runs locally or as a Databricks job task (spark_python_task). It writes to a
landing folder, which is a Unity Catalog volume in Databricks:

    <landing>/reference/store_locations/store_locations.csv   geocoded stores
    <landing>/sports_events/<YYYYMMDDHHMMSS>/<league>.jsonl     one batch per run
    <landing>/_cache/                                           geocoding/API cache

Examples
    # one-time backfill from a laptop, then upload the folder to the volume
    python src/ingestion/run_ingestion.py --mode backfill --start 2025-01-01 --end 2026-12-31 \
        --landing ./landing --city-file ../city.xlsx

    # daily run inside Databricks (job task); defaults to the last 14 days + next 120
    python src/ingestion/run_ingestion.py --mode incremental --landing /Volumes/workspace/taco_bueno/landing
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path



def _src_dir() -> Path:
    """Folder that contains the `ingestion` package.

    Serverless job tasks run the script through exec(), where __file__ is not
    defined; the code object still carries the real path.
    """
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        import inspect
        return Path(inspect.currentframe().f_code.co_filename).resolve().parents[1]


sys.path.insert(0, str(_src_dir()))  # makes `ingestion` importable as a package

from ingestion import sports_events, store_locations  # noqa: E402
from ingestion.geo import Geocoder  # noqa: E402


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Ingest store locations and sports calendars into the landing volume.")
    ap.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    ap.add_argument("--start", type=date.fromisoformat, help="Backfill start date (YYYY-MM-DD).")
    ap.add_argument("--end", type=date.fromisoformat, help="Backfill end date (YYYY-MM-DD).")
    ap.add_argument("--lookback-days", type=int, default=14, help="Incremental: days back to refresh results.")
    ap.add_argument("--lookahead-days", type=int, default=120, help="Incremental: days ahead for schedules.")
    ap.add_argument("--landing", required=True, help="Landing folder or /Volumes/<catalog>/<schema>/<volume> path.")
    ap.add_argument("--city-file", help="Business city file (.xlsx/.csv). Omit to keep the current store_locations.csv.")
    ap.add_argument("--leagues", default="", help="Comma-separated subset, e.g. NFL,NBA. Default: all.")
    ap.add_argument("--fail-on-error", default="false", choices=["true", "false"],
                    help="false = log API failures and exit 0 so the pipeline still runs on existing data.")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    landing = Path(args.landing)
    cache = landing / "_cache"
    geocoder = Geocoder(cache / "geocode.json")

    if args.mode == "backfill":
        if not (args.start and args.end):
            raise SystemExit("--start and --end are required in backfill mode")
        start, end = args.start, args.end
    else:
        today = date.today()
        start, end = today - timedelta(days=args.lookback_days), today + timedelta(days=args.lookahead_days)

    if args.city_file:
        rows = store_locations.build_store_locations(Path(args.city_file), geocoder)
        out = landing / "reference" / "store_locations" / "store_locations.csv"
        store_locations.write_csv(rows, out)
        sin_coord = sum(1 for r in rows if r["latitude"] is None)
        ambiguas = sorted({r["city"] for r in rows if r["geocode_ambiguous"]})
        print(f"Store locations: {len(rows)} stores -> {out} | without coordinates: {sin_coord} | ambiguous cities: {ambiguas}")

    leagues = [x.strip().upper() for x in args.leagues.split(",") if x.strip()] or None
    print(f"Sports events {args.mode}: {start} -> {end}")
    try:
        counts = sports_events.run(start, end, landing / "sports_events", leagues=leagues,
                                   cache_dir=cache / "http" if args.mode == "backfill" else None,
                                   geocoder=geocoder)
    except Exception as e:  # network/API failures must not block the sales pipeline
        print(f"WARNING: sports ingestion failed: {type(e).__name__}: {e}")
        if args.fail_on_error == "true":
            raise
        return 0
    print(f"Total games written: {sum(counts.values()):,}")
    return 0


if __name__ == "__main__":
    # Only exit explicitly on failure: serverless tasks run the script inside IPython,
    # where even SystemExit(0) is reported as an error.
    exit_code = main()
    if exit_code:
        sys.exit(exit_code)
