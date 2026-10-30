# One-time backfill of the landing volume from a laptop (PowerShell).
#
#   1. Geocodes the business city file and downloads two seasons of sports
#      schedules/results (2025-01-01 .. 2026-12-31) into ..\data\landing
#   2. Uploads them to the landing volume. The raw POS files are uploaded
#      separately and never modified.
#
# Defaults follow the local project layout (the repo sits next to data\):
#   <project>\data\reference\city.xlsx   business city file (.xlsx, or .csv with columns Store # and City)
#   <project>\data\landing\              local copy of the landing zone
#
# Needs the project's Python dependencies: python -m pip install -e .
#
# Usage (from the repo root; -ExecutionPolicy Bypass lets it run on a default Windows setup):
#   powershell -ExecutionPolicy Bypass -File .\setup\01_backfill_landing.ps1 -CityFile ..\data\reference\city.xlsx -Profile DEFAULT

param(
  [string]$CityFile = "..\data\reference\city.xlsx",
  [string]$Start = "2025-01-01",
  [string]$End = "2026-12-31",
  [string]$Landing = "..\data\landing",
  [string]$Volume = "dbfs:/Volumes/workspace/default/taco-bueno-landing",
  [string]$Profile = "DEFAULT"
)

python src/ingestion/run_ingestion.py --mode backfill --start $Start --end $End --landing $Landing --city-file $CityFile
if ($LASTEXITCODE -ne 0) { throw "Ingestion failed" }

databricks fs mkdir "$Volume/reference/raw" --profile $Profile
databricks fs cp $CityFile "$Volume/reference/raw/$(Split-Path $CityFile -Leaf)" --overwrite --profile $Profile
databricks fs cp -r "$Landing/reference/store_locations" "$Volume/reference/store_locations" --overwrite --profile $Profile
databricks fs cp -r "$Landing/sports_events" "$Volume/sports_events" --profile $Profile
databricks fs mkdir "$Volume/_cache" --profile $Profile
databricks fs cp "$Landing/_cache/geocode.json" "$Volume/_cache/geocode.json" --overwrite --profile $Profile
