# One-time backfill of the landing volume from a laptop (PowerShell).
#
#   1. Geocodes the business city file and downloads two seasons of sports
#      schedules/results (2025-01-01 .. 2026-12-31) into .\landing
#   2. Uploads them to the landing volume. The raw POS files are uploaded
#      separately and never modified.
#
# Usage (from the repo root):
#   .\setup\01_backfill_landing.ps1 -CityFile ..\city.xlsx -Profile DEFAULT

param(
  [string]$CityFile = "..\city.xlsx",
  [string]$Start = "2025-01-01",
  [string]$End = "2026-12-31",
  [string]$Landing = ".\landing",
  [string]$Volume = "dbfs:/Volumes/workspace/default/taco-bueno-landing",
  [string]$Profile = "DEFAULT"
)

python src/ingestion/run_ingestion.py --mode backfill --start $Start --end $End --landing $Landing --city-file $CityFile
if ($LASTEXITCODE -ne 0) { throw "Ingestion failed" }

databricks fs mkdir "$Volume/reference/raw" --profile $Profile
databricks fs cp $CityFile "$Volume/reference/raw/city.xlsx" --overwrite --profile $Profile
databricks fs cp -r "$Landing/reference/store_locations" "$Volume/reference/store_locations" --overwrite --profile $Profile
databricks fs cp -r "$Landing/sports_events" "$Volume/sports_events" --profile $Profile
databricks fs mkdir "$Volume/_cache" --profile $Profile
databricks fs cp "$Landing/_cache/geocode.json" "$Volume/_cache/geocode.json" --overwrite --profile $Profile
