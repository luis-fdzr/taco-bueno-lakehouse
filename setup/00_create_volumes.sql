-- One-time setup (run in the SQL editor). The bundle creates the pipeline schema;
-- the two volumes live outside it so `bundle destroy` can never delete data.

-- Raw daily POS exports (TB_Daily_TLD_MM-DD-YYYY-m.csv), uploaded unmodified.
CREATE VOLUME IF NOT EXISTS workspace.default.`taco-bueno-raw`
COMMENT 'Raw Taco Bueno POS exports (one CSV per day). Read-only for the pipeline.';

-- External and reference data written by the ingestion task:
--   reference/store_locations/store_locations.csv   geocoded store cities
--   reference/raw/city.xlsx                         business city file, as received
--   sports_events/<batch>/<league>.jsonl            sports schedules and results
CREATE VOLUME IF NOT EXISTS workspace.default.`taco-bueno-landing`
COMMENT 'Landing zone for sports calendars and store reference data used by the taco_bueno pipeline.';
