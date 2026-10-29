-- Bronze: store reference data (store -> city -> coordinates) as delivered in the
-- landing volume by the ingestion task (geocoded from the business city file).
CREATE OR REFRESH MATERIALIZED VIEW mv_store_reference_raw (
  CONSTRAINT has_store_id EXPECT (store_id IS NOT NULL)
)
COMMENT 'Bronze. Geocoded store reference file, one row per store listed by the business.'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT
  *,
  _metadata.file_name AS source_file
FROM read_files(
  '${taco.landing_volume_path}/reference/store_locations/',
  format => 'csv',
  header => true,
  schema => 'store_id STRING, city_raw STRING, city STRING, state STRING, latitude DOUBLE, longitude DOUBLE, population BIGINT, geocode_ambiguous BOOLEAN, geocode_candidates STRING, geocoded_at STRING'
);
