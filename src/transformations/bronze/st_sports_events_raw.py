"""Bronze: sports schedules/results landed by the ingestion task as JSON Lines.

Each ingestion run writes a new immutable batch folder; every version of a game
is kept here and silver keeps the latest one (AUTO CDC).
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

spark = SparkSession.getActiveSession()
LANDING_PATH = spark.conf.get("taco.landing_volume_path")

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("source", StringType()),
    StructField("source_event_id", StringType()),
    StructField("league", StringType()),
    StructField("division", StringType()),
    StructField("sport", StringType()),
    StructField("level", StringType()),
    StructField("season", LongType()),
    StructField("season_type", StringType()),
    StructField("event_name", StringType()),
    StructField("short_name", StringType()),
    StructField("headline", StringType()),
    StructField("start_time_utc", StringType()),
    StructField("official_date", StringType()),
    StructField("time_tbd", BooleanType()),
    StructField("status", StringType()),
    StructField("completed", BooleanType()),
    StructField("neutral_site", BooleanType()),
    StructField("venue_id", StringType()),
    StructField("venue_name", StringType()),
    StructField("venue_city", StringType()),
    StructField("venue_state", StringType()),
    StructField("venue_country", StringType()),
    StructField("venue_latitude", DoubleType()),
    StructField("venue_longitude", DoubleType()),
    StructField("home_team_id", StringType()),
    StructField("home_team_name", StringType()),
    StructField("home_team_abbrev", StringType()),
    StructField("away_team_id", StringType()),
    StructField("away_team_name", StringType()),
    StructField("away_team_abbrev", StringType()),
    StructField("home_score", LongType()),
    StructField("away_score", LongType()),
    StructField("winner", StringType()),
    StructField("national_tv", BooleanType()),
    StructField("broadcasts", StringType()),
    StructField("attendance", LongType()),
    StructField("fetched_at", StringType()),
])


@dp.table(
    name="st_sports_events_raw",
    comment="Bronze. Every fetched version of every game (NFL, NBA, MLB, MiLB, NHL, MLS, NCAA football) "
            "from ESPN, MLB Stats API and NHL API, as landed by the ingestion task.",
    table_properties={"quality": "bronze"},
)
@dp.expect("has_event_id", "event_id IS NOT NULL")
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def st_sports_events_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(EVENT_SCHEMA)
        .load(f"{LANDING_PATH}/sports_events/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
