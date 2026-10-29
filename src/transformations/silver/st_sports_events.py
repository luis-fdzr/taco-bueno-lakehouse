"""Silver: one current row per game (AUTO CDC, SCD type 1).

Every ingestion run lands a new version of the games in its window (a time
change, a postponement, a final score). AUTO CDC keeps the latest version per
event_id, ordered by fetched_at, so reruns and overlapping windows never
duplicate games.

Local date: stores are on US Central time, so the game date is the Central date
of the start time. Games without a confirmed time (time_tbd) are dated in
Eastern time, which is how the feeds encode a date-only kickoff. MLB provides
its own official local date.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()


@dp.temporary_view(name="v_sports_events_typed")
def v_sports_events_typed():
    # ESPN writes minutes only ("2025-09-05T00:20Z"); MLB/NHL include seconds. A plain cast
    # returns NULL for the ESPN format, so both patterns are parsed explicitly.
    start_utc = F.expr("""coalesce(
        try_to_timestamp(start_time_utc, "yyyy-MM-dd'T'HH:mmX"),
        try_to_timestamp(start_time_utc, "yyyy-MM-dd'T'HH:mm:ssX"),
        try_to_timestamp(start_time_utc))""")
    start_ct = F.from_utc_timestamp(start_utc, "America/Chicago")
    return (
        spark.readStream.table("st_sports_events_raw")
        .withColumn("start_ts_utc", start_utc)
        .withColumn("fetched_ts", F.to_timestamp("fetched_at"))
        .withColumn(
            "event_date_ct",
            F.coalesce(
                F.to_date("official_date"),
                F.when(F.coalesce("time_tbd", F.lit(False)),
                       F.to_date(F.from_utc_timestamp(start_utc, "America/New_York")))
                 .otherwise(F.to_date(start_ct)),
            ),
        )
        .withColumn("start_hour_ct", F.when(~F.coalesce("time_tbd", F.lit(False)), F.hour(start_ct)))
        .withColumn(
            "is_national_marquee",
            ((F.col("league") == "NFL") & (F.col("season_type") == "postseason"))
            | F.lower(F.coalesce("headline", F.lit(""))).rlike(
                "super bowl|world series|nba finals|stanley cup final|national championship"),
        )
        # Clustering columns first: Delta collects statistics on the first 32 columns only.
        .select(*COLUMN_ORDER)
    )


COLUMN_ORDER = [
    "event_id", "event_date_ct", "league", "division", "sport", "level", "season", "season_type",
    "start_ts_utc", "start_hour_ct", "time_tbd", "is_national_marquee", "event_name", "short_name", "headline",
    "status", "completed", "neutral_site", "venue_id", "venue_name", "venue_city", "venue_state", "venue_country",
    "venue_latitude", "venue_longitude", "home_team_id", "home_team_name", "home_team_abbrev", "away_team_id",
    "away_team_name", "away_team_abbrev", "home_score", "away_score", "winner", "national_tv", "broadcasts",
    "attendance", "official_date", "start_time_utc", "source", "source_event_id", "fetched_at", "fetched_ts",
]


dp.create_streaming_table(
    name="st_sports_events",
    comment="Silver. Current version of every game (one row per event_id) with Central-time date, start hour, "
            "venue coordinates and a national-marquee flag (NFL playoffs, Super Bowl, World Series, NBA Finals...).",
    cluster_by=["event_date_ct", "league"],
    table_properties={"quality": "silver"},
    expect_all_or_drop={
        "has_event_id": "event_id IS NOT NULL",
        "has_start_time": "start_ts_utc IS NOT NULL",
        "has_event_date": "event_date_ct IS NOT NULL",
    },
    expect_all={
        "us_venue_has_coordinates": "venue_country NOT IN ('US', 'CA') OR venue_latitude IS NOT NULL",
        "has_both_teams": "home_team_name IS NOT NULL AND away_team_name IS NOT NULL",
    },
)

dp.create_auto_cdc_flow(
    target="st_sports_events",
    source="v_sports_events_typed",
    keys=["event_id"],
    sequence_by="fetched_ts",
    stored_as_scd_type=1,
)
