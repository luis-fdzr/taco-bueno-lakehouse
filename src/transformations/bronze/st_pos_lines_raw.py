"""Bronze: POS line items from the daily TLD exports, ingested with Auto Loader.

Each file TB_Daily_TLD_MM-DD-YYYY-m.csv holds one business day (the day before
the date in its name) for every store. Three export layouts exist (19, 21 and
22 columns) and they share the same column order, so columns are mapped by
position with an explicit all-STRING schema: nothing is parsed or changed here.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.getActiveSession()
RAW_PATH = spark.conf.get("taco.raw_volume_path")

POS_COLUMNS = [
    "store_num", "date", "total", "check_number", "time_card", "begin_time", "end_time",
    "total_discount", "order_voided", "tax", "quantity", "transaction_type", "menu_item",
    "menu_item_ext_ref", "pop", "pos_code", "menu_price", "item_discount", "item_voided",
    "report_group", "tender_type", "total_rounding",
]
RAW_SCHEMA = StructType([StructField(c, StringType()) for c in POS_COLUMNS])


@dp.table(
    name="st_pos_lines_raw",
    comment="Bronze. One row per POS line exactly as exported (all STRING), plus source-file lineage. "
            "Source: daily TLD CSV files in the raw volume, ingested incrementally with Auto Loader.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "false"},
)
@dp.expect("file_name_matches_contract", r"source_file RLIKE '^TB_Daily_TLD_[0-9]{2}-[0-9]{2}-[0-9]{4}-m\\.csv$'")
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def st_pos_lines_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("pathGlobFilter", "TB_Daily_TLD_*-m.csv")
        .option("header", "false")          # header rows are filtered below; mapping is positional
        .option("escape", '"')              # RFC 4180 quotes: "6"""" Flour Tortillas - Side"""""
        .option("mode", "PERMISSIVE")       # shorter (19/21 column) files get NULL in the trailing columns
        .option("rescuedDataColumn", "_rescued_data")
        .schema(RAW_SCHEMA)
        .load(RAW_PATH)
        .filter(~(F.col("store_num").eqNullSafe("Store #") & F.col("date").eqNullSafe("Date")))
        .select(
            "*",
            F.col("_metadata.file_name").alias("source_file"),
            F.col("_metadata.file_size").alias("source_file_size"),
            F.col("_metadata.file_modification_time").alias("source_file_modified_at"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
