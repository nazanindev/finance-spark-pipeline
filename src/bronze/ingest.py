from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F
from config.pipeline_config import is_databricks
from src.utils.delta_helpers import upsert_to_delta

CHASE_RAW_SCHEMA = StructType([
    StructField("Transaction Date", StringType(), False),
    StructField("Post Date",        StringType(), True),
    StructField("Description",      StringType(), False),
    StructField("Category",         StringType(), True),
    StructField("Type",             StringType(), True),
    StructField("Amount",           DoubleType(), False),
    StructField("Memo",             StringType(), True),
])


def ingest_csv_to_bronze(spark: SparkSession, source_path: str,
                         bronze_path: str, mode: str = "batch") -> None:
    if mode == "stream":
        df = _read_stream(spark, source_path)
    else:
        df = (spark.read
              .schema(CHASE_RAW_SCHEMA)
              .option("header", "true")
              .option("mode", "FAILFAST")
              .csv(source_path))

    df = _add_metadata(df, source_path)
    # Filter out credits/payments — Chase positive Amount = money back to card
    df = df.filter(F.col("Amount") < 0)

    if mode == "stream":
        (df.writeStream
           .format("delta")
           .outputMode("append")
           .option("checkpointLocation", bronze_path + "/_checkpoint")
           .start(bronze_path))
    else:
        upsert_to_delta(spark, df, bronze_path, "_row_hash")


def _add_metadata(df, source_path: str):
    return (df
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.sha1(F.lit(source_path)))
        .withColumn("_row_hash",
            F.sha2(F.concat_ws("|",
                F.col("Transaction Date"), F.col("Post Date"),
                F.col("Description"), F.col("Category"),
                F.col("Type"), F.col("Amount").cast("string"),
                F.col("Memo")), 256)))


def _read_stream(spark: SparkSession, source_path: str):
    if is_databricks():
        return (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaLocation", source_path + "/_schema")
                .schema(CHASE_RAW_SCHEMA)
                .load(source_path))
    else:
        return (spark.readStream
                .schema(CHASE_RAW_SCHEMA)
                .option("header", "true")
                .option("maxFilesPerTrigger", 1)
                .csv(source_path))
