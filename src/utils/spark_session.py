from pyspark.sql import SparkSession
from config.pipeline_config import is_databricks


def get_spark(app_name: str = "FinancePipeline") -> SparkSession:
    if is_databricks():
        return SparkSession.getActiveSession()
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )
