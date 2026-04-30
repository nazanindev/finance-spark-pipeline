import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
        .appName("FinancePipelineTests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate())
