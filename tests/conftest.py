import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
        .appName("FinancePipelineTests")
        .master("local[2]")
        .getOrCreate())
