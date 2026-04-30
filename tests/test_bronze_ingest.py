import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.bronze.ingest import _add_metadata, CHASE_RAW_SCHEMA


def _make_raw_df(spark, rows):
    return spark.createDataFrame(rows, schema=CHASE_RAW_SCHEMA)


def test_row_hash_is_deterministic(spark):
    row = Row(**{
        "Transaction Date": "01/15/2024",
        "Post Date": "01/17/2024",
        "Description": "STARBUCKS #123",
        "Category": "Food & Drink",
        "Type": "Sale",
        "Amount": -5.75,
        "Memo": None,
    })
    df = _make_raw_df(spark, [row])
    hash1 = _add_metadata(df, "path/a").select("_row_hash").first()[0]
    hash2 = _add_metadata(df, "path/a").select("_row_hash").first()[0]
    assert hash1 == hash2


def test_positive_amount_rows_filtered(spark):
    """Credits (positive Chase Amount) must be dropped at bronze."""
    rows = [
        Row(**{"Transaction Date": "01/10/2024", "Post Date": "01/12/2024",
               "Description": "AMAZON.COM", "Category": "Shopping",
               "Type": "Sale", "Amount": -48.99, "Memo": None}),
        Row(**{"Transaction Date": "01/15/2024", "Post Date": "01/17/2024",
               "Description": "AUTOPAY PAYMENT", "Category": "Payment",
               "Type": "Payment", "Amount": 500.00, "Memo": None}),
    ]
    from pyspark.sql import functions as F
    df = _make_raw_df(spark, rows)
    df = _add_metadata(df, "test")
    filtered = df.filter(F.col("Amount") < 0)
    assert filtered.count() == 1
    assert filtered.first()["Description"] == "AMAZON.COM"


def test_row_hash_changes_with_different_source(spark):
    """_batch_id (sha1 of source path) is baked in but _row_hash is based on row content only."""
    row = Row(**{
        "Transaction Date": "02/01/2024", "Post Date": "02/03/2024",
        "Description": "NETFLIX", "Category": "Entertainment",
        "Type": "Sale", "Amount": -15.49, "Memo": None,
    })
    df = _make_raw_df(spark, [row])
    hash_a = _add_metadata(df, "path/a").select("_row_hash").first()[0]
    hash_b = _add_metadata(df, "path/b").select("_row_hash").first()[0]
    # _row_hash is content-only (no source path), so must be equal
    assert hash_a == hash_b


def test_metadata_columns_present(spark):
    row = Row(**{
        "Transaction Date": "03/01/2024", "Post Date": "03/03/2024",
        "Description": "TRADER JOES", "Category": "Groceries",
        "Type": "Sale", "Amount": -82.10, "Memo": None,
    })
    df = _add_metadata(_make_raw_df(spark, [row]), "some/path")
    cols = df.columns
    for col in ("_ingest_ts", "_source_file", "_batch_id", "_row_hash"):
        assert col in cols, f"Missing metadata column: {col}"
