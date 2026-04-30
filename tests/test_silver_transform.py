import pytest
from datetime import date
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)
from src.silver.transform import normalize_bronze
from config.categories import _normalize_text


# Minimal schema that normalize_bronze expects (post-bronze columns)
_BRONZE_SCHEMA = StructType([
    StructField("Transaction Date", StringType(), True),
    StructField("Post Date",        StringType(), True),
    StructField("Description",      StringType(), True),
    StructField("Category",         StringType(), True),
    StructField("Type",             StringType(), True),
    StructField("Amount",           DoubleType(),  True),
    StructField("Memo",             StringType(), True),
    StructField("_ingest_ts",       TimestampType(), True),
    StructField("_source_file",     StringType(), True),
    StructField("_batch_id",        StringType(), True),
    StructField("_row_hash",        StringType(), True),
])


def _make_bronze(spark, rows):
    padded = []
    for r in rows:
        padded.append(Row(
            **{"Transaction Date": r[0], "Post Date": r[1],
               "Description": r[2], "Category": r[3],
               "Type": "Sale", "Amount": r[4], "Memo": None,
               "_ingest_ts": None, "_source_file": "", "_batch_id": "",
               "_row_hash": "abc"}))
    return spark.createDataFrame(padded, schema=_BRONZE_SCHEMA)


def test_statement_month_rollover(spark):
    """Transactions on day 29+ should belong to next month's statement."""
    df = _make_bronze(spark, [
        ("01/29/2024", "01/31/2024", "AMAZON.COM", "Shopping", -50.0),
        ("01/15/2024", "01/17/2024", "STARBUCKS", "Food & Drink", -6.0),
    ])
    result = normalize_bronze(df).collect()
    by_desc = {r["description"]: r["statement_month"] for r in result}
    assert by_desc["AMAZON.COM"] == "2024-02", "Day 29 should roll to Feb statement"
    assert by_desc["STARBUCKS"] == "2024-01", "Day 15 stays in Jan statement"


def test_amount_sign_flip(spark):
    """Chase negative charge → positive silver amount."""
    df = _make_bronze(spark, [
        ("02/10/2024", "02/12/2024", "NETFLIX", "Entertainment", -15.49),
    ])
    result = normalize_bronze(df).first()
    assert result["amount"] == pytest.approx(15.49)


def test_normalize_text_strips_symbols():
    assert _normalize_text("Trader Joe's #42") == "TRADER JOES 42"
    assert _normalize_text("  Starbucks &amp; Co. ") == "STARBUCKS AMP CO"
    assert _normalize_text("") == ""


def test_categorize_uses_keyword_fallback(spark):
    """Description keyword takes precedence over Chase category."""
    df = _make_bronze(spark, [
        ("03/05/2024", "03/07/2024", "TRADER JOES #99", "Shopping", -75.0),
    ])
    result = normalize_bronze(df).first()
    assert result["category"] == "Groceries"


def test_is_credit_flag(spark):
    """is_credit should be False for charges (positive after flip), True only if still negative."""
    df = _make_bronze(spark, [
        ("04/01/2024", "04/03/2024", "WHOLE FOODS", "Groceries", -92.0),
    ])
    result = normalize_bronze(df).first()
    assert result["is_credit"] is False
