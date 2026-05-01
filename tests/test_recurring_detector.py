import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
)
from src.gold.recurring import RecurringDetector


_SCHEMA = StructType([
    StructField("description_norm", StringType(),  True),
    StructField("category",         StringType(),  True),
    StructField("amount",           DoubleType(),  True),
    StructField("statement_month",  StringType(),  True),
    StructField("is_credit",        BooleanType(), True),
])


def _make_df(spark, rows):
    return spark.createDataFrame(
        [Row(description_norm=r[0], category=r[1], amount=r[2],
             statement_month=r[3], is_credit=False)
         for r in rows],
        schema=_SCHEMA,
    )


def test_stable_monthly_merchant_flagged_recurring(spark):
    """Netflix at $15.49 every month for 3 months → is_recurring=True, MONTHLY."""
    rows = [
        ("NETFLIX", "Subscriptions", 15.49, "2024-01"),
        ("NETFLIX", "Subscriptions", 15.49, "2024-02"),
        ("NETFLIX", "Subscriptions", 15.49, "2024-03"),
    ]
    df = _make_df(spark, rows)
    result = RecurringDetector(min_months=2, amount_cv_threshold=0.05).detect(df)
    netflix = result.filter(result.description_norm == "NETFLIX").first()
    assert netflix is not None
    assert netflix["is_recurring"] is True
    assert netflix["recurrence_type"] == "OCCASIONAL"  # < 6 months → OCCASIONAL


def test_six_month_merchant_flagged_monthly(spark):
    """Merchant seen 6 months → recurrence_type == MONTHLY."""
    rows = [(f"SPOTIFY", "Subscriptions", 10.99, f"2024-{m:02d}") for m in range(1, 7)]
    df = _make_df(spark, rows)
    result = RecurringDetector(min_months=2).detect(df)
    spotify = result.filter(result.description_norm == "SPOTIFY").first()
    assert spotify["recurrence_type"] == "MONTHLY"


def test_single_occurrence_is_irregular(spark):
    """Merchant appearing once → is_recurring=False, IRREGULAR."""
    rows = [("RANDOM SHOP", "Shopping", 42.00, "2024-01")]
    df = _make_df(spark, rows)
    result = RecurringDetector(min_months=2).detect(df)
    row = result.filter(result.description_norm == "RANDOM SHOP").first()
    assert row["is_recurring"] is False
    assert row["recurrence_type"] == "IRREGULAR"


def test_high_cv_merchant_not_recurring(spark):
    """Merchant with wildly varying amounts should not be flagged as recurring."""
    rows = [
        ("AMAZON", "Shopping", 5.0,   "2024-01"),
        ("AMAZON", "Shopping", 500.0, "2024-02"),
        ("AMAZON", "Shopping", 12.0,  "2024-03"),
    ]
    df = _make_df(spark, rows)
    result = RecurringDetector(min_months=2, amount_cv_threshold=0.05).detect(df)
    amazon = result.filter(result.description_norm == "AMAZON").first()
    assert amazon["is_recurring"] is False
