import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, BooleanType, TimestampType,
)
from src.gold.aggregations import monthly_by_category, fixed_vs_variable


_SILVER_SCHEMA = StructType([
    StructField("trans_date",       DateType(),      True),
    StructField("post_date",        DateType(),      True),
    StructField("description",      StringType(),    True),
    StructField("description_norm", StringType(),    True),
    StructField("chase_category",   StringType(),    True),
    StructField("transaction_type", StringType(),    True),
    StructField("memo",             StringType(),    True),
    StructField("amount",           DoubleType(),    True),
    StructField("category",         StringType(),    True),
    StructField("statement_month",  StringType(),    True),
    StructField("is_credit",        BooleanType(),   True),
    StructField("_row_hash",        StringType(),    True),
    StructField("_silver_ts",       TimestampType(), True),
])


def _make_silver(spark, rows):
    return spark.createDataFrame(
        [Row(trans_date=None, post_date=None, description=r[0],
             description_norm=r[0].upper(), chase_category=r[1],
             transaction_type="Sale", memo=None, amount=r[2],
             category=r[1], statement_month=r[3], is_credit=r[4],
             _row_hash="x", _silver_ts=None)
         for r in rows],
        schema=_SILVER_SCHEMA,
    )


def test_mom_delta_null_for_first_month(spark):
    """First month a category appears must have mom_delta = null."""
    rows = [
        ("Netflix", "Subscriptions", 15.49, "2024-01", False),
        ("Netflix", "Subscriptions", 15.49, "2024-02", False),
    ]
    df = _make_silver(spark, rows)
    result = monthly_by_category(df).orderBy("statement_month").collect()
    jan = [r for r in result if r["statement_month"] == "2024-01"][0]
    feb = [r for r in result if r["statement_month"] == "2024-02"][0]
    assert jan["mom_delta"] is None, "First month mom_delta must be null"
    assert feb["mom_delta"] is not None


def test_fixed_vs_variable_cv_thresholds(spark):
    """CV < 0.15 → FIXED; 0.15–0.50 → VARIABLE; > 0.50 → DISCRETIONARY."""
    rows = []
    # FIXED: same amount every month for 6 months
    for m in range(1, 7):
        rows.append(("Netflix", "Subscriptions", 15.49, f"2024-{m:02d}", False))
    # DISCRETIONARY: highly variable amounts
    rows += [
        ("Amazon", "Shopping", 10.0, "2024-01", False),
        ("Amazon", "Shopping", 200.0, "2024-02", False),
        ("Amazon", "Shopping", 5.0, "2024-03", False),
    ]
    df = _make_silver(spark, rows)
    result = {r["category"]: r["classification"] for r in fixed_vs_variable(df).collect()}
    assert result["Subscriptions"] == "FIXED"
    assert result["Shopping"] == "DISCRETIONARY"


def test_credits_excluded_from_aggregations(spark):
    """is_credit=True rows must not appear in monthly totals."""
    rows = [
        ("Starbucks", "Food & Drink", 5.0,   "2024-01", False),
        ("Refund",    "Food & Drink", -10.0,  "2024-01", True),
    ]
    df = _make_silver(spark, rows)
    result = monthly_by_category(df).collect()
    assert len(result) == 1
    assert result[0]["total_spend"] == pytest.approx(5.0)
