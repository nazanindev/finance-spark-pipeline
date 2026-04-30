import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from config.categories import (
    _normalize_text, FALLBACK_KEYWORD_MAP, CHASE_CATEGORY_MAP,
)


def _make_categorize_udf():
    keyword_map = FALLBACK_KEYWORD_MAP
    chase_map = CHASE_CATEGORY_MAP

    def _auto_categorize(description: str, chase_category: str) -> str:
        norm = _normalize_text(description or "")
        for keyword, cat in keyword_map:
            if keyword in norm:
                return cat
        if chase_category:
            mapped = chase_map.get(chase_category.strip(), None)
            if mapped:
                return mapped
        return "Uncategorized"

    return udf(_auto_categorize, StringType())


normalize_udf = udf(_normalize_text, StringType())
categorize_udf = _make_categorize_udf()


def normalize_bronze(df: DataFrame) -> DataFrame:
    return (df
        .withColumnRenamed("Transaction Date", "trans_date_str")
        .withColumnRenamed("Post Date",        "post_date_str")
        .withColumnRenamed("Description",      "description")
        .withColumnRenamed("Category",         "chase_category")
        .withColumnRenamed("Type",             "transaction_type")
        .withColumnRenamed("Amount",           "amount_raw")
        .withColumnRenamed("Memo",             "memo")
        .withColumn("trans_date", F.to_date("trans_date_str", "MM/dd/yyyy"))
        .withColumn("post_date",  F.to_date("post_date_str",  "MM/dd/yyyy"))
        # Flip sign: Chase negative = charge → positive spend amount
        .withColumn("amount", (-F.col("amount_raw")).cast("double"))
        .withColumn("description_norm", normalize_udf(F.col("description")))
        .withColumn("category", categorize_udf(F.col("description"), F.col("chase_category")))
        # Day >= 29 rolls to the next month's statement (Chase billing cycle)
        .withColumn("statement_month",
            F.when(F.dayofmonth(F.col("trans_date")) >= 29,
                F.date_format(F.add_months(F.date_trunc("month", F.col("trans_date")), 1), "yyyy-MM")
            ).otherwise(
                F.date_format(F.col("trans_date"), "yyyy-MM")
            ))
        .withColumn("is_credit", F.col("amount") < 0)
        .withColumn("_silver_ts", F.current_timestamp())
        .drop("trans_date_str", "post_date_str", "amount_raw")
    )


def deduplicate(new_df: DataFrame, existing_silver_path: str, spark: SparkSession) -> DataFrame:
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, existing_silver_path):
        return new_df
    existing_hashes = spark.read.format("delta").load(existing_silver_path).select("_row_hash")
    return new_df.join(existing_hashes, on="_row_hash", how="left_anti")


def build_silver(spark: SparkSession, bronze_path: str, silver_path: str) -> None:
    bronze_df = spark.read.format("delta").load(bronze_path)
    silver_df = normalize_bronze(bronze_df)
    silver_df = deduplicate(silver_df, silver_path, spark)
    (silver_df.write
        .format("delta")
        .partitionBy("statement_month")
        .mode("append")
        .save(silver_path))
