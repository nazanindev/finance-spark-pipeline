import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


def monthly_by_category(silver_df: DataFrame) -> DataFrame:
    monthly = (silver_df
        .filter(~F.col("is_credit"))
        .groupBy("statement_month", "category")
        .agg(
            F.round(F.sum("amount"), 2).alias("total_spend"),
            F.count("*").alias("tx_count"),
            F.round(F.avg("amount"), 2).alias("avg_tx"),
        ))
    month_totals = (monthly
        .groupBy("statement_month")
        .agg(F.sum("total_spend").alias("month_total")))
    monthly = monthly.join(month_totals, "statement_month")
    monthly = monthly.withColumn("pct_of_month",
        F.round(F.col("total_spend") / F.col("month_total") * 100, 1))
    w = Window.partitionBy("category").orderBy("statement_month")
    monthly = monthly.withColumn("mom_delta",
        F.round(F.col("total_spend") - F.lag("total_spend", 1).over(w), 2))
    return monthly.drop("month_total")


def fixed_vs_variable(silver_df: DataFrame) -> DataFrame:
    monthly = (silver_df
        .filter(~F.col("is_credit"))
        .groupBy("statement_month", "category")
        .agg(F.sum("amount").alias("monthly_spend")))
    stats = (monthly
        .groupBy("category")
        .agg(
            F.round(F.avg("monthly_spend"), 2).alias("avg_monthly"),
            F.round(F.stddev("monthly_spend"), 2).alias("stddev_monthly"),
            F.count("*").alias("sample_months"),
        ))
    stats = stats.withColumn("cv",
        F.when(F.col("avg_monthly") > 0,
            F.round(F.col("stddev_monthly") / F.col("avg_monthly"), 3)
        ).otherwise(None))
    return stats.withColumn("classification",
        F.when(F.col("cv") < 0.15, "FIXED")
         .when(F.col("cv") < 0.50, "VARIABLE")
         .otherwise("DISCRETIONARY"))


def build_gold(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    silver_df = spark.read.format("delta").load(silver_path)
    silver_df.cache()
    silver_df.count()  # materialize cache — silver is read by multiple gold builds

    monthly_by_category(silver_df).coalesce(1).write \
        .format("delta").mode("overwrite") \
        .save(f"{gold_path}/monthly_by_category")

    fixed_vs_variable(silver_df).coalesce(1).write \
        .format("delta").mode("overwrite") \
        .save(f"{gold_path}/fixed_vs_variable")

    silver_df.unpersist()
