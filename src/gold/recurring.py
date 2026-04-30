import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


class RecurringDetector:
    def __init__(self, min_months: int = 2, amount_cv_threshold: float = 0.05):
        self.min_months = min_months
        self.amount_cv_threshold = amount_cv_threshold

    def detect(self, silver_df: DataFrame) -> DataFrame:
        charges = silver_df.filter(~F.col("is_credit"))
        grouped = (charges
            .groupBy("description_norm", "category")
            .agg(
                F.countDistinct("statement_month").alias("occurrence_months"),
                F.round(F.expr("percentile_approx(amount, 0.5)"), 2).alias("median_amount"),
                F.round(F.stddev("amount"), 2).alias("amount_stddev"),
                F.collect_set("statement_month").alias("months_seen"),
            ))
        grouped = grouped.withColumn("amount_cv",
            F.when(F.col("median_amount") > 0,
                F.col("amount_stddev") / F.col("median_amount")
            ).otherwise(None))
        grouped = grouped.withColumn("is_recurring",
            (F.col("occurrence_months") >= self.min_months) &
            (F.col("amount_cv") < self.amount_cv_threshold))
        grouped = grouped.withColumn("recurrence_type",
            F.when(~F.col("is_recurring"), "IRREGULAR")
             .when(F.col("occurrence_months") >= 6, "MONTHLY")
             .otherwise("OCCASIONAL"))
        return grouped.select(
            "description_norm", "category", "median_amount",
            "occurrence_months", "is_recurring", "recurrence_type", "amount_cv",
        )
