# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transform
# MAGIC
# MAGIC Cleans, types, and enriches bronze data. Partitioned by `statement_month`.
# MAGIC
# MAGIC **Transformations applied:**
# MAGIC - Dates parsed from `MM/dd/yyyy` strings → `DateType`
# MAGIC - Amount sign flipped (Chase negative charge → positive spend)
# MAGIC - Description normalized (upper, strip symbols) via UDF
# MAGIC - Auto-categorization: keyword map → Chase category map → "Uncategorized"
# MAGIC - Statement month: day ≥ 29 → next month (Chase billing cycle boundary)
# MAGIC - Deduplication: left_anti join on `_row_hash` against existing silver

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<username>/finance-spark-pipeline")  # Databricks
# sys.path.insert(0, ".")  # local

from config.pipeline_config import get_config
from src.utils.spark_session import get_spark
from src.silver.transform import build_silver
from src.silver.quality import DataQualityChecker

spark = get_spark()
cfg = get_config(use_sample_data=True)

# COMMAND ----------

build_silver(spark, cfg.bronze_path, cfg.silver_path)

# COMMAND ----------

silver_df = spark.read.format("delta").load(cfg.silver_path)
print(f"Silver row count: {silver_df.count()}")
print(f"Partitions: {silver_df.rdd.getNumPartitions()}")
silver_df.groupBy("statement_month").count().orderBy("statement_month").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Check

# COMMAND ----------

results = DataQualityChecker().run(silver_df, spark, cfg.quality_log_path)
for r in results:
    status = "PASS" if r.failed_rows == 0 else f"FAIL ({r.failed_rows} rows)"
    print(f"[{r.severity}] {r.rule_name}: {status}")

# COMMAND ----------

silver_df.select("trans_date", "description", "category", "amount", "statement_month") \
         .orderBy("statement_month", "trans_date") \
         .show(20, truncate=False)
