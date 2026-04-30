# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Aggregations
# MAGIC
# MAGIC Builds four analytical views from silver:
# MAGIC 1. **monthly_by_category** — spend per category per statement month, with MoM delta and % of month total
# MAGIC 2. **fixed_vs_variable** — CV-based classification (FIXED / VARIABLE / DISCRETIONARY)
# MAGIC 3. **recurring** — merchants appearing consistently with stable amounts
# MAGIC
# MAGIC Silver is cached once and shared across all gold writes (avoid re-reading Delta twice).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<username>/finance-spark-pipeline")  # Databricks
# sys.path.insert(0, ".")  # local

from config.pipeline_config import get_config
from src.utils.spark_session import get_spark
from src.gold.aggregations import build_gold
from src.gold.recurring import RecurringDetector

spark = get_spark()
cfg = get_config(use_sample_data=True)

# COMMAND ----------

build_gold(spark, cfg.silver_path, cfg.gold_path)

# COMMAND ----------

# MAGIC %md ### Monthly Spend by Category

# COMMAND ----------

spark.read.format("delta").load(f"{cfg.gold_path}/monthly_by_category") \
     .orderBy("statement_month", "category").show(40, truncate=False)

# COMMAND ----------

# MAGIC %md ### Fixed vs Variable Classification

# COMMAND ----------

spark.read.format("delta").load(f"{cfg.gold_path}/fixed_vs_variable") \
     .orderBy("classification", "category").show(truncate=False)

# COMMAND ----------

# MAGIC %md ### Recurring Charges

# COMMAND ----------

silver_df = spark.read.format("delta").load(cfg.silver_path)
RecurringDetector().detect(silver_df).orderBy("is_recurring", ascending=False).show(20, truncate=False)
