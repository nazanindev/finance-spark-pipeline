# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingest
# MAGIC
# MAGIC Reads raw Chase CSVs with an enforced schema and upserts to a Delta table.
# MAGIC
# MAGIC **Key design decisions:**
# MAGIC - `_row_hash` (SHA-256 of all content fields) is the MERGE key → re-dropping the same CSV is safe
# MAGIC - `FAILFAST` schema enforcement catches corrupt input at ingest rather than silently writing nulls
# MAGIC - Positive-Amount rows (credits/payments) are filtered here so all downstream layers deal only with charges

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<username>/finance-spark-pipeline")  # Databricks
# sys.path.insert(0, ".")  # local

from config.pipeline_config import get_config
from src.utils.spark_session import get_spark
from src.bronze.ingest import ingest_csv_to_bronze

spark = get_spark()
cfg = get_config(use_sample_data=True)

# COMMAND ----------

ingest_csv_to_bronze(spark, cfg.source_csv_path, cfg.bronze_path)

# COMMAND ----------

bronze_df = spark.read.format("delta").load(cfg.bronze_path)
print(f"Bronze row count: {bronze_df.count()}")
bronze_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Idempotency check
# MAGIC
# MAGIC Re-run ingest — row count must not change (MERGE on `_row_hash` deduplicates).

# COMMAND ----------

ingest_csv_to_bronze(spark, cfg.source_csv_path, cfg.bronze_path)
count_after = spark.read.format("delta").load(cfg.bronze_path).count()
print(f"Row count after re-ingest (must equal first run): {count_after}")
