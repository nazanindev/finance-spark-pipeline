# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Structured Streaming Demo
# MAGIC
# MAGIC Demonstrates checkpoint-based exactly-once semantics:
# MAGIC 1. Drop Q1 CSV → start stream → Q1 rows land in bronze
# MAGIC 2. Drop Q2 CSV → Q2 rows appear **without re-processing Q1** (checkpoint tracks progress)
# MAGIC 3. Verify via `_source_file` column that Q1 and Q2 files are distinct
# MAGIC
# MAGIC **Why streaming for personal finance?**
# MAGIC Not latency — the checkpoint log gives us exactly-once guarantees for free.
# MAGIC Dropping a CSV into the source directory is the entire ETL trigger.
# MAGIC No job scheduler, no manual dedup tracking, no "did I already process this?" logic.

# COMMAND ----------

import sys, time, shutil, os
sys.path.insert(0, "/Workspace/Repos/<username>/finance-spark-pipeline")  # Databricks
# sys.path.insert(0, ".")  # local

from config.pipeline_config import get_config, is_databricks
from src.utils.spark_session import get_spark
from src.bronze.ingest import ingest_csv_to_bronze, CHASE_RAW_SCHEMA

spark = get_spark()
cfg = get_config(use_sample_data=True)

STREAM_SOURCE = "./data/stream_demo"
os.makedirs(STREAM_SOURCE, exist_ok=True)

# COMMAND ----------

# MAGIC %md ### Step 1 — Drop Q1, start stream

# COMMAND ----------

shutil.copy("data/sample/chase_sample_2024_q1.csv", f"{STREAM_SOURCE}/q1.csv")
query = ingest_csv_to_bronze(spark, STREAM_SOURCE, cfg.bronze_path, mode="stream")

time.sleep(10)
bronze_df = spark.read.format("delta").load(cfg.bronze_path)
print(f"After Q1 drop — bronze rows: {bronze_df.count()}")
print("Active streams:", [q.name for q in spark.streams.active])

# COMMAND ----------

# MAGIC %md ### Step 2 — Drop Q2, stream picks it up

# COMMAND ----------

shutil.copy("data/sample/chase_sample_2024_q2.csv", f"{STREAM_SOURCE}/q2.csv")
time.sleep(15)

bronze_df = spark.read.format("delta").load(cfg.bronze_path)
print(f"After Q2 drop — bronze rows: {bronze_df.count()}")
bronze_df.select("_source_file").distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification
# MAGIC
# MAGIC `_source_file` shows exactly two distinct files — Q1 was not reprocessed when Q2 arrived.
# MAGIC The checkpoint directory (`bronze_path/_checkpoint`) stores the offset log that makes this possible.

# COMMAND ----------

query.stop()
print("Stream stopped.")
