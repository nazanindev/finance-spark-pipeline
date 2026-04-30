# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup Environment
# MAGIC
# MAGIC Creates Delta table directories and, on Databricks, uploads sample CSVs to DBFS.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<username>/finance-spark-pipeline")  # Databricks
# sys.path.insert(0, ".")  # local

from config.pipeline_config import get_config, is_databricks
from src.utils.spark_session import get_spark

spark = get_spark()
cfg = get_config(use_sample_data=True)  # flip to False for real data

# COMMAND ----------

import os

paths = [
    cfg.bronze_path, cfg.silver_path, cfg.gold_path,
    cfg.checkpoint_path, cfg.quality_log_path, cfg.merchant_rules_path,
]

if not is_databricks():
    for p in paths:
        os.makedirs(p, exist_ok=True)
    print("Local Delta table directories created.")
else:
    # MAGIC %md
    # MAGIC ### Databricks: Upload sample CSVs to DBFS
    # MAGIC
    # MAGIC Run the cell below **once** after cloning the repo:
    # MAGIC ```
    # MAGIC %sh
    # MAGIC cp -r /Workspace/Repos/<username>/finance-spark-pipeline/data/sample/ \
    # MAGIC        /dbfs/FileStore/finance/sample/
    # MAGIC ```
    print("On Databricks — upload CSVs via %sh cell above.")

# COMMAND ----------

print("Config:")
print(f"  source  : {cfg.source_csv_path}")
print(f"  bronze  : {cfg.bronze_path}")
print(f"  silver  : {cfg.silver_path}")
print(f"  gold    : {cfg.gold_path}")
