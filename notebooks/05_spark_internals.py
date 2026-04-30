# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Spark Internals
# MAGIC
# MAGIC Four demonstrations of Spark/Delta internals for interview depth:
# MAGIC 1. Join strategy: BroadcastHashJoin vs SortMergeJoin
# MAGIC 2. Partition control: `repartitionByRange` before a partitioned Delta write
# MAGIC 3. Caching: materialize once, use many times, release with `unpersist`
# MAGIC 4. Delta OPTIMIZE + ZORDER: file compaction + min/max data skipping

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<username>/finance-spark-pipeline")  # Databricks
# sys.path.insert(0, ".")  # local

from config.pipeline_config import get_config
from src.utils.spark_session import get_spark
from src.utils.delta_helpers import optimize_table

spark = get_spark()
cfg = get_config(use_sample_data=True)

silver_df = spark.read.format("delta").load(cfg.silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Join Strategy: BroadcastHashJoin vs SortMergeJoin
# MAGIC
# MAGIC Spark's default `autoBroadcastJoinThreshold` (10 MB) causes the small
# MAGIC `existing_hashes` table in `deduplicate()` to be broadcast automatically.
# MAGIC
# MAGIC **BroadcastHashJoin**: small table is sent to all executors → no shuffle.
# MAGIC **SortMergeJoin**: both sides sorted and merged → full shuffle on join key.
# MAGIC
# MAGIC For our silver dedup (thousands of hashes vs. millions of rows),
# MAGIC broadcast is always faster. The threshold is a tuning knob — lower it
# MAGIC when broadcast tables are too large and cause OOM on executors.

# COMMAND ----------

existing_hashes = silver_df.select("_row_hash").limit(100)
joined = silver_df.join(existing_hashes, on="_row_hash", how="left_anti")

print("=== With autoBroadcastJoinThreshold (default) — expect BroadcastHashJoin ===")
joined.explain(mode="formatted")

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
print("=== autoBroadcastJoinThreshold disabled — expect SortMergeJoin ===")
joined.explain(mode="formatted")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))  # restore

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Partition Control: repartitionByRange
# MAGIC
# MAGIC Before writing a Delta table partitioned by `statement_month`, we want each
# MAGIC Spark task to handle rows from only one (or a few) months. `repartitionByRange`
# MAGIC does a range-based shuffle — rows land in sorted ranges of the key.
# MAGIC
# MAGIC **repartition**: full shuffle, distributes rows evenly (good for downstream parallelism).
# MAGIC **coalesce**: no shuffle, just reduces task count (use for small gold tables to avoid tiny files).
# MAGIC **repartitionByRange**: full shuffle, co-locates range-adjacent keys (use before partitioned write).

# COMMAND ----------

print(f"Partitions before: {silver_df.rdd.getNumPartitions()}")
re_df = silver_df.repartitionByRange("statement_month")
print(f"Partitions after repartitionByRange: {re_df.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Caching: materialize once, reuse, release
# MAGIC
# MAGIC `cache()` marks the DataFrame as cacheable; nothing is computed yet.
# MAGIC The first **action** (`count()`) materializes it into executor memory.
# MAGIC Subsequent actions (the two gold aggregations) read from cache, skipping
# MAGIC the Delta scan entirely. `unpersist()` frees the memory immediately
# MAGIC rather than waiting for LRU eviction.

# COMMAND ----------

silver_df.cache()
silver_df.count()  # materialization action

from src.gold.aggregations import monthly_by_category, fixed_vs_variable
monthly_by_category(silver_df).count()    # reads from cache
fixed_vs_variable(silver_df).count()      # reads from cache

silver_df.unpersist()
print("Cache released.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Delta OPTIMIZE + ZORDER
# MAGIC
# MAGIC **OPTIMIZE** compacts many small Parquet files into fewer larger ones.
# MAGIC Small files are the #1 Delta performance killer — each file requires a
# MAGIC separate task, and task overhead dominates for tiny files.
# MAGIC
# MAGIC **ZORDER BY (col1, col2)** reorders data within each compacted file so
# MAGIC that values of the ZORDER columns are co-located. Delta then writes
# MAGIC per-file min/max statistics to the transaction log. When a query filters
# MAGIC `WHERE statement_month = '2024-03' AND category = 'Groceries'`, the
# MAGIC query planner skips any file whose min/max range excludes those values.
# MAGIC
# MAGIC **Partition pruning vs data skipping:**
# MAGIC - Pruning: Spark skips entire *directories* (partition folders).
# MAGIC - Skipping: Delta reads per-file stats from the transaction log to skip
# MAGIC   individual *files within* a partition. ZORDER makes skipping effective
# MAGIC   for columns that are not the partition key (e.g., `category`).

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY delta.`{cfg.silver_path}`").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(5, truncate=False)

# COMMAND ----------

optimize_table(spark, cfg.silver_path, zorder_cols=["statement_month", "category"])

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY delta.`{cfg.silver_path}`").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(5, truncate=False)
# operationMetrics shows numRemovedFiles / numAddedFiles — fewer files = success
