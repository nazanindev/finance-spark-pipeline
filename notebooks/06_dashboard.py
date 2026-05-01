# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Dashboard
# MAGIC
# MAGIC Reads from the Gold Delta tables and produces four charts:
# MAGIC 1. Monthly spend by category (stacked bar)
# MAGIC 2. Month-over-month total spend (line)
# MAGIC 3. Fixed vs Variable vs Discretionary breakdown (horizontal bar)
# MAGIC 4. Recurring subscriptions ranked by median amount (bar)
# MAGIC
# MAGIC Run notebooks 00–03 first so the Gold tables exist.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<username>/finance-spark-pipeline")  # Databricks
# sys.path.insert(0, ".")  # local

from config.pipeline_config import get_config
from src.utils.spark_session import get_spark

spark = get_spark()
cfg = get_config(use_sample_data=True)

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

monthly_pd = spark.read.format("delta") \
    .load(f"{cfg.gold_path}/monthly_by_category") \
    .orderBy("statement_month", "category") \
    .toPandas()

fv_pd = spark.read.format("delta") \
    .load(f"{cfg.gold_path}/fixed_vs_variable") \
    .orderBy("classification", "avg_monthly") \
    .toPandas()

from src.gold.recurring import RecurringDetector
silver_df = spark.read.format("delta").load(cfg.silver_path)
recurring_pd = RecurringDetector().detect(silver_df) \
    .filter("is_recurring = true") \
    .orderBy("median_amount", ascending=False) \
    .toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Monthly Spend by Category

# COMMAND ----------

pivot = monthly_pd.pivot_table(
    index="statement_month", columns="category",
    values="total_spend", aggfunc="sum", fill_value=0,
)

fig, ax = plt.subplots(figsize=(12, 5))
pivot.plot(kind="bar", stacked=True, ax=ax, colormap="tab20")
ax.set_title("Monthly Spend by Category")
ax.set_xlabel("")
ax.set_ylabel("USD ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax.legend(loc="upper left", bbox_to_anchor=(1, 1), fontsize=8)
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Month-over-Month Total Spend

# COMMAND ----------

mom = monthly_pd.groupby("statement_month")["total_spend"].sum().reset_index()

fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(mom["statement_month"], mom["total_spend"], marker="o", linewidth=2, color="#1f77b4")
ax.fill_between(mom["statement_month"], mom["total_spend"], alpha=0.15, color="#1f77b4")
ax.set_title("Total Spend per Month")
ax.set_xlabel("")
ax.set_ylabel("USD ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Fixed vs Variable vs Discretionary

# COMMAND ----------

colors = {"FIXED": "#2ecc71", "VARIABLE": "#f39c12", "DISCRETIONARY": "#e74c3c"}

fig, ax = plt.subplots(figsize=(9, max(4, len(fv_pd) * 0.35)))
bars = ax.barh(
    fv_pd["category"],
    fv_pd["avg_monthly"],
    color=[colors.get(c, "#999") for c in fv_pd["classification"]],
)
ax.set_title("Avg Monthly Spend by Category (Fixed / Variable / Discretionary)")
ax.set_xlabel("Avg Monthly Spend ($)")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
from matplotlib.patches import Patch
ax.legend(handles=[Patch(color=v, label=k) for k, v in colors.items()], loc="lower right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Recurring Subscriptions

# COMMAND ----------

if recurring_pd.empty:
    print("No recurring charges detected — try lowering RecurringDetector thresholds.")
else:
    fig, ax = plt.subplots(figsize=(9, max(3, len(recurring_pd) * 0.4)))
    ax.barh(
        recurring_pd["description_norm"],
        recurring_pd["median_amount"],
        color="#8e44ad",
    )
    ax.set_title("Recurring Charges — Median Amount")
    ax.set_xlabel("Median Amount ($)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.2f}"))
    plt.tight_layout()
    plt.show()

    print(f"\n{len(recurring_pd)} recurring merchants detected")
    print(f"Estimated monthly subscription cost: ${recurring_pd['median_amount'].sum():,.2f}")
