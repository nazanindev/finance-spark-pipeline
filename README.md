# finance-spark-pipeline

PySpark personal finance pipeline that turns raw Chase credit card CSV exports into a medallion-architecture Delta Lake pipeline.

> **Intentionally overengineered.** The data is a few thousand rows of personal credit card transactions — pandas would be faster and simpler. The point is to practice Spark, Delta Lake, and medallion architecture patterns at a scale where mistakes are cheap, before applying them where they actually matter.

---

## Architecture

```
Chase CSV exports
       │
       ▼
  ┌─────────┐   _row_hash MERGE (idempotent)
  │  Bronze  │   raw + metadata columns, credits filtered
  └─────────┘
       │
       ▼
  ┌─────────┐   partitioned by statement_month
  │  Silver  │   typed, normalized, auto-categorized, quality-checked
  └─────────┘
       │
       ▼
  ┌─────────┐   monthly_by_category · fixed_vs_variable · recurring
  │   Gold   │   aggregated, query-ready Delta tables
  └─────────┘
```

---

## Local Setup

> **Critical**: `delta-spark` version must match `pyspark`. PySpark 3.5.x requires `delta-spark==3.1.x`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-local.txt
```

Generate synthetic sample data (already committed, but re-generate anytime):
```bash
python3 scripts/generate_sample_data.py
```

Run the test suite:
```bash
pytest tests/ -v
```

---

## Running the Pipeline Locally

```python
from src.utils.spark_session import get_spark
from config.pipeline_config import get_config
from src.bronze.ingest import ingest_csv_to_bronze
from src.silver.transform import build_silver
from src.gold.aggregations import build_gold

spark = get_spark()
cfg = get_config(use_sample_data=True)

ingest_csv_to_bronze(spark, cfg.source_csv_path, cfg.bronze_path)
build_silver(spark, cfg.bronze_path, cfg.silver_path)
build_gold(spark, cfg.silver_path, cfg.gold_path)
```

---

## Databricks Community Edition Setup

1. Import this repo via **Repos** → **Add Repo**
2. Open `notebooks/00_setup_environment.py` — update the `sys.path` line with your username
3. Run notebooks `00` → `03` in order against sample data
4. To use real Chase exports: upload CSVs to `dbfs:/FileStore/finance/real/` and set `use_sample_data=False`

---

## Repo Structure

```
├── config/
│   ├── categories.py       # CATEGORIES, CHASE_CATEGORY_MAP, FALLBACK_KEYWORD_MAP, _normalize_text()
│   └── pipeline_config.py  # PipelineConfig dataclass, dual Databricks/local path resolution
├── data/
│   ├── sample/             # synthetic Chase CSVs (committed, safe to share)
│   └── real/               # gitignored — place real exports here
├── notebooks/
│   ├── 00_setup_environment.py
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_aggregations.py
│   ├── 04_streaming_demo.py   # checkpoint exactly-once semantics
│   └── 05_spark_internals.py  # BroadcastHashJoin, ZORDER, caching, partitioning
├── scripts/
│   └── generate_sample_data.py
├── src/
│   ├── bronze/ingest.py        # CSV → Delta with _row_hash MERGE
│   ├── silver/transform.py     # normalize, categorize, deduplicate
│   ├── silver/quality.py       # DataQualityChecker with ERROR/WARN rules
│   ├── gold/aggregations.py    # monthly_by_category, fixed_vs_variable
│   ├── gold/recurring.py       # RecurringDetector using CV on amount
│   └── utils/                  # SparkSession factory, Delta upsert/optimize helpers
└── tests/                      # pytest with session-scoped local SparkSession
```

---

## Key Design Decisions

| Decision | Why |
|---|---|
| `_row_hash` MERGE at bronze | Re-dropping any CSV is safe — idempotent at every layer |
| Anti-join dedup at silver | Catches duplicates that slip through if bronze MERGE key changes |
| `statement_month` partition key | All finance queries are month-scoped — pruning eliminates whole months |
| CV for subscription detection | Robust to minor price changes; threshold is a constructor param |
| `coalesce(1)` on gold tables | Small tables — avoids tiny-file overhead without a shuffle |
| `cache()` + `count()` before multi-gold builds | Silver read once, shared across both gold writes |

---

## Interview Talking Points

| Question | Where to look |
|---|---|
| "Walk me through medallion" | `src/bronze/`, `src/silver/`, `src/gold/` |
| "How do you handle reprocessing?" | `ingest.py` `_row_hash` MERGE + `transform.py` `deduplicate()` |
| "Why Delta over Parquet?" | `delta_helpers.py` MERGE; notebook 05 DESCRIBE HISTORY |
| "Partition pruning vs data skipping?" | Notebook 05 OPTIMIZE+ZORDER section |
| "Why streaming for personal finance?" | Notebook 04 — checkpoint exactly-once, no scheduler needed |
| "How do you detect subscriptions?" | `recurring.py` CV threshold as constructor param |
| "repartition vs coalesce?" | Notebook 05 partition control section |
