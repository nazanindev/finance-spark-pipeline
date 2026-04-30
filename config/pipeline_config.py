import os
from dataclasses import dataclass


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass
class PipelineConfig:
    bronze_path: str
    silver_path: str
    gold_path: str
    checkpoint_path: str
    source_csv_path: str
    quality_log_path: str
    merchant_rules_path: str


def get_config(use_sample_data: bool = False) -> PipelineConfig:
    if is_databricks():
        base = "dbfs:/user/hive/warehouse/finance_pipeline"
        source = (
            "dbfs:/FileStore/finance/real"
            if not use_sample_data
            else "dbfs:/FileStore/finance/sample"
        )
    else:
        base = "./delta_tables"
        source = "./data/real" if not use_sample_data else "./data/sample"

    return PipelineConfig(
        bronze_path=f"{base}/bronze/transactions",
        silver_path=f"{base}/silver/transactions",
        gold_path=f"{base}/gold",
        checkpoint_path=f"{base}/checkpoints",
        source_csv_path=source,
        quality_log_path=f"{base}/gold/quality_log",
        merchant_rules_path=f"{base}/silver/merchant_rules",
    )
