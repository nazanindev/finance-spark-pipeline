from dataclasses import dataclass, field, asdict
from typing import List
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from config.categories import CATEGORIES


@dataclass
class QualityRule:
    name: str
    condition: str  # SQL predicate — True = valid row
    severity: str   # "ERROR" | "WARN"


@dataclass
class QualityResult:
    rule_name: str
    severity: str
    total_rows: int
    failed_rows: int
    failure_pct: float


SILVER_QUALITY_RULES = [
    QualityRule("no_null_trans_date",  "trans_date IS NOT NULL",                       "ERROR"),
    QualityRule("positive_amount",     "amount > 0",                                   "ERROR"),
    QualityRule("valid_stmt_month",    "statement_month RLIKE '^[0-9]{4}-[0-9]{2}$'", "ERROR"),
    QualityRule("no_null_description", "description IS NOT NULL",                      "ERROR"),
    QualityRule("known_category",      f"category IN {tuple(CATEGORIES)}",             "WARN"),
    QualityRule("no_future_dates",     "trans_date <= current_date()",                 "WARN"),
    QualityRule("reasonable_amount",   "amount < 50000",                               "WARN"),
]


class DataQualityChecker:
    def __init__(self, rules: List[QualityRule] = None):
        self.rules = rules or SILVER_QUALITY_RULES

    def run(self, df: DataFrame, spark: SparkSession, quality_log_path: str) -> List[QualityResult]:
        total = df.count()
        results = []
        for rule in self.rules:
            failed = df.filter(f"NOT ({rule.condition})").count()
            results.append(QualityResult(
                rule_name=rule.name,
                severity=rule.severity,
                total_rows=total,
                failed_rows=failed,
                failure_pct=round(failed / total * 100, 2) if total else 0,
            ))
        log_df = (spark.createDataFrame([asdict(r) for r in results])
                  .withColumn("checked_at", F.current_timestamp()))
        log_df.write.format("delta").mode("append").save(quality_log_path)
        errors = [r for r in results if r.severity == "ERROR" and r.failed_rows > 0]
        if errors:
            raise ValueError(f"Quality check failed: {[r.rule_name for r in errors]}")
        return results
