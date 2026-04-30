from pyspark.sql import SparkSession, DataFrame


def upsert_to_delta(spark: SparkSession, new_df: DataFrame, table_path: str, merge_key: str):
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, table_path):
        dt = DeltaTable.forPath(spark, table_path)
        (
            dt.alias("t")
            .merge(new_df.alias("s"), f"t.{merge_key} = s.{merge_key}")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_df.write.format("delta").save(table_path)


def optimize_table(spark: SparkSession, table_path: str, zorder_cols: list = None):
    zorder = f"ZORDER BY ({', '.join(zorder_cols)})" if zorder_cols else ""
    spark.sql(f"OPTIMIZE delta.`{table_path}` {zorder}")
