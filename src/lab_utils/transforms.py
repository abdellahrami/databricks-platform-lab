"""Pure PySpark transforms shared by notebooks and unit-tested in CI.

Keeping logic in plain Python modules (not notebooks) is what makes Databricks
code testable: notebooks import these, pytest exercises them with a local Spark.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ORDER_COLUMNS = ["order_id", "op", "sku", "warehouse", "quantity", "status", "event_ts", "source_lsn"]


def cast_order_events(df: DataFrame) -> DataFrame:
    """Enforce types on raw CDC events (bronze -> silver contract)."""
    return df.select(
        F.col("order_id").cast("bigint").alias("order_id"),
        F.upper(F.col("op")).alias("op"),
        F.col("sku").cast("string").alias("sku"),
        F.col("warehouse").cast("string").alias("warehouse"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("status").cast("string").alias("status"),
        F.to_timestamp("event_ts").alias("event_ts"),
        F.col("source_lsn").cast("bigint").alias("source_lsn"),
    )


def latest_per_key(df: DataFrame, keys: list, sequence_col: str, tiebreak_col: str = None) -> DataFrame:
    """Keep only the most recent event per key inside one micro-batch.

    MERGE fails if several source rows match the same target row, so CDC batches
    must be deduplicated to one row per key before merging.
    """
    order = [F.col(sequence_col).desc()]
    if tiebreak_col:
        order.append(F.col(tiebreak_col).desc())
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
