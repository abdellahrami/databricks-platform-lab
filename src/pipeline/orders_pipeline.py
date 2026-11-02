# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Declarative Pipeline — orders CDC (ex-Delta Live Tables)
# MAGIC
# MAGIC Same flow as notebook 03, but **declarative**: you define tables, Databricks handles
# MAGIC checkpoints, ordering, retries, dependency graph, data-quality metrics and the MERGE logic.
# MAGIC
# MAGIC Do **not** run this notebook directly. Attach it to a pipeline:
# MAGIC - via the bundle (`resources/orders_pipeline.yml`) — `databricks bundle deploy`, or
# MAGIC - in the UI: Jobs & Pipelines → Create → ETL pipeline → add this file as source,
# MAGIC   set configuration `lab.source_path = /Volumes/workspace/tecsys_lab/raw/orders`.
# MAGIC
# MAGIC Older docs/code use `import dlt` and `dlt.apply_changes(...)`. The current names are
# MAGIC `from pyspark import pipelines as dp` and `dp.create_auto_cdc_flow(...)` — same concepts.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE = spark.conf.get("lab.source_path")

# COMMAND ----------

# Bronze: streaming table fed by Auto Loader (no checkpoint/schema paths to manage — the pipeline does it)
@dp.table(name="ldp_bronze_orders", comment="Raw CDC events from the landing volume")
def ldp_bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(SOURCE)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# COMMAND ----------

# Validated events with expectations (data-quality rules tracked in the pipeline UI / event log)
#   expect          -> keep row, record violation
#   expect_or_drop  -> drop row, record violation
#   expect_or_fail  -> fail the update
@dp.table(name="ldp_orders_validated", comment="Typed, validated CDC events")
@dp.expect_or_drop("valid_key", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_op", "op IN ('INSERT', 'UPDATE', 'DELETE')")
@dp.expect("positive_quantity", "quantity > 0 OR op = 'DELETE'")
def ldp_orders_validated():
    return spark.readStream.table("ldp_bronze_orders").select(
        F.col("order_id").cast("bigint").alias("order_id"),
        F.upper("op").alias("op"),
        "sku",
        "warehouse",
        F.col("quantity").cast("int").alias("quantity"),
        "status",
        F.to_timestamp("event_ts").alias("event_ts"),
        F.col("source_lsn").cast("bigint").alias("source_lsn"),
    )


# COMMAND ----------

# Silver SCD type 1: current state per order (equivalent to the MERGE in notebook 03)
dp.create_streaming_table("ldp_silver_orders", comment="Current state per order (SCD1)")
dp.create_auto_cdc_flow(
    target="ldp_silver_orders",
    source="ldp_orders_validated",
    keys=["order_id"],
    sequence_by=F.col("event_ts"),          # out-of-order events handled for you
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op"],
    stored_as_scd_type=1,
)

# Silver SCD type 2: full history with __START_AT / __END_AT columns
dp.create_streaming_table("ldp_silver_orders_history", comment="Order history (SCD2)")
dp.create_auto_cdc_flow(
    target="ldp_silver_orders_history",
    source="ldp_orders_validated",
    keys=["order_id"],
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op"],
    stored_as_scd_type=2,
)

# COMMAND ----------

# Gold: materialized view, refreshed (incrementally when possible) on each pipeline update
@dp.materialized_view(name="ldp_gold_orders_by_warehouse", comment="Orders and units per warehouse/status")
def ldp_gold_orders_by_warehouse():
    return (
        spark.read.table("ldp_silver_orders")
        .groupBy("warehouse", "status")
        .agg(F.count("*").alias("orders"), F.sum("quantity").alias("units"))
    )
