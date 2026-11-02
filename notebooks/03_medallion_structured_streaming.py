# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Medallion pipeline with Auto Loader + Structured Streaming + MERGE (CDC)
# MAGIC
# MAGIC ```
# MAGIC  /Volumes/.../raw/orders/*.json
# MAGIC         │  Auto Loader (cloudFiles), incremental, schema tracking
# MAGIC         ▼
# MAGIC  bronze_orders   append-only, raw + ingestion metadata
# MAGIC         │  readStream.table + foreachBatch: cast, dedupe, MERGE
# MAGIC         ▼
# MAGIC  silver_orders   one current row per order_id (SCD type 1)
# MAGIC         │  batch aggregate
# MAGIC         ▼
# MAGIC  gold_orders_by_warehouse
# MAGIC ```
# MAGIC
# MAGIC This is the **hand-written** version. Notebook `src/pipeline/orders_pipeline.py` does the same
# MAGIC with Lakeflow Declarative Pipelines (ex-DLT) — know both and when to choose each.
# MAGIC
# MAGIC Run `01_produce_cdc_batch` once or twice first, then run this notebook. Run it again after
# MAGIC producing more batches: only **new** files are processed (that's what the checkpoint is for).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "tecsys_lab", "Schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

landing = f"/Volumes/{catalog}/{schema}/raw/orders"
ckpt = f"/Volumes/{catalog}/{schema}/checkpoints"
bronze_tbl = f"{catalog}.{schema}.bronze_orders"
silver_tbl = f"{catalog}.{schema}.silver_orders"
gold_tbl = f"{catalog}.{schema}.gold_orders_by_warehouse"

# COMMAND ----------

# Import shared, unit-tested code from the repo (Git folders put files next to notebooks)
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))
from lab_utils.transforms import cast_order_events, latest_per_key

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Auto Loader
# MAGIC - `cloudFiles` tracks which files were already ingested (in the checkpoint), so each file is read **once**.
# MAGIC - `schemaLocation` stores the inferred schema; new columns trigger schema evolution, bad values land in `_rescued_data`.
# MAGIC - `trigger(availableNow=True)` = process everything new, then stop. Same code runs continuously by changing the trigger
# MAGIC   (serverless only supports `availableNow`). This is how you run "streaming" as cheap scheduled batch jobs.
# MAGIC - `_metadata.file_path` gives lineage back to the source file.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{ckpt}/bronze_orders/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(landing)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream.option("checkpointLocation", f"{ckpt}/bronze_orders/cp")
    .trigger(availableNow=True)
    .toTable(bronze_tbl)
)
bronze_query.awaitTermination()
print(bronze_query.lastProgress and bronze_query.lastProgress["numInputRows"], "rows in last micro-batch")

# --- In a Kafka-based setup (like at Triton) only the source changes: ---
# (spark.readStream.format("kafka")
#    .option("kafka.bootstrap.servers", "...")
#    .option("subscribe", "wms.orders.cdc")
#    .option("startingOffsets", "earliest")
#    .load()
#    .select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*"))

# COMMAND ----------

display(spark.sql(f"SELECT _source_file, count(*) AS events FROM {bronze_tbl} GROUP BY ALL ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — CDC upsert with `foreachBatch` + `MERGE`
# MAGIC Why each step matters:
# MAGIC 1. **Dedupe per key inside the batch** — MERGE errors if multiple source rows match one target row.
# MAGIC 2. **Sequence guard** (`s.event_ts > t.event_ts`) — late/out-of-order events must not overwrite newer data.
# MAGIC 3. **Deletes** handled with a conditional `whenMatchedDelete`.
# MAGIC 4. `foreachBatch` + idempotent MERGE + checkpoint → effectively **exactly-once** results even if a batch is retried.

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {silver_tbl} (
    order_id BIGINT, sku STRING, warehouse STRING, quantity INT, status STRING,
    event_ts TIMESTAMP, source_lsn BIGINT, _updated_at TIMESTAMP)
  CLUSTER BY (order_id)
""")

from delta.tables import DeltaTable


def upsert_to_silver(batch_df, batch_id):
    updates = latest_per_key(cast_order_events(batch_df), ["order_id"], "event_ts", "source_lsn")
    target = DeltaTable.forName(batch_df.sparkSession, silver_tbl)
    cols = ["order_id", "sku", "warehouse", "quantity", "status", "event_ts", "source_lsn"]
    values = {c: f"s.{c}" for c in cols} | {"_updated_at": "current_timestamp()"}
    (
        target.alias("t")
        .merge(updates.alias("s"), "t.order_id = s.order_id")
        .whenMatchedDelete(condition="s.op = 'DELETE' AND s.event_ts > t.event_ts")
        .whenMatchedUpdate(condition="s.op <> 'DELETE' AND s.event_ts > t.event_ts", set=values)
        .whenNotMatchedInsert(condition="s.op <> 'DELETE'", values=values)
        .execute()
    )


silver_query = (
    spark.readStream.table(bronze_tbl)
    .writeStream.foreachBatch(upsert_to_silver)
    .option("checkpointLocation", f"{ckpt}/silver_orders/cp")
    .trigger(availableNow=True)
    .start()
)
silver_query.awaitTermination()

# COMMAND ----------

# Proof the CDC logic worked: one row per key, and the late event never won
display(spark.sql(f"""
  SELECT count(*) AS rows, count(DISTINCT order_id) AS keys,
         count_if(status = 'LATE-SHOULD-BE-IGNORED') AS late_events_applied
  FROM {silver_tbl}
"""))
display(spark.sql(f"DESCRIBE HISTORY {silver_tbl} LIMIT 5").select("version", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — business aggregate
# MAGIC Simple full recompute here. Alternatives to know: a **materialized view** (incrementally refreshed by Databricks),
# MAGIC or reading silver's **Change Data Feed** to update gold incrementally.

# COMMAND ----------

(
    spark.table(silver_tbl)
    .groupBy("warehouse", "status")
    .agg(F.count("*").alias("orders"), F.sum("quantity").alias("units"))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_tbl)
)
display(spark.table(gold_tbl).orderBy("warehouse", "status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interview talking points from this notebook
# MAGIC - Checkpoint = source offsets (which files/Kafka offsets were processed) + state. Deleting it = reprocessing from scratch.
# MAGIC - Changing the query's stateful logic may require a new checkpoint; plan migrations accordingly.
# MAGIC - `availableNow` vs continuous: cost vs latency trade-off. Same code either way.
# MAGIC - For stateful streaming aggregations/joins, add `withWatermark()` to bound state size.
# MAGIC - Small-file problem from frequent micro-batches → liquid clustering / OPTIMIZE / auto-compaction.
