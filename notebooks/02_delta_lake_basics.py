# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Delta Lake essentials
# MAGIC
# MAGIC Delta = Parquet files + a transaction log (`_delta_log/`). The log gives you ACID writes,
# MAGIC time travel, schema enforcement/evolution, MERGE, and change data feed.
# MAGIC
# MAGIC Run cell by cell and read the output — every cell maps to a likely interview question.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "tecsys_lab", "Schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a managed table with liquid clustering
# MAGIC `CLUSTER BY` = **liquid clustering**, the modern replacement for partitioning + `ZORDER`.
# MAGIC Clustering keys can be changed later without rewriting the table.
# MAGIC (If `samples.tpch` isn't available in your workspace, swap in any table.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delta
# MAGIC CLUSTER BY (o_orderdate)
# MAGIC AS SELECT * FROM samples.tpch.orders LIMIT 200000;
# MAGIC
# MAGIC DESCRIBE DETAIL orders_delta;   -- format, location, numFiles, sizeInBytes, clusteringColumns

# COMMAND ----------

# MAGIC %md
# MAGIC ## DML creates new table versions

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders_delta SET o_orderstatus = 'X' WHERE o_orderkey % 100 = 0;
# MAGIC DELETE FROM orders_delta WHERE o_orderpriority = '5-LOW';
# MAGIC DESCRIBE HISTORY orders_delta;  -- version, operation, operationMetrics (rows updated/deleted, files added/removed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel and restore
# MAGIC Useful for audits, reproducing a bad run, or rolling back a bad deploy.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'v0' AS version, count(*) AS n FROM orders_delta VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'current', count(*) FROM orders_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE orders_delta TO VERSION AS OF 1;   -- undo the DELETE
# MAGIC SELECT count(*) FROM orders_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Data Feed (CDF)
# MAGIC Row-level change log of a Delta table: `insert`, `update_preimage`, `update_postimage`, `delete`.
# MAGIC This is how you propagate changes **incrementally** from silver to gold instead of recomputing everything.

# COMMAND ----------

spark.sql("ALTER TABLE orders_delta SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
start_version = spark.sql("DESCRIBE HISTORY orders_delta LIMIT 1").first()["version"] + 1

spark.sql("UPDATE orders_delta SET o_comment = 'touched' WHERE o_orderkey % 1000 = 0")
spark.sql("DELETE FROM orders_delta WHERE o_orderkey % 5000 = 0")

display(
    spark.sql(f"SELECT _change_type, _commit_version, count(*) AS n FROM table_changes('orders_delta', {start_version}) GROUP BY ALL ORDER BY ALL")
)
# Streaming equivalent: spark.readStream.option("readChangeFeed", "true").table("orders_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema enforcement vs evolution

# COMMAND ----------

from pyspark.sql import functions as F

extra = spark.table("orders_delta").limit(10).withColumn("o_channel", F.lit("EDI"))

try:
    extra.write.mode("append").saveAsTable("orders_delta")
except Exception as e:
    print("Schema enforcement blocked the write:\n", str(e)[:300])

extra.write.mode("append").option("mergeSchema", "true").saveAsTable("orders_delta")
print("With mergeSchema the new column is added:", "o_channel" in spark.table("orders_delta").columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table maintenance
# MAGIC - `OPTIMIZE` compacts small files (and applies liquid clustering).
# MAGIC - `VACUUM` deletes files no longer referenced by versions older than the retention (default 7 days) —
# MAGIC   it limits how far back time travel can go.
# MAGIC - With **predictive optimization** (on by default for UC managed tables in many workspaces) Databricks runs these for you.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE orders_delta;
# MAGIC VACUUM orders_delta;   -- default retention 168 hours
# MAGIC DESCRIBE HISTORY orders_delta LIMIT 5;
