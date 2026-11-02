# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — catalog, schema, volumes
# MAGIC
# MAGIC Creates the Unity Catalog objects every other notebook uses:
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `<catalog>.<schema>` | Schema that holds all lab tables |
# MAGIC | Volume `raw` | Landing zone where the CDC generator drops JSON files (think: S3 prefix) |
# MAGIC | Volume `checkpoints` | Structured Streaming checkpoints + Auto Loader schema tracking |
# MAGIC
# MAGIC **Unity Catalog hierarchy:** metastore → catalog → schema → table / view / volume / function / model.
# MAGIC Free Edition gives you one metastore and a default catalog called `workspace`.
# MAGIC
# MAGIC **Volumes** are UC-governed storage for *files* (vs tables for structured data). Paths look like
# MAGIC `/Volumes/<catalog>/<schema>/<volume>/...` and access is controlled with `READ VOLUME` / `WRITE VOLUME` grants.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "tecsys_lab", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
print(f"Using {catalog}.{schema}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Databricks interview-prep lab'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw COMMENT 'Landing zone for CDC JSON files'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints COMMENT 'Streaming checkpoints and Auto Loader schemas'")

landing = f"/Volumes/{catalog}/{schema}/raw/orders"
dbutils.fs.mkdirs(landing)
print("Landing path:", landing)

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Things to look at in the UI now
# MAGIC - **Catalog Explorer** → `workspace` → `tecsys_lab`: see the schema, the two volumes, owner, permissions tab.
# MAGIC - **Compute**: on Free Edition you only get *serverless* — note there is no cluster config to tune.
# MAGIC   In a real workspace you'd see all-purpose clusters, job clusters, pools and policies here.
# MAGIC - **SQL Warehouses**: the compute behind SQL editor / dashboards (serverless, pro, classic types).
