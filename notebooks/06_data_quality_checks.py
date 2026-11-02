# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Data quality checks (last task of the job)
# MAGIC
# MAGIC A task that **fails the job** when data is wrong is the simplest form of data observability.
# MAGIC In production you'd add: freshness SLAs, volume anomaly checks, alerting (job notifications / SQL alerts),
# MAGIC and Lakehouse Monitoring on key tables.
# MAGIC
# MAGIC It also compares the hand-written MERGE (notebook 03) with the declarative AUTO CDC result.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "tecsys_lab", "Schema")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = lambda t: f"{catalog}.{schema}.{t}"

# COMMAND ----------

failures = []


def check(name, sql, expect_zero=True):
    value = spark.sql(sql).first()[0]
    ok = (value == 0) if expect_zero else bool(value)
    print(f"{'PASS' if ok else 'FAIL'}  {name}: {value}")
    if not ok:
        failures.append(name)


check("silver has rows", f"SELECT count(*) FROM {fq('silver_orders')}", expect_zero=False)
check("silver: duplicate keys", f"SELECT count(*) - count(DISTINCT order_id) FROM {fq('silver_orders')}")
check("silver: null keys", f"SELECT count_if(order_id IS NULL) FROM {fq('silver_orders')}")
check("silver: late events applied", f"SELECT count_if(status = 'LATE-SHOULD-BE-IGNORED') FROM {fq('silver_orders')}")
check(
    "bronze freshness (minutes since last ingest > 24h)",
    f"SELECT CASE WHEN timestampdiff(MINUTE, max(_ingested_at), current_timestamp()) > 1440 THEN 1 ELSE 0 END FROM {fq('bronze_orders')}",
)

# COMMAND ----------

# Compare notebook MERGE vs declarative AUTO CDC (only if the pipeline has run)
if spark.catalog.tableExists(fq("ldp_silver_orders")):
    diff = spark.sql(f"""
        SELECT count(*) FROM (
          (SELECT order_id, status, quantity FROM {fq('silver_orders')}
           EXCEPT SELECT order_id, status, quantity FROM {fq('ldp_silver_orders')})
          UNION ALL
          (SELECT order_id, status, quantity FROM {fq('ldp_silver_orders')}
           EXCEPT SELECT order_id, status, quantity FROM {fq('silver_orders')}))
    """).first()[0]
    print(f"Rows differing between MERGE and AUTO CDC: {diff}")
    # Not a hard failure: if the two ran on different sets of files they legitimately differ.
    # Good interview discussion: AUTO CDC keeps delete tombstones to handle a late update
    # that arrives AFTER a delete; the simple MERGE would re-insert that row.

# COMMAND ----------

if failures:
    raise AssertionError(f"Data quality checks failed: {failures}")
print("All checks passed")
