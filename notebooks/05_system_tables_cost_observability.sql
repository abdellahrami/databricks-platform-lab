-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · System tables — cost, jobs, queries, lineage, audit
-- MAGIC
-- MAGIC System tables live in the `system` catalog and are how platform teams answer
-- MAGIC "what does this cost?", "which job failed?", "who touched this table?".
-- MAGIC They must be enabled by an account admin per schema; some may be unavailable on Free Edition.
-- MAGIC If a query errors, browse `system` in Catalog Explorer to see what you have.
-- MAGIC
-- MAGIC | Schema | Examples |
-- MAGIC |---|---|
-- MAGIC | `system.billing` | `usage` (DBUs per SKU/job/warehouse), `list_prices` |
-- MAGIC | `system.lakeflow` | `jobs`, `job_run_timeline`, `job_task_run_timeline`, `pipelines` |
-- MAGIC | `system.query` | `history` (SQL warehouse + serverless queries) |
-- MAGIC | `system.access` | `audit`, `table_lineage`, `column_lineage` |
-- MAGIC | `system.compute` | `clusters`, `warehouses`, `node_timeline` |

-- COMMAND ----------

SHOW SCHEMAS IN system;

-- COMMAND ----------

-- DBUs by product over the last 7 days
SELECT usage_date, billing_origin_product, sku_name, round(sum(usage_quantity), 2) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY ALL
ORDER BY usage_date DESC, dbus DESC;

-- COMMAND ----------

-- Cost per job (joins usage to list prices) — the classic FinOps query
SELECT u.usage_metadata.job_id AS job_id,
       round(sum(u.usage_quantity * p.pricing.default), 2) AS est_list_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_metadata.job_id IS NOT NULL
GROUP BY ALL
ORDER BY est_list_cost_usd DESC;

-- COMMAND ----------

-- Recent job runs and their outcome
SELECT job_id, run_id, period_start_time, period_end_time, result_state
FROM system.lakeflow.job_run_timeline
WHERE period_start_time >= current_date() - INTERVAL 2 DAYS
ORDER BY period_start_time DESC;

-- COMMAND ----------

-- Slowest queries today
SELECT executed_by, statement_type, total_duration_ms, read_bytes, left(statement_text, 120) AS sql
FROM system.query.history
WHERE start_time >= current_date()
ORDER BY total_duration_ms DESC
LIMIT 20;

-- COMMAND ----------

-- Lineage: what reads from / writes to the lab schema
SELECT source_table_full_name, target_table_full_name, entity_type, event_time
FROM system.access.table_lineage
WHERE target_table_full_name LIKE '%tecsys_lab%'
ORDER BY event_time DESC
LIMIT 50;
