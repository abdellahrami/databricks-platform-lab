-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Unity Catalog governance
-- MAGIC
-- MAGIC The job posting asks you to "manage catalog, schema, and access governance across environments".
-- MAGIC Key ideas:
-- MAGIC - **Privileges are hierarchical**: you need `USE CATALOG` + `USE SCHEMA` before `SELECT` on a table works.
-- MAGIC - Grant to **groups**, not users. Ownership is separate from grants.
-- MAGIC - Typical multi-env layout: one catalog per environment (`dev`, `staging`, `prod`), optionally bound to specific
-- MAGIC   workspaces (**workspace-catalog binding**) so prod data is only reachable from the prod workspace.
-- MAGIC - External data is reached through **storage credentials** (IAM role) + **external locations** (S3 path).
-- MAGIC - Fine-grained: **row filters**, **column masks**, **tags**, **lineage** (automatic), **audit logs** (system tables).
-- MAGIC
-- MAGIC Some statements may be restricted on Free Edition — if one fails, read it and move on.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'workspace';
CREATE WIDGET TEXT schema DEFAULT 'tecsys_lab';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);
SELECT current_catalog(), current_schema(), current_user();

-- COMMAND ----------

-- MAGIC %md ## Grants

-- COMMAND ----------

SHOW GRANTS ON SCHEMA IDENTIFIER(:schema);

-- COMMAND ----------

-- Read-only access for every user in the account to the curated layer only
GRANT USE SCHEMA ON SCHEMA IDENTIFIER(:schema) TO `account users`;
GRANT SELECT ON TABLE gold_orders_by_warehouse TO `account users`;
SHOW GRANTS ON TABLE gold_orders_by_warehouse;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Row filter + column mask
-- MAGIC Both are SQL UDFs attached to a table. They're evaluated at query time for every reader.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mtl_only_unless_admin(warehouse STRING)
RETURN is_account_group_member('admins') OR warehouse = 'MTL-01';

CREATE OR REPLACE FUNCTION mask_sku(sku STRING)
RETURN CASE WHEN is_account_group_member('admins') THEN sku ELSE concat('SKU-****') END;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_orders_secured AS SELECT * FROM silver_orders;
ALTER TABLE silver_orders_secured SET ROW FILTER mtl_only_unless_admin ON (warehouse);
ALTER TABLE silver_orders_secured ALTER COLUMN sku SET MASK mask_sku;

SELECT warehouse, count(*) FROM silver_orders_secured GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md ## Tags, comments and discovery

-- COMMAND ----------

COMMENT ON TABLE silver_orders IS 'Current state of WMS orders, one row per order_id (SCD1).';
ALTER TABLE silver_orders SET TAGS ('layer' = 'silver', 'domain' = 'fulfillment');
ALTER TABLE silver_orders ALTER COLUMN warehouse SET TAGS ('pii' = 'false');

SELECT table_name, table_type, comment
FROM information_schema.tables
WHERE table_schema = :schema
ORDER BY table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lineage
-- MAGIC Open **Catalog Explorer → silver_orders → Lineage**. Unity Catalog captured bronze → silver → gold automatically,
-- MAGIC down to column level. Programmatic access: `system.access.table_lineage` (see notebook 05).
