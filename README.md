# Databricks Platform Lab

Hands-on lab to ramp up on Databricks for a **Data Platform Engineer (DevOps)** role:
Databricks + Spark pipelines (batch and streaming CDC, medallion layers) **and** the platform side
(Unity Catalog governance, Asset Bundles, Terraform, CI/CD).

Everything runs on **Databricks Free Edition** (serverless only, Python/SQL — no Scala), except
`terraform/aws-account-reference/`, which is for reading.

The scenario mimics a WMS (warehouse management system) emitting order changes —
inserts, updates, deletes, duplicates and late events — into a landing zone.

## Repo map

```
notebooks/
  00_setup.py                           schema + volumes (Unity Catalog basics)
  01_produce_cdc_batch.py               generates one CDC file per run (run it several times)
  02_delta_lake_basics.py               time travel, MERGE/DML history, CDF, schema evolution, OPTIMIZE/VACUUM
  03_medallion_structured_streaming.py  Auto Loader -> bronze -> MERGE (CDC) -> silver -> gold
  04_unity_catalog_governance.sql       grants, row filters, column masks, tags, lineage
  05_system_tables_cost_observability.sql  DBU cost, job runs, query history, lineage
  06_data_quality_checks.py             assertion task that fails the job on bad data
src/
  lab_utils/transforms.py               shared PySpark logic (imported by notebooks, unit-tested)
  pipeline/orders_pipeline.py           same pipeline as a Lakeflow Declarative Pipeline (AUTO CDC, SCD1 + SCD2)
resources/                              Asset Bundle job + pipeline definitions
databricks.yml                          Asset Bundle root (dev / prod targets)
terraform/workspace/                    runnable Terraform: schema, volume, grants, secrets, job
terraform/aws-account-reference/        how an AWS workspace is provisioned (reference)
tests/                                  pytest for transforms (runs in GitHub Actions)
.github/workflows/ci.yml                tests + terraform fmt; manual bundle deploy
docs/INTERVIEW_CHEATSHEET.md            concepts + questions to rehearse
```

## 1. Connect this repo as a Git folder

1. In Databricks: **Settings → Linked accounts → Git integration → GitHub**, link your account
   (needed because this repo is private).
2. **Workspace → Home → Create → Git folder** → paste this repo's HTTPS URL → Create.
3. Open `notebooks/00_setup.py` and attach it to **Serverless** compute.

## 2. Suggested path (≈ 2 h + 2 h)

**Session 1 — data side**
1. `00_setup` → explore the result in **Catalog Explorer**.
2. `01_produce_cdc_batch` → run it **3 times** (3 files in the volume).
3. `02_delta_lake_basics` → cell by cell.
4. `03_medallion_structured_streaming` → then run `01` again and re-run `03`: only the new file is processed.
5. Create the declarative pipeline from the UI: **Jobs & Pipelines → Create → ETL pipeline**, source
   `src/pipeline/orders_pipeline.py`, catalog `workspace`, schema `tecsys_lab`, add configuration
   `lab.source_path = /Volumes/workspace/tecsys_lab/raw/orders`. Run it, look at the graph, the
   expectations metrics and the SCD2 history table. (Free Edition allows a limited number of pipelines —
   delete this one before deploying the bundle's version.)

**Session 2 — platform side**
1. `04_unity_catalog_governance`, `05_system_tables_cost_observability`.
2. Local CLI + bundle:
   ```bash
   # https://docs.databricks.com/dev-tools/cli/install
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run orders_medallion_job -t dev
   ```
   Look at the deployed job in the UI: task graph, `[dev <you>]` prefix, paused schedule.
3. Terraform:
   ```bash
   cd terraform/workspace
   export DATABRICKS_HOST=... DATABRICKS_TOKEN=...   # User settings → Developer → Access tokens
   terraform init && terraform apply -var-file=envs/dev.tfvars
   ```
   Then read `terraform/aws-account-reference/main.tf` top to bottom.
4. Optional: add `DATABRICKS_HOST` / `DATABRICKS_TOKEN` as GitHub secrets and run the **ci** workflow
   manually to deploy from GitHub Actions.
5. Rehearse with `docs/INTERVIEW_CHEATSHEET.md`.

## Run tests locally

```bash
pip install -r requirements-dev.txt   # needs Java 17
pytest -q
```
