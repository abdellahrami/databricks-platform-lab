# Databricks interview cheat sheet — Data Platform Engineer (DevOps)

Short answers to rehearse out loud. Bold = words the interviewer is listening for.

## Platform architecture

- **Control plane vs compute plane.** Control plane (Databricks' account): web app, job scheduler, notebooks,
  Unity Catalog service. Compute plane: where clusters run — **classic** = EC2 in *your* AWS account/VPC,
  **serverless** = in Databricks' account. Data stays in your S3.
- **Account vs workspace.** One account per company; many workspaces (per env/region/BU). Account-level:
  users/groups (SCIM), metastores, workspaces, networking. Workspace-level: jobs, clusters, notebooks, warehouses.
- **Unity Catalog:** one **metastore per region**, assigned to workspaces. Hierarchy metastore → catalog → schema →
  table/view/volume/function/model. **Managed** tables (UC owns files and lifecycle) vs **external** tables
  (files at an external location you own). Access to S3 via **storage credential** (IAM role) + **external location**.
- **Multi-env pattern:** catalogs `dev` / `staging` / `prod` (or per-customer catalogs), **workspace binding** so the
  prod catalog is only visible from the prod workspace; jobs run as **service principals**; grants to **groups**.

## Compute and cost

- **All-purpose** clusters (interactive, pricier) vs **job** clusters (created per run, cheaper) vs **serverless**
  (instant start, no config, billed per use). **SQL warehouses** for SQL/BI (serverless / pro / classic).
- Tuning levers: right **instance type** (memory- vs compute-optimized), **autoscaling** min/max, **spot** with
  on-demand driver, **Photon**, **pools** to cut start time, **cluster policies** to enforce limits/tags,
  auto-termination, newest **LTS runtime**.
- SQL warehouse: size = per-query power, **min/max clusters** = concurrency, auto-stop, serverless for bursty BI.
- Spark-level: avoid shuffles/skew (AQE, salting), file sizes (~128MB–1GB), broadcast joins, liquid clustering on
  filter columns, don't `collect()`.
- Measure: `system.billing.usage` × `list_prices`, tag clusters/jobs for chargeback, query history for hot spots.

## Delta Lake

- Parquet + `_delta_log` → **ACID**, time travel, MERGE, schema enforcement/evolution (`mergeSchema`).
- **Liquid clustering** (`CLUSTER BY`) replaces partitioning + Z-ORDER; keys changeable later.
- `OPTIMIZE` (compaction), `VACUUM` (removes unreferenced files older than retention, limits time travel),
  **predictive optimization** automates both.
- **Change Data Feed** for incremental downstream processing; `RESTORE` for rollback.

## Ingestion and streaming

- **Auto Loader** (`cloudFiles`): incremental file discovery with exactly-once ingestion, schema inference/evolution,
  `_rescued_data` for bad records. File notification mode (SQS/SNS) vs directory listing for big buckets.
- **Structured Streaming**: micro-batches; **checkpoint** stores offsets + state; **exactly-once** = replayable source
  + checkpoint + idempotent sink. **Triggers**: `availableNow` (batch-style, cost-efficient), processing-time, continuous.
  **Watermarks** bound state for aggregations/joins with late data.
- **CDC upsert:** `foreachBatch` → dedupe to latest per key → `MERGE` with a **sequence guard**
  (`s.seq > t.seq`) → handle deletes. Or declaratively: **AUTO CDC** (`APPLY CHANGES`) with `keys`, `sequence_by`,
  `apply_as_deletes`, **SCD type 1 or 2**.
- Sources: Kafka/MSK, Kinesis, files from S3, **Lakeflow Connect** managed connectors, Debezium CDC topics.

## Pipelines and orchestration

- **Medallion**: bronze (raw, append-only, replayable) → silver (cleaned, conformed, deduped, one row per key) →
  gold (business aggregates / data products).
- **Lakeflow Declarative Pipelines** (ex-DLT): streaming tables, materialized views, **expectations**
  (`expect`, `expect_or_drop`, `expect_or_fail`), automatic dependency graph, retries, event log.
  Choose it for standard ingest/CDC; hand-written Spark for unusual logic or full control.
- **Lakeflow Jobs** (ex-Workflows): multi-task DAGs (notebook, Python wheel, JAR, SQL, pipeline, dbt tasks),
  job parameters, retries, timeouts, **repair run**, file-arrival / table-update triggers, notifications.
- Initial **bulk load + CDC**: backfill a snapshot into silver, then start CDC from the snapshot's log position;
  sequence column makes overlap safe.

## DevOps on Databricks

- **Terraform `databricks` provider** for platform: workspaces (`databricks_mws_*`), metastore assignment,
  catalogs, external locations, grants, cluster policies, warehouses, service principals, secret scopes.
  Remote state in S3 with locking; state per env; plan on PR, apply on merge.
- **Databricks Asset Bundles** (`databricks.yml`) for workloads: jobs, pipelines, notebooks deployed from Git
  with `bundle validate/deploy/run`, **targets** for dev/prod (`mode: development` prefixes resources and pauses
  schedules). Common split: Terraform = infra, Bundles = jobs/pipelines.
- **CI/CD:** unit tests on pure PySpark functions (local Spark), integration tests on a dev workspace, deploy
  with a **service principal + OAuth**, promote the same artifact across envs. Java/Maven builds → JARs uploaded
  to a UC volume or artifact registry and referenced by tasks.
- **Secrets:** secret scopes (Databricks-backed or AWS Secrets Manager), `dbutils.secrets.get` (redacted in output),
  UC **service credentials** for cloud APIs; never in Git or notebooks.
- **Git folders** for development; production jobs pull from a Git ref or a bundle deployment, never from someone's notebook.

## Data quality and observability

- Expectations in pipelines; assertion tasks that fail jobs; **Lakehouse Monitoring** (profile + drift metrics);
  freshness/volume checks; SQL alerts; job notifications; pipeline **event log**; system tables for runs and cost.

## Questions they may ask you (prepare a story for each)

1. Walk me through a CDC pipeline you built. How did you handle late, duplicate and deleted records?
   → Map Kafka/Spark at Triton to bronze/silver MERGE with sequence guard.
2. A streaming job is falling behind / costs too much. What do you look at?
   → Batch duration vs trigger, input rate, shuffle/skew, state size, small files, autoscaling, `availableNow` scheduling.
   → Your Druid story: full 10TB scan hourly → incremental ingestion, ~80% storage saved.
3. How would you provision a new customer environment end to end?
   → Terraform (workspace/catalog/IAM/grants) → Helm/K8s job onboarding → bundle deploy of jobs → smoke tests.
4. How do you handle a schema change in a source?
   → Auto Loader schema evolution / rescue column, `mergeSchema`, contracts in silver, versioned releases.
5. How do you keep prod data safe in a multi-env setup?
   → Separate catalogs + workspace binding, service principals, group grants, secrets, audit logs.
6. Terraform state got out of sync with the workspace — what do you do?
   → `plan` to see drift, `import` or `state rm`, never hand-edit managed resources, locking prevents concurrent applies.

## Honest framing for the Databricks gap

"My production Spark work has been on self-managed Spark/HDFS/Kafka and Azure ML; I've built the same patterns
Databricks packages — incremental ingestion, CDC merges, data-quality gates, IaC and GitOps deploys. I've been
ramping on Databricks specifically: Unity Catalog, Auto Loader, Lakeflow pipelines with AUTO CDC, and Asset Bundles
plus the Terraform provider — I built a small lab end to end."
