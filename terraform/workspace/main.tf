# Workspace-level Terraform — RUNNABLE against your Free Edition workspace.
#
#   export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
#   export DATABRICKS_TOKEN=<personal access token>
#   terraform init
#   terraform plan  -var-file=envs/dev.tfvars
#   terraform apply -var-file=envs/dev.tfvars
#
# In a real multi-environment setup each env gets its own state (separate backend key
# or workspace) and its own tfvars; CI runs plan on PR and apply on merge.

terraform {
  required_version = ">= 1.5"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.60"
    }
  }

  # Remote state, as you'd have at Tecsys (S3 + lockfile/DynamoDB). Local state for the lab.
  # backend "s3" {
  #   bucket       = "tecsysiq-tfstate"
  #   key          = "databricks/workspace/dev.tfstate"
  #   region       = "ca-central-1"
  #   use_lockfile = true
  # }
}

# Reads DATABRICKS_HOST / DATABRICKS_TOKEN (or a profile) from the environment
provider "databricks" {}

data "databricks_current_user" "me" {}

# ---------- Unity Catalog objects ----------

resource "databricks_schema" "lab" {
  catalog_name = var.catalog
  name         = var.schema
  comment      = "Managed by Terraform (${var.env})"
  properties   = {
    env        = var.env
    managed_by = "terraform"
  }
}

resource "databricks_volume" "landing" {
  catalog_name = var.catalog
  schema_name  = databricks_schema.lab.name
  name         = "landing"
  volume_type  = "MANAGED"
  comment      = "Landing zone for source files"
}

# databricks_grants is AUTHORITATIVE for the securable: it replaces all grants on it.
# Use databricks_grant (singular) to manage one principal additively.
resource "databricks_grants" "lab_schema" {
  schema = "${var.catalog}.${databricks_schema.lab.name}"
  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

# ---------- Secrets (never in Git) ----------

resource "databricks_secret_scope" "lab" {
  name = "tecsys-lab-${var.env}"
}

resource "databricks_secret" "source_api_key" {
  scope        = databricks_secret_scope.lab.name
  key          = "source_api_key"
  string_value = var.source_api_key # pass via TF_VAR_source_api_key, not tfvars in Git
}
# In a notebook: dbutils.secrets.get("tecsys-lab-dev", "source_api_key")  -> value is redacted in output

# ---------- A notebook + serverless job ----------

resource "databricks_notebook" "setup" {
  source = "${path.module}/../../notebooks/00_setup.py"
  path   = "${data.databricks_current_user.me.home}/terraform-lab/00_setup"
}

resource "databricks_job" "setup_job" {
  name = "tf-setup-job-${var.env}"

  parameter {
    name    = "catalog"
    default = var.catalog
  }
  parameter {
    name    = "schema"
    default = var.schema
  }

  task {
    task_key = "setup"
    notebook_task {
      notebook_path = databricks_notebook.setup.path
    }
    # no cluster => serverless
  }
}
