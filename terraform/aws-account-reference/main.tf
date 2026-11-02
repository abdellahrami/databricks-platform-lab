# REFERENCE ONLY — not runnable on Free Edition (needs a Databricks account on AWS + AWS creds).
# Read it to be able to explain how a Databricks workspace is provisioned on AWS with Terraform,
# which is the first responsibility in the Tecsys posting. Not validated against a live account.
#
# Two providers:
#   - "mws" alias  -> ACCOUNT-level API (accounts.cloud.databricks.com): workspaces, metastores, users/groups
#   - default      -> WORKSPACE-level API (<ws>.cloud.databricks.com): jobs, clusters, catalogs, grants
#
# Order of operations for a customer-managed workspace:
#   1. S3 root bucket (DBFS root / workspace storage) + bucket policy allowing Databricks
#   2. Cross-account IAM role that Databricks assumes to launch EC2 in YOUR VPC
#   3. (Optional) customer-managed VPC: subnets in 2 AZs, security group, (PrivateLink endpoints)
#   4. Register 1-3 with the account: credentials, storage configuration, network
#   5. Create the workspace from those IDs
#   6. Assign a Unity Catalog metastore (one per region) to the workspace
#   7. Switch to the workspace provider: catalogs, external locations, grants, cluster policies, jobs

terraform {
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 5.0" }
    databricks = { source = "databricks/databricks", version = "~> 1.60" }
  }
  # Backend blocks can't use variables: pass the per-env key at init time, e.g.
  #   terraform init -backend-config="key=databricks/account/dev.tfstate"
  backend "s3" {
    bucket       = "tecsysiq-tfstate"
    region       = "ca-central-1"
    use_lockfile = true # S3-native locking (older setups use a DynamoDB table)
  }
}

variable "databricks_account_id" { type = string }
variable "region" { default = "ca-central-1" }
variable "prefix" { default = "tecsysiq-dev" }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "security_group_id" { type = string }
variable "metastore_id" { type = string }

provider "aws" { region = var.region }

provider "databricks" {
  alias      = "mws"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
  # auth: service principal OAuth via DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET
}

# 1. Root bucket ------------------------------------------------------------
resource "aws_s3_bucket" "root" {
  bucket        = "${var.prefix}-root"
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "root" {
  bucket                  = aws_s3_bucket.root.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "databricks_aws_bucket_policy" "root" {
  bucket = aws_s3_bucket.root.bucket
}

resource "aws_s3_bucket_policy" "root" {
  bucket = aws_s3_bucket.root.id
  policy = data.databricks_aws_bucket_policy.root.json
}

# 2. Cross-account role -------------------------------------------------------
data "databricks_aws_assume_role_policy" "this" {
  external_id = var.databricks_account_id
}

data "databricks_aws_crossaccount_policy" "this" {}

resource "aws_iam_role" "cross_account" {
  name               = "${var.prefix}-crossaccount"
  assume_role_policy = data.databricks_aws_assume_role_policy.this.json
}

resource "aws_iam_role_policy" "cross_account" {
  name   = "${var.prefix}-policy"
  role   = aws_iam_role.cross_account.id
  policy = data.databricks_aws_crossaccount_policy.this.json
}

# 4. Register with the Databricks account ------------------------------------
resource "databricks_mws_credentials" "this" {
  provider         = databricks.mws
  credentials_name = "${var.prefix}-creds"
  role_arn         = aws_iam_role.cross_account.arn
}

resource "databricks_mws_storage_configurations" "this" {
  provider                   = databricks.mws
  account_id                 = var.databricks_account_id
  storage_configuration_name = "${var.prefix}-storage"
  bucket_name                = aws_s3_bucket.root.bucket
}

resource "databricks_mws_networks" "this" {
  provider           = databricks.mws
  account_id         = var.databricks_account_id
  network_name       = "${var.prefix}-network"
  vpc_id             = var.vpc_id
  subnet_ids         = var.private_subnet_ids
  security_group_ids = [var.security_group_id]
}

# 5. Workspace ----------------------------------------------------------------
resource "databricks_mws_workspaces" "this" {
  provider                 = databricks.mws
  account_id               = var.databricks_account_id
  workspace_name           = var.prefix
  aws_region               = var.region
  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
  network_id               = databricks_mws_networks.this.network_id
}

# 6. Unity Catalog metastore assignment -----------------------------------------
resource "databricks_metastore_assignment" "this" {
  provider     = databricks.mws
  workspace_id = databricks_mws_workspaces.this.workspace_id
  metastore_id = var.metastore_id
}

output "workspace_url" {
  value = databricks_mws_workspaces.this.workspace_url
}

# 7. Then, in a separate root module / stack using the workspace provider
#    (host = databricks_mws_workspaces.this.workspace_url):
#    - databricks_storage_credential (IAM role for S3 data) + databricks_external_location
#    - databricks_catalog per env, databricks_workspace_binding to isolate prod
#    - databricks_cluster_policy, databricks_sql_endpoint (SQL warehouse), databricks_grants
#    - service principals for CI/CD and job run_as
