output "schema_full_name" {
  value = "${var.catalog}.${databricks_schema.lab.name}"
}

output "landing_volume_path" {
  value = "/Volumes/${var.catalog}/${databricks_schema.lab.name}/${databricks_volume.landing.name}"
}

output "setup_job_url" {
  value = databricks_job.setup_job.url
}
