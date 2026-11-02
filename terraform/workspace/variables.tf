variable "env" {
  type        = string
  description = "Environment name (dev, staging, prod)"
}

variable "catalog" {
  type    = string
  default = "workspace"
}

variable "schema" {
  type = string
}

variable "source_api_key" {
  type      = string
  sensitive = true
  default   = "change-me"
}
