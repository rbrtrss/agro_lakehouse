variable "aws_region" {
  description = "AWS region to deploy resources into"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name — used in bucket names and tags"
  type        = string
  default     = "agro-lakehouse"
}

variable "environment" {
  description = "Environment tag value"
  type        = string
  default     = "prod"
}

variable "bronze_expiry_days" {
  description = "Days before raw Bronze objects expire"
  type        = number
  default     = 90
}

variable "athena_results_expiry_days" {
  description = "Days before Athena query result objects expire"
  type        = number
  default     = 30
}

variable "athena_bytes_scanned_limit" {
  description = "Per-query data scanned limit in bytes (cost guard)"
  type        = number
  default     = 1073741824 # 1 GB
}

locals {
  bronze_bucket         = "${var.project}-bronze"
  silver_bucket         = "${var.project}-silver"
  gold_bucket           = "${var.project}-gold"
  athena_results_bucket = "${var.project}-athena-results"
}
