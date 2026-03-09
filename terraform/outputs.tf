# ─── S3 Buckets ───────────────────────────────────────────────────────────────

output "bronze_bucket_id" {
  description = "Bronze S3 bucket name — use as BRONZE_BUCKET env var in ingestion scripts"
  value       = aws_s3_bucket.bronze.id
}

output "bronze_bucket_arn" {
  description = "Bronze S3 bucket ARN"
  value       = aws_s3_bucket.bronze.arn
}

output "silver_bucket_id" {
  description = "Silver S3 bucket name — use in Glue job definitions and dbt profiles.yml"
  value       = aws_s3_bucket.silver.id
}

output "silver_bucket_arn" {
  description = "Silver S3 bucket ARN"
  value       = aws_s3_bucket.silver.arn
}

output "gold_bucket_id" {
  description = "Gold S3 bucket name — use in dbt profiles.yml"
  value       = aws_s3_bucket.gold.id
}

output "gold_bucket_arn" {
  description = "Gold S3 bucket ARN"
  value       = aws_s3_bucket.gold.arn
}

output "athena_results_bucket_id" {
  description = "Athena results bucket name — use as dbt s3_staging_dir"
  value       = aws_s3_bucket.athena_results.id
}

# ─── Glue Databases ───────────────────────────────────────────────────────────

output "glue_database_bronze" {
  description = "Glue Catalog database name for Bronze layer"
  value       = aws_glue_catalog_database.bronze.name
}

output "glue_database_silver" {
  description = "Glue Catalog database name for Silver layer — use in dbt database"
  value       = aws_glue_catalog_database.silver.name
}

output "glue_database_gold" {
  description = "Glue Catalog database name for Gold layer — use in dbt database"
  value       = aws_glue_catalog_database.gold.name
}

# ─── Athena ───────────────────────────────────────────────────────────────────

output "athena_workgroup_name" {
  description = "Athena workgroup name — use as dbt work_group"
  value       = aws_athena_workgroup.main.name
}

# ─── IAM Roles ────────────────────────────────────────────────────────────────

output "ingestion_role_arn" {
  description = "ARN of the ingestion IAM role — assign to Lambda functions or EC2 instances"
  value       = aws_iam_role.ingestion.arn
}

output "glue_role_arn" {
  description = "ARN of the Glue IAM role — set in Glue job definitions"
  value       = aws_iam_role.glue.arn
}

output "dbt_role_arn" {
  description = "ARN of the dbt IAM role — assign to ECS tasks or EC2 instances running dbt"
  value       = aws_iam_role.dbt.arn
}

# ─── CI User ──────────────────────────────────────────────────────────────────

output "ci_access_key_id" {
  description = "CI IAM user access key ID — store as AWS_ACCESS_KEY_ID in GitHub Actions secrets"
  value       = aws_iam_access_key.ci.id
}

output "ci_secret_access_key" {
  description = "CI IAM user secret access key — store as AWS_SECRET_ACCESS_KEY in GitHub Actions secrets"
  value       = aws_iam_access_key.ci.secret
  sensitive   = true
}
