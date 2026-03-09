resource "aws_athena_workgroup" "main" {
  name        = "agro_workgroup"
  description = "Athena workgroup for agro-lakehouse queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_limit
  }
}
