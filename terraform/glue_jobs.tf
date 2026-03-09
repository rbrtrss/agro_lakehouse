# Silver layer Glue ETL jobs — one per Bronze source.
# Scripts are uploaded to s3://agro-lakehouse-silver/glue-scripts/ and
# referenced by the aws_glue_job resources below.

# ─── Script uploads ────────────────────────────────────────────────────────────

resource "aws_s3_object" "glue_script_indec" {
  bucket = aws_s3_bucket.silver.id
  key    = "glue-scripts/silver_indec.py"
  source = "${path.module}/../glue/jobs/silver_indec.py"
  etag   = filemd5("${path.module}/../glue/jobs/silver_indec.py")
}

resource "aws_s3_object" "glue_script_senasa" {
  bucket = aws_s3_bucket.silver.id
  key    = "glue-scripts/silver_senasa.py"
  source = "${path.module}/../glue/jobs/silver_senasa.py"
  etag   = filemd5("${path.module}/../glue/jobs/silver_senasa.py")
}

resource "aws_s3_object" "glue_script_worldbank" {
  bucket = aws_s3_bucket.silver.id
  key    = "glue-scripts/silver_worldbank.py"
  source = "${path.module}/../glue/jobs/silver_worldbank.py"
  etag   = filemd5("${path.module}/../glue/jobs/silver_worldbank.py")
}

# ─── Shared job arguments ──────────────────────────────────────────────────────

locals {
  # Glue 4.0 with --datalake-formats iceberg auto-configures:
  #   spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog
  #   spark.sql.catalog.glue_catalog.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
  #   spark.sql.extensions (IcebergSparkSessionExtensions)
  # No --conf override needed; passing GlueCatalog directly causes CatalogPlugin error.
  glue_common_args = {
    "--enable-glue-datacatalog" = "true"
    "--datalake-formats"        = "iceberg"
    "--BRONZE_BUCKET"           = aws_s3_bucket.bronze.id
    "--SILVER_BUCKET"           = aws_s3_bucket.silver.id
    "--SILVER_DB"               = aws_glue_catalog_database.silver.name
  }
}

# ─── Glue jobs ─────────────────────────────────────────────────────────────────

resource "aws_glue_job" "silver_indec" {
  name              = "agro-silver-indec"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.silver.id}/glue-scripts/silver_indec.py"
    python_version  = "3"
  }

  default_arguments = local.glue_common_args
  depends_on        = [aws_s3_object.glue_script_indec]
}

resource "aws_glue_job" "silver_senasa" {
  name              = "agro-silver-senasa"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.silver.id}/glue-scripts/silver_senasa.py"
    python_version  = "3"
  }

  default_arguments = local.glue_common_args
  depends_on        = [aws_s3_object.glue_script_senasa]
}

resource "aws_glue_job" "silver_worldbank" {
  name              = "agro-silver-worldbank"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.silver.id}/glue-scripts/silver_worldbank.py"
    python_version  = "3"
  }

  default_arguments = local.glue_common_args
  depends_on        = [aws_s3_object.glue_script_worldbank]
}
