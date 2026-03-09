data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
}

# ─── Ingestion Role ────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "ingestion_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com", "ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "ingestion" {
  # Write to Bronze
  statement {
    sid = "BronzeWrite"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
    ]
  }

  # Read-only on Silver and Gold
  statement {
    sid = "SilverGoldRead"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.silver.arn,
      "${aws_s3_bucket.silver.arn}/*",
      aws_s3_bucket.gold.arn,
      "${aws_s3_bucket.gold.arn}/*",
    ]
  }
}

resource "aws_iam_role" "ingestion" {
  name               = "agro-ingestion-role"
  assume_role_policy = data.aws_iam_policy_document.ingestion_trust.json
}

resource "aws_iam_role_policy" "ingestion" {
  name   = "agro-ingestion-policy"
  role   = aws_iam_role.ingestion.id
  policy = data.aws_iam_policy_document.ingestion.json
}

# ─── Glue Role ─────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue" {
  # Read Bronze
  statement {
    sid = "BronzeRead"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
    ]
  }

  # Read + Write Silver
  statement {
    sid = "SilverReadWrite"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.silver.arn,
      "${aws_s3_bucket.silver.arn}/*",
    ]
  }

  # Glue Catalog operations on Bronze + Silver databases
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
    ]
    resources = [
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:catalog",
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:database/${aws_glue_catalog_database.bronze.name}",
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/${aws_glue_catalog_database.bronze.name}/*",
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:database/${aws_glue_catalog_database.silver.name}",
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/${aws_glue_catalog_database.silver.name}/*",
    ]
  }

  # CloudWatch Logs for Glue job output
  statement {
    sid = "CloudWatchLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:${local.partition}:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_role" "glue" {
  name               = "agro-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

resource "aws_iam_role_policy" "glue" {
  name   = "agro-glue-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ─── dbt Role ──────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "dbt_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com", "ecs-tasks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "dbt" {
  # Read Silver
  statement {
    sid = "SilverRead"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.silver.arn,
      "${aws_s3_bucket.silver.arn}/*",
    ]
  }

  # Read + Write Gold and Athena results
  statement {
    sid = "GoldAndAthenaWrite"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.gold.arn,
      "${aws_s3_bucket.gold.arn}/*",
      aws_s3_bucket.athena_results.arn,
      "${aws_s3_bucket.athena_results.arn}/*",
    ]
  }

  # Athena query execution on the project workgroup
  statement {
    sid = "AthenaExecute"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [
      aws_athena_workgroup.main.arn,
    ]
  }

  # Glue Catalog operations on Silver + Gold databases
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
    ]
    resources = [
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:catalog",
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:database/${aws_glue_catalog_database.silver.name}",
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/${aws_glue_catalog_database.silver.name}/*",
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:database/${aws_glue_catalog_database.gold.name}",
      "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/${aws_glue_catalog_database.gold.name}/*",
    ]
  }
}

resource "aws_iam_role" "dbt" {
  name               = "agro-dbt-role"
  assume_role_policy = data.aws_iam_policy_document.dbt_trust.json
}

resource "aws_iam_role_policy" "dbt" {
  name   = "agro-dbt-policy"
  role   = aws_iam_role.dbt.id
  policy = data.aws_iam_policy_document.dbt.json
}

# ─── CI IAM User ───────────────────────────────────────────────────────────────

resource "aws_iam_user" "ci" {
  name = "agro-ci-user"
}

resource "aws_iam_access_key" "ci" {
  user = aws_iam_user.ci.name
}

data "aws_iam_policy_document" "ci_plan" {
  # S3 — bucket metadata reads (plan only needs to describe, not access objects)
  statement {
    sid = "S3Describe"
    actions = [
      "s3:GetBucketLocation",
      "s3:GetBucketVersioning",
      "s3:GetBucketPublicAccessBlock",
      "s3:GetEncryptionConfiguration",
      "s3:GetLifecycleConfiguration",
      "s3:ListBucket",
      "s3:ListAllMyBuckets",
    ]
    resources = ["*"]
  }

  # Glue read
  statement {
    sid = "GlueRead"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
    ]
    resources = ["*"]
  }

  # Athena read
  statement {
    sid = "AthenaRead"
    actions = [
      "athena:GetWorkGroup",
      "athena:ListWorkGroups",
    ]
    resources = ["*"]
  }

  # IAM read
  statement {
    sid = "IAMRead"
    actions = [
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:GetPolicy",
      "iam:GetPolicyVersion",
      "iam:GetUser",
      "iam:GetUserPolicy",
      "iam:ListAccessKeys",
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "ci_apply" {
  # S3 — full lifecycle for managed buckets
  statement {
    sid = "S3Manage"
    actions = [
      "s3:CreateBucket",
      "s3:DeleteBucket",
      "s3:PutBucketVersioning",
      "s3:PutBucketPublicAccessBlock",
      "s3:PutEncryptionConfiguration",
      "s3:PutLifecycleConfiguration",
      "s3:DeleteLifecycleConfiguration",
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      "arn:${local.partition}:s3:::${local.bronze_bucket}",
      "arn:${local.partition}:s3:::${local.bronze_bucket}/*",
      "arn:${local.partition}:s3:::${local.silver_bucket}",
      "arn:${local.partition}:s3:::${local.silver_bucket}/*",
      "arn:${local.partition}:s3:::${local.gold_bucket}",
      "arn:${local.partition}:s3:::${local.gold_bucket}/*",
      "arn:${local.partition}:s3:::${local.athena_results_bucket}",
      "arn:${local.partition}:s3:::${local.athena_results_bucket}/*",
    ]
  }

  # Glue — full CRUD
  statement {
    sid = "GlueManage"
    actions = [
      "glue:CreateDatabase",
      "glue:DeleteDatabase",
      "glue:UpdateDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
    ]
    resources = ["*"]
  }

  # Athena — workgroup CRUD
  statement {
    sid = "AthenaManage"
    actions = [
      "athena:CreateWorkGroup",
      "athena:DeleteWorkGroup",
      "athena:UpdateWorkGroup",
      "athena:GetWorkGroup",
      "athena:TagResource",
      "athena:UntagResource",
    ]
    resources = ["*"]
  }

  # IAM — role + user management for resources in this plan
  statement {
    sid = "IAMManage"
    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:PassRole",
      "iam:CreateUser",
      "iam:DeleteUser",
      "iam:PutUserPolicy",
      "iam:DeleteUserPolicy",
      "iam:GetUser",
      "iam:GetUserPolicy",
      "iam:CreateAccessKey",
      "iam:DeleteAccessKey",
      "iam:ListAccessKeys",
      "iam:TagRole",
      "iam:TagUser",
      "iam:UntagRole",
      "iam:UntagUser",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_user_policy" "ci_plan" {
  name   = "ci-plan-policy"
  user   = aws_iam_user.ci.name
  policy = data.aws_iam_policy_document.ci_plan.json
}

resource "aws_iam_user_policy" "ci_apply" {
  name   = "ci-apply-policy"
  user   = aws_iam_user.ci.name
  policy = data.aws_iam_policy_document.ci_apply.json
}
