terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "local" {
    path = "terraform.tfstate"
  }

  # Uncomment to migrate to S3 remote state + DynamoDB locking:
  # backend "s3" {
  #   bucket         = "agro-lakehouse-tfstate"
  #   key            = "agro-lakehouse/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "agro-lakehouse-tfstate-lock"
  #   encrypt        = true
  # }
}

# CI verification: confirms terraform_plan.yml workflow runs on PRs touching terraform/
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
    }
  }
}
