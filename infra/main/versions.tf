terraform {
  required_version = "= 1.17.2"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.72.0"
    }
  }

  # Backend parcial: o bucket vem no init (-backend-config="bucket=..."), saída do infra/bootstrap.
  # Lock nativo do S3 (use_lockfile), sem DynamoDB.
  backend "s3" {
    key          = "main/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.regiao

  default_tags {
    tags = {
      projeto = "brasileirao"
      stack   = "main"
    }
  }
}
