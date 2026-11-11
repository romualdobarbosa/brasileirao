terraform {
  required_version = "= 1.17.2"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.72.0"
    }
  }

  # O state do bootstrap fica local (infra/bootstrap/terraform.tfstate, fora do git):
  # ele só cria o bucket que guarda o state da stack principal.
}

provider "aws" {
  region = var.regiao

  default_tags {
    tags = {
      projeto = "brasileirao"
      stack   = "bootstrap"
    }
  }
}
