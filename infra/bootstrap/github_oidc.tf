# OIDC do GitHub Actions para o `terraform plan` do infra/main (workflow terraform-plan.yml).
# Só leitura: a role lê os recursos do projeto e o state, e mexe apenas no arquivo de lock.
# Fica no bootstrap porque precisa existir com a stack principal destruída, e sai com ele no FIM.

# O repositório usa o `sub` imutável do GitHub, que leva o id do dono e do repositório junto do
# nome (`dono@id/repo@id`). Assim um repositório recriado com o mesmo nome não herda a role.
# Conferir com `gh api repos/<dono>/<repo>/actions/oidc/customization/sub`.
variable "github_repo" {
  description = "Repositório no formato do sub imutável (dono@id/nome@id) que pode assumir a role."
  type        = string
  default     = "romualdobarbosa@110114577/brasileirao@1422565812"
}

variable "github_ref" {
  description = "Ref do GitHub que pode assumir a role; o plan só roda a partir da main."
  type        = string
  default     = "refs/heads/main"
}

locals {
  conta  = data.aws_caller_identity.atual.account_id
  arn_s3 = "arn:aws:s3:::"

  # Nomes dos recursos do infra/main, pela mesma convenção de lá (aquela stack é destruída a
  # cada sessão, então o bootstrap não pode ler os ARNs dela).
  buckets_main = ["brasileirao-lake-${local.conta}", "brasileirao-athena-${local.conta}"]
  arn_glue     = "arn:aws:glue:${var.regiao}:${local.conta}"
  state_main   = "${aws_s3_bucket.state.arn}/main/terraform.tfstate"
}

# Sem thumbprint: a AWS valida o certificado do GitHub pela própria CA confiável.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_confianca" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Igualdade exata: nem outra branch, nem pull request, nem outro repositório.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:ref:${var.github_ref}"]
    }
  }
}

resource "aws_iam_role" "github_plan" {
  name                 = "brasileirao-github-plan"
  description          = "terraform plan do infra/main pelo GitHub Actions (OIDC), só leitura."
  assume_role_policy   = data.aws_iam_policy_document.github_confianca.json
  max_session_duration = 3600
}

# As ações saíram das chamadas que um `terraform plan` do infra/main fez de verdade (log DEBUG
# do provider, 11/11/2026). Todas aceitam ARN de recurso, então nenhuma usa "*". O
# `sts:GetCallerIdentity` não precisa de permissão.
data "aws_iam_policy_document" "github_plan" {
  statement {
    sid       = "ListarState"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.state.arn]
  }

  statement {
    sid       = "LerState"
    actions   = ["s3:GetObject"]
    resources = [local.state_main]
  }

  # Lock nativo do S3 (use_lockfile): o plan cria e apaga o .tflock.
  statement {
    sid       = "LockDoState"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${local.state_main}.tflock"]
  }

  statement {
    sid = "LerBucketsDoMain"
    actions = [
      "s3:ListBucket", # HeadBucket
      "s3:GetAccelerateConfiguration",
      "s3:GetBucketAcl",
      "s3:GetBucketCORS",
      "s3:GetBucketLogging",
      "s3:GetBucketObjectLockConfiguration",
      "s3:GetBucketOwnershipControls",
      "s3:GetBucketPolicy",
      "s3:GetBucketPublicAccessBlock",
      "s3:GetBucketRequestPayment",
      "s3:GetBucketTagging",
      "s3:GetBucketVersioning",
      "s3:GetBucketWebsite",
      "s3:GetEncryptionConfiguration",
      "s3:GetLifecycleConfiguration",
      "s3:GetReplicationConfiguration",
    ]
    resources = [for b in local.buckets_main : "${local.arn_s3}${b}"]
  }

  statement {
    sid = "LerCatalogo"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartitionIndexes",
      "glue:GetTags",
    ]
    resources = [
      "${local.arn_glue}:catalog",
      "${local.arn_glue}:database/brasileirao",
      "${local.arn_glue}:table/brasileirao/*",
    ]
  }

  statement {
    sid       = "LerWorkgroup"
    actions   = ["athena:GetWorkGroup", "athena:ListTagsForResource"]
    resources = ["arn:aws:athena:${var.regiao}:${local.conta}:workgroup/brasileirao"]
  }

  statement {
    sid       = "LerPolitica"
    actions   = ["iam:GetPolicy", "iam:GetPolicyVersion"]
    resources = ["arn:aws:iam::${local.conta}:policy/brasileirao-lake"]
  }

  # ViewBudget cobre DescribeBudget, DescribeNotificationsForBudget e
  # DescribeSubscribersForNotification.
  statement {
    sid       = "LerBudget"
    actions   = ["budgets:ViewBudget", "budgets:ListTagsForResource"]
    resources = ["arn:aws:budgets::${local.conta}:budget/brasileirao-cost-guard"]
  }
}

resource "aws_iam_role_policy" "github_plan" {
  name   = "terraform-plan-leitura"
  role   = aws_iam_role.github_plan.id
  policy = data.aws_iam_policy_document.github_plan.json
}

output "github_plan_role_arn" {
  description = "ARN da role do plan; vai na variável AWS_PLAN_ROLE_ARN do repositório no GitHub."
  value       = aws_iam_role.github_plan.arn
}
