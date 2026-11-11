# O pipeline roda com o Spark local e a sessão do Romualdo, então não há role de serviço.
# Esta política gerenciada é o acesso mínimo do projeto: fica pronta para a role do OIDC do CI
# (AW3) ou para uma role assumível no AW2. Nenhum recurso de dados com "*".
locals {
  arn_glue = "arn:aws:glue:${var.regiao}:${local.conta}"
}

data "aws_iam_policy_document" "lake" {
  statement {
    sid     = "ListarBuckets"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.this["lake"].arn,
      aws_s3_bucket.this["athena"].arn,
    ]
  }

  statement {
    sid = "ObjetosDoLake"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.this["lake"].arn}/*"]
  }

  statement {
    sid = "ResultadosDoAthena"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.this["athena"].arn}/resultados/*"]
  }

  statement {
    sid = "CatalogoDoProjeto"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
    ]
    resources = [
      "${local.arn_glue}:catalog",
      "${local.arn_glue}:database/${aws_glue_catalog_database.this.name}",
      "${local.arn_glue}:table/${aws_glue_catalog_database.this.name}/*",
    ]
  }

  statement {
    sid = "WorkgroupDoProjeto"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.this.arn]
  }
}

resource "aws_iam_policy" "lake" {
  name        = "brasileirao-lake"
  description = "Acesso mínimo aos buckets, ao database do Glue e ao workgroup do Athena do Brasileirão."
  policy      = data.aws_iam_policy_document.lake.json
}
