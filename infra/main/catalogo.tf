# Database do Glue Catalog.
resource "aws_glue_catalog_database" "this" {
  name        = "brasileirao"
  description = "Lake do Brasileirão (Delta no S3), lido pelo Athena."
}

resource "aws_athena_workgroup" "this" {
  name          = "brasileirao"
  force_destroy = true # leva junto as consultas salvas no destroy

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.athena_bytes_por_consulta

    # Delta nativo só existe no engine v3; fixado para não depender do AUTO.
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena"].bucket}/resultados/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Tabelas Delta da silver e do gold, lidas nativamente pelo Athena engine v3. A lista sai do
# tabelas_delta.json, que um teste unitário compara com os registros do pipeline
# (`silver.TABELAS` e `gold.TABELAS`). No Glue, cada uma vira `<camada>_<tabela>`.
locals {
  tabelas_delta = merge([
    for camada, nomes in jsondecode(file("${path.module}/tabelas_delta.json")) : {
      for nome in nomes : "${camada}_${nome}" => "s3://${aws_s3_bucket.this["lake"].bucket}/${camada}/${nome}/"
    }
  ]...)
}

# Formato da doc do Athena ("Synchronize Delta Lake metadata", criação pela API do Glue): o
# provider `delta` e o caminho no `path` da SerDe. A coluna `col` é a de mentira que o Spark
# também grava; o Athena lê o schema, as partições e as estatísticas do `_delta_log`, então o
# Terraform não precisa repetir colunas nem acompanhar mudança de schema.
resource "aws_glue_catalog_table" "delta" {
  for_each = local.tabelas_delta

  database_name = aws_glue_catalog_database.this.name
  name          = each.key
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                     = "TRUE"
    "spark.sql.sources.provider" = "delta"
  }

  storage_descriptor {
    location = each.value

    columns {
      name = "col"
      type = "array<string>"
    }

    ser_de_info {
      parameters = {
        "serialization.format" = "1"
        path                   = each.value
      }
    }
  }
}
