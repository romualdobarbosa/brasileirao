# Database do Glue Catalog. As tabelas (gold em Delta) entram no AW3.
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

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena"].bucket}/resultados/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
