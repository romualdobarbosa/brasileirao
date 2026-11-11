output "bucket_lake" {
  description = "Bucket do lake; o AW2 usa s3a://<bucket>/ no BRASILEIRAO_DATA_ROOT."
  value       = aws_s3_bucket.this["lake"].bucket
}

output "bucket_athena" {
  description = "Bucket dos resultados do Athena."
  value       = aws_s3_bucket.this["athena"].bucket
}

output "glue_database" {
  description = "Database do Glue Catalog."
  value       = aws_glue_catalog_database.this.name
}

output "athena_workgroup" {
  description = "Workgroup do Athena."
  value       = aws_athena_workgroup.this.name
}

output "politica_lake_arn" {
  description = "Política gerenciada com o acesso mínimo do projeto."
  value       = aws_iam_policy.lake.arn
}

output "tabelas_glue" {
  description = "Tabelas Delta registradas no Glue Catalog, com o caminho no lake."
  value       = local.tabelas_delta
}
