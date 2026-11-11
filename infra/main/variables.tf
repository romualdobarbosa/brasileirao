variable "regiao" {
  description = "Região da AWS do projeto."
  type        = string
  default     = "us-east-1"
}

variable "email_alerta" {
  description = "E-mail que recebe os alertas do budget. Sem padrão: passe por TF_VAR_email_alerta, nunca em arquivo."
  type        = string
  sensitive   = true
}

variable "athena_bytes_por_consulta" {
  description = "Corte de bytes lidos por consulta no workgroup do Athena (mínimo da AWS: 10 MB)."
  type        = number
  default     = 104857600 # 100 MB; o gold inteiro tem poucos MB
}

variable "athena_dias_resultados" {
  description = "Dias até os resultados do Athena expirarem no bucket."
  type        = number
  default     = 7
}
