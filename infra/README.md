# infra

Terraform da parte AWS, em `us-east-1`. Terraform 1.17.2 e provider `hashicorp/aws` 6.72.0, fixados; o `.terraform.lock.hcl` vai pro git.

- `bootstrap/`: bucket do state (`brasileirao-tfstate-<conta>`), com versionamento, SSE-S3 e bloqueio público. O state dele fica local, em `bootstrap/terraform.tfstate` (fora do git). É o único recurso que fica de pé entre sessões.
- `main/`: bucket do lake, bucket dos resultados do Athena, política IAM `brasileirao-lake`, database `brasileirao` no Glue, workgroup `brasileirao` no Athena e o budget `brasileirao-cost-guard` (alertas em US$ 5, 10 e 15). State no bucket do bootstrap, com lock nativo do S3.

## Credencial

Uso a sessão do `aws login`, passada só pelo ambiente:

```bash
eval "$(aws configure export-credentials --format env)"
```

O e-mail dos alertas também vem do ambiente, nunca de arquivo: `export TF_VAR_email_alerta=...`.

## Comandos

```bash
# uma vez
terraform -chdir=infra/bootstrap init
terraform -chdir=infra/bootstrap apply

# cada sessão
terraform -chdir=infra/main init -backend-config="bucket=brasileirao-tfstate-<conta>"
terraform -chdir=infra/main apply
terraform -chdir=infra/main output
terraform -chdir=infra/main destroy   # sempre, no fim da sessão
```

O bucket do state só sai no fechamento do módulo, com `terraform -chdir=infra/bootstrap destroy` depois de esvaziar as versões.
