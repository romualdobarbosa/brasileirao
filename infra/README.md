# infra

Terraform da parte AWS, em `us-east-1`. Terraform 1.17.2 e provider `hashicorp/aws` 6.72.0, fixados; o `.terraform.lock.hcl` vai pro git.

- `bootstrap/`: bucket do state (`brasileirao-tfstate-<conta>`), com versionamento, SSE-S3 e bloqueio público. O state dele fica local, em `bootstrap/terraform.tfstate` (fora do git). É o único recurso que fica de pé entre sessões.
- `main/`: bucket do lake, bucket dos resultados do Athena, política IAM `brasileirao-lake`, database `brasileirao` no Glue com as tabelas Delta da silver e do gold (`silver_<tabela>` e `gold_<tabela>`, lista em `main/tabelas_delta.json`), workgroup `brasileirao` no Athena (engine v3) e o budget `brasileirao-cost-guard` (alertas em US$ 5, 10 e 15). State no bucket do bootstrap, com lock nativo do S3.

## Credencial

Uso a sessão do `aws login`, sem chave de acesso, por um perfil em `~/.aws/config` que guarda só o comando:

```ini
[profile brasileirao]
region = us-east-1
credential_process = aws configure export-credentials --profile default --format process
```

Terraform, CLI e o Spark do ambiente `aws` leem esse perfil com `AWS_PROFILE=brasileirao` (os alvos `*-aws` do Makefile já exportam). O SDK roda o comando de novo quando a credencial está para vencer, então um processo longo não cai.

O e-mail dos alertas vem do ambiente, nunca de arquivo: `export TF_VAR_email_alerta=...`.

## Comandos

```bash
# uma vez
terraform -chdir=infra/bootstrap init
terraform -chdir=infra/bootstrap apply

# cada sessão
export AWS_PROFILE=brasileirao
terraform -chdir=infra/main init -backend-config="bucket=brasileirao-tfstate-<conta>"
terraform -chdir=infra/main apply
terraform -chdir=infra/main output
terraform -chdir=infra/main destroy   # sempre, no fim da sessão
```

O bucket do state só sai no fechamento do módulo, com `terraform -chdir=infra/bootstrap destroy` depois de esvaziar as versões.
