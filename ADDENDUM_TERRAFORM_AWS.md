# Adendo — Infra AWS com Terraform (projeto Brasileirão PySpark)

> Contexto para o Claude Code. Este adendo **estende** o plano original do projeto (pipeline batch em PySpark com dados históricos do Campeonato Brasileiro + visualização no Metabase). Ele não muda o escopo de dados. Adiciona a execução na AWS com toda a infraestrutura declarada em Terraform.

## Objetivo

Projeto de portfólio que demonstre **Spark + AWS + Terraform (IaC)**. Não é produção. Prioridades, nesta ordem:
1. Custo mínimo (orçamento total: ~US$ 120 em créditos; meta < US$ 15 no projeto inteiro)
2. Infra 100% reproduzível: `terraform apply` → roda pipeline → `terraform destroy`
3. Boas práticas visíveis pro recrutador (least privilege, state remoto, CI, guardrails de custo)

O volume de dados é pequeno. O Spark está aqui pra demonstrar a stack, e o README deve assumir isso explicitamente.

## Arquitetura alvo

```
fonte (CSV/API) → S3 raw → [PySpark no EMR Serverless] → S3 bronze → silver → gold
                                                                          │
                                         Glue Data Catalog ← tabelas gold ┘
                                                  │
                                               Athena  ←  Metabase (Docker local)
```

- **Storage:** S3, com prefixos ou buckets `raw/`, `bronze/`, `silver/`, `gold/`
- **Compute:** PySpark no **EMR Serverless** (preferido) ou Glue Jobs. Ver "Decisões em aberto".
- **Catálogo/Query:** Glue Data Catalog + Athena sobre a camada gold
- **Viz:** Metabase rodando **local em Docker**, conectado ao Athena. **Não hospedar na AWS.** Evidências entram no README como prints/GIF.
- **Orquestração:** começar com `Makefile`/script (`make run`). Step Functions é opcional, para depois.

## O que o Terraform provisiona

Estrutura sugerida:

```
infra/
├── backend.tf          # state remoto
├── providers.tf
├── variables.tf
├── outputs.tf
├── envs/dev.tfvars
└── modules/
    ├── storage/        # buckets S3, lifecycle, encryption, block public access
    ├── iam/            # roles least-privilege (job Spark, Athena)
    ├── compute/        # aws_emrserverless_application + upload do script pro S3
    ├── catalog/        # glue database + tabelas (ou crawler)
    ├── analytics/      # athena workgroup
    └── cost_guard/     # aws_budgets_budget
```

Recursos:
- **S3:** SSE habilitado, `block_public_access` total, lifecycle expirando `raw/` antigo e resultados de query do Athena
- **IAM:** role de execução do EMR Serverless com acesso **só** aos buckets do projeto e ao Glue Catalog. Nada de `*`.
- **EMR Serverless:** `aws_emrserverless_application` (type `spark`), com os guardrails abaixo
- **Glue:** `aws_glue_catalog_database`. Tabelas declaradas ou crawler.
- **Athena:** `aws_athena_workgroup` com output location no S3 e limite de bytes por query
- **Budgets:** `aws_budgets_budget` mensal com alertas por e-mail em US$ 5, 10 e 20
- **State remoto:** backend S3 com `use_lockfile = true` (Terraform ≥ 1.10, dispensa DynamoDB). O bucket do state é criado à parte, num bootstrap manual ou em uma pasta `bootstrap/` separada.

## Guardrails de custo (OBRIGATÓRIOS)

- **NÃO criar VPC/NAT Gateway.** NAT custa ~US$ 30+/mês só por existir. O EMR Serverless acessa S3 sem VPC.
- EMR Serverless:
  - `maximum_capacity` definido (ex.: 8 vCPU / 32 GB)
  - `auto_stop_configuration { enabled = true, idle_timeout_minutes = 5 }`
  - **Sem** `initial_capacity` (pre-initialized capacity cobra parada)
  - No job: workers pequenos (ex.: 2 vCPU / 8 GB) e `spark.dynamicAllocation.maxExecutors` baixo (ex.: 2–3)
- Athena workgroup com `bytes_scanned_cutoff_per_query`
- Nenhum recurso always-on (sem EC2, RDS, MSK, Metabase hospedado)
- Fluxo de dev: **desenvolver e testar o PySpark local** (Docker ou `pip install pyspark`). AWS só para as execuções finais.
- `terraform destroy` documentado no README como passo final

Estimativa de referência (us-east-1, aproximada): job de 3 workers × 2 vCPU/8 GB por 10 min ≈ US$ 0,08. S3, Athena e Glue Catalog ficam em centavos nesse volume.

## CI (diferencial)

GitHub Actions:
- Autenticação na AWS via **OIDC** (`aws_iam_openid_connect_provider` + role assumível pelo repo). **Nunca** access key em secret.
- Em PR: `terraform fmt -check`, `terraform validate`, `terraform plan` (comentando o plan no PR é bônus)
- Lint do PySpark (ruff) + testes unitários das transformações com Spark local
- `apply` só manual (`workflow_dispatch`). Nada de apply automático.

## Decisões em aberto (perguntar ao Romualdo antes de implementar)

1. **EMR Serverless vs Glue Jobs.** Recomendação: EMR Serverless, que é Spark mais "puro" e aparece mais em vaga de DE. Glue é mais simples e mais AWS-nativo.
2. **Parquet puro vs Apache Iceberg** no Glue Catalog. Iceberg pesa mais no portfólio, mas aumenta a configuração.
3. Região AWS (default sugerido: `us-east-1`, a mais barata e com tudo disponível).

## README deve conter

- Diagrama da arquitetura
- Seção "Custos": estimativa, guardrails aplicados e valor real gasto
- Passo a passo: bootstrap do state → `apply` → rodar job → consultar no Athena/Metabase → `destroy`
- Prints/GIF do Metabase e do job rodando no console do EMR Serverless
- Nota honesta: volume pequeno, Spark usado para demonstrar a stack
