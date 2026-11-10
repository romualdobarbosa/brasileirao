# Mapa de capacidades: Brasileirão PySpark

Índice dos módulos do projeto. Cada módulo tem a própria spec em `SPEC-<id>.md`, escrita e aprovada na ordem de construção. Os ids são fixos.

| Módulo | Responsabilidade | Depende de | Spec |
|---|---|---|---|
| `lakehouse` | CSV do Kaggle → bronze → silver em Delta local; mapa de nomes dos times; carga por temporada idempotente (MERGE); checagens de qualidade | — | [SPEC-lakehouse.md](SPEC-lakehouse.md) |
| `gold` | Tabelas analíticas (classificação por rodada, mandante/visitante, sequências, ranking histórico, disciplina, artilharia, gols por minuto, resumo da temporada) e explain plans documentados | lakehouse | [SPEC-gold.md](SPEC-gold.md) |
| `serving` | Postgres + Metabase (Docker Compose), só local; carga JDBC do gold; usuário read-only; dashboard local | gold | [SPEC-serving.md](SPEC-serving.md) |
| `vitrine` | 10 tabelas gold novas, exportação para Parquet e app Streamlit público no Community Cloud | gold | [SPEC-vitrine.md](SPEC-vitrine.md) |
| `ci` | GitHub Actions: ruff + pytest com Spark local em PR; checks de Terraform quando `cloud-aws` existir | lakehouse | [SPEC-ci.md](SPEC-ci.md) |
| `otimizacao` | Tuning medido, constraints e manutenção do Delta (OPTIMIZE, VACUUM, Z-Order, time travel) | gold | [SPEC-otimizacao.md](SPEC-otimizacao.md) |
| `enrichment` | ~~Base dos Dados via BigQuery, juntando com o silver~~ Cancelado em 10/11/2026: licença do dado desconhecida ([sondagem](docs/enriquecimento/sondagem.md)) | lakehouse | [SPEC-enrichment.md](SPEC-enrichment.md) |
| `databricks` | O mesmo pipeline rodando no Databricks Free Edition | gold | [SPEC-databricks.md](SPEC-databricks.md) |
| `cloud-aws` | Terraform (S3, IAM, EMR Serverless, Glue, Athena, Budgets, state remoto, OIDC) rodando o mesmo job no EMR | gold | [SPEC-cloud-aws.md](SPEC-cloud-aws.md) |
| `orchestration` | Dado que muda (simulação), Airflow local e observabilidade | gold | [SPEC-orchestration.md](SPEC-orchestration.md) |

**Ordem de construção:** `lakehouse` (+ `ci` em paralelo) → `gold` → `serving` (local) → `vitrine` → `otimizacao` → ~~`enrichment`~~ → `databricks` → `cloud-aws` → `orchestration`. Plano e tickets em [tasks/plan-fechamento.md](tasks/plan-fechamento.md).

**Fora do projeto:** cluster/escala e Structured Streaming (o dado não justifica) e serving em produção (Streamlit Community Cloud e Metabase local já cobrem).

## Decisões transversais

- Compute local primeiro; AWS e Databricks rodam o mesmo código, sem reescrita.
- Formato das tabelas: Delta Lake.
- Publicação: app Streamlit no Community Cloud (custo zero). Postgres + Metabase ficam locais, porque o cadastro da Oracle no Brasil pede CNPJ. A fase AWS consome só créditos do Free plan (teto de US$ 15), sem dinheiro de verdade.
- Python 3.12; dependências gerenciadas com `uv`.
- Os rascunhos de planejamento (`brasileirao-pyspark-rascunho.md`, `ADDENDUM_TERRAFORM_AWS.md`) ficam fora do git; as specs aprovadas são a fonte da verdade.
