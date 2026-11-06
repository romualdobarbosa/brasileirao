# Mapa de capacidades: Brasileirão PySpark

Índice dos módulos do projeto. Cada módulo tem a própria spec em `SPEC-<id>.md`, escrita e aprovada na ordem de construção. Os ids são fixos.

| Módulo | Responsabilidade | Depende de | Spec |
|---|---|---|---|
| `lakehouse` | CSV do Kaggle → bronze → silver em Delta local; mapa de nomes dos times; carga por temporada idempotente (MERGE); checagens de qualidade | — | [SPEC-lakehouse.md](SPEC-lakehouse.md) |
| `gold` | Tabelas analíticas (classificação por rodada, mandante/visitante, sequências, ranking histórico, disciplina, artilharia, gols por minuto, resumo da temporada) e explain plans documentados | lakehouse | [SPEC-gold.md](SPEC-gold.md) |
| `serving` | Postgres + Metabase (Docker Compose), só local; carga JDBC do gold; usuário read-only; dashboard local | gold | [SPEC-serving.md](SPEC-serving.md) |
| `vitrine` | 10 tabelas gold novas, exportação para Parquet e app Streamlit público no Community Cloud | gold | [SPEC-vitrine.md](SPEC-vitrine.md) |
| `ci` | GitHub Actions: ruff + pytest com Spark local em PR; checks de Terraform quando `cloud-aws` existir | lakehouse | [SPEC-ci.md](SPEC-ci.md) |
| `enrichment` | Base dos Dados via BigQuery, juntando com o silver | lakehouse | — |
| `cloud-aws` | Terraform (S3, IAM, EMR Serverless, Glue, Athena, Budgets, state remoto, OIDC) rodando o mesmo job no EMR | gold | — |
| `orchestration-aws` | Step Functions disparando o job no EMR | cloud-aws | — |
| `databricks` | O mesmo pipeline rodando no Databricks Free Edition | gold | — |

**Ordem de construção:** `lakehouse` (+ `ci` em paralelo) → `gold` → `serving` (local) → `vitrine` (MVP com link público) → `enrichment` → `cloud-aws` → `orchestration-aws`. `databricks` por último, podendo correr em paralelo com a AWS.

## Decisões transversais

- Compute local primeiro; AWS e Databricks rodam o mesmo código, sem reescrita.
- Formato das tabelas: Delta Lake.
- Publicação: app Streamlit no Community Cloud (custo zero). Postgres + Metabase ficam locais, porque o cadastro da Oracle no Brasil pede CNPJ. A fase AWS consome créditos (meta < US$ 15).
- Python 3.12; dependências gerenciadas com `uv`.
- Os rascunhos de planejamento (`brasileirao-pyspark-rascunho.md`, `ADDENDUM_TERRAFORM_AWS.md`) ficam fora do git; as specs aprovadas são a fonte da verdade.
