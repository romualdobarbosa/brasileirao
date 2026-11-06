# Spec: databricks

> Status: **aprovada para execução**. Tickets DB1 e DB2 em [tasks/todo-fechamento.md](tasks/todo-fechamento.md).

## Objetivo

Rodar o mesmo pipeline (bronze, silver e gold) no Databricks Free Edition, mudando só a camada de ambiente, sem gastar nada.

Fora do escopo: Unity Catalog além do necessário, Lakeflow, app dentro do Databricks, serving (`carga`, `exportar`, Metabase) e agendamento recorrente.

## Limites confirmados na documentação (06/11/2026)

| Limite | O que obriga no projeto |
|---|---|
| Só serverless, só APIs do Spark Connect, sem RDD | Confirmar que nada usa RDD. O grep de hoje não achou `.rdd` nem `sparkContext` |
| `df.checkpoint()` não suportado; `localCheckpoint` **não documentado**; `cache`/`persist` suportados | Sondar `localCheckpoint`. Se falhar, `materializar(df)` em `utils/spark.py` escolhe por ambiente |
| Poucas propriedades configuráveis. `shuffle.partitions` pode; `snapshotPartitions` e extensões não | `get_spark` usa a sessão existente e não define o resto |
| Não instalar PySpark nem lib que o traga; sem Maven | O wheel do job não pode declarar `pyspark`/`delta-spark` como dependência |
| Internet de saída restrita | Os CSVs e o arquivo do enriquecimento são enviados a um Volume; `download` não roda lá |
| DBFS limitado; sem Spark UI | `BRASILEIRAO_DATA_ROOT=/Volumes/...`; evidência vem de logs e do histórico do job |
| Cota diária; sem uso comercial; conta inativa pode ser apagada | Rodar pouco e guardar evidência no repositório |

**Armadilha já vista no código:** `pipeline.py` importa `serving` no topo, o que puxa `psycopg`. Os imports de `carga` e `exportar` precisam ficar dentro dos subcomandos.

## Aceite

1. `BRASILEIRAO_SPARK_ENV=databricks` funciona, e o ambiente `local` segue igual (`make test` verde, gold idêntico).
2. Teste unitário cobre a escolha de ambiente sem precisar do Databricks.
3. Job real termina com sucesso e as contagens das tabelas gold batem com as do local. Toda diferença é explicada.
4. `docs/databricks/README.md` traz o passo a passo, os limites encontrados, o que mudou no código e screenshots do job.
5. Nada é afirmado no README sem ter rodado.
