# Plano de implementação: lakehouse

Spec: [SPEC-lakehouse.md](../SPEC-lakehouse.md). Tarefas detalhadas em [todo.md](todo.md).

## Visão geral

Construir o pacote `brasileirao` que leva os CSVs do Kaggle (versão 18) até bronze e silver em Delta local, com carga por temporada idempotente. A ordem é: fundação (Spark + Delta rodando e testável), depois uma fatia vertical completa só com **partidas** (download → bronze → silver → qualidade → CLI), validada nos dados reais, e por fim as três tabelas filhas, cada uma como uma fatia própria.

## Decisões de arquitetura

- **Tabelas Delta por caminho**, sem metastore. Fica mais simples de levar para S3 e Databricks depois; o caminho base vem de `BRASILEIRAO_DATA_ROOT`.
- **`get_spark()` escolhe o builder por `BRASILEIRAO_SPARK_ENV`.** Só `local` é implementado agora, com `configure_spark_with_delta_pip`. Os outros ambientes levantam `NotImplementedError` até os módulos de nuvem.
- **A unidade de trabalho é a temporada.** `processar_temporada(spark, temporada)` faz bronze (4 tabelas) → transformações → checagens → silver (4 tabelas). `--todas` só chama essa função em ordem crescente de temporada. Isso é a "carga sucessiva" da demo de MERGE.
- **Checagens rodam antes da escrita na silver.** Se falhar, a bronze da temporada já foi gravada (é dado cru, idempotente), mas a silver não recebe nada daquela temporada. As temporadas anteriores ficam como estavam.
- **Transformação pura separada de I/O.** `limpeza.py` só tem funções `DataFrame → DataFrame`, testadas sem disco. `bronze.py` e `silver.py` fazem leitura e escrita.
- **Fixtures sintéticas nos testes.** Nenhum teste lê `data/raw/`; o download real só é usado nos checkpoints manuais.

## Grafo de dependências

```
T1 fundação (deps, config, spark, fixture de teste)
 ├── T2 download (kagglehub → data/raw)
 └── T3 temporada + bronze/partidas
      └── T4 silver/partidas (limpeza, mapa de times, MERGE)
           └── T5 qualidade (checagens antes da escrita)
                └── T6 CLI + Makefile + idempotência   ← checkpoint nos dados reais
                     ├── T7 gols
                     ├── T8 cartões        (T7, T8, T9 independentes entre si)
                     └── T9 estatísticas
                          └── T10 rodada completa + critérios de sucesso
```

## Lista de tarefas

### Fase 1: Fundação
- [x] T1: Fundação do pacote: dependências, config, Spark session com Delta, fixture de teste
- [x] T2: Download do dataset fixado na versão 18

### Checkpoint: Fundação
- [x] `make test` e `make lint` passam; um teste escreve e lê uma tabela Delta
- [x] `make download` traz os 4 CSVs

### Fase 2: Fatia vertical de partidas
- [x] T3: Regra de temporada e bronze de partidas
- [x] T4: Silver de partidas com mapa de times e MERGE
- [x] T5: Checagens de qualidade na silver
- [x] T6: CLI `lakehouse`, temporada em andamento, Makefile e teste de idempotência

### Checkpoint: Partidas de ponta a ponta (revisão com o Romualdo)
- [x] `make lakehouse` nos dados reais carrega 2003–2025 em `bronze/partidas` e `silver/partidas`
- [x] Contagem por temporada bate (2016 = 379; 2020 e 2021 = 380)
- [x] Rodar `TEMPORADA=2019` 2× não muda nada
- [x] Revisar `reference/times.csv` (nomes com acento)

### Fase 3: Tabelas filhas
- [ ] T7: Bronze e silver de gols
- [ ] T8: Bronze e silver de cartões
- [ ] T9: Bronze e silver de estatísticas

### Checkpoint: Completo
- [ ] T10: Rodada completa nos dados reais e conferência dos 8 critérios de sucesso da spec
- [ ] Revisão com o Romualdo antes de fechar o módulo

## Riscos e mitigações

| Risco | Impacto | Mitigação |
|---|---|---|
| Spark 4.2 + Delta 4.4.1 + Java 25 dar problema em runtime (o Spark declara suporte, mas a combinação com Delta é nova) | Alto | T1 roda primeiro e testa escrita/leitura Delta de verdade. Plano B: instalar JDK 21 |
| `configure_spark_with_delta_pip` baixa os jars do Maven Central na primeira sessão | Médio | Aceitar no dev (fica no cache do Ivy). No CI, cachear `~/.ivy2` (fica para o módulo `ci`) |
| Suíte lenta por causa do Spark | Médio | Uma `SparkSession` por sessão de teste, `shuffle.partitions=2`, fixtures mínimas |
| Erro nos nomes com acento do `times.csv` | Baixo | Revisão do Romualdo no checkpoint da Fase 2 |
| MERGE com tabela particionada e schema enforcement dar conflito na primeira escrita | Médio | T4 cobre primeira carga (tabela inexistente) e recarga no mesmo teste |

## Paralelização

T7, T8 e T9 são independentes depois de T6 (cada uma mexe nas próprias funções e fixtures). O resto é sequencial. O módulo `ci` pode começar depois de T1.

## Open Questions

Nenhuma.
