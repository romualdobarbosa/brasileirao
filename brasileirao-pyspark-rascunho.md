# Brasileirão PySpark — Rascunho do Projeto

> Rascunho inicial para refinar no plan mode do Claude Code. As decisões marcadas como **[decidido]** estão fechadas. As marcadas como **[aberto]** ainda precisam ser definidas.

## Objetivo

Projeto de portfólio de engenharia de dados: um pipeline **batch** em **PySpark** com o histórico do Campeonato Brasileiro Série A (2003 até hoje), usando arquitetura medallion, Delta Lake e um dashboard público no Metabase.

A proposta é mostrar domínio do **motor Spark** (window functions, MERGE, particionamento, explain plans), e não só da arquitetura medallion, que já aparece em outros projetos do portfólio.

## Decisões

- **[decidido]** Somente batch, sem ingestão incremental rodada a rodada nem orquestração contínua.
- **[decidido]** Processamento em PySpark + Delta Lake (`delta-spark`), rodando local via Docker Compose.
- **[decidido]** Visualização no Metabase (em vez de Streamlit, para variar o portfólio).
- **[decidido]** Serving layer em Postgres (o Metabase não lê Delta direto).
- **[decidido]** Deploy do serving na Oracle Cloud Always Free (VM ARM), com o link público do dashboard para recrutadores.
- **[aberto]** Rodar também no Databricks Free Edition como bônus?
- **[aberto]** Enriquecer com dados da Base dos Dados?

## Fontes de dados

| Fonte | Uso | Observação |
|---|---|---|
| Kaggle — "Campeonato Brasileiro de Futebol" (adaoduque) | Fonte principal | Partidas, gols, cartões e estatísticas desde 2003 |
| Base dos Dados (BigQuery) | Enriquecimento opcional | Brasileirão tratado; o free tier do GCP basta |

## Arquitetura

```
CSV (Kaggle)
   │
   ▼
[Bronze]  dados crus em Delta, particionados por temporada
   │
   ▼
[Silver]  limpeza, tipagem, dedup, normalização dos nomes dos times, schema enforcement
   │
   ▼
[Gold]    tabelas analíticas em Delta
   │
   ▼  df.write.jdbc(...)
[Postgres]  serving layer (usuário read-only, schema gold)
   │
   ▼
[Metabase]  dashboard com link público (Public Sharing)
```

**Desacoplamento:** o Spark roda local. Só o Postgres e o Metabase ficam na VM. O gold processado é carregado no Postgres remoto.

## Camadas

### Bronze
- Ingestão dos CSVs sem transformação.
- Colunas de metadado: `_ingested_at` e `_source_file`.
- Particionamento por temporada.

### Silver
- Tipagem correta (datas, inteiros e placares).
- Deduplicação.
- **Normalização dos nomes dos times**, o principal problema de qualidade do dataset (ex.: "Athletico-PR" vs "Atlético-PR"). Resolver com uma tabela de mapeamento versionada no repo.
- Schema enforcement do Delta.

### Gold (ideias)
- `classificacao_por_rodada`: tabela acumulada rodada a rodada, calculada com window functions. É a peça central do projeto.
- `desempenho_mandante_visitante`: aproveitamento por time e temporada.
- `sequencias`: maior invencibilidade e maior jejum de cada time (problema de gaps-and-islands).
- `ranking_historico`: pontos acumulados na era dos pontos corridos (desde 2003).
- `disciplina`: cartões por time e por árbitro.

## Demonstração de MERGE / idempotência

Mesmo sendo batch, carregar as temporadas **uma por vez**, simulando cargas sucessivas com `MERGE` (upsert) no Delta. Provar, com teste, que reprocessar a mesma temporada não duplica registros.

## Estrutura do repo (proposta)

```
brasileirao-spark/
├── data/{raw,bronze,silver,gold}/
├── src/
│   ├── ingestion/      # raw -> bronze
│   ├── transform/      # bronze -> silver
│   ├── aggregate/      # silver -> gold
│   ├── serving/        # gold -> Postgres
│   └── utils/          # spark session, schemas, mapa de times
├── tests/              # pytest + chispa
├── docs/               # explain plans e decisões de particionamento
├── docker-compose.yml
└── Makefile
```

## Qualidade e testes
- Testes com `pytest` + `chispa` para comparar DataFrames.
- Teste de idempotência do MERGE.
- Checagens de qualidade na silver: nulos, placares negativos, times não mapeados.

## Deploy do serving
- VM na Oracle Cloud Always Free (ARM).
- Postgres + Metabase via Docker Compose na VM.
- Metabase conectado com usuário **read-only**, restrito ao schema gold.
- Public Sharing ativado no Metabase para gerar o link público do dashboard.

## README (pontos a destacar)
- Diagrama da arquitetura.
- Justificativa das decisões: lakehouse para processamento, relacional para serving, serving desacoplado em infra gratuita.
- Explain plans e escolhas de particionamento.
- GIF e prints do dashboard, além do link público.
- Instruções para rodar local com um comando.

## Perguntas para o plan mode
- Versões do Spark, do Delta e do Python a fixar.
- Estratégia de particionamento das camadas silver e gold.
- Como estruturar a Spark session (local vs cluster) para facilitar uma eventual migração ao Databricks.
- Formato da carga no Postgres: overwrite completo ou incremental por tabela gold.
- Quais dashboards e perguntas de negócio o Metabase vai responder.
- Makefile e ordem de execução do pipeline.
