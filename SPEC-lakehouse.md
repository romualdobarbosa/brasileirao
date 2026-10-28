# Spec: lakehouse

> Módulo `lakehouse` do [mapa de capacidades](CAPABILITY_MAP.md). Status: **implementada** em 27/10/2026 (aprovada no mesmo dia; contrato de dados ajustado depois do primeiro download e durante a implementação).

## Objetivo

Levar o dataset do Kaggle "Campeonato Brasileiro de Futebol" (adaoduque) de CSV cru até uma camada silver confiável em Delta Lake, rodando local. A silver é o contrato que o módulo `gold` consome.

O que o módulo precisa demonstrar no portfólio:

- Ingestão reprodutível com metadado de origem (bronze).
- Limpeza, tipagem e, principalmente, **normalização dos nomes dos times** (silver).
- **Carga por temporada idempotente**: reprocessar uma temporada não duplica nem altera nada. Bronze usa overwrite de partição (`replaceWhere`); silver de partidas usa `MERGE`. O README explica por que cada camada usa uma técnica.
- Código portável: o mesmo pacote roda depois no EMR Serverless e no Databricks só trocando configuração (caminho base dos dados e builder da Spark session).

Fora do escopo: tabelas gold, Postgres, nuvem, CI, Base dos Dados.

### Critérios de aceite (histórias)

1. Como dev, rodo `make download` e os CSVs do Kaggle aparecem em `data/raw/`.
2. Como dev, rodo `make lakehouse` e todas as temporadas encerradas são carregadas em bronze e silver. A temporada em andamento fica de fora.
3. Como dev, rodo `make lakehouse TEMPORADA=2019` duas vezes e o estado final das tabelas é idêntico ao de uma execução só.
4. Se aparecer na fonte um nome de time sem mapeamento, a execução falha e lista os nomes que faltam.

## Tech Stack

| Item | Versão | Observação |
|---|---|---|
| Python | 3.12 | `.python-version` |
| PySpark | `==4.2.0` | `delta-spark 4.4.1` exige `pyspark<=4.2.0`; a 4.2.1 ainda não é compatível |
| delta-spark | `==4.4.1` | |
| Java | 17, 21 ou 25 | Spark 4.2 suporta os três; o host tem OpenJDK 25 |
| kagglehub | `>=1.0.3` | Download do dataset; aceita o token novo em `~/.kaggle/access_token` (testado) |
| pytest | `>=9.1.1` | Já no grupo dev |
| chispa | `>=0.12.0` | Comparação de DataFrames nos testes |
| pytest-cov | última | Medição de cobertura |
| ruff | `>=0.17.1` | Lint e format, já no grupo dev |

Mudanças no `pyproject.toml`: adicionar `[build-system]` (layout `src/`), as dependências acima e `[tool.ruff]`. Remover `requests` (não usado) e o placeholder `main.py`.

## Commands

```bash
make setup                       # uv sync --all-groups
make download                    # uv run python -m brasileirao.ingestion.download
make lakehouse                   # uv run python -m brasileirao.pipeline lakehouse --todas
make lakehouse TEMPORADA=2019    # uv run python -m brasileirao.pipeline lakehouse --temporada 2019
make test                        # uv run pytest --cov=brasileirao --cov-report=term-missing
make lint                        # uv run ruff check . && uv run ruff format --check .
make fmt                         # uv run ruff check --fix . && uv run ruff format .
```

Configuração por variável de ambiente:

| Variável | Default | Uso |
|---|---|---|
| `BRASILEIRAO_DATA_ROOT` | `./data` | Raiz das camadas; vira `s3://...` na fase AWS |
| `BRASILEIRAO_SPARK_ENV` | `local` | Escolhe o builder da Spark session (`local` agora; `emr` e `databricks` depois) |
| `~/.kaggle/access_token` | — | Credencial do Kaggle. **Opcional:** o dataset é público e baixa sem login; o token só é usado se o Kaggle passar a exigir autenticação |

## Project Structure

```
src/brasileirao/
├── config.py               # lê env vars, monta caminhos das camadas
├── pipeline.py             # CLI: lakehouse --temporada N | --todas
├── ingestion/
│   ├── download.py         # kagglehub → data/raw/
│   └── bronze.py           # raw CSV → bronze Delta
├── transform/
│   ├── silver.py           # bronze → silver (orquestra as transformações e escritas)
│   ├── limpeza.py          # transformações puras DataFrame → DataFrame
│   └── qualidade.py        # checagens da silver
├── utils/
│   ├── spark.py            # get_spark(): builder por ambiente, com Delta configurado
│   └── temporada.py        # derivação da temporada a partir da data
└── reference/
    ├── times.csv           # mapa nome_fonte → nome_canonico (versionado, vai no wheel)
    └── atletas.csv         # mapa nome_fonte (+ clube) → nome_popular dos artilheiros

tests/
├── conftest.py             # fixture de SparkSession por sessão, data root em tmp_path
├── fixtures/raw/           # CSVs pequenos feitos à mão no formato do Kaggle
├── unit/                   # funções puras
└── integration/            # bronze/silver de ponta a ponta, idempotência

data/                       # fora do git (.gitignore)
├── raw/  bronze/  silver/
Makefile
```

## Contrato de dados

Schema confirmado no download de 27/10/2026. O download fixa a **versão 18** do dataset (`adaoduque/campeonato-brasileiro-de-futebol/versions/18`, última partida em 07/12/2025), para o pipeline ser reprodutível. Trocar de versão é decisão explícita.

Arquivos da fonte: `campeonato-brasileiro-full.csv` (partidas), `campeonato-brasileiro-gols.csv`, `campeonato-brasileiro-cartoes.csv`, `campeonato-brasileiro-estatisticas-full.csv`, todos em UTF-8 com aspas em todos os campos, além de `Legenda.txt`. A coluna de rodada se chama `rodata` (erro de digitação da fonte) em todos os arquivos. A partida usa `ID` no arquivo principal e `partida_id` nos outros. Não existe coluna de árbitro.

### Cobertura da fonte

| Dado | Temporadas com dado | Buraco conhecido |
|---|---|---|
| Partidas e placar | 2003–2025 (9.165 partidas) | — |
| Técnico e formação | 2014–2025 (formação quase só a partir de 2015) | Formação com 364 nulos em 2014 |
| Gols (autor, minuto, tipo) | 2014–2025 | Só partidas com gol; bate 100% com o placar |
| Cartões | 2014–2024 | **2025 inteira sem cartões**; `num_camisa` nulo em 386 linhas, todas de 2014; `posicao` nula em 190–271 linhas por temporada, em todas |
| Estatísticas | Arquivo cobre 2003–2025, mas só tem número real a partir de 2014 | Antes de 2014 e **2024 inteira** vêm zeradas; `precisao_passes` nula em 2014 (32 linhas) e em 2015–2017 inteiras |
| Arrecadação | 2025 | Só existe em 2025 |

### Bronze

Tabelas Delta por caminho (sem metastore): `bronze/partidas`, `bronze/gols`, `bronze/cartoes`, `bronze/estatisticas`.

- Todas as colunas da fonte como `string`, sem transformação.
- Metadados: `_ingested_at` (timestamp) e `_source_file` (string).
- Coluna de partição `temporada` (int). Em `partidas`, ela é derivada da data; nas outras, vem de `partida_id` via join com `bronze/partidas`.
- Escrita por temporada com `mode("overwrite")` + `replaceWhere("temporada = N")`.

### Silver

Tabelas Delta por caminho, particionadas por `temporada`. Times sempre pelo nome canônico.

**`silver/partidas`**: uma linha por `partida_id`. Escrita via `MERGE` na chave `partida_id`. A fonte do MERGE leva só as linhas novas ou alteradas, então recarga sem mudança não cria versão no Delta. Partida que sai da fonte (ou muda de temporada) é apagada da silver daquela temporada, para a silver não divergir da bronze.

| Coluna | Tipo | Regra |
|---|---|---|
| `partida_id` | int | não nulo, único |
| `temporada` | int | não nulo |
| `rodada` | int | não nulo |
| `data` | date | não nulo |
| `hora` | string | `HH:MM`, pode ser nulo |
| `mandante` / `visitante` | string | canônico, não nulo |
| `placar_mandante` / `placar_visitante` | int | não nulo, ≥ 0 |
| `vencedor` | string | canônico, ou nulo em empate |
| `arena` | string | |
| `uf_mandante` / `uf_visitante` | string | |
| `tecnico_mandante` / `tecnico_visitante` | string | |
| `formacao_mandante` / `formacao_visitante` | string | |
| `arrecadacao` | decimal(14,2) | em reais, com centavos (93 dos 380 valores de 2025 têm); só 2025, nulo nas demais |

Mapeamento da fonte: `ID` → `partida_id`, `rodata` → `rodada`, `data` (`dd/MM/yyyy`) → date, `vencedor = "-"` → nulo (empate), `mandante_Estado` → `uf_mandante`, `arena` com `trim` (5 mil linhas vêm com espaço no início). String vazia vira nulo em todas as colunas.

**`silver/gols`**: `partida_id`, `temporada`, `rodada`, `clube` (canônico), `atleta` (nome popular, ver regras de domínio), `minuto` (int), `acrescimo` (int, `"45+2"` vira 45 e 2; nulo sem acréscimo), `tipo_de_gol` (`Penalty`, `Gol Contra` ou nulo para gol normal).

**`silver/cartoes`**: `partida_id`, `temporada`, `rodada`, `clube` (canônico), `cartao` (`Amarelo` | `Vermelho`), `atleta` (nome popular), `num_camisa` (int), `posicao`, `minuto`, `acrescimo`. A `posicao` "Zagueira" (22 linhas) vira "Zagueiro"; vazia vira nulo.

**`silver/estatisticas`**: uma linha por (`partida_id`, `clube`). Colunas numéricas como int; percentuais (`posse_de_bola`, `precisao_passes`) como double entre 0 e 1 (`"45%"` → 0.45; `"None"` e vazio → nulo). **Linhas sem dado real são descartadas:** a fonte preenche com zeros as partidas sem estatística (antes de 2014 e em 2024), e partida com 0 chutes e 0 passes não existe de verdade. A regra é descartar a linha quando `chutes`, `passes` e `posse_de_bola` estão todos zerados ou nulos.

As três tabelas filhas são escritas com overwrite de partição (`replaceWhere`): não têm chave natural confiável, e substituir a temporada inteira é idempotente por construção. Linhas idênticas são removidas.

### Regras de domínio

- **Temporada.** O CSV não tem temporada, então ela sai da data. Atenção à temporada 2020, que terminou em fevereiro de 2021 por causa da pandemia: partidas de jan–mar/2021 pertencem a 2020 (na fonte são 112, em jan e fev, o que fecha 2020 e 2021 com 380 cada). A regra fica em `utils/temporada.py`, com teste dedicado.
- **Nomes dos times.** `reference/times.csv` com colunas `nome_fonte,nome_canonico,uf`. Todo nome que aparece na fonte (mandante, visitante, vencedor, clube) precisa estar lá. A versão 18 já vem consistente: são 46 nomes, sem acento e sem alias ("Athletico-PR" até em 2003). Por isso o mapa cumpre dois papéis. O primeiro é dar o nome de exibição com acento ("Gremio" → "Grêmio", "Sao Paulo" → "São Paulo") e a UF. O segundo é proteger contra alias que apareça numa versão futura da fonte.
- **Nomes dos atletas** (adicionado em 28/10/2026). A fonte usa o nome do registro do jogador: às vezes é o apelido (`Hulk`), às vezes o nome civil (`Mário Sérgio Santos Costa` = Marinho), e a grafia pode mudar entre temporadas (`Gabriel Barbosa Almeida` em 2022). O seed `reference/atletas.csv` (`nome_fonte,clube,nome_popular`) troca esse nome pelo nome que o torcedor reconhece, em gols e cartões, depois de canonizar o clube. Linha com `clube` vale só praquele clube e ganha da linha geral. O mesmo seed separa homônimos que a fonte junta num nome só: o jogador famoso fica sem sufixo e o outro vira "Apelido (Clube)" (Fred do Grêmio 2016, Paulinho do Vasco 2023). Sem isso, os gols dele somariam no artilheiro. Nome sem mapeamento continua como veio da fonte, sem ir pra qualidade. Escopo: os artilheiros do top 10 de cada temporada e as outras grafias deles. O seed não tem temporada, então um homônimo marcado por clube também pega o jogador famoso se ele passou por aquele clube (Paulinho no Vasco 2017 aparece como "Paulinho (Vasco)").
- **Temporada em andamento.** Fica fora de bronze e silver. É a maior temporada da fonte quando ela tem menos partidas que o esperado. `--todas` ignora essa temporada e `--temporada N` com ela dá erro explicando o motivo.
- **Deduplicação.** Em `partidas`, se o mesmo `partida_id` aparecer mais de uma vez, fica o registro de `_ingested_at` mais recente.

### Checagens de qualidade (silver)

| Checagem | Severidade |
|---|---|
| Nome de time sem mapeamento | **falha** a execução, listando os nomes |
| `partida_id` nulo ou duplicado após dedup | **falha** |
| Placar negativo ou não numérico | **falha** |
| Filha com `partida_id` inexistente em `partidas` | aviso com contagem |
| Nº de partidas da temporada ≠ esperado (552 em 2003–04, 462 em 2005, 379 em 2016, 380 nas demais a partir de 2006). Os 379 de 2016 são a partida Chapecoense × Atlético-MG, que não foi disputada depois do acidente aéreo | aviso |
| Nulos em colunas opcionais, exceto os buracos conhecidos da tabela "Cobertura da fonte" (ex.: técnico e formação antes de 2014, arrecadação antes de 2025) | aviso com contagem por coluna |

As checagens rodam antes da escrita na silver: uma falha não deixa a tabela pela metade. O resumo vai para o log ao fim de cada temporada.

## Code Style

Transformação pura separada de I/O. Funções de domínio e colunas em português, sem acento; termos técnicos do Spark ficam como são.

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalizar_times(partidas: DataFrame, mapa: DataFrame, colunas: list[str]) -> DataFrame:
    """Troca cada nome da fonte pelo canônico. Nome sem mapeamento vira nulo e é pego pela qualidade."""
    for coluna in colunas:
        canonico = mapa.select(
            F.col("nome_fonte").alias(coluna),
            F.col("nome_canonico").alias(f"{coluna}_canonico"),
        )
        partidas = (
            partidas.join(F.broadcast(canonico), on=coluna, how="left")
            .drop(coluna)
            .withColumnRenamed(f"{coluna}_canonico", coluna)
        )
    return partidas
```

- Ruff com `select = ["E", "F", "I", "B", "UP"]`, `line-length = 100`; `ruff format` como formatador.
- Type hints em toda função pública.
- Nada de `collect()`/`toPandas()` em tabela inteira fora de teste.
- Caminhos sempre via `config.py`; nunca absolutos no código.
- Docstring curta só onde a regra não é óbvia (temporada 2020, idempotência).

## Testing Strategy

- **Framework:** pytest + chispa (`assert_df_equality`), com uma `SparkSession` `local[2]` por sessão de teste (`spark.sql.shuffle.partitions=2`, Delta configurado) e `BRASILEIRAO_DATA_ROOT` apontando para `tmp_path`.
- **Dados de teste:** CSVs mínimos em `tests/fixtures/raw/` no formato do Kaggle, feitos para cobrir os casos difíceis: alias de time, partida de 2020 jogada em 2021, `partida_id` duplicado, minuto `"45+2"`, placar inválido, empate com `vencedor = "-"`, arena com espaço no início, estatística zerada, posse `"None"`. Os testes nunca dependem do download real (o CI não terá token do Kaggle).
- **Unitários** (`tests/unit/`): derivação da temporada, parse de minuto, normalização de times, deduplicação, cada checagem de qualidade.
- **Integração** (`tests/integration/`): raw → bronze → silver de ponta a ponta nas fixtures; idempotência (rodar a mesma temporada 2× e comparar o conteúdo das tabelas com 1×); falha com time não mapeado sem escrever nada na silver.
- **Cobertura:** ≥ 80% de linhas em `src/brasileirao/` (exceto `ingestion/download.py`, que depende de rede).
- **Tempo:** suíte completa abaixo de 3 minutos local. Cada temporada grava 8 tabelas Delta, e esse é o custo fixo do Spark.

## Boundaries

- **Sempre:** rodar `make lint` e `make test` antes de commit; manter transformações puras e testáveis; todo alias novo de time entra em `reference/times.csv` com caso de teste; atualizar a seção "Contrato de dados" antes de mudar schema.
- **Perguntar antes:** adicionar dependência fora da lista acima; mudar o schema ou o particionamento da silver depois que o `gold` começar a consumir; mudar a regra de temporada; trocar severidade de uma checagem.
- **Nunca:** versionar dados (`data/`) ou credencial do Kaggle; editar `.venv`; apagar ou pular teste que falha sem aprovação; caminhos absolutos no código.

## Success Criteria

1. `make download` deixa os 4 CSVs da versão 18 em `data/raw/`.
2. `make lakehouse` a partir de `data/` vazio cria as 4 tabelas bronze e as 4 silver, com todas as temporadas encerradas e sem a temporada em andamento, em menos de 5 minutos numa máquina de dev.
3. `make lakehouse TEMPORADA=2019` executado duas vezes deixa as 8 tabelas com o mesmo conteúdo de uma execução só (teste de integração + conferência manual por contagem).
4. `silver/partidas` tem exatamente uma linha por `partida_id`, nenhum time fora do mapa e nenhum placar negativo.
5. Toda partida de jan–mar/2021 está com `temporada = 2020`.
6. As temporadas fechadas batem com a contagem esperada de partidas, ou cada divergência está explicada no log de qualidade.
7. `make test` passa com cobertura ≥ 80%; `make lint` passa limpo.
8. Trocar `BRASILEIRAO_DATA_ROOT` muda onde tudo é lido e escrito, sem mudança de código (coberto pelos testes, que rodam em `tmp_path`).

## Decisões

- **Time não mapeado:** falha a execução. O nome que falta entra no mapa e a temporada é reprocessada.
- **Temporada em andamento:** fica fora do pipeline (ver "Regras de domínio").
- **Cartões por árbitro (`gold`):** o dataset não tem árbitro (confirmado). A métrica sai da tabela `disciplina`.

### Pontos para a spec do `gold`

- Cartões não existem em 2025 e estatísticas não existem em 2024. A `disciplina` vai ter buraco, ou vai precisar usar as contagens de cartão de `estatisticas` em 2025.
- Gols, cartões e estatísticas só existem a partir de 2014. As métricas que dependem deles cobrem uma janela menor que as de partidas (2003–2025).
- Barueri (2009) e Grêmio Prudente (2010) são o mesmo clube com nomes diferentes na fonte. Hoje ficam separados no `times.csv`; decidir no `ranking_historico` se contam juntos.

## Open Questions

Nenhuma. As duas questões que restavam (credencial do Kaggle e schema da fonte) foram resolvidas no download de 27/10/2026.
