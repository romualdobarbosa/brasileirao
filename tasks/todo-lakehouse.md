# Tarefas: lakehouse

Plano: [plan-lakehouse.md](plan-lakehouse.md). Spec: [SPEC-lakehouse.md](../SPEC-lakehouse.md).

Toda tarefa só conta como feita com `make lint` e `make test` passando.

---

## Fase 1: Fundação

### T1: Fundação do pacote

**Descrição:** Transformar o repo num pacote `src/brasileirao` instalável pelo `uv`, com as dependências da spec, a leitura de configuração e uma `get_spark()` local com Delta. Inclui a fixture de Spark dos testes e um teste que prova que Delta funciona nesta máquina (Java 25). É a tarefa de maior risco técnico, por isso vem primeiro.

**Critérios de aceite:**
- [x] `pyproject.toml` com `[build-system]`, `pyspark==4.2.0`, `delta-spark==4.4.1`, `kagglehub>=1.0.3`, dev `chispa` e `pytest-cov`, e `[tool.ruff]` conforme a spec; `requests` e `main.py` removidos
- [x] `config.py` resolve os caminhos das camadas a partir de `BRASILEIRAO_DATA_ROOT` (default `./data`)
- [x] `get_spark()` sobe sessão local com Delta; ambiente desconhecido dá erro claro
- [x] `Makefile` com `setup`, `test`, `lint`, `fmt`; `data/` no `.gitignore`

**Verificação:**
- [x] `make setup && make lint && make test`
- [x] Teste de integração escreve uma tabela Delta em `tmp_path`, lê de volta e compara com chispa

**Dependências:** nenhuma

**Arquivos:** `pyproject.toml`, `Makefile`, `.gitignore`, `src/brasileirao/{__init__,config}.py`, `src/brasileirao/utils/spark.py`, `tests/conftest.py`, `tests/unit/test_config.py`, `tests/integration/test_spark_delta.py`

**Tamanho:** M

### T2: Download do dataset

**Descrição:** `python -m brasileirao.ingestion.download` baixa a versão 18 do dataset com `kagglehub` e copia os CSVs para `data/raw/`.

**Critérios de aceite:**
- [x] Versão fixada em constante (`adaoduque/campeonato-brasileiro-de-futebol/versions/18`)
- [x] Os 4 CSVs e a `Legenda.txt` acabam em `<data_root>/raw/`; rodar de novo sobrescreve sem erro
- [x] Falta de credencial gera mensagem dizendo onde colocar o token

**Verificação:**
- [x] `make download` e `ls data/raw`
- [x] Teste unitário da cópia com o `kagglehub` mockado (a função de rede fica fora da cobertura, conforme a spec)

**Dependências:** T1

**Arquivos:** `src/brasileirao/ingestion/download.py`, `Makefile`, `tests/unit/test_download.py`

**Tamanho:** S

### Checkpoint: Fundação
- [x] `make lint` e `make test` passam
- [x] `make download` traz os arquivos

---

## Fase 2: Fatia vertical de partidas

### T3: Regra de temporada e bronze de partidas

**Descrição:** Função `derivar_temporada` (coluna Spark a partir da data `dd/MM/yyyy`, com jan–mar/2021 → 2020) e escrita de `bronze/partidas`: todas as colunas como string, `_ingested_at`, `_source_file`, partição `temporada`, overwrite com `replaceWhere`.

**Critérios de aceite:**
- [x] `derivar_temporada` cobre ano normal, jan/2021, fev/2021, abr/2021 e mar/2003
- [x] `escrever_bronze(spark, entidade, temporada)` grava só a partição da temporada pedida
- [x] Reescrever a mesma temporada não duplica e não toca as outras partições

**Verificação:**
- [x] `uv run pytest tests/unit/test_temporada.py tests/integration/test_bronze.py`

**Dependências:** T1

**Arquivos:** `src/brasileirao/utils/temporada.py`, `src/brasileirao/ingestion/bronze.py`, `tests/fixtures/raw/campeonato-brasileiro-full.csv`, `tests/unit/test_temporada.py`, `tests/integration/test_bronze.py`

**Tamanho:** M

### T4: Silver de partidas com mapa de times e MERGE

**Descrição:** Transformações puras de partidas (renomear, tipar, `trim` na arena, `"-"` → nulo, vazio → nulo, dedup por `partida_id`), `reference/times.csv` com os 46 nomes da fonte (canônico com acento + UF), `normalizar_times` e escrita em `silver/partidas` via `MERGE` por `partida_id`.

**Critérios de aceite:**
- [x] Schema de `silver/partidas` igual ao contrato da spec (inclui `arrecadacao`)
- [x] `times.csv` cobre os 46 nomes da versão 18; nome fora do mapa vira nulo (a checagem vem em T5)
- [x] MERGE funciona na primeira carga (tabela inexistente) e numa recarga com linha alterada

**Verificação:**
- [x] `uv run pytest tests/unit/test_limpeza_partidas.py tests/integration/test_silver_partidas.py`
- [x] Manual: conferir os nomes com acento no `times.csv`

**Dependências:** T3

**Arquivos:** `src/brasileirao/transform/limpeza.py`, `src/brasileirao/transform/silver.py`, `src/brasileirao/reference/times.csv`, `tests/unit/test_limpeza_partidas.py`, `tests/integration/test_silver_partidas.py`

**Tamanho:** M

### T5: Checagens de qualidade

**Descrição:** `qualidade.py` com as checagens de falha (time sem mapa, `partida_id` nulo/duplicado, placar inválido) e de aviso (contagem por temporada com 2016 = 379, nulos por coluna). A escrita da silver chama as checagens antes de gravar.

**Critérios de aceite:**
- [x] Falha levanta exceção própria listando os nomes/ids problemáticos
- [x] Com falha, `silver/partidas` fica intacta (teste confere)
- [x] Avisos saem no log com contagem, sem interromper

**Verificação:**
- [x] `uv run pytest tests/unit/test_qualidade.py tests/integration/test_silver_partidas.py`

**Dependências:** T4

**Arquivos:** `src/brasileirao/transform/qualidade.py`, `src/brasileirao/transform/silver.py`, `tests/unit/test_qualidade.py`, `tests/integration/test_silver_partidas.py`

**Tamanho:** S

### T6: CLI, temporada em andamento e idempotência

**Descrição:** `python -m brasileirao.pipeline lakehouse --temporada N | --todas` com `processar_temporada`. Detecção da temporada em andamento (maior temporada com menos partidas que o esperado): `--todas` pula, `--temporada N` recusa. Alvos `lakehouse` e `download` no Makefile.

**Critérios de aceite:**
- [x] `--todas` processa as temporadas em ordem crescente e pula a em andamento
- [x] `--temporada` com temporada em andamento sai com código ≠ 0 e mensagem clara
- [x] Teste de idempotência: rodar a mesma temporada 2× deixa bronze e silver iguais a 1× (comparando conteúdo, sem metadados de ingestão)

**Verificação:**
- [x] `uv run pytest tests/integration/test_pipeline.py`
- [x] Manual: `make lakehouse` nos dados reais

**Dependências:** T5, T2

**Arquivos:** `src/brasileirao/pipeline.py`, `Makefile`, `tests/integration/test_pipeline.py`

**Tamanho:** S

### Checkpoint: Partidas de ponta a ponta (revisão com o Romualdo)
- [x] `make lakehouse` carrega 2003–2025 em `bronze/partidas` e `silver/partidas`
- [x] Contagem por temporada bate (2016 = 379; 2020 e 2021 = 380)
- [x] `make lakehouse TEMPORADA=2019` 2× não muda nada
- [x] `times.csv` revisado

---

## Fase 3: Tabelas filhas

> T7, T8 e T9 são independentes entre si. A T7 cria o helper de bronze das filhas (temporada via join com `bronze/partidas`), que T8 e T9 reaproveitam. Se forem paralelas, a T7 vai primeiro ou o helper sai antes.

### T7: Gols

**Descrição:** `bronze/gols` e `silver/gols`: parse de minuto (`"45+2"` → 45 e 2), `tipo_de_gol` vazio → nulo, clube normalizado, overwrite de partição. Entra em `processar_temporada`.

**Critérios de aceite:**
- [x] Schema conforme a spec
- [x] Aviso de `partida_id` órfão funciona para gols
- [x] Idempotência coberta pelo teste do pipeline

**Verificação:**
- [x] `uv run pytest tests/unit/test_limpeza_gols.py tests/integration/test_pipeline.py`

**Dependências:** T6

**Arquivos:** `src/brasileirao/ingestion/bronze.py`, `src/brasileirao/transform/{limpeza,silver}.py`, `tests/fixtures/raw/campeonato-brasileiro-gols.csv`, `tests/unit/test_limpeza_gols.py`

**Tamanho:** M

### T8: Cartões

**Descrição:** `bronze/cartoes` e `silver/cartoes`: minuto/acréscimo, `num_camisa` int, "Zagueira" → "Zagueiro", posição vazia → nulo.

**Critérios de aceite:**
- [x] Schema conforme a spec
- [x] `cartao` só aceita `Amarelo` ou `Vermelho` (outro valor é falha)
- [x] Idempotência coberta pelo teste do pipeline

**Verificação:**
- [x] `uv run pytest tests/unit/test_limpeza_cartoes.py tests/integration/test_pipeline.py`

**Dependências:** T7 (helper de bronze das filhas)

**Arquivos:** `src/brasileirao/transform/{limpeza,silver}.py`, `tests/fixtures/raw/campeonato-brasileiro-cartoes.csv`, `tests/unit/test_limpeza_cartoes.py`

**Tamanho:** S

### T9: Estatísticas

**Descrição:** `bronze/estatisticas` e `silver/estatisticas`: inteiros, percentuais `"45%"` → 0.45, `"None"`/vazio → nulo, descarte das linhas sem dado real (chutes, passes e posse zerados ou nulos), unicidade por (`partida_id`, `clube`).

**Critérios de aceite:**
- [x] Schema conforme a spec
- [x] Linha zerada é descartada; linha com dado real e posse nula é mantida
- [x] Duplicado de (`partida_id`, `clube`) é falha

**Verificação:**
- [x] `uv run pytest tests/unit/test_limpeza_estatisticas.py tests/integration/test_pipeline.py`

**Dependências:** T7 (helper de bronze das filhas)

**Arquivos:** `src/brasileirao/transform/{limpeza,silver,qualidade}.py`, `tests/fixtures/raw/campeonato-brasileiro-estatisticas-full.csv`, `tests/unit/test_limpeza_estatisticas.py`

**Tamanho:** S

---

## Checkpoint: Completo

### T10: Rodada completa e critérios de sucesso

**Descrição:** Partindo de `data/` vazio, rodar `make download && make lakehouse` e conferir os 8 critérios de sucesso da spec, um por um. Divergência vira correção (com teste) ou atualização da spec, nunca ajuste silencioso.

**Critérios de aceite:**
- [x] Os 8 critérios de sucesso da spec conferidos e anotados aqui
- [x] `make lakehouse` completo em < 5 min
- [x] Cobertura ≥ 80%

**Verificação:**
- [x] `time make lakehouse`
- [x] `make test` mostra a cobertura
- [x] Revisão com o Romualdo antes de fechar o módulo

**Dependências:** T8, T9

**Arquivos:** só correções, se aparecerem

**Tamanho:** XS

**Resultado (27/10/2026, rodada a partir de `data/` só com `raw/` da versão 18):**

1. **OK, sem novo download.** Os 4 CSVs e a `Legenda.txt` estão em `data/raw/`, com as contagens da versão 18 (9.165 partidas, 10.820 gols, 20.953 cartões, 18.330 estatísticas). O `make download` foi conferido na Fase 1 e não rodou de novo.
2. **OK.** `make lakehouse` levou 2min58s e criou as 8 tabelas. A bronze tem 2003–2025 em `partidas` e `estatisticas`, 2014–2025 em `gols` e 2014–2024 em `cartoes`. A 2025 está completa na v18 (380 partidas), então nenhuma temporada ficou de fora por estar em andamento.
3. **OK.** `make lakehouse TEMPORADA=2019` rodou 2×. Contagem e hash de conteúdo (sem `_ingested_at`/`_source_file`) das 8 tabelas ficaram idênticos aos de antes, e `silver/partidas` não ganhou versão nova no Delta. O teste `test_rodar_duas_vezes_e_igual_a_uma` cobre as 8 tabelas.
4. **OK.** `silver/partidas` tem 9.165 linhas e 9.165 `partida_id` distintos, nenhum nulo. Nenhum time fora do mapa em partidas nem nas 3 filhas, e nenhum placar negativo ou nulo.
5. **OK.** As 112 partidas de jan–mar/2021 estão todas com `temporada = 2020`.
6. **OK.** Todas as temporadas batem com o esperado (2016 = 379), sem nenhum aviso de contagem no log.
7. **OK.** `make lint` passa limpo e `make test` dá 139 testes passando, com 99% de cobertura. A suíte leva ~2min (120s), dentro da meta de 3 min da Testing Strategy.
8. **OK.** Toda a suíte de integração roda com `BRASILEIRAO_DATA_ROOT` em `tmp_path`, e `test_config.py` cobre a troca. Não precisou de mudança de código.

Conferência extra: em 2014–2025, a contagem de gols de `silver/gols` bate 100% com o placar de `silver/partidas`.

Ajuste pós-revisão: os 4 buracos novos da "Cobertura da fonte" (formação em 2014, `num_camisa` em 2014, `posicao` em todas as temporadas, `precisao_passes` em 2014–2017) não geram mais aviso de nulo. Conferido com `make lakehouse TEMPORADA=2014`, `2015` e `2018`.
