# Spec: gold

> Módulo `gold` do [mapa de capacidades](CAPABILITY_MAP.md). Status: **aprovada** em 27/10/2026. Depende do contrato da silver em [SPEC-lakehouse.md](SPEC-lakehouse.md).

## Objetivo

Transformar a silver em tabelas analíticas prontas para o dashboard, mostrando domínio do motor Spark: window functions (classificação acumulada, gaps-and-islands), joins com broadcast e explain plans lidos e comentados. O gold é o contrato que o módulo `serving` carrega no Postgres.

O que o módulo precisa demonstrar no portfólio:

- **Classificação rodada a rodada** só com window functions, a peça central do projeto.
- **Gaps-and-islands** para sequências de invencibilidade e jejum.
- **Explain plans documentados** em `docs/explain/`, com o que cada plano mostra (exchange, broadcast, sort, AQE) e por que o gold não é particionado.
- Regras de domínio explícitas e versionadas: punições do STJD, número de rebaixados, buracos da fonte.

Fora do escopo: Postgres, Metabase, nuvem, Base dos Dados, mudanças na silver.

### Critérios de aceite (histórias)

1. Como dev, rodo `make gold` depois de `make lakehouse` e as 8 tabelas gold aparecem em `data/gold/`.
2. Como dev, rodo `make gold` duas vezes e o conteúdo das tabelas é idêntico ao de uma execução só.
3. Como recrutador no dashboard, vejo a tabela de qualquer temporada em qualquer rodada, e a tabela final bate com a oficial, punições do STJD incluídas.
4. Como dev, rodo `make explain` e os planos físicos das consultas escolhidas são regravados em `docs/explain/planos/`.

## Tech Stack

Mesma do `lakehouse` (Python 3.12, PySpark 4.2.0, delta-spark 4.4.1, pytest + chispa, ruff). **Nenhuma dependência nova.**

## Commands

```bash
make gold        # uv run python -m brasileirao.pipeline gold
make explain     # uv run python -m brasileirao.pipeline explain
make test        # inalterado
make lint        # inalterado
```

`make gold` lê a silver inteira e regrava todas as tabelas gold com overwrite. Falha com mensagem clara se a silver não existir.

## Project Structure

```
src/brasileirao/
├── config.py                  # CAMADAS ganha "gold"
├── pipeline.py                # CLI ganha os subcomandos `gold` e `explain`
├── aggregate/
│   ├── gold.py                # orquestra: lê silver, chama as funções, checa, grava
│   ├── classificacao.py       # classificacao_por_rodada (+ aplicação dos ajustes)
│   ├── desempenho.py          # desempenho_mandante_visitante
│   ├── sequencias.py          # gaps-and-islands
│   ├── ranking.py             # ranking_historico
│   ├── disciplina.py
│   ├── artilharia.py
│   ├── gols_por_minuto.py
│   ├── resumo.py              # resumo_temporada
│   ├── qualidade.py           # checagens do gold
│   └── explain.py             # gera os planos físicos em texto
├── utils/temporada.py         # ganha rebaixados_por_temporada()
└── reference/
    └── ajustes_pontos.csv     # punições do STJD (versionado, vai no wheel)

tests/
├── unit/aggregate/            # uma suíte por tabela, com DataFrames montados em memória
└── integration/test_gold.py   # silver (fixtures) → gold, idempotência, falha sem silver

docs/explain/
├── README.md                  # leitura comentada dos planos + decisão de não particionar
└── planos/*.txt               # saída de explain("formatted"), regenerada por `make explain`
```

## Contrato de dados

Tabelas Delta por caminho em `gold/<tabela>`, **sem particionamento** (a maior tem ~17 mil linhas; particionar por temporada geraria arquivos minúsculos, e o `docs/explain/README.md` explica isso). Escrita com `mode("overwrite")` e `overwriteSchema` desligado: mudança de schema tem que ser explícita.

Regras gerais:

- Só tipos que o Postgres recebe via JDBC sem conversão: `int`, `bigint`, `double`, `string`, `date`. Nada de array, map ou struct; listas viram string separada por `", "`.
- Times sempre pelo nome canônico da silver.
- `aproveitamento` e percentuais como double entre 0 e 1, arredondados em 4 casas.
- Barueri (2009) e Grêmio Prudente (2010) ficam separados, como na fonte. O README explica que são o mesmo clube.

### Fatos da silver que moldam o gold (conferidos em 27/10/2026)

- 24 times em 2003–04 (46 rodadas), 22 em 2005 (42), 20 a partir de 2006 (38).
- **140 partidas foram jogadas fora da ordem da rodada** (adiadas). A classificação segue a rodada; as sequências seguem a data.
- 2016 tem 379 partidas: Chapecoense × Atlético-MG não foi jogada. A tabela oficial conta W.O. duplo (0 × 3 para os dois, com 38 jogos); aqui as duas ficam com 37 jogos e 3 gols sofridos a menos. Os pontos e as posições não mudam.
- Placar da fonte com 1 gol de diferença do oficial em duas partidas: Athletico-PR × Criciúma em 2003 e Internacional × Criciúma em 2004 (gols pró e contra de um lado só). Os pontos não mudam; fica como está e o README registra.
- Gols: `minuto` vai de 0 a 90 e o `acrescimo` só aparece com minuto 45 ou 90. O `clube` do gol contra é o time beneficiado (gols batem 100% com o placar).
- Cartões: `silver/cartoes` cobre 2014–2024; as colunas `cartao_amarelo`/`cartao_vermelho` de `silver/estatisticas` têm 2015–2023 e 2025 completos (2014 só 32 linhas, 2019 tem 714 de 760, 2024 vazia). As duas fontes divergem um pouco (2017: 1.811 amarelos em `cartoes`, 1.773 em `estatisticas`).

### Referência: `reference/ajustes_pontos.csv`

Colunas `temporada,clube,pontos,motivo,fonte`. `pontos` com sinal (perda negativa, ganho positivo); um caso que tira pontos de um clube e dá a outro vira uma linha por clube. `fonte` é uma URL. `clube` usa o nome canônico, e clube fora do mapa é falha. O ajuste entra só na **última rodada** da temporada: as datas das decisões do STJD não são confiáveis (várias saíram depois do campeonato), e a classificação das rodadas anteriores fica como resultado de campo.

Lista conferida em 27/10/2026 contra a tabela final oficial de cada temporada (Wikipédia, dados da CBF). Com estes ajustes, a silver reproduz os pontos e a ordem oficiais das 6 temporadas usando o desempate da spec:

| Temporada | Clube | Pontos | Motivo |
|---|---|---|---|
| 2003 | Ponte Preta | −4 | escalação irregular de Roberto contra Internacional e Juventude |
| 2003 | Internacional | +2 | pontos do jogo contra a Ponte Preta |
| 2003 | Juventude | +3 | pontos do jogo contra a Ponte Preta |
| 2003 | Paysandu | −8 | escalação irregular de Júnior Amorim e Aldrovani em 4 jogos |
| 2003 | Ponte Preta | +3 | pontos do jogo contra o Paysandu |
| 2003 | São Caetano | +3 | pontos do jogo contra o Paysandu |
| 2003 | Corinthians | +2 | pontos do jogo contra o Paysandu |
| 2003 | Fluminense | +2 | pontos do jogo contra o Paysandu |
| 2004 | São Caetano | −24 | morte do jogador Serginho |
| 2010 | Grêmio Prudente | −3 | escalação irregular de jogador suspenso |
| 2013 | Portuguesa | −4 | escalação irregular de Héverton |
| 2013 | Flamengo | −4 | escalação irregular de André Santos |
| 2016 | Santa Cruz | −3 | atraso salarial (decisão de 01/06/2017) |
| 2018 | Sport | −3 | atraso salarial (decisão de 24/04/2019) |

URLs das fontes: as páginas `pt.wikipedia.org/wiki/Campeonato_Brasileiro_de_Futebol_de_<ano>_-_Série_A` de cada temporada.

Totais que a conferência usa (pontos finais oficiais): 2003 Ponte Preta 50, Paysandu 49, Internacional 72, Juventude 53, São Caetano 74, Corinthians 59, Fluminense 52; 2004 São Caetano 53; 2010 Grêmio Prudente 28; 2013 Flamengo 45, Portuguesa 44; 2016 Santa Cruz 28; 2018 Sport 39.

### Tabelas

**`classificacao_por_rodada`**: uma linha por (`temporada`, `rodada`, `clube`) para **toda** rodada de 1 até a última, inclusive aquela em que o time não jogou (partida adiada): os acumulados repetem os da rodada anterior.

| Coluna | Tipo | Regra |
|---|---|---|
| `temporada`, `rodada` | int | |
| `clube` | string | |
| `posicao` | int | 1..N sem repetição dentro de (`temporada`, `rodada`) |
| `pontos` | int | 3 por vitória, 1 por empate, mais `ajuste_pontos` |
| `ajuste_pontos` | int | soma dos ajustes do clube na temporada, só na última rodada; 0 nas demais |
| `jogos`, `vitorias`, `empates`, `derrotas` | int | acumulados até a rodada (partidas com `rodada` ≤ atual) |
| `gols_pro`, `gols_contra`, `saldo` | int | acumulados |
| `aproveitamento` | double | `(pontos - ajuste_pontos) / (3 * jogos)`; nulo com 0 jogos |
| `ultimos_5` | string | resultado dos 5 últimos jogos até a rodada, mais antigo primeiro, `V`/`E`/`D` (ex.: `"VVEDV"`); menos letras no começo da temporada |

Desempate: pontos → vitórias → saldo → gols pró → nome do clube. O confronto direto e os cartões, que a CBF usa depois de gols pró, ficam de fora (o README registra). Construção só com window functions: grade clube × rodada (cross join dos times da temporada com as rodadas), soma acumulada com `Window.partitionBy("temporada", "clube").orderBy("rodada")` e `row_number()` por (`temporada`, `rodada`) para a posição.

**`desempenho_mandante_visitante`**: uma linha por (`temporada`, `clube`, `mando`), `mando` ∈ {`mandante`, `visitante`}. Colunas: `jogos`, `vitorias`, `empates`, `derrotas`, `gols_pro`, `gols_contra`, `pontos`, `aproveitamento`. Só resultado de campo, sem ajustes.

**`sequencias`**: a maior sequência de cada (`temporada`, `clube`, `tipo`), com os jogos em ordem de `data` (depois `hora`, depois `partida_id`). Tipos:

| `tipo` | Conta enquanto o resultado for |
|---|---|
| `invencibilidade` | vitória ou empate |
| `vitorias` | vitória |
| `jejum` | empate ou derrota |
| `derrotas` | derrota |

Colunas: `temporada`, `clube`, `tipo`, `jogos` (tamanho), `data_inicio`, `data_fim` (date), `em_aberto` (int 0/1: a sequência chegou até o último jogo do time na temporada). Empate de tamanho fica com a sequência mais antiga. Time sem nenhum jogo do tipo (ex.: nunca perdeu) tem linha com `jogos = 0` e datas nulas, para o dashboard não sumir com ele. Construção por gaps-and-islands: `row_number()` geral menos `row_number()` dentro do resultado define a ilha.

**`ranking_historico`**: uma linha por `clube`, cobrindo 2003–2025. Colunas: `posicao` (por pontos, depois vitórias, saldo, gols pró, nome), `temporadas`, `primeira_temporada`, `ultima_temporada`, `jogos`, `vitorias`, `empates`, `derrotas`, `gols_pro`, `gols_contra`, `saldo`, `pontos` (com ajustes), `aproveitamento`, `titulos`. Os acumulados saem da última rodada de cada temporada em `classificacao_por_rodada`; `titulos` conta as vezes em que o clube terminou em 1º.

**`disciplina`**: uma linha por (`temporada`, `clube`), 2014–2025. Colunas: `jogos`, `amarelos`, `vermelhos`, `amarelos_por_jogo`, `vermelhos_por_jogo` (double, 2 casas), `fonte` (`cartoes` | `estatisticas`). Em 2014–2024 a fonte é `silver/cartoes`; em 2025, que não tem cartões, são as somas de `silver/estatisticas`. O README avisa que as duas fontes contam um pouco diferente. Não há métrica por árbitro (a fonte não tem árbitro).

**`artilharia`**: uma linha por (`temporada`, `atleta`), 2014–2025, sem gol contra. Colunas: `posicao` (`rank()`: empatados dividem a posição), `atleta`, `clubes` (clubes distintos em ordem alfabética, separados por `", "`, para quem trocou de time na temporada), `gols`, `penaltis`. Não há artilharia histórica: o nome do atleta não é chave confiável entre temporadas.

**`gols_por_minuto`**: uma linha por (`temporada`, `faixa`, `lado`), 2014–2025. `faixa` ∈ {`0-15`, `16-30`, `31-45+`, `46-60`, `61-75`, `76-90+`}, pelo `minuto` (o acréscimo do 45 cai em `31-45+`, o do 90 em `76-90+`). `lado` ∈ {`mandante`, `visitante`}, pelo time que marcou (no gol contra, o beneficiado). Colunas: `gols` e `pct_gols` (fração dos gols da temporada).

**`resumo_temporada`**: uma linha por `temporada`, 2003–2025. Colunas: `times`, `partidas`, `gols`, `media_gols` (2 casas), `pct_vitoria_mandante`, `pct_empate`, `pct_vitoria_visitante`, `campeao`, `vice`, `rebaixados` (string, ordem da classificação), `artilheiro` (string com `", "` se houver empate; nulo antes de 2014), `gols_artilheiro`. Campeão, vice e rebaixados saem da última rodada da classificação, com ajustes.

### Regras de domínio

- **Rebaixados por temporada:** 2 em 2003, 4 a partir de 2004. A regra fica em `utils/temporada.py`, com teste.
- **Ajustes de pontos:** só entram em `classificacao_por_rodada` (e no que deriva dela: ranking e resumo). `desempenho_mandante_visitante` e `sequencias` são resultado de campo.
- **Temporada em andamento:** não existe na silver, então não existe no gold.

### Checagens de qualidade (gold)

Rodam antes da escrita; qualquer falha interrompe sem gravar nenhuma tabela gold.

| Checagem | Severidade |
|---|---|
| `posicao` repetida ou fora de 1..N em (`temporada`, `rodada`) | **falha** |
| Na última rodada, `jogos` ≠ 2 × (times − 1), exceto Chapecoense e Atlético-MG em 2016 (37) | **falha** |
| Na última rodada, Σ`vitorias` ≠ Σ`derrotas` na temporada | **falha** |
| Clube de `ajustes_pontos.csv` fora do mapa ou sem jogo na temporada | **falha** |
| Gols de `gols_por_minuto` ≠ gols de `silver/gols` na temporada | **falha** |
| Clube de `disciplina` sem partida na temporada | aviso |

## Code Style

Igual ao `lakehouse`: funções puras `DataFrame → DataFrame` separadas de I/O, nomes em português sem acento, termos do Spark como são. Cada módulo de tabela expõe uma função `calcular_<tabela>(...)` que recebe DataFrames da silver (e de referência) e devolve o DataFrame gold; só `gold.py` lê e grava.

```python
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ACUMULADO = (
    Window.partitionBy("temporada", "clube")
    .orderBy("rodada")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)


def acumular(jogos_por_rodada: DataFrame) -> DataFrame:
    """Soma acumulada por rodada. A grade já tem linha zerada nas rodadas sem jogo."""
    metricas = ["jogos", "vitorias", "empates", "derrotas", "gols_pro", "gols_contra"]
    return jogos_por_rodada.select(
        "temporada", "rodada", "clube",
        *[F.sum(m).over(ACUMULADO).alias(m) for m in metricas],
    )
```

Demais regras do `lakehouse` valem (ruff, type hints, sem `collect()` em tabela inteira fora de teste, caminhos via `config.py`).

## Testing Strategy

- **Unitários** (`tests/unit/aggregate/`): cada `calcular_<tabela>` com DataFrames montados em memória. Fixture principal: um minicampeonato de 4 times em turno e returno (12 partidas, 6 rodadas), feito à mão com a tabela final conhecida, cobrindo: partida adiada (rodada 2 jogada depois da rodada 4), empate em pontos decidido por vitórias e por saldo, ajuste de pontos que só aparece na última rodada e muda a posição final, sequência em aberto no fim, time que nunca perdeu, gol contra, gol aos 45+2 e aos 90+3, atleta com gols por dois clubes.
- **Integração** (`tests/integration/test_gold.py`): fixtures raw do lakehouse → silver → gold de ponta a ponta; idempotência (2× = 1×); `make gold` sem silver falha com mensagem; checagem de falha não grava nada.
- **Conferência manual nos dados reais** (checkpoint, não teste): campeão de todas as temporadas 2003–2025 bate com o histórico oficial; nas 6 temporadas com ajuste, os pontos finais da lista acima, a ordem e os rebaixados batem com a tabela oficial.
- **Cobertura:** ≥ 80% em `src/brasileirao/` (meta atual do projeto é 99%; não deixar cair muito).
- **Tempo:** a suíte inteira continua abaixo de 3 minutos; `make gold` nos dados reais abaixo de 2 minutos.

## Boundaries

- **Sempre:** `make lint` e `make test` antes de commit; toda punição nova entra em `ajustes_pontos.csv` com `fonte`; atualizar o "Contrato de dados" antes de mudar schema de tabela gold; texto público (README, `docs/explain/README.md`) passa pela skill `voz-romualdo`.
- **Perguntar antes:** mudar a silver ou o `times.csv`; adicionar dependência; mudar schema de tabela gold depois que o `serving` começar a consumir; mudar critério de desempate ou a lista de ajustes.
- **Nunca:** versionar `data/`; tipo complexo (array/map/struct) em tabela gold; teste que dependa do download real; apagar ou pular teste que falha sem aprovação.

## Success Criteria

1. `make gold` a partir da silver completa cria as 8 tabelas em `data/gold/` em menos de 2 minutos.
2. `make gold` 2× deixa o conteúdo idêntico ao de 1× (teste de integração + conferência por hash).
3. Todas as checagens de falha passam nos dados reais.
4. O campeão de cada temporada 2003–2025 bate com o oficial. Nas temporadas com ajuste (2003, 2004, 2010, 2013, 2016, 2018), os pontos finais dos clubes ajustados e os rebaixados também.
5. `classificacao_por_rodada` tem exatamente N × rodadas linhas por temporada (24 × 46, 22 × 42, 20 × 38).
6. `docs/explain/README.md` comenta pelo menos 3 planos: a classificação (window + exchange), as sequências (duas windows sobre a mesma partição) e um join com broadcast (disciplina ou artilharia), além da decisão de não particionar.
7. `make test` passa com cobertura ≥ 80%; `make lint` limpo.

## Decisões

- **Punições do STJD:** tabela de referência versionada, aplicada só na última rodada; a classificação final bate com a oficial.
- **Sequências:** dentro da temporada.
- **Barueri e Grêmio Prudente:** separados.
- **Tabelas além do rascunho:** `artilharia`, `gols_por_minuto` e `resumo_temporada`.
- **Carga:** recálculo completo com overwrite. Ranking e sequências cruzam a temporada toda (ou todas), e o MERGE já é demonstrado na silver.
- **Sem particionamento no gold**, justificado em `docs/explain/`.

## Open Questions

Nenhuma. A lista de ajustes foi conferida em 27/10/2026 (ver "Referência: `reference/ajustes_pontos.csv`").
