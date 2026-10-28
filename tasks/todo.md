# Tarefas: gold

Plano: [plan.md](plan.md). Spec: [SPEC-gold.md](../SPEC-gold.md).

Toda tarefa só conta como feita com `make lint` e `make test` passando.

---

## Fase 1: Fatia vertical da classificação

### T1: Base do gold

**Descrição:** Preparar o terreno: camada `gold` em `config.CAMADAS`, `rebaixados_por_temporada(temporada)` em `utils/temporada.py` (2 em 2003, 4 a partir de 2004), `reference/ajustes_pontos.csv` com as 14 linhas da spec e um leitor `carregar_ajustes(spark)` no pacote `aggregate`, no mesmo molde do `carregar_mapa_times`.

**Critérios de aceite:**
- [x] `config.caminho_tabela("gold", "x")` funciona; camada desconhecida continua dando erro
- [x] `rebaixados_por_temporada` testado para 2003, 2004 e 2025; temporada antes de 2003 dá erro
- [x] `ajustes_pontos.csv` com colunas `temporada,clube,pontos,motivo,fonte`, as 14 linhas da tabela da spec, nomes canônicos e URL da página da temporada na Wikipédia
- [x] `carregar_ajustes` devolve DataFrame com `temporada int, clube string, pontos int, motivo string, fonte string`
- [x] Teste unitário: todo `clube` do CSV existe como `nome_canonico` em `times.csv`; a soma por temporada e clube bate com os totais da spec (ex.: Ponte Preta 2003 = −1)

**Verificação:**
- [x] `uv run pytest tests/unit/test_config.py tests/unit/test_temporada.py tests/unit/aggregate/`

**Dependências:** nenhuma

**Arquivos:** `src/brasileirao/config.py`, `src/brasileirao/utils/temporada.py`, `src/brasileirao/reference/ajustes_pontos.csv`, `src/brasileirao/aggregate/{__init__,referencia}.py`, `tests/unit/aggregate/test_referencia.py` (+ ajustes em `test_config.py` e `test_temporada.py`)

**Tamanho:** S

### T2: `calcular_classificacao`

**Descrição:** `aggregate/classificacao.py` com `calcular_classificacao(partidas, ajustes) -> DataFrame`, conforme o schema de `classificacao_por_rodada` da spec. Por dentro: partidas viram duas linhas por jogo (visão de cada clube), resultado por rodada, grade clube × rodada com zeros nas rodadas sem jogo, acumulados por window, `ultimos_5`, ajuste só na última rodada, `aproveitamento` e `posicao` por `row_number()` com o desempate da spec. Só window functions e joins, sem `collect()`.

Fixture: minicampeonato de 4 times, turno e returno (12 partidas, 6 rodadas), num módulo de fixtures de `tests/unit/aggregate/` reaproveitável pelas próximas tarefas. A tabela final esperada é calculada à mão e escrita no teste.

**Critérios de aceite:**
- [x] 4 × 6 = 24 linhas; `posicao` 1..4 sem repetição em toda rodada
- [x] Partida da rodada 2 jogada depois da rodada 4: entra na rodada 2 (a classificação segue a rodada) e o time tem linha nas 6 rodadas
- [x] Empate em pontos decidido por vitórias num caso e por saldo em outro
- [x] Ajuste negativo aparece só na rodada 6, muda a posição final e não entra no `aproveitamento`
- [x] `ultimos_5` certo na rodada 1 (`"V"`, uma letra) e na rodada 6 (5 letras, mais antigo primeiro)
- [x] `aproveitamento` com 4 casas

**Verificação:**
- [x] `uv run pytest tests/unit/aggregate/test_classificacao.py`

**Dependências:** T1

**Arquivos:** `src/brasileirao/aggregate/classificacao.py`, `tests/unit/aggregate/{conftest,test_classificacao}.py`

**Tamanho:** M

### T3: `gold.py`, checagens, CLI e integração

**Descrição:** `aggregate/gold.py` com o registro ordenado de tabelas (por enquanto só `classificacao_por_rodada`), leitura da silver (erro claro se não existir), cálculo, checagens e gravação com overwrite (sem particionar, `overwriteSchema` desligado). `aggregate/qualidade.py` com as checagens de falha da classificação e dos ajustes: posição, jogos na última rodada (exceção de 2016), Σ vitórias = Σ derrotas, clube do ajuste sem jogo na temporada. Subcomando `gold` na CLI e alvo `make gold`.

**Critérios de aceite:**
- [x] `python -m brasileirao.pipeline gold` grava `gold/classificacao_por_rodada`; sem silver, falha com mensagem que manda rodar `make lakehouse`
- [x] Cada checagem de falha tem teste unitário com um DataFrame que a dispara
- [x] Integração: fixtures raw do lakehouse → silver → gold; rodar `gold` 2× deixa o conteúdo igual a 1×
- [x] Integração: com uma checagem forçada a falhar, nenhuma tabela gold é gravada
- [x] Resumo no log: linhas por tabela e tempo total

**Verificação:**
- [x] `make lint && make test`
- [x] `make gold` nos dados reais (dados em `data/silver/`)

**Dependências:** T2

**Arquivos:** `src/brasileirao/aggregate/{gold,qualidade}.py`, `src/brasileirao/pipeline.py`, `Makefile`, `tests/unit/aggregate/test_qualidade.py`, `tests/integration/test_gold.py`

**Tamanho:** M

### Checkpoint: Classificação nos dados reais
- [x] Linhas por temporada: 24 × 46 (2003–04), 22 × 42 (2005), 20 × 38 (2006–2025)
- [x] Campeão de 2003–2025 bate com o oficial: 2003 Cruzeiro, 2004 Santos, 2005 Corinthians, 2006–08 São Paulo, 2009 Flamengo, 2010 Fluminense, 2011 Corinthians, 2012 Fluminense, 2013–14 Cruzeiro, 2015 Corinthians, 2016 Palmeiras, 2017 Corinthians, 2018 Palmeiras, 2019–20 Flamengo, 2021 Atlético-MG, 2022–23 Palmeiras, 2024 Botafogo-RJ, 2025 Flamengo
- [x] Pontos finais dos clubes ajustados e rebaixados de 2003, 2004, 2010, 2013, 2016 e 2018 batem com a spec
- [x] `make gold` 2× sem mudança de conteúdo; tempo anotado
- [ ] Revisão com o Romualdo

Resultado (27/10/2026): 18.332 linhas, N × rodadas em todas as 23 temporadas; os 23 campeões batem; os 13 pontos ajustados da spec e os rebaixados das 6 temporadas batem (ordem inclusa); hash do conteúdo igual em 2 execuções; `make gold` 14,5s (10,7s no gold). `make lint` só reprova o bloco de código do `SPEC-gold.md` (já reprovava no f69ac9a).

---

## Fase 2: Tabelas de partidas

### T4: `desempenho_mandante_visitante`

**Descrição:** `calcular_desempenho(partidas)` com uma linha por (`temporada`, `clube`, `mando`), resultado de campo, sem ajustes. Entra no registro de `gold.py`.

**Critérios de aceite:**
- [x] No minicampeonato, cada clube tem 2 linhas, e a soma de mandante + visitante bate com a última rodada da classificação, sem o ajuste
- [x] `aproveitamento` com 4 casas

**Verificação:**
- [x] `uv run pytest tests/unit/aggregate/test_desempenho.py tests/integration/test_gold.py`

**Dependências:** T3

**Arquivos:** `src/brasileirao/aggregate/desempenho.py`, `src/brasileirao/aggregate/gold.py`, `tests/unit/aggregate/test_desempenho.py`

**Tamanho:** S

### T5: `sequencias`

**Descrição:** `calcular_sequencias(partidas)` por gaps-and-islands, com os jogos de cada clube em ordem de `data`, `hora`, `partida_id`. Os 4 tipos da spec, a maior sequência de cada (`temporada`, `clube`, `tipo`), empate de tamanho fica com a mais antiga, `em_aberto` e a linha com `jogos = 0` para quem não tem o tipo.

**Critérios de aceite:**
- [x] 4 linhas por clube e temporada no minicampeonato
- [x] A partida adiada entra na ordem da data, não da rodada (o teste prova que muda a sequência)
- [x] Clube que nunca perdeu: `derrotas` com `jogos = 0` e datas nulas; `invencibilidade` com `em_aberto = 1`
- [x] Duas sequências do mesmo tamanho: fica a mais antiga

**Verificação:**
- [x] `uv run pytest tests/unit/aggregate/test_sequencias.py tests/integration/test_gold.py`

**Dependências:** T3

**Arquivos:** `src/brasileirao/aggregate/sequencias.py`, `src/brasileirao/aggregate/gold.py`, `tests/unit/aggregate/test_sequencias.py` (+ partidas extras na fixture, se precisar)

**Tamanho:** M

### T6: `ranking_historico`

**Descrição:** `calcular_ranking(classificacao)` a partir da última rodada de cada temporada: soma dos acumulados, pontos com ajuste, `temporadas`, primeira e última, `titulos` e `posicao` pelo desempate da spec.

**Critérios de aceite:**
- [x] Com duas temporadas na fixture (o minicampeonato mais uma segunda temporada com um time diferente), o ranking soma as duas e conta 1 título para cada campeão
- [x] Pontos do ranking incluem o ajuste; `aproveitamento` não

**Verificação:**
- [x] `uv run pytest tests/unit/aggregate/test_ranking.py tests/integration/test_gold.py`

**Dependências:** T3

**Arquivos:** `src/brasileirao/aggregate/ranking.py`, `src/brasileirao/aggregate/gold.py`, `tests/unit/aggregate/test_ranking.py`

**Tamanho:** S

### Checkpoint: Partidas
- [x] `make lint`, `make test`, `make gold` nos dados reais
- [x] Amostras: títulos no ranking batem com a lista de campeões do checkpoint da Fase 1; a maior invencibilidade do campeão de 2025 confere com o calendário da silver
- [ ] Revisão com o Romualdo

Resultado (27/10/2026): `make gold` 18,9s (15,0s no gold) com 18.332 / 940 / 1.880 / 46 linhas; 23 títulos no ranking, batendo clube a clube; Barueri e Grêmio Prudente separados; maior invencibilidade do Flamengo em 2025 = 12 jogos, 20/07 (1 × 0 Fluminense, R15) a 02/10 (0 × 0 Cruzeiro, R26), entre as derrotas para Santos (16/07) e Bahia (05/10), conferida no calendário da silver; hash das 4 tabelas igual em 2 execuções. Suíte: 183 testes, 99%, 2min46s.

---

## Fase 3: Tabelas de eventos e resumo

### T7: `disciplina`

**Descrição:** `calcular_disciplina(partidas, cartoes, estatisticas)`: 2014–2024 de `cartoes` e 2025 de `estatisticas`, com `fonte`, `jogos` do clube na temporada e as médias com 2 casas. A regra de qual temporada usa qual fonte é "temporada sem nenhuma linha em `cartoes` usa `estatisticas`", sem ano fixo no código. Aviso (não falha) para clube sem partida na temporada.

**Critérios de aceite:**
- [x] Fixture com uma temporada que tem cartões e uma que só tem estatísticas: `fonte` certa em cada uma
- [x] Clube sem nenhum cartão na temporada aparece com 0, não some
- [x] Temporada antes de 2014 não aparece

**Verificação:**
- [x] `uv run pytest tests/unit/aggregate/test_disciplina.py tests/integration/test_gold.py`

**Dependências:** T3

**Arquivos:** `src/brasileirao/aggregate/disciplina.py`, `src/brasileirao/aggregate/{gold,qualidade}.py`, `tests/unit/aggregate/test_disciplina.py`

**Tamanho:** S

### T8: `artilharia`

**Descrição:** `calcular_artilharia(gols)`: uma linha por (`temporada`, `atleta`), sem gol contra, `clubes` em ordem alfabética separados por `", "`, `penaltis` e `posicao` com `rank()`.

**Critérios de aceite:**
- [x] Gol contra não conta para ninguém
- [x] Atleta com gols por dois clubes: uma linha, `clubes = "A, B"`, gols somados
- [x] Empate no topo divide a posição 1, e o próximo fica em 3

**Verificação:**
- [x] `uv run pytest tests/unit/aggregate/test_artilharia.py tests/integration/test_gold.py`

**Dependências:** T3

**Arquivos:** `src/brasileirao/aggregate/artilharia.py`, `src/brasileirao/aggregate/gold.py`, `tests/unit/aggregate/test_artilharia.py`

**Tamanho:** S

### T9: `gols_por_minuto`

**Descrição:** `calcular_gols_por_minuto(gols, partidas)`: faixa pelo `minuto`, `lado` pelo clube que marcou contra o mandante da partida, `gols` e `pct_gols`. Inclui a checagem de falha da spec (total de gols da tabela = gols da silver na temporada).

**Critérios de aceite:**
- [x] Gol aos 45+2 em `31-45+`, aos 90+3 em `76-90+`, aos 0 em `0-15`, aos 46 em `46-60`
- [x] Gol contra entra no `lado` do beneficiado
- [x] `pct_gols` soma 1 por temporada (tolerância de arredondamento)
- [x] Teste da checagem com um DataFrame que perde um gol

**Verificação:**
- [x] `uv run pytest tests/unit/aggregate/test_gols_por_minuto.py tests/unit/aggregate/test_qualidade.py tests/integration/test_gold.py`

**Dependências:** T3

**Arquivos:** `src/brasileirao/aggregate/gols_por_minuto.py`, `src/brasileirao/aggregate/{gold,qualidade}.py`, `tests/unit/aggregate/test_gols_por_minuto.py`

**Tamanho:** S

### T10: `resumo_temporada`

**Descrição:** `calcular_resumo(partidas, classificacao, artilharia)`: uma linha por temporada com os totais, percentuais, campeão, vice, `rebaixados` pela regra do T1 (ordem da classificação, separados por `", "`) e artilheiro (empate junta os nomes; nulo sem dado de gols).

**Critérios de aceite:**
- [x] Na fixture, campeão, vice e rebaixados batem com a classificação final (com ajuste)
- [x] `pct_vitoria_mandante + pct_empate + pct_vitoria_visitante = 1` (tolerância de arredondamento)
- [x] Temporada sem gols na silver de gols: `artilheiro` e `gols_artilheiro` nulos
- [x] Artilheiros empatados: os dois nomes em ordem alfabética

**Verificação:**
- [x] `uv run pytest tests/unit/aggregate/test_resumo.py tests/integration/test_gold.py`

**Dependências:** T8

**Arquivos:** `src/brasileirao/aggregate/resumo.py`, `src/brasileirao/aggregate/gold.py`, `tests/unit/aggregate/test_resumo.py`

**Tamanho:** S

### Checkpoint: Gold completo
- [x] `make gold` nos dados reais grava as 8 tabelas em menos de 2 minutos, com todas as checagens passando
- [x] Amostras: artilheiro de 2 temporadas e o campeão no `resumo_temporada` batem com o oficial; `disciplina` de 2025 vem de `estatisticas`
- [ ] Revisão com o Romualdo

Resultado (27/10/2026): `make gold` 24,5s (20,6s no gold), sem aviso, com 18.332 / 940 / 1.880 / 46 / 240 / 3.606 / 144 / 23 linhas. Artilheiros: 2019 Gabriel Barbosa (Flamengo) 25, 2021 Hulk (Atlético-MG) 19, 2022 Germán Cano (Fluminense) 26. Campeões do `resumo_temporada` iguais aos da Fase 1; artilheiro nulo em 2003–2013. `disciplina`: 2014–2024 de `cartoes` e 2025 de `estatisticas` (1.849 / 94), 20 clubes por temporada. `gols_por_minuto` soma 10.820, igual à `silver/gols` em cada temporada. Hash das 8 tabelas igual em 2 execuções. Suíte: 198 testes, 99%, 2min58s.

---

## Fase 4: Explain plans e fechamento

### T11: `explain.py` e `make explain`

**Descrição:** `aggregate/explain.py` lê a silver real, chama as funções `calcular_*` escolhidas e grava `df.explain("formatted")` em `docs/explain/planos/<tabela>.txt`. Os planos são `classificacao_por_rodada`, `sequencias` e um join com broadcast (`disciplina` ou `artilharia`, o que mostrar `BroadcastHashJoin` com mais clareza). Subcomando `explain` na CLI e alvo `make explain`. `shuffle.partitions` fixo para o texto variar pouco entre execuções.

**Critérios de aceite:**
- [ ] `make explain` regrava os 3 arquivos
- [ ] Teste de integração: com a silver das fixtures, `explain` grava arquivos não vazios num diretório temporário (o diretório de saída é parâmetro, com default `docs/explain/planos`)

**Verificação:**
- [ ] `make explain && ls docs/explain/planos`

**Dependências:** T10

**Arquivos:** `src/brasileirao/aggregate/explain.py`, `src/brasileirao/pipeline.py`, `Makefile`, `tests/integration/test_explain.py`, `docs/explain/planos/*.txt`

**Tamanho:** S

### T12: `docs/explain/README.md` e critérios de sucesso

**Descrição:** Leitura comentada dos 3 planos (exchange e sort da window, as duas windows das sequências sobre a mesma partição, o broadcast, o que o AQE mudou) e a decisão de não particionar o gold. O texto é público e passa pela skill `voz-romualdo`. Depois, conferir os 7 critérios de sucesso da spec nos dados reais e anotar o resultado aqui, como no T10 do lakehouse.

**Critérios de aceite:**
- [ ] README comenta os 3 planos citando trechos reais dos `.txt`
- [ ] Seção sobre particionamento com o número real de linhas e arquivos de cada tabela gold
- [ ] Os 7 critérios de sucesso conferidos e anotados abaixo

**Verificação:**
- [ ] Leitura do Romualdo; `make lint && make test`

**Dependências:** T11

**Arquivos:** `docs/explain/README.md`, `tasks/todo.md` (só a anotação do resultado)

**Tamanho:** S

### Checkpoint: Completo
- [ ] Revisão com o Romualdo antes de fechar o módulo
