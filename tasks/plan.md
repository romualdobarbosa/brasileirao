# Plano de implementação: gold

Spec: [SPEC-gold.md](../SPEC-gold.md). Tarefas detalhadas em [todo.md](todo.md). O plano do módulo anterior está em [plan-lakehouse.md](plan-lakehouse.md).

## Visão geral

Construir o pacote `brasileirao.aggregate`, que lê a silver inteira e grava 8 tabelas gold em Delta, com checagens antes da escrita e explain plans documentados. A ordem: primeiro uma fatia vertical completa só com **`classificacao_por_rodada`** (referência de ajustes → cálculo → checagens → gravação → CLI), validada nos dados reais contra as tabelas oficiais. Depois vêm as tabelas que só dependem de partidas, as tabelas de eventos (gols e cartões, 2014+) e por fim os explain plans.

## Decisões de arquitetura

- **Uma função pura por tabela.** `calcular_<tabela>(...)` recebe DataFrames (silver, referência ou outra tabela gold já calculada) e devolve o DataFrame gold. Só `gold.py` lê a silver e grava. Os testes unitários montam DataFrames em memória e não tocam em disco.
- **Tabelas gold que dependem de outra recebem o DataFrame calculado, não releem do disco.** `ranking_historico` e `resumo_temporada` recebem a classificação, e o `resumo` recebe a artilharia. `gold.py` calcula na ordem das dependências e faz `localCheckpoint()` nas tabelas reaproveitadas, como `silver.py` já faz.
- **Tudo ou nada.** `gold.py` calcula as 8 tabelas, roda todas as checagens e só então grava. Uma falha não deixa o gold misturado entre versões.
- **Registro de tabelas em `gold.py`.** Um dicionário ordenado `nome → função` que cada tarefa estende. A CLI, a gravação e o teste de idempotência percorrem esse registro, então tabela nova entra no teste sem mudar o teste.
- **`ajustes_pontos.csv` lido do pacote** (`importlib.resources`), igual ao `times.csv`.
- **`explain.py` reaproveita as funções `calcular_*`.** Ele roda `explain("formatted")` no DataFrame que a função devolve, sobre a silver real, e grava em `docs/explain/planos/<tabela>.txt`. Não existe consulta paralela só para o explain.

## Grafo de dependências

```
T1 base (config gold, rebaixados, ajustes_pontos.csv)
 └── T2 classificacao_por_rodada (cálculo puro)
      └── T3 gold.py + checagens + CLI gold + Makefile + integração   ← checkpoint nos dados reais
           ├── T4 desempenho_mandante_visitante
           ├── T5 sequencias                     (T4, T5, T6, T7, T8, T9 independentes entre si)
           ├── T6 ranking_historico
           ├── T7 disciplina
           ├── T8 artilharia ──┐
           └── T9 gols_por_minuto
                               └── T10 resumo_temporada (precisa de T8)
                                    └── T11 explain.py + make explain
                                         └── T12 docs/explain/README.md + critérios de sucesso
```

## Lista de tarefas

### Fase 1: Fatia vertical da classificação
- [ ] T1: Base do gold: camada `gold` no config, `rebaixados_por_temporada`, `ajustes_pontos.csv` e leitor
- [ ] T2: `calcular_classificacao` com o minicampeonato de teste
- [ ] T3: `gold.py`, checagens de falha da classificação, CLI `gold`, Makefile e teste de integração

### Checkpoint: Classificação nos dados reais (revisão com o Romualdo)
- [ ] `make gold` grava `gold/classificacao_por_rodada` com 24 × 46, 22 × 42 e 20 × 38 linhas por temporada
- [ ] Campeão de 2003–2025 bate com o oficial
- [ ] Nas 6 temporadas com ajuste, os pontos finais e os rebaixados batem com a spec
- [ ] `make gold` 2× deixa o conteúdo igual

### Fase 2: Tabelas de partidas
- [ ] T4: `desempenho_mandante_visitante`
- [ ] T5: `sequencias` (gaps-and-islands)
- [ ] T6: `ranking_historico`

### Checkpoint: Partidas
- [ ] `make lint`, `make test` e `make gold` nos dados reais; conferência por amostra (ex.: maior invencibilidade de uma temporada conhecida, títulos no ranking)

### Fase 3: Tabelas de eventos e resumo
- [ ] T7: `disciplina`
- [ ] T8: `artilharia`
- [ ] T9: `gols_por_minuto` + checagem de soma dos gols
- [ ] T10: `resumo_temporada`

### Checkpoint: Gold completo
- [ ] As 8 tabelas nos dados reais, `make gold` abaixo de 2 minutos, todas as checagens passando

### Fase 4: Explain plans e fechamento
- [ ] T11: `explain.py` e `make explain`
- [ ] T12: `docs/explain/README.md` e conferência dos 7 critérios de sucesso da spec

### Checkpoint: Completo
- [ ] Revisão com o Romualdo antes de fechar o módulo

## Riscos e mitigações

| Risco | Impacto | Mitigação |
|---|---|---|
| Grade clube × rodada errada em temporada com partida adiada (time sem linha numa rodada) | Alto | O minicampeonato do T2 tem partida adiada. A checagem de N × rodadas linhas roda no T3, e o checkpoint confere nos dados reais |
| Desempate da spec divergir do oficial em alguma temporada sem ajuste (confronto direto) | Médio | O checkpoint confere o campeão de todas as temporadas. Divergência de posição fora do topo e do Z4 é aceita e vai para o README |
| Plano de execução enorme na classificação (várias windows, cross join) deixando `make gold` lento | Médio | Medir no checkpoint da Fase 1. Se passar de 1 min só nela, `localCheckpoint()` antes das windows de posição |
| Nome do atleta inconsistente na fonte (mesmo atleta grafado de dois jeitos) quebrando a artilharia | Baixo | Conferir o artilheiro de algumas temporadas no checkpoint da Fase 3; alias de atleta fica fora do escopo e vai para o README |
| Plano físico mudar entre máquinas (AQE, número de cores) e os `.txt` de `docs/explain/planos/` ficarem diferentes a cada `make explain` | Baixo | O README comenta a estrutura do plano, não números de partição. `explain.py` fixa `spark.sql.shuffle.partitions` |

## Paralelização

Depois do T3, T4–T9 são independentes: cada uma mexe no próprio módulo, no próprio teste e numa entrada do registro de `gold.py`. Pelo fluxo de uma sessão por fase, elas rodam em sequência dentro das Fases 2 e 3. O T10 espera o T8.

## Open Questions

Nenhuma.
