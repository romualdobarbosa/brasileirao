# Tarefas: vitrine

Spec: [SPEC-vitrine.md](../SPEC-vitrine.md). Principal: `brasileirao-1a`.

```
T1 (a9) ┐
T2 (a9) ├─→ T5 (principal: integra) ─→ T6 (d5: app) ─→ T7 (Romualdo: publica) ─→ T9 (principal: fecha)
T3 (30) │
T4 (30) ┘
T8 (91: docs Metabase), em paralelo ───────────────────────────────────────────→ T9
```

## Regras dos agentes

- Não commitar. Não editar spec nem este plano além de marcar checkboxes da própria tarefa.
- **T1–T4 não editam** `aggregate/gold.py`, `aggregate/qualidade.py`, `serving/` nem o `pipeline.py`: a principal integra na T5. A checagem de cada tabela fica no próprio módulo, em `checar_<tabela>(df, ...) -> list[str]`.
- `a9` e `30` dividem o mesmo working tree: rodar só `pytest` e `ruff` dos próprios arquivos. Nada de `make test` nem `make gold`, e nada de escrever em `data/`.
- Conferência em dados reais: script no scratchpad, lendo `data/silver` e `data/gold`, chamando `calcular_*` sem gravar.
- Divergência com a spec ou com os dados: perguntar à principal antes de decidir.

## T1: confrontos, goleadas, campanhas e linha de corte (`brasileirao-a9`)

**Arquivos:** `aggregate/confrontos.py`, `goleadas.py`, `campanhas.py`, `linha_corte.py` e as suítes.

- [x] `calcular_confrontos(partidas)`: duas linhas por par, jogos somam 2 × 9.165
- [x] `calcular_goleadas(partidas)`
- [x] `calcular_campanhas(classificacao)`: `situacao` bate com `resumo_temporada` em todas as temporadas (conferido em dados reais)
- [x] `calcular_linha_corte(campanhas)`
- [x] Suítes unitárias verdes, ruff limpo nos arquivos da tarefa
- [x] Relatório com o top 5 de cada análise em dados reais, para conferência

## T2: técnicos e esquemas (`brasileirao-a9`)

**Arquivos:** `reference/tecnicos.csv`, `aggregate/tecnicos.py`, `aggregate/esquemas.py` e as suítes.

- [x] Seed cobre todo técnico com ≥ 30 jogos e unifica as grafias; dúvidas listadas no relatório
- [x] `calcular_passagens_tecnicos(partidas, seed)` com gaps-and-islands; sai e volta = duas passagens
- [x] `passagens_tecnicos` cobre 2014–2024 (2025 fora, ver spec); jogos das passagens somam os jogos de clube com técnico informado nesse período
- [x] `calcular_esquemas_taticos(partidas)` a partir de 2015; `uso_pct` soma 1 por temporada
- [x] Suítes unitárias verdes, ruff limpo nos arquivos da tarefa
- [x] Relatório com o top 10 de técnicos por jogos e os clubes com mais trocas num ano

## T3: viradas e gols decisivos (`brasileirao-30`)

**Arquivos:** `aggregate/viradas.py`, `aggregate/gols_decisivos.py` e as suítes.

- [x] `calcular_viradas(gols, partidas)`, com placar corrente por window function na ordem da spec
- [x] 45+2 antes do 46 e gol contra para o beneficiado cobertos por teste
- [x] O placar reconstruído na última linha de cada jogo bate com o placar da partida
- [x] `calcular_gols_decisivos(gols, partidas)`: linha também para clube sem gol na faixa; jogo sem gols conta em `jogos`
- [x] Pontos finais = pontos antes da faixa + ganhos − perdidos, por clube e temporada (conferido em dados reais)
- [x] Suítes unitárias verdes, ruff limpo nos arquivos da tarefa
- [x] Relatório com as 5 maiores viradas e o top 5 de pontos ganhos e perdidos depois dos 85'

## T4: disciplina por jogador e faltas (`brasileirao-30`)

**Arquivos:** `aggregate/cartoes_jogadores.py`, `aggregate/faltas.py` e as suítes.

- [x] `calcular_cartoes_jogadores(cartoes)`: totais batem com a silver, menos as linhas sem `atleta`
- [x] `calcular_faltas_clubes(estatisticas)`: 2014 fora, 2024 ausente, `jogos_com_dado` certo em 2019
- [x] Suítes unitárias verdes, ruff limpo nos arquivos da tarefa
- [x] Relatório com o top 5 de vermelhos por jogador e de faltas por jogo

## T5: integração (principal)

- [x] Registro `gold.TABELAS` com as 10 novas, checagens ligadas
- [x] Testes do serving como subconjunto; fixture de integração cobre as novas tabelas
- [x] `make gold` real 2×, mesmo conteúdo; critérios 1 a 3 da spec conferidos
- [x] `make lint` e `make test` verdes

## T6: exportação e app (`brasileirao-d5`)

- [x] Subcomando `exportar` no pipeline e `make exportar`, com um Parquet por tabela, via Spark, ordenado
- [x] Grupo `app` no `pyproject.toml`, `app/requirements.txt` fixado e conferência de como o Community Cloud escolhe o arquivo de dependências
- [x] As 8 páginas da spec, com período coberto em cada uma; funciona em tela estreita
- [x] Testes com `AppTest`; `make app` roda sem PySpark
- [x] Interface revisada pela `voz-romualdo`
- [x] `make lint` e `make test` verdes

## T7: publicação (Romualdo, com a principal)

- [x] Login no Streamlit Community Cloud com o GitHub; app apontando para `app/streamlit_app.py`, Python 3.12
- [x] Link abre em janela anônima, sem login; as 8 páginas funcionam

## T8: registro do Metabase (`brasileirao-91`)

- [x] `docs/serving/README.md`: arquitetura local, papéis e segurança, carga atômica, como rodar, e até onde foi (peças de deploy prontas, nuvem não publicada e por quê)
- [x] Screenshots em `docs/serving/img/`: conexão como `leitor`, as 8 tabelas e os cards do `dashboard.md`; nenhuma credencial visível
- [x] Texto revisado pela `voz-romualdo`

## T9: fechamento (principal)

- [x] `oracle.md` removido; `SPEC-serving.md`, `todo-serving.md` e `CAPABILITY_MAP.md` atualizados
- [x] README com link do app, seção da vitrine e seção do serving local, revisado pela `voz-romualdo`
- [ ] Os 9 critérios da spec conferidos; CI verde
