# Spec: vitrine

> Substitui as Fases 3 a 5 de [SPEC-serving.md](SPEC-serving.md). Status: **em execução**, pedida pelo Romualdo em 05/11/2026. Depende do contrato do gold em [SPEC-gold.md](SPEC-gold.md).

## Objetivo

Publicar o projeto de graça, num app Streamlit com as análises que um torcedor gostaria de ver, sem servidor próprio e sem custo. A finalidade é de portfólio: mostrar um caso real de uso do Spark, para somar aos certificados do Romualdo.

**Por que mudou:** o cadastro da Oracle no Brasil passou a exigir CNPJ, e pagar uma VM todo mês só pra manter um link de portfólio não compensa. O Postgres e o Metabase continuam no projeto, rodando local e documentados com screenshots.

O que precisa ficar visível:

- **O Spark faz o trabalho pesado.** Toda análise nova é uma tabela do gold, calculada no pipeline com window functions, gaps-and-islands e joins. O app só lê o resultado e faz filtros e somas simples.
- **Sem IA, sem banco e sem custo** no app publicado.
- **Honestidade sobre os dados:** cada análise diz o período que cobre.

Fora do escopo: linguagem natural para SQL (vira outro projeto, com outros dados), VM, Oracle, DuckDNS, publicar o Metabase, levar as tabelas novas para o Postgres.

### Critérios de aceite (histórias)

1. Como torcedor, abro o link sem login, escolho dois clubes e vejo o confronto direto.
2. Como torcedor, vejo a tabela de qualquer temporada em qualquer rodada e a curva de posição do meu time.
3. Como torcedor, vejo viradas, gols no fim, linha de rebaixamento, goleadas, técnicos, esquemas táticos e disciplina.
4. Como recrutador, abro o repositório e vejo que cada número do app sai de uma tabela gold calculada em Spark, com testes.
5. Como recrutador, vejo no README a camada Postgres + Metabase rodando local, com screenshots, e entendo por que ela não está na nuvem.
6. Como dev, rodo `make gold && make exportar` e o app local mostra os dados novos.

## Tech Stack

Pipeline: igual ao gold (PySpark 4.2.0, delta-spark 4.4.1, Python 3.12).

App: Streamlit (versão exata fixada e conferida no PyPI na implementação), com gráficos em Altair (já vem com o Streamlit) e `pandas`/`pyarrow` para ler Parquet. **Dependência nova aprovada:** um grupo `app` no `pyproject.toml`, instalado no CI pelo `--all-groups`. O app publicado usa só `app/requirements.txt`, sem PySpark.

Hospedagem: Streamlit Community Cloud, ligado ao repositório público. O app hiberna sem uso, e o visitante acorda com um clique: o README avisa.

## Commands

```bash
make gold        # inalterado; passa a gravar 18 tabelas
make exportar    # gold (Delta) → app/dados/<tabela>.parquet, via Spark
make app         # uv run streamlit run app/streamlit_app.py
make test        # ganha as suítes novas e os testes do app
make lint        # inalterado
```

## Contrato de dados: as 10 tabelas novas

Regras do gold valem aqui: Delta sem particionamento; só `int`, `bigint`, `double`, `string`, `date`; `aproveitamento` e percentuais entre 0 e 1 com 4 casas; `aproveitamento = (3V + E) / (3J)`, resultado de campo, salvo em `campanhas`, que repete o da classificação. Clubes pelo nome canônico da silver.

| Tabela | Grão | Colunas |
|---|---|---|
| `confrontos` | (`clube`, `adversario`), duas linhas por par, 2003–2025 | `jogos`, `vitorias`, `empates`, `derrotas`, `gols_pro`, `gols_contra`, `aproveitamento`, `maior_vitoria`, `maior_derrota`, `data_ultimo_jogo`, `ultimo_jogo` |
| `goleadas` | `partida_id`; entra quem tem diferença ≥ 4 ou 8+ gols no total | `temporada`, `rodada`, `data`, `mandante`, `visitante`, `placar_mandante`, `placar_visitante`, `diferenca`, `total_gols`, `arena` |
| `campanhas` | (`temporada`, `clube`), tabela final | `posicao`, `pontos`, `ajuste_pontos`, `jogos`, `vitorias`, `empates`, `derrotas`, `gols_pro`, `gols_contra`, `saldo`, `aproveitamento`, `situacao` (`campeao`, `vice`, `rebaixado`, `permaneceu`) |
| `linha_corte` | `temporada` | `times`, `rebaixados`, `ultimo_salvo`, `pontos_ultimo_salvo`, `aproveitamento_ultimo_salvo`, `primeiro_rebaixado`, `pontos_primeiro_rebaixado`, `aproveitamento_primeiro_rebaixado`, `pontos_campeao`, `pontos_vice` |
| `passagens_tecnicos` | (`temporada`, `clube`, `ordem`), uma linha por passagem, 2014–2024 | `tecnico`, `data_inicio`, `data_fim`, `jogos`, `vitorias`, `empates`, `derrotas`, `gols_pro`, `gols_contra`, `pontos`, `aproveitamento` |
| `esquemas_taticos` | (`temporada`, `esquema`), 2015–2025 | `jogos`, `uso_pct`, `vitorias`, `empates`, `derrotas`, `aproveitamento` |
| `viradas` | (`partida_id`, `clube`), uma linha por virada, 2014–2025; `clube` é quem virou | `temporada`, `rodada`, `data`, `adversario`, `mando`, `gols_pro`, `gols_contra`, `maior_desvantagem` |
| `gols_decisivos` | (`temporada`, `clube`), 2014–2025 | `jogos`, `gols_85`, `gols_acrescimo`, `pontos_ganhos_85`, `pontos_perdidos_85`, `pontos_ganhos_acrescimo`, `pontos_perdidos_acrescimo` |
| `cartoes_jogadores` | (`temporada`, `atleta`, `clube`), 2014–2024 | `amarelos`, `vermelhos` |
| `faltas_clubes` | (`temporada`, `clube`), 2015–2023 e 2025 | `jogos_com_dado`, `faltas`, `faltas_por_jogo` |

### Definições

- **`maior_vitoria` / `maior_derrota`:** texto `"5 × 0 (2019)"`, com o placar na visão do `clube`. Maior saldo; no empate, mais gols marcados e depois o jogo mais recente. Nulo se não houver.
- **`ultimo_jogo`:** texto `"Flamengo 2 × 1 Vasco"`, sempre mandante primeiro.
- **`situacao`:** posição final, com `rebaixados_por_temporada()` (2 em 2003, 4 depois). Precisa bater com `campeao`, `vice` e `rebaixados` de `resumo_temporada`.
- **`linha_corte`:** sai de `campanhas`. As temporadas têm de 38 a 46 rodadas: por isso o aproveitamento, que é o que permite comparar.
- **Passagem de técnico:** ilhas de jogos seguidos do mesmo técnico no clube, por data (`data`, `hora`, `partida_id`), dentro da temporada. Quem sai e volta gera duas passagens. Jogo com técnico nulo fica de fora e não interrompe a passagem. Trocas no ano = passagens − 1. Interino conta.
- **Virada:** o clube esteve atrás no placar em algum momento e venceu. `maior_desvantagem` é a maior diferença contra ele no jogo.
- **Ordem dos gols:** `minuto`, depois `coalesce(acrescimo, 0)`. Assim 45+2 vem antes do 46.
- **`gols_85`:** gols com `minuto >= 85`. **`gols_acrescimo`:** `minuto = 90` com `acrescimo` preenchido. Gol contra conta para o clube beneficiado, como na fonte.
- **Pontos ganhos e perdidos:** para cada jogo, pontos com o placar final menos pontos com o placar antes da faixa. Positivo soma em `pontos_ganhos_*`; negativo soma, em módulo, em `pontos_perdidos_*`.
- **`uso_pct`:** `jogos` do esquema sobre os jogos de clube da temporada com esquema informado. Um jogo conta duas vezes, uma para cada clube.

### Fatos da silver que moldam estas tabelas (conferidos em 05/11/2026)

- **Gols:** só 2014–2025; nessas temporadas a contagem bate com o placar em 100% das partidas. Silver/gols só tem linhas a partir de 2014.
- **Empate de minuto:** um único caso de gols de clubes diferentes no mesmo minuto (partida 5486, 2016, aos 89'). Não muda virada: o Atlético-MG já vencia por 4 × 1.
- **Acréscimo:** cerca de 85% dos gols no minuto 90 têm `acrescimo`; os outros podem ou não ter sido nos acréscimos. Por isso `gols_acrescimo` é um piso, e o app diz isso.
- **Técnicos:** 2014–2024 (na visão de clube, 5 nulos em 2014 e 1 em cada um de 2015, 2016 e 2017). **2025 fica fora porque a coluna está errada na fonte:** em 19 dos 20 clubes, um único nome cobre 36 ou 37 jogos, contra média de 23 a 29 nas outras temporadas. O nome parece ser o técnico na hora da coleta: o Cruzeiro aparece com Tite em 36 jogos, mas ele só foi anunciado em 16/12/2025, depois de o Jardim sair. Os nomes são sujos: 310 grafias. Há variantes da mesma pessoa (`F. Diniz`, `F. Diniz Silva`, `Fernando Diniz Silva`; `R. Ceni`, `Rogerio Ceni`; `Renato Gaúcho`, `Renato Portaluppi`), nomes de registro (`L. Venker de Menezes` é Mano Menezes; `A. Stival` é Cuca) e armadilhas (`Renato Manuel Alves Paiva` é Renato Paiva, outra pessoa que não o Renato Gaúcho).
- **Esquemas:** 2014 tem só 16 partidas com formação, então a tabela começa em 2015. Faltam poucas em 2016, 2017 e 2024. São 22 esquemas, alguns raríssimos (`3-3-3-1` tem 1 jogo; `4-2-4-0` vem da fonte e fica como está).
- **Cartões:** `silver/cartoes` cobre 2014–2024. Há 6 linhas sem `atleta`, que ficam de fora de `cartoes_jogadores`.
- **Faltas:** `silver/estatisticas` tem 2015–2023 e 2025 (2014 só 32 linhas, por isso sai; 2024 vazia; 2019 com 714 de 760). Daí `jogos_com_dado`.
- **Arrecadação** só existe em 2025, e não há público na fonte. Nenhuma análise usa esses campos.

### Seed de técnicos

`src/brasileirao/reference/tecnicos.csv`, no molde do `atletas.csv`: `nome_fonte,clube,temporada,nome_popular`, com clube e temporada opcionais. Aplicado no gold, sem mudar a silver. Unifica as grafias da mesma pessoa e dá o nome conhecido a todo técnico com 30 jogos ou mais. Na dúvida se é a mesma pessoa, confere clube e época; se a dúvida continuar, deixa como está e registra no relatório. Sem seed, vale o nome da fonte.

## O app

`app/streamlit_app.py`, multipágina, lendo só `app/dados/*.parquet` (versionados no git, poucos MB). **Não importa o pacote `brasileirao`**, para não puxar o PySpark.

| Página | Conteúdo | Tabelas |
|---|---|---|
| Temporada | tabela por rodada, curva de posição, campeão/vice/rebaixados, artilharia, mando, minuto dos gols, sequências | `classificacao_por_rodada`, `resumo_temporada`, `artilharia`, `desempenho_mandante_visitante`, `gols_por_minuto`, `sequencias` |
| Histórico | ranking desde 2003, títulos, força do mando e média de gols por ano | `ranking_historico`, `resumo_temporada` |
| Confronto direto | dois clubes à escolha; freguês e carrasco de cada um (mínimo de 10 jogos) | `confrontos` |
| Viradas e fim de jogo | quem mais vira, quem mais toma virada, maiores viradas, gols depois dos 85', pontos ganhos e perdidos | `viradas`, `gols_decisivos` |
| Rebaixamento e campanhas | linha de corte por ano, quem caiu com mais pontos, vice com mais pontos, melhor e pior campanha | `linha_corte`, `campanhas` |
| Goleadas | maiores goleadas e jogos com mais gols | `goleadas` |
| Técnicos e esquemas | dança dos técnicos, aproveitamento por técnico, uso e rendimento dos esquemas (com mínimo de jogos) | `passagens_tecnicos`, `esquemas_taticos` |
| Disciplina | jogadores com mais vermelhos, times mais faltosos | `cartoes_jogadores`, `faltas_clubes` |

Toda página diz o período que cobre. Funciona em tela de celular. O rodapé aponta para o repositório e diz que os números saem do pipeline em Spark. O texto da interface passa pela `voz-romualdo`.

## Metabase e Postgres

Ficam no repositório, rodando local, com as 8 tabelas originais. `docs/serving/README.md` conta o que foi construído, como rodar e até onde foi, com screenshots em `docs/serving/img/`. As peças de deploy (Caddy, `carga-remota`, `deploy`, `backup-metabase`) ficam, descritas como prontas e nunca usadas em produção. O `docs/serving/oracle.md` sai, porque ensina um caminho que hoje não funciona para pessoa física.

O registro `gold.TABELAS` passa a ter 18 tabelas, e `serving/tabelas.py` continua com 8: os testes passam a exigir que as do serving sejam um subconjunto do registro, com as mesmas colunas.

## Code Style e Testes

Igual ao gold: uma função `calcular_*` pura por tabela, em `src/brasileirao/aggregate/`, sem I/O; `Window` montada na chamada; reaproveitar `jogos_por_clube`. Uma suíte por tabela em `tests/unit/aggregate/`, com DataFrames em memória e o minicampeonato do `conftest.py` quando servir.

Testes do app com `streamlit.testing.v1.AppTest`: toda página abre sem exceção e os filtros principais funcionam, além de a tabela final de 2025 no Parquet bater com a da gold. Cobertura continua ≥ 80% em `src/brasileirao/`.

## Boundaries

- **Sempre:** `make lint` e `make test` antes de commit; texto público passa pela `voz-romualdo`; o período coberto aparece junto do número.
- **Perguntar antes:** mudar schema das 8 tabelas originais; mudar a silver; dependência além das citadas aqui.
- **Nunca:** chamada de IA ou banco no app publicado; lógica analítica pesada em pandas que devia estar em Spark; inventar nome de técnico ou jogador; versionar `.env`, senha ou print que mostre credencial.

## Success Criteria

1. `make gold` grava 18 tabelas; rodar duas vezes dá o mesmo conteúdo.
2. Os totais batem com a silver: gols de `viradas` e `gols_decisivos` coerentes com o placar; jogos de `confrontos` somam 2 × partidas; `campanhas` bate com `resumo_temporada`.
3. Conferência à mão de fatos conhecidos, registrada no relatório: ao menos uma virada, uma goleada e um confronto checados contra fonte externa.
4. `make exportar` gera os 18 Parquet; o app roda local só com eles, sem PySpark.
5. Link público no Streamlit Community Cloud, aberto em janela anônima, sem login.
6. Todas as 8 páginas funcionam no link público.
7. `docs/serving/README.md` com screenshots do Metabase local; nenhuma credencial visível.
8. README com link do app, seção da vitrine e seção do serving local, revisado pela `voz-romualdo`.
9. `make lint` limpo, `make test` verde, CI verde.

## Decisões

- **Streamlit Community Cloud** em vez de VM: grátis, sem CNPJ, sem manutenção.
- **Parquet versionado no git** em vez de banco: o dado só muda quando a temporada acaba.
- **Sem IA no app:** evita custo e fica pra um projeto separado.
- **Tabelas novas só no Delta e no Parquet**, sem Postgres: o Metabase é registro do que foi construído, não caminho de produção.
- **Seed de técnicos aplicado no gold**, para não mexer na silver.

## Open Questions

Nenhuma.
