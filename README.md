# Brasileirão em PySpark

[![CI](https://github.com/romualdobarbosa/brasileirao/actions/workflows/ci.yml/badge.svg)](https://github.com/romualdobarbosa/brasileirao/actions/workflows/ci.yml)

Fiz este projeto para o portfólio, com a ideia de mostrar o motor do Spark trabalhando (window functions, MERGE, broadcast, explain plan) além da arquitetura medallion. Peguei o histórico do Campeonato Brasileiro Série A de 2003 a 2025, são 9.165 partidas, e montei um pipeline batch em PySpark com Delta Lake que responde perguntas como quem liderava o campeonato na rodada 20 de 2009, qual foi a maior sequência sem vitória do seu time ou quem foi o artilheiro de cada ano.

Os dados vêm do dataset [Campeonato Brasileiro de Futebol](https://www.kaggle.com/datasets/adaoduque/campeonato-brasileiro-de-futebol), do Adão Duque, no Kaggle (versão 18).

## Como está organizado

O pipeline segue as três camadas de sempre, todas em Delta Lake e rodando local.

- Bronze, os CSVs do Kaggle como vieram, com metadado de origem e particionados por temporada.
- Silver, os dados limpos e tipados, com os nomes dos times padronizados ("Gremio" vira "Grêmio") e os nomes dos jogadores trocados pelo nome que o torcedor conhece ("Gabriel Barbosa" vira "Gabigol").
- Gold, oito tabelas analíticas prontas para o dashboard.

A carga é por temporada e idempotente, ou seja, reprocessar 2019 duas vezes deixa as tabelas exatamente como uma vez só. Cada camada chega nisso de um jeito. A bronze e as tabelas de gols, cartões e estatísticas da silver substituem a partição da temporada inteira (`replaceWhere`), porque essas tabelas não têm uma chave confiável. As partidas da silver têm chave (`partida_id`), então usam `MERGE` e só gravam o que mudou, de modo que uma recarga sem novidade nem cria versão nova no Delta.

## Como reproduzir

Precisa de Python 3.12, [uv](https://docs.astral.sh/uv/) e Java 17 ou mais novo.

```bash
make setup                       # instala as dependências
make download                    # baixa os CSVs do Kaggle para data/raw/
make lakehouse                   # carrega bronze e silver de todas as temporadas encerradas
make lakehouse TEMPORADA=2019    # ou só uma temporada
make gold                        # gera as tabelas gold em data/gold/
make explain                     # gera os planos físicos em docs/explain/planos/
make test                        # testes com cobertura
make lint                        # ruff
```

O dataset é público e baixa sem login. A temporada em andamento fica fora do pipeline até terminar.

## As tabelas do gold

| Tabela | O que tem |
|---|---|
| `classificacao_por_rodada` | a tabela do campeonato em cada rodada, com pontos, saldo, aproveitamento e os últimos 5 resultados |
| `desempenho_mandante_visitante` | o desempenho de cada time em casa e fora |
| `sequencias` | a maior sequência de invencibilidade, de vitórias, de jejum e de derrotas de cada time por temporada |
| `ranking_historico` | o acumulado de cada clube de 2003 a 2025, com títulos |
| `disciplina` | cartões amarelos e vermelhos por time e temporada |
| `artilharia` | os goleadores de cada temporada, com pênaltis e clubes |
| `gols_por_minuto` | em que faixa do jogo saem os gols, separando mandante e visitante |
| `resumo_temporada` | campeão, vice, rebaixados, artilheiro, média de gols e quanto o mandante vence |

A classificação é feita só com window functions, sem nenhum loop por rodada, e as sequências usam a técnica de gaps-and-islands. Em [docs/explain](docs/explain/README.md) eu comento os planos físicos de três dessas tabelas e explico por que nenhuma tabela do gold é particionada.

As punições do STJD entram por uma tabela versionada (`reference/ajustes_pontos.csv`), com o motivo e a fonte de cada uma. Com elas, a classificação final das temporadas afetadas bate com a oficial. Os pontos tirados da Portuguesa e do Flamengo em 2013, por exemplo, estão lá.

## O que a fonte não resolve

A fonte tem buracos, e é melhor saber onde estão antes de tirar conclusão de alguma tabela.

- Gols, cartões e estatísticas só existem a partir de 2014. Antes disso tem só partida e placar, então artilharia, disciplina e gols por minuto cobrem 2014 a 2025.
- 2025 não tem cartões e 2024 não tem estatísticas. A `disciplina` de 2025 usa a contagem de cartões do arquivo de estatísticas, que conta um pouco diferente (em 2017, por exemplo, são 1.811 amarelos num arquivo e 1.773 no outro).
- A fonte usa às vezes o apelido e às vezes o nome de registro (Hulk aparece como Hulk, mas o Marinho aparece como Mário Sérgio Santos Costa), e ainda junta jogadores diferentes que têm o mesmo nome. Eu corrigi isso à mão para quem já esteve entre os 10 artilheiros de alguma temporada (`reference/atletas.csv`), e foi assim que o Fred do Grêmio de 2016, que é outro jogador, parou de somar gols no Fred artilheiro. Quem está fora desse grupo continua com o nome que veio da fonte.
- A classificação desempata por pontos, vitórias, saldo e gols pró. O confronto direto e os cartões, que a CBF usa depois, ficaram de fora.
- Dois placares divergem do oficial por um gol, Athletico-PR × Criciúma em 2003 e Internacional × Criciúma em 2004. Os pontos não mudam, então deixei como veio.
- Chapecoense × Atlético-MG, da última rodada de 2016, não foi jogada por causa da queda do avião da Chapecoense. A tabela oficial conta W.O. duplo, aqui os dois times ficam com 37 jogos. Pontos e posições são os mesmos.

## Próximos passos

O mapa completo está em [CAPABILITY_MAP.md](CAPABILITY_MAP.md), e cada módulo tem a sua spec na raiz do repositório. O próximo é o dashboard público no Metabase, com Postgres numa VM gratuita da Oracle. Depois vêm o enriquecimento com a Base dos Dados e o mesmo pipeline rodando no EMR Serverless da AWS (com Terraform) e no Databricks, sem reescrever o código.
