# Planos físicos do gold

Aqui estão os planos físicos de três tabelas do gold, gerados com o `explain` do Spark, e a minha leitura de cada um. O plano mostra o que o Spark faz com a consulta escrita, quantas vezes ele redistribui os dados, onde ele ordena e quando ele manda uma tabela inteira para todos os executores. Escolhi uma tabela de cada assunto:

- `classificacao_por_rodada`, que é feita só com window functions;
- `sequencias`, o gaps-and-islands das séries de invencibilidade e jejum;
- `disciplina`, que tem o join com broadcast mais claro do gold.

No fim tem a explicação de por que nenhuma tabela do gold é particionada.

## Como os arquivos são gerados

```bash
make explain
```

O comando calcula as três tabelas com as mesmas funções do `make gold` (o registro `TABELAS` de `aggregate/gold.py`), executa cada consulta e grava a saída de `explain("formatted")` em `planos/<tabela>.txt`. A raiz dos dados aparece como `$BRASILEIRAO_DATA_ROOT`, para o arquivo não carregar o caminho da minha máquina.

Executar antes do `explain` faz diferença. Com o AQE (Adaptive Query Execution) ligado, que é o padrão do Spark, um `explain` sem ação mostra só o plano que o Spark pretende seguir, marcado com `isFinalPlan=false`. Depois que a consulta roda, o mesmo `explain` mostra o plano final e o inicial, um embaixo do outro. Por isso todo arquivo começa assim:

```text
== Physical Plan ==
AdaptiveSparkPlan (119)
+- == Final Plan ==
```

e, mais abaixo, tem um `+- == Initial Plan ==`. Os números entre parênteses são ids de operador e mudam de uma execução para outra, assim como os sufixos `#123` das colunas e o número de partições. Neste texto eu falo da forma do plano, que é o que se mantém.

Os trechos abaixo são do plano final, a não ser quando eu digo que é do inicial.

## Classificação: um exchange para três windows

A classificação usa três windows com partições diferentes.

- `max("rodada")` por `temporada`, para achar a última rodada (é onde entram os ajustes do STJD);
- as somas acumuladas por (`temporada`, `clube`), ordenadas por `rodada`;
- o `row_number()` da posição por (`temporada`, `rodada`).

Cada uma poderia pedir um shuffle próprio, e o plano mostra um só:

```text
:- * Project (47)
:  +- Window (46)
:     +- * Sort (45)
:        +- Window (44)
:           +- * Sort (43)
:              +- AQEShuffleRead (42), coalesced
:                 +- ShuffleQueryStage (41), Statistics(sizeInBytes=1940.7 KiB, rowCount=1.83E+4)
:                    +- Exchange (40)
```

O `Exchange` distribui as linhas por `temporada`:

```text
(40) Exchange
Arguments: hashpartitioning(temporada#1, 4), ENSURE_REQUIREMENTS, [plan_id=1714]
```

Cada window exige que as linhas da mesma partição dela estejam juntas, e quem está junto por `temporada` também está junto por (`temporada`, `clube`) e por (`temporada`, `rodada`). Então o Spark faz o shuffle uma vez, pela chave que serve para as três, e depois só reordena. A window da última rodada (44) vem logo depois do primeiro `Sort`, a das somas acumuladas (46) depois do segundo, e a da posição está no topo do plano, depois do join com os ajustes, de novo sem `Exchange`:

```text
+- * Project (63)
   +- Window (62)
      +- * Sort (61)
         +- * Project (60)
            +- * Project (59)
               +- * BroadcastHashJoin LeftOuter BuildRight (58)
```

O `Sort (61)` ordena por `temporada`, `rodada` e pelos critérios de desempate:

```text
(61) Sort [codegen id : 17]
Arguments: [temporada#1 ASC NULLS FIRST, rodada#66 ASC NULLS FIRST, pontos#228 DESC NULLS LAST, vitorias#193 DESC NULLS LAST, saldo#229 DESC NULLS LAST, gols_pro#196 DESC NULLS LAST, clube#23 ASC NULLS FIRST], false, 0
```

O join com os ajustes (58) não estraga a distribuição porque é broadcast. A soma das punições por clube (13 linhas, porque a Ponte Preta de 2003 tem duas) vai inteira para cada executor, e o lado grande não sai do lugar.

No mesmo plano, o `ultimos_5` (os cinco últimos resultados até a rodada) e a ordem dos jogos usam windows com a mesma partição e a mesma ordenação, e o Spark junta as duas num operador só:

```text
(30) Window
Arguments: [collect_list(resultado#189, 0, 0, true) windowspecdefinition(temporada#146, clube#163, rodada#147 ASC NULLS FIRST, data#148 ASC NULLS FIRST, hora#149 ASC NULLS FIRST, partida_id#145 ASC NULLS FIRST, specifiedwindowframe(RowFrame, -4, currentrow$())) AS _we0#116, row_number() windowspecdefinition(temporada#146, clube#163, ...
```

## Sequências: duas windows sobre a mesma partição

Nas sequências, cada jogo do clube recebe sua posição na temporada (`ordem`, um `row_number()` por data) e o total de jogos do clube (`_total`, um `count` sem ordenação), as duas por (`temporada`, `clube`). No plano elas aparecem empilhadas, em cima de um único `Sort` e de um único `Exchange`:

```text
:- Window (41)
:  +- * Project (40)
:     +- Window (39)
:        +- * Sort (38)
:           +- AQEShuffleRead (37), coalesced
:              +- ShuffleQueryStage (36), Statistics(sizeInBytes=1510.9 KiB, rowCount=1.83E+4)
:                 +- Exchange (35)
```

```text
(35) Exchange
Arguments: hashpartitioning(temporada#1430, clube#1447, 4), ENSURE_REQUIREMENTS, [plan_id=2188]

(39) Window
Arguments: [row_number() windowspecdefinition(temporada#1430, clube#1447, data#1432 ASC NULLS FIRST, hora#1433 ASC NULLS FIRST, partida_id#1429 ASC NULLS FIRST, ...) AS ordem#1474], [temporada#1430, clube#1447], ...

(41) Window
Arguments: [count(1) windowspecdefinition(temporada#1430, clube#1447, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS _total#1475L], [temporada#1430, clube#1447]
```

A window (41) não tem `Sort` próprio. Ela não tem ordenação, só precisa das linhas do clube juntas, e elas já estão.

O gaps-and-islands vem em seguida. O segundo `row_number()` é por (`temporada`, `clube`, `tipo`), e a diferença entre os dois define a ilha. Essa terceira window também não pede shuffle, porque as linhas de um (`temporada`, `clube`) já estão na mesma partição e o `tipo` só subdivide o grupo. O agrupamento por ilha também fica na mesma etapa, com os dois `HashAggregate` (parcial e final) colados, sem `Exchange` entre eles:

```text
+- * HashAggregate (52)
   +- * HashAggregate (51)
      +- * Project (50)
         +- Window (49)
            +- * Sort (48)
               +- * Project (47)
                  +- * BroadcastHashJoin Inner BuildRight (46)
```

O join (46) é o que multiplica cada jogo pelos tipos de série em que ele conta. A tabela de tipos tem 6 linhas e vai em broadcast, então também não move o lado grande.

Para escolher a maior ilha de cada tipo eu uso `row_number() = 1`, e o Spark troca isso por um `WindowGroupLimit` antes da window, que descarta cedo o que não vai ser o primeiro:

```text
+- Window (55)
   +- WindowGroupLimit (54)
      +- * Sort (53)
```

## Disciplina: o broadcast

A `disciplina` decide, por temporada, se os cartões vêm de `silver/cartoes` ou das somas de `silver/estatisticas`. O resultado dessa decisão tem uma linha por temporada, e no código ele vai com `F.broadcast(...)` para o join com os jogos de cada clube. No plano, esse é o `BroadcastHashJoin Inner`, com o lado pequeno num `BroadcastQueryStage`:

```text
:  +- * Project (46)
:     +- * BroadcastHashJoin Inner BuildRight (45)
:        :- * HashAggregate (12)
:        :  +- AQEShuffleRead (11), coalesced
:        :     +- ShuffleQueryStage (10), Statistics(sizeInBytes=40.8 KiB, rowCount=940)
:        :        +- Exchange (9)
:        +- BroadcastQueryStage (44), Statistics(sizeInBytes=32.0 MiB, rowCount=12)
:           +- BroadcastExchange (43)
```

```text
(45) BroadcastHashJoin [codegen id : 18]
Left keys [1]: [temporada#1]
Right keys [1]: [temporada#1821]
Join type: Inner
Join condition: None
```

`BuildRight` quer dizer que o lado direito (as 12 temporadas com alguma fonte de cartão) vira uma tabela hash em memória, mandada inteira para cada executor. O lado esquerdo, os jogos por clube, é lido onde está. Sem o broadcast seria um `SortMergeJoin`, com shuffle e ordenação dos dois lados para juntar 12 linhas.

O mesmo plano tem um broadcast que eu não pedi. O `left_anti` que tira das estatísticas as temporadas que já têm cartões não leva dica no código, e mesmo assim saiu como `BroadcastHashJoin LeftAnti BuildRight`. O Spark estimou que a lista de temporadas era pequena (fica abaixo do `spark.sql.autoBroadcastJoinThreshold`) e escolheu sozinho. Como essa contagem aparece duas vezes na consulta, o segundo uso nem refaz o broadcast:

```text
(61) ReusedExchange [Reuses operator id: 29]
```

E tem um join que não virou broadcast, o `full` do fim, que junta jogos e cartões:

```text
+- * SortMergeJoin FullOuter (74)
```

O broadcast hash join do Spark não faz full outer, então esse fica com shuffle e sort dos dois lados, mesmo com 240 linhas de cada lado.

## O que o AQE mudou

Comparei o plano final com o inicial de cada arquivo.

No plano inicial o `Exchange` é só uma intenção. No final ele aparece dentro de um `ShuffleQueryStage` com as estatísticas reais daquele ponto, como o `rowCount=1.83E+4` das 18.332 linhas da classificação. O AQE roda a consulta em etapas, para em cada shuffle, olha o que saiu e replaneja o resto.

Depois de cada shuffle vem um `AQEShuffleRead (...), coalesced`. Com tabelas deste tamanho, muitas partições do shuffle saem quase vazias, e o AQE agrupa várias numa tarefa só. Isso aparece nos três planos.

Nas sequências, um join trocou de estratégia. O join final da grade (clube × tipo) com as maiores séries estava planejado como `SortMergeJoin`, com um `Sort` de cada lado:

```text
+- == Initial Plan ==
   Project (104)
   +- SortMergeJoin LeftOuter (103)
      :- Sort (77)
      :  +- Exchange (76)
```

Quando o lado direito terminou e o AQE viu que ele tinha 1.880 linhas, o join virou broadcast:

```text
+- * BroadcastHashJoin LeftOuter BuildRight (63)
   :- AQEShuffleRead (25), local
   ...
   +- BroadcastQueryStage (62), Statistics(sizeInBytes=2.1 MiB, rowCount=1.88E+3)
      +- BroadcastExchange (61)
         +- AQEShuffleRead (60), local
```

Os dois `Exchange` continuam no plano, porque o shuffle já tinha acontecido quando o AQE mediu. O ganho é que os dois `Sort` do `SortMergeJoin` sumiram, e a leitura `local` pega os blocos do shuffle direto de onde eles foram escritos, sem redistribuir de novo. Na classificação e na disciplina, os joins ficaram com a estratégia do plano inicial.

## Por que o gold não é particionado

A silver é particionada por `temporada` (o `make lakehouse` processa uma temporada por vez, e a partição deixa ler e regravar só aquela). No gold eu não particionei nada, e o motivo é o tamanho. Estes são os números depois do `make gold` nos dados reais, contando a versão atual de cada tabela Delta (o overwrite deixa as versões antigas no disco até um `VACUUM`):

| Tabela | Linhas | Arquivos | Tamanho |
|---|---:|---:|---:|
| `classificacao_por_rodada` | 18.332 | 1 | 164 KB |
| `desempenho_mandante_visitante` | 940 | 1 | 11 KB |
| `sequencias` | 1.880 | 1 | 20 KB |
| `ranking_historico` | 46 | 1 | 7 KB |
| `disciplina` | 240 | 1 | 4 KB |
| `artilharia` | 3.606 | 1 | 32 KB |
| `gols_por_minuto` | 144 | 1 | 3 KB |
| `resumo_temporada` | 23 | 1 | 6 KB |

O gold inteiro tem uns 248 KB. Particionar a maior tabela por temporada trocaria um arquivo de 164 KB por 23 arquivos de uns 7 KB, cada um com o próprio rodapé de Parquet e uma entrada a mais no log do Delta. Ler a tabela inteira ficaria mais lento, não mais rápido, e o `ranking_historico` (46 linhas, uma por clube) nem tem temporada para particionar.

O ganho que a partição daria também não aparece aqui. Não tem leitura filtrada por temporada que precise pular arquivos, porque quem vai ler o gold é o `serving`, que carrega as tabelas inteiras no Postgres, e o dashboard consulta o Postgres. E a escrita é sempre um overwrite completo, já que o ranking e as sequências dependem da temporada toda (ou de todas).
