# Delta na prática

Até aqui, este projeto usava Delta só para gravar e fazer `MERGE`, sem constraints, sem manutenção e sem ler versão antiga. Este documento registra o que cada uma dessas operações fez neste projeto, com constraints, histórico, `OPTIMIZE`, Z-Order, `VACUUM` e time travel. Toda saída foi copiada de uma execução real, no lake real ou numa cópia dele, e o texto diz qual.

Uma ideia explica quase tudo aqui. Uma tabela Delta é uma pasta com arquivos Parquet e um diário, o `_delta_log`. Escrever nunca altera um Parquet existente, o Delta cria arquivos novos e anota no diário quais passam a valer. Cada anotação é uma versão. Os arquivos antigos continuam na pasta, e é por isso que dá para voltar no tempo, e também por isso que a pasta só cresce até alguém limpar.

## Comandos

```bash
make lakehouse                                     # silver, já aplicando as constraints
make gold                                          # gold, já aplicando as constraints
make manutencao                                    # OPTIMIZE, Z-Order e VACUUM, com o antes e depois
make historico TABELA=silver/partidas              # lista as versões de uma tabela
make historico TABELA=gold/goleadas COMPARAR="18 19"   # e compara duas delas
```

O código está em `src/brasileirao/lake/`.

## Constraints: a regra que fica na tabela

Uma constraint `CHECK` é uma regra gravada na própria tabela. Quem tentar escrever uma linha que a desobedeça leva um erro, seja o pipeline, um notebook ou alguém no SQL. As checagens de `qualidade.py` só valem para quem passa pelo pipeline, a constraint vale para todo mundo.

As duas coisas convivem:

| | `qualidade.py` | Constraint |
|---|---|---|
| Quando roda | antes de gravar, no pipeline | em toda escrita, venha de onde vier |
| O que enxerga | a tabela inteira, joins, agregações | uma linha por vez |
| Exemplo | posição repetida em (temporada, rodada) | `placar_mandante >= 0` |

Nenhuma checagem saiu de `qualidade.py`. São 93 constraints, 23 na silver e 70 no gold, definidas em `src/brasileirao/lake/constraints.py`. Em `silver/cartoes`, por exemplo, como o Delta guarda:

```text
rodada_valida = rodada BETWEEN 1 AND 46
num_camisa_valido = num_camisa IS NULL OR num_camisa >= 0
cartao_valido = cartao IS NOT NULL AND cartao IN ( 'Amarelo' , 'Vermelho' )
partida_id_nao_nulo = partida_id IS NOT NULL
acrescimo_valido = acrescimo IS NULL OR acrescimo >= 0
minuto_valido = minuto IS NULL OR minuto >= 0
temporada_valida = temporada >= 2003
```

A maioria repete uma regra que já existia no código, como tipo de cartão, critério de goleada, pontos em ordem na linha de corte e virada de verdade. Algumas são novas, porque nada conferia aquilo linha a linha, como `rodada_valida` (de 1 a 46, o máximo com 24 times), `jogos = vitorias + empates + derrotas` e `saldo = gols_pro - gols_contra`. Antes de aplicar, contei as violações de cada regra no dado real, e não havia nenhuma. Só que o dado real não bastou. Rodando no minicampeonato dos testes, duas regras que eu tinha escrito em `gold/sequencias` reprovaram linhas corretas, porque clube sem série fica com 0 jogos e datas nulas. As regras estavam erradas, e foram elas que corrigi.

### Como entra no pipeline

`make lakehouse` e `make gold` chamam `aplicar_camada` depois de gravar, porque na primeira execução a tabela ainda não existe. Da segunda em diante, as regras já estão na tabela e valem durante a própria gravação. A função lê as constraints que a tabela tem e só adiciona o que falta. Na primeira execução, foram 23 na silver e 70 no gold. Na segunda, nenhuma:

```text
10_lakehouse_1: 23 constraints aplicadas
11_gold_1: 70 constraints aplicadas
13_lakehouse_2: 0 constraints aplicadas
14_gold_2: 0 constraints aplicadas
```

Não dá para fazer `DROP CONSTRAINT IF EXISTS` seguido de `ADD` a cada execução, porque cada um grava uma versão, mesmo quando não há nada para apagar. Num teste, o `DROP IF EXISTS` de uma constraint inexistente criou uma versão nova. Esse caminho quebraria a promessa do `MERGE`, de que rodar de novo com o mesmo dado não cria versão.

### O que acontece numa escrita inválida

Numa cópia do lake real, tentei escrever dado errado por quatro caminhos. Todos foram recusados. Nos três primeiros, medi também a versão e o número de linhas, que não mudaram:

```text
[recusou] MERGE com placar -1 em silver/partidas: Py4JJavaError: : org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint placar_mandante_valido ((placar_mandante IS NOT NULL) AND (placar_mandante >= 0)) violated by row with v | - 
versão 29 -> 29, linhas 9165 -> 9165
[recusou] replaceWhere com cartão Azul em silver/cartoes: AnalysisException: [DELTA_REPLACE_WHERE_MISMATCH] Written data does not conform to partial table overwrite condition or constraint 'temporada = 2019'. | [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint cartao_valido ((cartao IS NOT NUL
versão 125 -> 125, linhas 20953 -> 20953
[recusou] overwrite do gold com linha fora do critério: Py4JJavaError: : org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint criterio_de_goleada ((diferenca >= 4) OR (total_gols >= 8)) violated by row with values: | - diferenca 
versão 21 -> 21, linhas 382 -> 382
[recusou] UPDATE direto: rodada = 0: Py4JJavaError: : org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint rodada_valida between(rodada, 1, 46) violated by row with values: | - rodada : 0
```

O teste `tests/integration/test_constraints.py` cobre esses caminhos e também `append`, `INSERT` em SQL e `overwriteSchema`.

Na mensagem do `replaceWhere`, o Delta fala primeiro em "partial table overwrite condition or constraint". É o mesmo mecanismo, o `replaceWhere` também é checado como uma regra, e o nome da constraint violada vem na segunda parte.

### O que surpreende

- `NULL` reprova. No SQL padrão, um `CHECK` que dá `NULL` passa. No Delta, recusa. `gols >= 0` barrou uma linha com `gols` nulo no experimento. Por isso toda coluna opcional tem `IS NULL OR`, como em `minuto IS NULL OR minuto >= 0`.
- O dado que já está lá também é conferido. Uma constraint nova que o dado existente viola não entra:

  ```text
  [recusou] ADD CONSTRAINT goleada_sempre CHECK (placar_mandante > 3): AnalysisException: [DELTA_NEW_CHECK_CONSTRAINT_VIOLATION] 8533 rows in delta.`file:.../silver/partidas` violate the new CHECK constraint (placar
  ```

- O protocolo sobe e não desce. A tabela nasce pedindo escritor na versão 2. A primeira constraint sobe para 3, para que um programa antigo, que não sabe o que é `CHECK`, não consiga escrever ignorando a regra. Apagar a constraint não devolve o 2, como mostrou o experimento. Leitura segue na versão 1:

  ```text
  silver/cartoes (1, 3) ['appendOnly', 'checkConstraints', 'invariants']
  ```

- O overwrite mantém as constraints. Elas ficam nos metadados da tabela, não nos arquivos, então o `make gold`, que troca tudo, não as leva embora. Nem `overwriteSchema=true`, que o projeto não usa, as remove. Num experimento, tirar a coluna que a regra usa foi recusado, porque ela virou nula.
- O texto volta diferente. `cartao IN ('Amarelo', 'Vermelho')` é guardado como `cartao IN ( 'Amarelo' , 'Vermelho' )`. Comparado ao pé da letra, a função acharia que a regra mudou e a recriaria a cada execução, por isso a comparação ignora espaços.
- Caminho relativo não funciona no SQL. `` ALTER TABLE delta.`./data/...` `` falha com `TABLE_OR_VIEW_NOT_FOUND`, e o caminho padrão do projeto é `./data`. A função converte para caminho absoluto.

### O limite

A recusa vale tabela por tabela. O `make gold` grava as 18 em sequência, e cada uma tem a sua transação. Se a décima fosse recusada, as nove anteriores já estariam gravadas. Quem garante "nada gravado se algo estiver errado" continua sendo `qualidade.py`, que roda antes da primeira escrita. A constraint é a última linha de defesa, para o caso em que a checagem deixou passar ou alguém escreveu por fora do pipeline.

## Histórico: o diário da tabela

`make historico` mostra as versões, da mais nova para a mais antiga. Esta é `gold/goleadas` depois da manutenção, cortada nas 12 mais recentes:

```text
gold/goleadas: 22 versão(ões) no histórico
versão  quando               operação          métricas
    21  2026-11-06 17:41:43  VACUUM END        numDeletedFiles=0
    20  2026-11-06 17:41:42  VACUUM START      numFilesToDelete=0
    19  2026-11-06 17:41:42  OPTIMIZE          numAddedFiles=1, numRemovedFiles=12
    18  2026-11-06 17:40:03  WRITE             numFiles=12, numOutputRows=382, numRemovedFiles=12
    17  2026-11-06 17:35:46  ADD CONSTRAINT
    16  2026-11-06 17:35:46  ADD CONSTRAINT
    15  2026-11-06 17:35:45  ADD CONSTRAINT
    14  2026-11-06 17:35:45  ADD CONSTRAINT
    13  2026-11-06 17:35:45  WRITE             numFiles=12, numOutputRows=382, numRemovedFiles=12
    12  2026-11-06 16:48:08  WRITE             numFiles=12, numOutputRows=382, numRemovedFiles=12
    11  2026-11-06 15:42:51  WRITE             numFiles=12, numOutputRows=382, numRemovedFiles=12
    10  2026-11-06 13:53:50  WRITE             numFiles=12, numOutputRows=382, numRemovedFiles=12
```

Dá para ler a história de baixo para cima. O `make gold` regravou a tabela (13), as quatro constraints entraram (14 a 17), o `make gold` seguinte não adicionou nenhuma (18), o `OPTIMIZE` juntou 12 arquivos em 1 (19), e o `VACUUM` deixou duas anotações, uma ao começar e outra ao terminar (20 e 21). Cada comando de manutenção também é uma versão.

## OPTIMIZE: juntar arquivos pequenos

Uma tabela com muitos arquivos pequenos é mais lenta de abrir, porque cada arquivo tem custo fixo. O `OPTIMIZE` junta os pequenos em arquivos maiores e grava uma versão nova. O conteúdo não muda. A `make manutencao` imprime o antes e depois de cada tabela:

```text
tabela                                arquivos atuais         KB atuais  arquivos na pasta     MB na pasta     versão  z-order
silver/partidas                               23 → 23     290.6 → 290.6            23 → 23     0.28 → 0.28    27 → 29  -
silver/gols                                   12 → 12     152.8 → 152.8           126 → 97     1.58 → 1.21  131 → 133  -
silver/cartoes                                11 → 11     245.4 → 245.4           116 → 89     2.54 → 1.94  123 → 125  -
silver/estatisticas                           11 → 11     147.5 → 147.5           116 → 89     1.51 → 1.17  122 → 124  -
gold/classificacao_por_rodada                   1 → 1     163.9 → 163.9            27 → 18     4.32 → 2.88    32 → 35  temporada, rodada
gold/desempenho_mandante_visitante              1 → 1       11.3 → 11.3            25 → 17     0.28 → 0.19    28 → 30  -
gold/sequencias                                 1 → 1       20.4 → 20.4            25 → 18     0.50 → 0.36    28 → 31  temporada
gold/ranking_historico                          1 → 1         6.7 → 6.7            25 → 17     0.16 → 0.11    28 → 30  -
gold/disciplina                                 1 → 1         4.4 → 4.4            23 → 17     0.10 → 0.07    25 → 27  -
gold/artilharia                                 1 → 1       31.0 → 31.0            23 → 18     0.70 → 0.55    26 → 29  temporada
gold/gols_por_minuto                            1 → 1         3.3 → 3.3            23 → 17     0.07 → 0.06    27 → 29  -
gold/resumo_temporada                           1 → 1         5.9 → 5.9            23 → 17     0.13 → 0.10    25 → 27  -
gold/confrontos                                 1 → 1       35.4 → 35.4            15 → 16     0.52 → 0.55    17 → 20  clube, adversario
gold/goleadas                                  12 → 1       52.8 → 10.9          180 → 181     0.77 → 0.78    18 → 21  -
gold/campanhas                                  1 → 1       10.3 → 10.3            15 → 15     0.15 → 0.15    19 → 21  -
gold/linha_corte                                1 → 1         4.5 → 4.5            15 → 15     0.07 → 0.07    17 → 19  -
gold/passagens_tecnicos                         1 → 1       23.1 → 23.1            15 → 15     0.34 → 0.34    19 → 21  -
gold/esquemas_taticos                           1 → 1         5.9 → 5.9            15 → 15     0.09 → 0.09    18 → 20  -
gold/viradas                                    1 → 1         8.7 → 8.7            15 → 15     0.13 → 0.13    19 → 21  -
gold/gols_decisivos                             1 → 1         4.5 → 4.5            15 → 15     0.07 → 0.07    16 → 18  -
gold/cartoes_jogadores                          1 → 1       45.8 → 45.8            15 → 16     0.67 → 0.72    17 → 20  temporada
gold/faltas_clubes                              1 → 1         3.9 → 3.9            15 → 15     0.06 → 0.06    17 → 19  -
```

Há duas medidas, e a diferença importa. Arquivos atuais é o que a versão de agora usa. Arquivos na pasta é tudo que está no disco, inclusive de versões antigas. O `OPTIMIZE` mexe na primeira, e o `VACUUM`, na segunda. Na soma das 22 tabelas, os arquivos atuais foram de 86 para 75, e os da pasta, de 890 para 755.

O que o resultado mostra:

- Só `gold/goleadas` tinha o que juntar, de 12 para 1 arquivo e de 52,8 KB para 10,9 KB, já que cada arquivo Parquet carrega rodapé e dicionários próprios.
- Nas demais, o `OPTIMIZE` não fez nada, e isso é correto. O gold já grava um arquivo por tabela, e a silver, um por partição (temporada). Quando não há nada a juntar, ele nem cria versão, e `silver/partidas` foi da versão 27 para a 29 só por causa das duas anotações do `VACUUM`.
- A pasta de `gold/goleadas` cresceu um arquivo (180 para 181). O `OPTIMIZE` não apaga nada, só cria o arquivo novo e marca os antigos como fora de uso. Os antigos ficam até o `VACUUM` poder levá-los.
- A próxima `make gold` desfaz o trabalho em `gold/goleadas`, porque cada `make gold` grava 12 arquivos de novo, como mostram as versões 13 e 18 do histórico. A manutenção é para rodar depois da carga.

A conferência por linhas ordenadas das 18 tabelas gold, contra a foto de antes das constraints, deu igual depois da carga e depois da manutenção, no lake real e também na cópia em que refiz todo o fluxo:

```text
depois de make lakehouse e make gold com constraints: tabelas conferidas: 18; idênticas: 18; divergentes: []; extras: []
na segunda execução dos dois: tabelas conferidas: 18; idênticas: 18; divergentes: []; extras: []
depois de make manutencao: tabelas conferidas: 18; idênticas: 18; divergentes: []; extras: []
```

## Z-Order: agrupar valores parecidos

Cada arquivo Parquet registra no diário o menor e o maior valor de cada coluna. Com um filtro `temporada = 2019`, o Delta pula os arquivos cujo intervalo não inclui 2019. Isso se chama data skipping. Só funciona bem se os valores estiverem agrupados. Se 2019 estiver espalhada por todos os arquivos, nenhum pode ser pulado. O Z-Order reescreve os arquivos aproximando as linhas com valores próximos, em uma ou mais colunas ao mesmo tempo.

### Quais colunas e por quê

| Tabela | Colunas | Quem filtra assim |
|---|---|---|
| `gold/classificacao_por_rodada` (18.332 linhas) | `temporada`, `rodada` | app (página da temporada) e dashboard, que filtra as duas |
| `gold/cartoes_jogadores` (5.891) | `temporada` | app (disciplina) |
| `gold/artilharia` (3.614) | `temporada` | dashboard |
| `gold/sequencias` (1.880) | `temporada` | dashboard |
| `gold/confrontos` (1.546) | `clube`, `adversario` | app (confronto) |

Só entraram tabelas com um filtro real no app ou no dashboard e pelo menos mil linhas. O corte de mil linhas é um critério meu para não ordenar tabela de 23 linhas, não um número da documentação do Delta.

A silver fica sem Z-Order. O único filtro do código na silver é por `temporada`, e ela é a coluna de partição. O Delta recusa:

```text
[DELTA_ZORDERING_ON_PARTITION_COLUMN] temporada is a partition column. Z-Ordering can only be performed on data columns
```

Nem precisa, porque o filtro por partição já escolhe a pasta certa. As outras colunas da silver aparecem em joins e na condição do `MERGE` (`s.partida_id = n.partida_id`), sem filtro por um valor fixo, e cada partição tem um arquivo só.

As demais tabelas do gold também ficam sem, por não terem filtro (`ranking_historico`, `linha_corte`) ou por serem pequenas (`resumo_temporada`, com 23 linhas, e as tabelas abaixo de mil linhas).

### O que ele não faz aqui

Duas ressalvas mudam o que dá para esperar do Z-Order neste projeto.

1. O app e o dashboard não leem o Delta. O app lê os Parquet de `app/dados/`, com pandas. O dashboard lê o Postgres. O filtro é real, mas acontece em outro lugar. Neste momento, só leria o Delta com filtro quem consultasse o gold diretamente.
2. Com um arquivo só, não há o que pular. O `OPTIMIZE` mira arquivos muito maiores que estas tabelas, então o Z-Order grava um arquivo. A consulta por `temporada = 2019 AND rodada = 38` abre esse arquivo antes e depois.

O Z-Order ainda custa uma reescrita a cada `make manutencao`. Rodei a segunda execução numa cópia, e `gold/classificacao_por_rodada` foi da versão 35 para a 38 e a pasta ganhou um arquivo, mesmo já estando ordenada, enquanto a compactação de `gold/goleadas` não reescreveu nada:

```text
tabela                                arquivos atuais         KB atuais  arquivos na pasta     MB na pasta     versão  z-order
gold/goleadas                                   1 → 1       10.9 → 10.9          182 → 182     0.78 → 0.78    21 → 23  -
gold/classificacao_por_rodada                   1 → 1     163.9 → 163.9            18 → 19     2.88 → 3.04    35 → 38  temporada, rodada
```

E a ordem se perde na próxima `make gold`, que reescreve a tabela. Ele está aqui para eu aprender a operação de verdade (lembrando que é um projeto de portfólio), e a escolha das colunas fica pronta para quando alguém consultar o gold direto.

## VACUUM: apagar o que ficou para trás

O `VACUUM` apaga os arquivos que nenhuma versão recente usa. É o único comando daqui que destrói dado de verdade. A regra é a retenção. Por padrão, um arquivo só sai 7 dias depois de ter deixado de valer (168 horas). A `make manutencao` usa esse padrão e não mexe na trava de segurança. Os parâmetros gravados no histórico confirmam. Abaixo está o que li do histórico de cada tabela, num resumo meu, com retenção pedida `None`, retenção padrão de 604.800.000 ms (7 dias) e trava ligada (`check=true`):

```text
silver/partidas                      VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
silver/gols                          VACUUM paraDeletar=29 bytes=384936 deletados=29 retencao=None/604800000 check=true
silver/cartoes                       VACUUM paraDeletar=27 bytes=622767 deletados=27 retencao=None/604800000 check=true
silver/estatisticas                  VACUUM paraDeletar=27 bytes=365200 deletados=27 retencao=None/604800000 check=true
gold/classificacao_por_rodada        VACUUM paraDeletar=10 bytes=1678590 deletados=10 retencao=None/604800000 check=true
gold/desempenho_mandante_visitante   VACUUM paraDeletar=8 bytes=92688 deletados=8 retencao=None/604800000 check=true
gold/sequencias                      VACUUM paraDeletar=8 bytes=167184 deletados=8 retencao=None/604800000 check=true
gold/ranking_historico               VACUUM paraDeletar=8 bytes=54992 deletados=8 retencao=None/604800000 check=true
gold/disciplina                      VACUUM paraDeletar=6 bytes=27144 deletados=6 retencao=None/604800000 check=true
gold/artilharia                      VACUUM paraDeletar=6 bytes=193595 deletados=6 retencao=None/604800000 check=true
gold/gols_por_minuto                 VACUUM paraDeletar=6 bytes=20448 deletados=6 retencao=None/604800000 check=true
gold/resumo_temporada                VACUUM paraDeletar=6 bytes=36661 deletados=6 retencao=None/604800000 check=true
gold/confrontos                      VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/goleadas                        VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/campanhas                       VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/linha_corte                     VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/passagens_tecnicos              VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/esquemas_taticos                VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/viradas                         VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/gols_decisivos                  VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/cartoes_jogadores               VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
gold/faltas_clubes                   VACUUM paraDeletar=0 bytes=0 deletados=0 retencao=None/604800000 check=true
TOTAL deletados 141
```

No lake real, removeu **141 arquivos, 3.644.205 bytes**. Os arquivos removidos são todos de 27 e 28 de outubro (91 e 50 arquivos, conferidos contra a cópia de segurança), em `silver/gols`, `silver/cartoes`, `silver/estatisticas` e oito tabelas do gold. Nas tabelas gold criadas em novembro, ele não apagou nada. Em `silver/partidas` também não, porque a pasta tem 23 arquivos e a versão atual usa os 23. As regravações de novembro continuam na pasta, dentro da retenção. O espaço em Parquet caiu de 15,04 MB para 11,88 MB, somando as 22 tabelas.

A retenção existe por um motivo prático. Quem está lendo uma versão antiga, ou um job gravando ao mesmo tempo, depende daqueles arquivos. Por isso a trava recusa retenção curta:

```text
[recusou] vacuum(0) com a trava ligada: IllegalArgumentException: [DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT] The specified VACUUM retention period is too low and may corrupt this Delta table if any writes are in progress.
```

### O que acontece com a versão apagada

Numa cópia, e só nela, desliguei a trava para ver a remoção por inteiro, usando `gold/goleadas`. A pasta foi de 182 arquivos para 1:

```text
dry run padrão (retenção de 7 dias): 0 arquivos
dry run 0 horas: 181 arquivos
antes Medida(arquivos=1, bytes=11143, arquivos_disco=182, bytes_disco=823084, versao=23)
depois Medida(arquivos=1, bytes=11143, arquivos_disco=1, bytes_disco=11143, versao=25)
```

A versão atual continua lendo. A leitura da versão 0, não, mas depende de como se lê:

```text
[passou] versão atual, collect: 382
[passou] versão 0, count: 382
[recusou] versão 0, collect: Py4JJavaError: : org.apache.spark.SparkException: [FAILED_READ_FILE.FILE_NOT_EXIST] Encountered error while reading file file:... | Caused by: java.io.FileNotFoundException: File file:...
```

O `count()` responde 382 sem abrir arquivo nenhum, porque a contagem está nas estatísticas do diário. Se o teste usasse `count()`, ele passaria mesmo com os arquivos apagados e não provaria nada. Por isso o teste usa `collect()`, e um segundo teste documenta esse comportamento. No teste, a retenção curta fica restrita a uma fixture, que devolve o valor original à sessão no fim.

O diário também fica. O histórico continua listando a versão 0, embora ela não possa mais ser lida, então ele mostra o que aconteceu e nada garante que ainda dá para ler.

## Time travel: ler e comparar versões

`ler_versao` lê a tabela como ela estava numa versão, e `comparar_versoes` mostra a diferença entre duas. Antes e depois do `OPTIMIZE` e do Z-Order, a diferença é zero, que é o que se espera deles:

```text
$ make historico TABELA=gold/goleadas COMPARAR="18 19"
versão 18: 382 linhas
versão 19: 382 linhas
inseridas: 0
removidas: 0

$ make historico TABELA=gold/classificacao_por_rodada COMPARAR="32 33"
versão 32: 18332 linhas
versão 33: 18332 linhas
inseridas: 0
removidas: 0
```

Para ver uma diferença real, numa cópia de `silver/partidas` corrigi um placar e apaguei uma partida. Sem chave, a linha corrigida aparece duas vezes, a antiga em "removidas" e a nova em "inseridas". Com a chave `partida_id`, ela vai para "alteradas":

```text
versões 29 e 31
--- sem chave
versão 29: 9165 linhas
versão 31: 9164 linhas
inseridas: 1
removidas: 2
--- com chave
versão 29: 9165 linhas
versão 31: 9164 linhas
inseridas: 0
removidas: 1
alteradas: 1
alterada: [(9165, 9)]
removida: [9164]
```

### O limite de 7 dias vale antes do VACUUM

Como o `VACUUM` não apagou nenhum arquivo de `silver/partidas`, daria para achar que qualquer versão dela ainda abre. Não abre:

```text
$ make historico TABELA=silver/partidas COMPARAR="10 22"
Erro: silver/partidas não tem a versão 10 disponível
```

O Delta recusa time travel além da retenção de 7 dias mesmo com o arquivo na pasta (`DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION`). A função converte isso numa mensagem curta. Em `gold/classificacao_por_rodada`, as versões de 27 e 28 de outubro falham ao abrir, e as de 5 de novembro em diante leem as 18.332 linhas. Ou seja, o histórico serve para comparação recente e para recuperar erro recente. Para guardar dado por muito tempo, ele não serve.

## E a velocidade?

**Com 9.165 partidas, o esperado é não haver ganho visível, e não houve.** As maiores tabelas têm dezenas de milhares de linhas e centenas de KB. Z-Order e compactação existem para tabelas muito maiores do que estas.

Medi uma vez cada consulta, antes e depois da manutenção. É uma execução só, então serve para ver a ordem de grandeza, sem comparação fina. A primeira consulta de cada lista inclui o aquecimento da sessão.

Antes:

```text
gold/classificacao_por_rodada    filtro [temporada = 2019 AND rodada = 38] linhas=20    arquivos_na_tabela=1   arquivos_da_consulta=1   tempo=3318ms
gold/goleadas                    filtro [temporada = 2019] linhas=15    arquivos_na_tabela=12  arquivos_da_consulta=1   tempo=618ms
gold/cartoes_jogadores           filtro [temporada = 2019] linhas=510   arquivos_na_tabela=1   arquivos_da_consulta=1   tempo=471ms
gold/confrontos                  filtro [clube = 'Santos' AND adversario = 'Palmeiras'] linhas=1     arquivos_na_tabela=1   arquivos_da_consulta=1   tempo=430ms
silver/partidas                  filtro [temporada = 2019] linhas=380   arquivos_na_tabela=23  arquivos_da_consulta=1   tempo=457ms
```

Depois:

```text
gold/classificacao_por_rodada    filtro [temporada = 2019 AND rodada = 38] linhas=20    arquivos_na_tabela=1   arquivos_da_consulta=1   tempo=3360ms
gold/goleadas                    filtro [temporada = 2019] linhas=15    arquivos_na_tabela=1   arquivos_da_consulta=1   tempo=650ms
gold/cartoes_jogadores           filtro [temporada = 2019] linhas=510   arquivos_na_tabela=1   arquivos_da_consulta=1   tempo=483ms
gold/confrontos                  filtro [clube = 'Santos' AND adversario = 'Palmeiras'] linhas=1     arquivos_na_tabela=1   arquivos_da_consulta=1   tempo=397ms
silver/partidas                  filtro [temporada = 2019] linhas=380   arquivos_na_tabela=23  arquivos_da_consulta=1   tempo=510ms
```

Os tempos ficam na mesma faixa, e a variação está no tamanho do ruído de uma medição única. O número de arquivos lidos também não muda em nenhuma delas, inclusive `gold/goleadas`, que antes de compactar já abria só 1 dos 12 arquivos, porque o data skipping pulava os outros 11.

O ganho que dá para defender é outro. As regras valem para qualquer escrita, e a pasta ficou 3,16 MB menor.

As constraints têm custo. Medi uma execução de cada comando, numa cópia que partiu do lake de antes delas, então pode haver ruído. O `make gold` foi da mediana de 38,5 s, medida antes das constraints, para 70 s na primeira execução, que aplica as 70, e 43 s na segunda. `make lakehouse` foi de 176,1 s para 191 s e 184 s. `make manutencao` levou 65 s nas 22 tabelas. No pipeline, o preço aparece na primeira execução, com uns 0,5 s por constraint, medidos em `ALTER TABLE ... ADD CONSTRAINT` numa tabela pequena. Na suíte, o custo é maior, porque cada lake novo dos testes aplica tudo de novo. O `make test` foi da mediana de 198,4 s para 346 s em uma execução, somando esse efeito aos 111 testes novos. Não tentei reduzir esse custo.

Uma configuração entrou por causa do `VACUUM`. Ele lista a pasta com um job de `spark.sql.sources.parallelPartitionDiscovery.parallelism` tarefas, e o padrão é 10.000. Numa tabela pequena das fixtures, uma execução levou 9,3 s com o padrão e 0,8 s com 8, então o `_spark_local` usa 8. O gold continuou idêntico com ela, e as comparações acima já rodaram com essa configuração.

## Em uma frase

| Operação | O que ela faz |
|---|---|
| Constraint `CHECK` | Regra gravada na tabela, que recusa a escrita inválida de qualquer origem. |
| Protocolo | Versão mínima que um programa precisa ter para ler ou escrever a tabela. Sobe e não desce. |
| Versão | Uma anotação no diário dizendo quais arquivos valem a partir dali. |
| `OPTIMIZE` | Junta arquivos pequenos em maiores, sem mudar o conteúdo e sem apagar nada. |
| Z-Order | Reescreve os arquivos aproximando valores parecidos, para o filtro pular arquivos. |
| Data skipping | Usa o mínimo e o máximo de cada arquivo, anotados no diário, para não abrir o que não interessa. |
| `VACUUM` | Apaga do disco os arquivos que passaram da retenção. É o que tira versões antigas de vez. |
| Retenção | O tempo que um arquivo fora de uso fica guardado, 7 dias por padrão. |
| Time travel | Ler a tabela como ela estava numa versão, dentro da retenção. |
