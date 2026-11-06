# Tuning medido

O job de testes do CI foi de pouco mais de 6 para quase 10 minutos quando entraram as dez tabelas gold novas e o app, com o timeout em 15. Fui medir onde esse tempo estava, antes de sair mudando configuração. O alvo acabou sendo a suíte. O `make test` local caiu de 368,2 s para 198,4 s, sem mexer no código de produção, sem remover teste e com o gold idêntico nas 18 tabelas.

Todo número aqui saiu de uma execução real, inclusive os das tentativas que não deram certo.

## Como medi

- A máquina é um Intel i5-13420H com 12 CPUs lógicas, 15 GB de RAM, Java 25.0.4.1 e Python 3.12.13, em uso normal, com navegador aberto.
- Cada tempo é a mediana de três execuções, do início ao fim do comando. Duas execuções do `make test` e as três com 4 CPUs do código original rodaram mais tarde, voltando ao código original, porque essas comparações entraram depois.
- Antes de cada execução, o script esperava a carga de 1 minuto do sistema ficar abaixo de 2,0.
- O Postgres dos testes ficou de pé em todas as execuções da suíte e os testes `postgres` rodaram. O Metabase ficou parado.
- O `make test` tem cobertura e o `pytest --durations=30` não, por isso aparecem separados. As mudanças 1 a 4 foram medidas com o segundo e as 5 e 6 com o primeiro, que é o que o CI roda.

## Linha de base

| Comando | Execuções (s) | Mediana (s) |
|---|---|---:|
| `make gold` | 38,3 / 38,5 / 38,8 | 38,5 |
| `make lakehouse` | 176,1 / 175,2 / 177,4 | 176,1 |
| `pytest --durations=30` | 344,1 / 342,7 / 346,7 | 344,1 |
| `make test` | 374,1 / 361,0 / 368,2 | 368,2 |
| `make test` limitado a 4 CPUs lógicas (`taskset -c 0-3`) | 364,6 / 371,0 / 407,4 | 371,0 |

### CI

Tempo do job `test`, de ponta a ponta:

| Run | Commit | Testes | Job (s) | Passo `make test` (s) |
|---|---|---:|---:|---:|
| 42048144823 | 26a21d2 | 224, com 7 pulados | 378 | 347 |
| 42056650350 | 9ba4ccf | 231 | 382 | 327 |
| 42125091280 | a055de2 | 344 | 580 | 529 |
| 42137280445 | 7be229a | 344 | 665 | 620 |
| 42234035960 | 9b981f6 | 344 | 568 | 507 |

A linha de base são as três últimas, com os mesmos 344 testes, e a mediana dá 580 s. Entre elas só mudou documentação, e mesmo assim o job variou de 568 s a 665 s. Uma diferença de até uns 100 s no CI pode ser só ruído do runner.

### Por que a suíte do CI subiu

O aumento veio dos commits 3db75ec e a055de2. Os testes foram de 231 para 344 e as tabelas do gold, de 8 para 18. Usando o horário em que o log do CI fecha cada arquivo de teste, comparei o run 42056650350 com a mediana dos três runs de 344 testes:

| Grupo | Antes (s) | Depois (s) | Diferença (s) |
|---|---:|---:|---:|
| 10 arquivos novos em `tests/unit/aggregate` | 0 | 70 | +70 |
| `test_exportar.py` (novo) | 0 | 74 | +74 |
| 2 arquivos novos em `tests/app` | 0 | 14 | +14 |
| `test_gold.py` | 35 | 65 | +30 |
| `test_serving.py` | 29 | 46 | +17 |
| Demais arquivos | 260 | 252 | -8 |
| Total | 324 | 521 | +197 |

A maior parte é teste novo, que tem motivo para existir. Só que uma parte era desperdício. `test_exportar`, `test_gold`, `test_serving` e `test_explain` montavam, cada um, o mesmo lake das fixtures, e cada gold montado ficou mais caro com 18 tabelas. Localmente, os setups desses quatro arquivos somavam cerca de 86 s.

## Resultados

As mudanças foram testadas uma por vez, cada uma sobre o estado anterior. As mudanças 4, 5 e 6 foram medidas com a mudança 3 ainda ligada, porque ela só foi revertida na conferência final.

| # | Mudança | Medido com | Antes (s) | Depois (s) | Ganho | Resultado |
|---|---|---|---:|---:|---:|---|
| 1 | Lake das fixtures montado uma vez por sessão | `pytest` | 344,1 | 301,1 | -12,5% | Ficou |
| 2 | JVM só com o compilador C1 (`-XX:TieredStopAtLevel=1`) | `pytest` | 301,1 | 408,1 | +35,5% | Revertida |
| 3 | `spark.sql.codegen.wholeStage=false` nos testes | `pytest` | 301,1 | 273,9 | -9,0% | Revertida |
| 4 | `local[2]` para `local[4]` nos testes | `pytest` | 273,9 | 276,1 | +0,8% | Revertida |
| 5 | Cobertura com `core = "sysmon"` | `make test` | 285,3 | 272,3 | -4,6% | Ficou |
| 6 | `pytest-xdist` com 2 workers, por arquivo | `make test` | 272,3 | 187,8 | -31,0% | Ficou |
| 6 | O mesmo, limitado a 4 CPUs lógicas | `make test` | 279,5 | 234,8 | -16,0% | Ficou |

### Antes e depois de tudo

Estado final, com as mudanças 1, 5 e 6 e sem as outras:

| Medida | Execuções depois (s) | Antes | Depois | Ganho |
|---|---|---:|---:|---:|
| `make test` local | 197,8 / 198,8 / 198,4 | 368,2 s | 198,4 s | -46,1% |
| `make test` local, limitado a 4 CPUs lógicas | 243,7 / 241,1 / 242,2 | 371,0 s | 242,2 s | -34,7% |
| `pytest --durations=30` local, sem xdist | 281,5 / 280,4 / 280,0 | 344,1 s | 280,4 s | -18,5% |
| Job `test` do CI | PENDENTE-CI | 580 s | PENDENTE-CI | PENDENTE-CI |

A meta do projeto é o job do CI ficar pelo menos 25% mais rápido, o que dá 435 s ou menos. Os números locais não respondem a isso, porque o runner tem outro hardware, usa Java 21 em vez de 25 e varia bastante entre execuções.

## O que ficou

### 1. Lake compartilhado

Silver e gold das fixtures agora são montados uma vez por sessão, e só quando algum teste pede. As asserções continuam as mesmas, e o teste de idempotência ainda roda o gold uma segunda vez e compara. Em troca, esses módulos passam a dividir o mesmo diretório. Isso funciona porque nenhum deles muda o conteúdo, no máximo regravam o gold com as mesmas linhas.

### 5. Cobertura com `sysmon`

Usa o monitoramento nativo do Python 3.12 no lugar do rastreamento antigo. As três execuções de cada lado não se sobrepõem (284,7 a 288,0 s contra 271,0 a 272,6 s), e o relatório de cobertura por arquivo saiu igual, com 1158 linhas, 3 sem cobrir e 99%.

### 6. Dois workers do `pytest-xdist`

Foi o maior ganho. Cada worker abre a própria sessão Spark, e `--dist loadfile` mantém os testes de um arquivo no mesmo worker. A opção fica no `make test`, então o CI recebe a mudança sem editar o workflow. Com 12 CPUs o ganho foi de 31%, e limitado a 4 CPUs lógicas, de 16%. Ele depende de haver CPU sobrando, então nenhum dos dois dá para transportar para outra máquina.

## O que não funcionou

### 2. Só o compilador C1

A hipótese era que a suíte, cheia de consultas curtas, nem chegaria a aproveitar o compilador C2 da JVM. A medição desmentiu, ficou 35,5% mais lenta. Talvez o código passe tempo suficiente em caminhos quentes para o C2 compensar, mas essa parte eu não medi.

### 3. Sem geração de código nos testes

Por padrão, o Spark gera e compila Java para as consultas. Desligar isso nos testes deu 9% na medição isolada, e reverti assim mesmo, por dois motivos.

O primeiro é que a suíte passaria a executar as consultas por um caminho diferente do usado em produção. Nenhuma asserção mudava, mas é teste enfraquecido para ganhar tempo, e a spec do projeto proíbe isso.

O segundo é que o ganho não se sustentou. No estado final, a suíte com geração de código mediu 198,4 s no `make test`, contra 194,9 s sem ela, e 280,4 s no `pytest` sem xdist, contra 282,0 s sem ela. Quase esse mesmo estado do `pytest` (o lake compartilhado, ainda sem a trava do Ivy, descrita abaixo) tinha medido 301,1 s horas antes. Ou seja, o tempo escorrega uns 20 s ao longo do dia, o que já engole quase toda a diferença de 27 s da medição isolada.

### 4. Mais threads no Spark

Os 2,2 s de diferença estão dentro da variação entre execuções da medição anterior (271,2 s a 293,6 s). Tabelas tão pequenas talvez não tenham trabalho que justifique mais tarefas, mas não medi a causa.

### xdist com o cache do Ivy vazio

A primeira versão da mudança 6 passou em todas as execuções com o cache cheio e falhou em 3 de 3 com o cache vazio, num teste com dois arquivos e um diretório de cache novo a cada tentativa. Eram dois workers baixando os jars ao mesmo tempo, e um deles morria com `unresolved dependency`. No CI, isso aconteceria quando uma versão de jar mudasse, porque a chave do cache muda junto. A correção foi uma trava de arquivo na criação da sessão Spark dos testes, para um worker esperar o outro. Com ela, passou em 3 de 3 com o cache vazio. Não medi o custo da trava isolada.

## O que não tentei, e por quê

- `shuffle.partitions` de 2 para 1 nos testes. Com uma partição só, um erro que dependa da distribuição dos dados entre partições deixaria de aparecer, então nem medi.
- Pular os testes `postgres` ou reduzir fixtures, pelo mesmo motivo.
- Mudar o pipeline. Nas três execuções, o `make lakehouse` levou de 5,4 s a 9,4 s por temporada depois da primeira, que levou de 14,6 s a 16,0 s, e nenhuma destoa. No `make gold`, uma execução com cProfile mostrou 12,7 s de 36,9 s em gravações Delta, cerca de 0,7 s por tabela, e o resto espalhado entre as 18 tabelas e as checagens. Sem um ponto que concentre o tempo, não havia o que atacar, e o código de produção não foi alterado.

## Conferência

- O gold foi comparado nas 18 tabelas, linha a linha e ordenado por todas as colunas, com uma foto tirada antes de tudo. Deu 18 idênticas no estado final, logo depois de um `make gold`, e em cada estado intermediário. Nas mudanças 1 e 3, essa comparação foi feita depois, recompondo o estado do código.
- Os identificadores dos 344 testes são os mesmos antes e depois. O `make test` final passou com 344 testes, nenhum pulado, e cobertura de 99%.
- Rodar `test_serving.py` sem a variável `BRASILEIRAO_PG_TESTE_URL` continua dando 3 testes passando e 7 pulados.
