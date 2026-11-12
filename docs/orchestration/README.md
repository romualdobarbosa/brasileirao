# Orquestração com Airflow

A [simulação](simulacao.md) roda rodada a rodada pelo `make simulacao`, num processo só. Aqui ela ganha uma DAG no Airflow, com uma tarefa por etapa, para que cada rodada apareça como uma execução com estado, log e tempo de cada passo, e para que uma etapa que falha segure as seguintes.

Tudo continua sendo simulação, num lake separado em `data_sim/`. A DAG nunca exporta para `app/dados` e não toca no `./data`.

## O que sobe

`orchestration/` tem o compose e a imagem, separados do [serving](../serving/).

| Peça | Versão | Observação |
|---|---|---|
| Airflow | 3.4.0 (`apache/airflow:3.4.0-python3.12`) | A estável mais recente no PyPI em 11/11/2026 |
| Executor | `LocalExecutor` | As tarefas rodam como processos do scheduler, sem Redis nem worker |
| Banco do Airflow | `postgres:17.11` | Banco e volume próprios, sem porta publicada, então não disputa a 5432 com o Postgres do serving |
| Java | Temurin 21.0.13 (`eclipse-temurin:21.0.13_9-jre`) | O mesmo Java 21 do CI, suportado pelo PySpark 4.2.0 |
| Projeto | `pyspark==4.2.0`, `delta-spark==4.4.1` e o pacote `brasileirao` | Instalados na imagem junto com o Airflow fixado, e um teste confere que as versões batem com o `pyproject.toml` |
| AWS CLI | 2.38.0 | Só para o alvo `aws` |

Os serviços são `postgres`, `airflow-init` (migra o banco e sai), `airflow-apiserver` (interface e API em `127.0.0.1:8080`), `airflow-scheduler` e `airflow-dag-processor`. Não tem triggerer porque a DAG não usa tarefa adiável.

O que fica montado no container.

- `orchestration/dags` em `/opt/airflow/dags`, só leitura.
- `data/raw` (a fonte completa) em `/opt/brasileirao/data/raw`, só leitura.
- `data_sim` em `/opt/brasileirao/data_sim`, onde a simulação grava.
- O volume `ivy` guarda os jars do Delta que o Spark baixa na primeira sessão. Da segunda em diante o log da tarefa mostra `0 artifacts copied, 33 already retrieved`.
- `postgres` e `logs` guardam o banco e os logs das tarefas entre um `down` e outro `up`.

O container roda com o UID do seu usuário, então o que o Airflow grava em `data_sim/` continua sendo seu. A interface não pede login, porque só escuta em `127.0.0.1`.

Segredo nenhum vai para o git. Na primeira vez, o `make airflow-up` cria o `orchestration/.env` (fora do git, modelo em `orchestration/.env.example`) com o seu UID, a senha do Postgres do Airflow, o segredo dos tokens entre os componentes e a chave Fernet, todos aleatórios.

## Como subir e disparar

```
make download                     # se ainda não tiver data/raw
make simulacao-preparar HISTORICO=1
make airflow-up                   # build da imagem, sobe e espera tudo ficar saudável
make airflow-rodada RODADA=1      # dispara uma execução da DAG
make airflow-estado               # execuções da DAG
make airflow-estado RUN=<run_id>  # estado de cada tarefa de uma execução
make airflow-down                 # derruba e mantém os volumes
```

A interface fica em http://127.0.0.1:8080. O primeiro build levou 103 s e o primeiro `up`, com a migração do banco, 104 s.

O `simulacao-preparar` esvazia `data_sim/` sem apagar a pasta. Isso importa porque ela está montada no container, e uma pasta apagada e criada de novo só apareceria lá depois de reiniciar o Airflow.

## A DAG `brasileirao_rodada`

```
retrato → bronze_silver → gold → checagens → registra_execucao
```

| Tarefa | O que faz |
|---|---|
| `retrato` | Grava em `raw/` o retrato da fonte depois da rodada, com os eventos de adiamento e placar corrigido. Só arquivo, sem abrir o Spark |
| `bronze_silver` | `lakehouse --em-andamento` da temporada 2025 a partir do retrato |
| `gold` | O gold com as checagens de temporada parcial. Se reprovar, a tarefa falha e nada é gravado |
| `checagens` | Confere se `silver/partidas` tem as mesmas partidas de 2025 que o retrato e se o Change Data Feed da rodada bate com a comparação de versões. Imprime o relatório da rodada |
| `registra_execucao` | Escreve no log uma linha JSON com o resumo da execução inteira. Só roda quando tudo passou |

As quatro primeiras chamam a CLI da simulação (`python -m brasileirao.simulacao retrato|lakehouse|gold|checar N`), uma etapa por processo. Cada tarefa abre e fecha a própria sessão Spark, e a falha é o código de saída. As etapas são as mesmas funções que o `make simulacao` usa em sequência, então não tem lógica duplicada na DAG. O que a DAG monta (ordem, comandos, registro) está em `src/brasileirao/orquestracao.py`.

O registro de cada etapa, inclusive da que falha, fica na tabela `ops/execucoes`, gravada pela própria CLI (ver [Observabilidade](#observabilidade)). O `registra_execucao` ficou como o resumo da DAG no log, porque só ele sabe o `run_id` e a duração da execução inteira.

### Parâmetros

| Parâmetro | Valores | Padrão |
|---|---|---|
| `rodada` | inteiro de 1 a 38 | nenhum, o disparo precisa informar |
| `alvo` | `local` ou `aws` | `local` |

O Airflow valida os dois antes de criar a execução. Rodada 0, 39 ou texto são recusados no disparo. A rodada e o alvo chegam às tarefas por variável de ambiente e nunca entram no texto do comando.

A DAG tem `max_active_runs=1`, porque duas execuções ao mesmo tempo gravariam no mesmo lake. Disparar as rodadas 2 e 3 juntas deixa a 3 na fila até a 2 terminar.

### Agendamento

A DAG é manual (`schedule=None`). A rodada simulada não segue o calendário, e quem decide quando a rodada seguinte "acontece" sou eu. Com uma temporada real em andamento, o natural seria um agendamento semanal, por exemplo segunda de manhã, depois da rodada do fim de semana.

Mesmo com agendamento, existe um limite. O Airflow roda em Docker na minha máquina, então só dispara com a máquina ligada e o compose de pé. Com `catchup=False`, ao voltar ele roda só o intervalo mais recente que perdeu, e não os anteriores. Para disparar sem depender da minha máquina, o Airflow teria de morar num servidor, o que está fora do escopo.

## Alvo `aws`

O alvo `aws` é o mesmo do `make lakehouse-aws`, com o Spark local e o lake no bucket do `infra/main`. A tarefa tira do ambiente qualquer credencial exportada, usa o perfil `AWS_PROFILE` (`brasileirao` por padrão), descobre a conta com `aws sts get-caller-identity` e grava em `s3a://brasileirao-lake-<conta>/data_sim`. A simulação recusa qualquer destino remoto que não termine em `data_sim`, para nunca escrever por cima do lake normal que mora na raiz do bucket. O retrato é montado numa pasta temporária e copiado para `raw/` pelo sistema de arquivos do Hadoop da própria sessão Spark, com a mesma credencial.

Rodei a rodada 1 em 12/11/2026, com a infraestrutura de pé só durante o teste. Passou inteira, mas levou 1213 s, contra 191 s no alvo local. O Spark continua na minha máquina e cada leitura e escrita do Delta vai até o S3 pela internet, e acho que é daí que vem a maior parte da diferença (não medi separado). O gold, com as 18 tabelas, ficou com mais da metade do tempo.

| Execução | Total | retrato | bronze_silver | gold | checagens |
|---|---:|---:|---:|---:|---:|
| rodada 1, alvo `aws` | 1213 s | 51,4 s | 316,8 s | 800,3 s | 41,8 s |

```
{"evento": "execucao_brasileirao", "dag_id": "brasileirao_rodada", "run_id": "manual__2026-11-12T03:12:19.389785+00:00", "temporada": 2025, "rodada": 1, "alvo": "aws", "status": "sucesso", "inicio": "2026-11-12T03:12:19.884555+00:00", "fim": "2026-11-12T03:32:32.911298+00:00", "segundos": 1213.0, "tarefas": ["retrato", "bronze_silver", "gold", "checagens"]}
```

Os passos, na ordem.

```
terraform -chdir=infra/main apply        # o lake no S3
aws login
make simulacao-preparar-aws               # opcional, esvazia s3a://<lake>/data_sim
make airflow-up-aws                       # soma o docker-compose.aws.yml, que monta ~/.aws no scheduler
make airflow-rodada RODADA=1 ALVO=aws
terraform -chdir=infra/main destroy
```

O `docker-compose.aws.yml` monta o `~/.aws` da máquina no scheduler, onde as tarefas rodam. Sem leitura apenas, porque a CLI renova a sessão do `aws login` no cache de `~/.aws/login`. A montagem não usa `:z`, que trocaria o rótulo do SELinux da pasta na máquina. O scheduler roda com `label=disable`.

## Prova no alvo local

Rodei em 11/11/2026, na mesma máquina do [tuning](../tuning/README.md), depois de `make simulacao-preparar HISTORICO=1` (198 s).

```
$ docker compose -f orchestration/docker-compose.yml exec airflow-scheduler airflow dags list
dag_id             | fileloc                                 | owners  | is_paused | bundle_name | bundle_version
===================+=========================================+=========+===========+=============+===============
brasileirao_rodada | /opt/airflow/dags/brasileirao_rodada.py | airflow | False     | dags-folder | None
```

### Três rodadas seguidas

Disparei as rodadas 1, 2 e 3, com as duas últimas juntas, e a 3 esperou a 2 na fila.

```
$ make airflow-estado
dag_id             | run_id          | state   | run_after                        | logical_date | start_date                       | end_date
===================+=================+=========+==================================+==============+==================================+=================================
brasileirao_rodada | rodada-03       | success | 2026-11-12T02:25:42.690016+00:00 |              | 2026-11-12T02:27:25.858864+00:00 | 2026-11-12T02:29:08.988930+00:00
brasileirao_rodada | rodada-02       | success | 2026-11-12T02:25:40.607506+00:00 |              | 2026-11-12T02:25:41.581751+00:00 | 2026-11-12T02:27:24.818507+00:00
brasileirao_rodada | rodada-01       | success | 2026-11-12T02:22:04.081801+00:00 |              | 2026-11-12T02:22:04.437631+00:00 | 2026-11-12T02:25:15.596247+00:00
```

Estado das tarefas da rodada 3.

```
$ make airflow-estado RUN=rodada-03
dag_id             | logical_date | task_id           | state   | start_date                       | end_date
===================+==============+===================+=========+==================================+=================================
brasileirao_rodada |              | gold              | success | 2026-11-12T02:27:58.154227+00:00 | 2026-11-12T02:28:48.436733+00:00
brasileirao_rodada |              | checagens         | success | 2026-11-12T02:28:48.776186+00:00 | 2026-11-12T02:29:07.115469+00:00
brasileirao_rodada |              | registra_execucao | success | 2026-11-12T02:29:08.077634+00:00 | 2026-11-12T02:29:08.135124+00:00
brasileirao_rodada |              | retrato           | success | 2026-11-12T02:27:25.891393+00:00 | 2026-11-12T02:27:26.422852+00:00
brasileirao_rodada |              | bronze_silver     | success | 2026-11-12T02:27:26.477467+00:00 | 2026-11-12T02:27:57.974133+00:00
```

| Execução | Total | retrato | bronze_silver | gold | checagens |
|---|---:|---:|---:|---:|---:|
| rodada-01 | 191 s | 3,4 s | 86,9 s | 77,6 s | 20,0 s |
| rodada-02 | 103 s | 0,5 s | 30,1 s | 51,5 s | 18,8 s |
| rodada-03 | 103 s | 0,5 s | 31,5 s | 50,3 s | 18,3 s |

A rodada 1 é mais lenta porque o Ivy baixou os jars do Delta para o volume e porque o gold foi criado pela primeira vez. Comparando com o `make simulacao` (mediana de 8 s de lakehouse e 20,5 s de gold por rodada), cada tarefa paga a subida de uma JVM e a resolução do Ivy, e é esse o preço de ter uma etapa por processo.

O `registra_execucao` da rodada 1 deixou no log esta linha.

```
{"evento": "execucao_brasileirao", "dag_id": "brasileirao_rodada", "run_id": "rodada-01", "temporada": 2025, "rodada": 1, "alvo": "local", "status": "sucesso", "inicio": "2026-11-12T02:22:04.437631+00:00", "fim": "2026-11-12T02:25:14.614464+00:00", "segundos": 190.2, "tarefas": ["retrato", "bronze_silver", "gold", "checagens"]}
```

![Grid da DAG brasileirao_rodada no Airflow: rodada-01 a rodada-03 verdes, a rodada-04-falha com o gold vermelho e a rodada 4 de novo, verde](img/grid.png)

### Uma falha no meio

Para provocar uma falha de verdade no gold, apaguei de `data_sim/silver/partidas` uma partida de 2024 (a 8785, entre Grêmio e Corinthians), com um `DELETE` do Delta, e disparei a rodada 4. A silver de 2025 passou, e o gold reprovou nas checagens da temporada encerrada.

```
brasileirao.aggregate.qualidade.QualidadeGoldError: gold reprovado, nenhuma tabela gravada:
- classificacao_por_rodada: jogos na última rodada: 2024 Corinthians: 37 jogos, esperado 38, 2024 Grêmio: 37 jogos, esperado 38
- gols_por_minuto: total de gols ≠ silver/gols: 2024: 926 na tabela, 929 na silver
- viradas: placar reconstruído ≠ placar da partida: 8785
- gols_decisivos: totais ≠ silver: 2024: jogos 758 na tabela, 758 na silver, gols_85 126 na tabela, 128 na silver, gols_acrescimo 74 na tabela, 75 na silver
```

As tarefas depois do `gold` não rodaram.

```
$ make airflow-estado RUN=rodada-04-falha
dag_id             | logical_date | task_id           | state           | start_date                       | end_date
===================+==============+===================+=================+==================================+=================================
brasileirao_rodada |              | gold              | failed          | 2026-11-12T02:30:42.993525+00:00 | 2026-11-12T02:31:17.756817+00:00
brasileirao_rodada |              | retrato           | success         | 2026-11-12T02:30:09.472384+00:00 | 2026-11-12T02:30:09.983358+00:00
brasileirao_rodada |              | bronze_silver     | success         | 2026-11-12T02:30:10.547644+00:00 | 2026-11-12T02:30:42.308539+00:00
brasileirao_rodada |              | checagens         | upstream_failed | 2026-11-12T02:31:18.552394+00:00 | 2026-11-12T02:31:18.552394+00:00
brasileirao_rodada |              | registra_execucao | upstream_failed | 2026-11-12T02:31:19.606143+00:00 | 2026-11-12T02:31:19.606143+00:00
```

![Execução rodada-04-falha no Airflow: retrato e bronze_silver com sucesso, gold com falha, checagens e registra_execucao com falha no upstream](img/rodada-04-falha.png)

Depois devolvi a partida, lida da versão anterior ao `DELETE` pelo time travel do Delta, e disparei a rodada 4 de novo. Passou inteira, e as checagens mostraram a correção do placar de Juventude x Ceará que a simulação guarda para essa rodada.

```
rodada 4: silver/partidas versões 31 a 31 (Change Data Feed)
entraram 10 partida(s)
  ...
mudaram 1 partida(s)
  #8806 rodada 3 Juventude 2 x 1 Ceará: placar_mandante 1 → 2, vencedor nulo → Juventude
saíram 0 partida(s)
conferência com a comparação de versões (30 → 31): entraram 10, mudaram 1, saíram 0: bate com o feed
rodada 4: checagens aprovadas
```

Lista final das execuções.

```
dag_id             | run_id          | state   | run_after                        | logical_date | start_date                       | end_date
===================+=================+=========+==================================+==============+==================================+=================================
brasileirao_rodada | rodada-04       | success | 2026-11-12T02:31:54.689407+00:00 |              | 2026-11-12T02:31:55.640369+00:00 | 2026-11-12T02:33:40.404960+00:00
brasileirao_rodada | rodada-04-falha | failed  | 2026-11-12T02:30:09.141195+00:00 |              | 2026-11-12T02:30:09.431308+00:00 | 2026-11-12T02:31:20.359002+00:00
brasileirao_rodada | rodada-03       | success | 2026-11-12T02:25:42.690016+00:00 |              | 2026-11-12T02:27:25.858864+00:00 | 2026-11-12T02:29:08.988930+00:00
brasileirao_rodada | rodada-02       | success | 2026-11-12T02:25:40.607506+00:00 |              | 2026-11-12T02:25:41.581751+00:00 | 2026-11-12T02:27:24.818507+00:00
brasileirao_rodada | rodada-01       | success | 2026-11-12T02:22:04.081801+00:00 |              | 2026-11-12T02:22:04.437631+00:00 | 2026-11-12T02:25:15.596247+00:00
```

## Observabilidade

### A tabela `ops/execucoes`

Toda execução de `python -m brasileirao.pipeline` e de `python -m brasileirao.simulacao` grava uma linha numa tabela Delta, `ops/execucoes`, dentro do lake do próprio processo. Na execução normal ela fica em `data/ops/execucoes`, na simulação em `data_sim/ops/execucoes` e no alvo `aws` em `s3a://<lake>/data_sim/ops/execucoes`. Como a DAG roda a CLI da simulação, cada tarefa do Spark deixa a sua linha.

| Coluna | O que guarda |
|---|---|
| `id` | UUID da execução |
| `comando` | A linha de comando, como `simulacao --destino data_sim --temporada 2025 gold 6` |
| `alvo` | `local`, `aws` ou `databricks` (o `BRASILEIRAO_SPARK_ENV` do processo) |
| `inicio`, `fim` | Quando começou e terminou |
| `status` | `sucesso` ou `falha` |
| `linhas` | Linhas gravadas por tabela, como `gold/campanhas` ou `silver/partidas` |
| `avisos` | Os avisos que a execução logou e, na falha, o erro |

A linha é gravada no fim, com a execução dando certo ou não. Se a gravação falhar, fica só um aviso no log e a execução termina como terminaria sem o registro, com o mesmo erro e o mesmo código de saída. Há teste para os dois casos.

A tarefa `retrato` da DAG fica de fora. No alvo local ela só copia arquivo e não abre o Spark, para não pagar a subida da JVM, e sem Spark não tem como gravar no Delta. Se ela falhar, a falha aparece no Airflow e no alerta.

`make status` mostra as últimas execuções, de `data/` por padrão. `make status DATA_ROOT=data_sim` mostra as da simulação e `LIMITE=20` muda quantas aparecem (o padrão é 10). As horas saem no fuso da máquina.

### Alerta por e-mail

Toda tarefa da DAG tem um `on_failure_callback` que manda um e-mail pelo SMTP do Gmail, com a DAG, o `run_id`, a tarefa que falhou, a rodada, o alvo, o erro e o link do log. A lógica fica em `src/brasileirao/alerta.py`, sem importar o Airflow, e o teste usa um SMTP falso no lugar do `smtplib`.

A configuração vem do `orchestration/.env`, que fica fora do git, e o compose passa as variáveis para o container. O modelo com valores de exemplo está no `orchestration/.env.example`.

| Variável | O que é |
|---|---|
| `ALERTA_SMTP_HOST` | `smtp.gmail.com` |
| `ALERTA_SMTP_PORTA` | 587 com STARTTLS (padrão) ou 465 com SSL |
| `ALERTA_SMTP_USUARIO` | A conta do Gmail que envia |
| `ALERTA_SMTP_SENHA` | Uma senha de app do Gmail, não a senha da conta |
| `ALERTA_EMAIL_PARA` | Quem recebe |
| `ALERTA_EMAIL_DE` | Remetente, opcional (o padrão é o usuário) |

Sem host, usuário, senha ou destinatário, o alerta fica desligado e o callback só escreve isso no log da tarefa. Um envio que falha (senha errada, servidor fora do ar) também vira só um aviso no log. Depois de mudar o `.env`, é preciso subir o Airflow de novo com `make airflow-up`.

Com o Gmail configurado, repeti a falha na rodada 7, do mesmo jeito da rodada 6 descrita abaixo (`DELETE` da partida 8785, rodada disparada, partida devolvida pelo time travel e rodada de novo). O `gold` reprovou, e o log da tarefa terminou com esta linha.

```
alerta enviado para <meu e-mail>: falha de gold em manual__2026-11-12T16:43:02.189402+00:00
```

O e-mail chegou.

![E-mail do alerta com a DAG, o run_id, a tarefa gold, a rodada 7, o alvo local, o erro e o link do log](img/alerta-email.png)

O campo `erro` traz só o que o Airflow sabe, que o comando saiu com código 1. O motivo de verdade (o `QualidadeGoldError` com as checagens que reprovaram) está no log da tarefa, pelo link, e no `make status`.

### Prova no alvo local

Rodei em 12/11/2026, com o `data_sim` na rodada 4 e o alerta ainda sem configuração. Antes da primeira execução com o registro, a tabela não existia.

```
$ make status DATA_ROOT=data_sim
nenhuma execução registrada em data_sim/ops/execucoes
```

A rodada 5 passou inteira, em 106 s, e deixou três linhas, uma por tarefa do Spark.

```
$ make status DATA_ROOT=data_sim
últimas 3 execução(ões) em data_sim/ops/execucoes
2026-11-12 01:02:37  sucesso    17s  local  f8d1d046  simulacao --destino data_sim --temporada 2025 checar 5
2026-11-12 01:01:47  sucesso    48s  local  c0e7dbda  simulacao --destino data_sim --temporada 2025 gold 5
    linhas: 34500 (gold/artilharia 3382, gold/campanhas 470, gold/cartoes_jogadores 5891, gold/classificacao_por_rodada 17672, gold/confrontos 1518, gold/desempenho_mandante_visitante 940, gold/disciplina 240, gold/esquemas_taticos 194, gold/faltas_clubes 200, gold/goleadas 364, gold/gols_decisivos 240, gold/gols_por_minuto 144, gold/linha_corte 23, gold/passagens_tecnicos 859, gold/ranking_historico 46, gold/resumo_temporada 23, gold/sequencias 1880, gold/viradas 414)
2026-11-12 01:01:16  sucesso    30s  local  dfeb7ec4  simulacao --destino data_sim --temporada 2025 lakehouse 5
    linhas: 534 (bronze/cartoes 0, bronze/estatisticas 100, bronze/gols 117, bronze/partidas 50, silver/cartoes 0, silver/estatisticas 100, silver/gols 117, silver/partidas 50)
    aviso: brasileirao.transform.silver: silver/partidas temporada 2025: 50 partidas na temporada, esperado 380
    aviso: brasileirao.transform.silver: silver/estatisticas temporada 2025: nulos em colunas opcionais: precisao_passes=4
```

Depois repeti a falha da seção anterior. Apaguei a partida 8785 de `data_sim/silver/partidas` com um `DELETE` do Delta (versão 34) e disparei a rodada 6. O `gold` reprovou e as tarefas seguintes ficaram em `upstream_failed`.

```
$ make airflow-estado RUN=manual__2026-11-12T04:03:41.610202+00:00
dag_id             | logical_date | task_id           | state           | start_date                       | end_date
===================+==============+===================+=================+==================================+=================================
brasileirao_rodada |              | retrato           | success         | 2026-11-12T04:03:42.678609+00:00 | 2026-11-12T04:03:43.131963+00:00
brasileirao_rodada |              | bronze_silver     | success         | 2026-11-12T04:03:43.578808+00:00 | 2026-11-12T04:04:12.858945+00:00
brasileirao_rodada |              | gold              | failed          | 2026-11-12T04:04:13.046209+00:00 | 2026-11-12T04:04:46.763812+00:00
brasileirao_rodada |              | checagens         | upstream_failed | 2026-11-12T04:04:47.355101+00:00 | 2026-11-12T04:04:47.355101+00:00
brasileirao_rodada |              | registra_execucao | upstream_failed | 2026-11-12T04:04:47.644431+00:00 | 2026-11-12T04:04:47.644431+00:00
```

O log da tarefa `gold` mostra o registro gravado com a falha e, depois, o callback avisando que o alerta está desligado (trecho, só os campos `event`).

```
INFO brasileirao.execucoes: execução 682de55a-f811-4698-8936-8d047d325e46 registrada em ops/execucoes: falha
brasileirao.aggregate.qualidade.QualidadeGoldError: gold reprovado, nenhuma tabela gravada:
Task failed with exception
alerta desligado: faltam ALERTA_SMTP_HOST, ALERTA_SMTP_USUARIO, ALERTA_SMTP_SENHA, ALERTA_EMAIL_PARA no ambiente
alerta desligado: falha de gold em manual__2026-11-12T04:03:41.610202+00:00 não enviada
```

E a falha ficou na tabela, com o erro do gold.

```
$ make status DATA_ROOT=data_sim LIMITE=3
últimas 3 execução(ões) em data_sim/ops/execucoes
2026-11-12 01:04:13  falha      32s  local  682de55a  simulacao --destino data_sim --temporada 2025 gold 6
    aviso: Erro: QualidadeGoldError: gold reprovado, nenhuma tabela gravada:
           - classificacao_por_rodada: jogos na última rodada: 2024 Corinthians: 37 jogos, esperado 38, 2024 Grêmio: 37 jogos, esperado 38
           - gols_por_minuto: total de gols ≠ silver/gols: 2024: 926 na tabela, 929 na silver
           - viradas: placar reconstruído ≠ placar da partida: 8785
           - gols_decisivos: totais ≠ silver: 2024: jogos 758 na tabela, 758 na silver, gols_85 126 na tabela, 128 na silver, gols_acrescimo 74 na tabela, 75 na silver
2026-11-12 01:03:43  sucesso    28s  local  37791b91  simulacao --destino data_sim --temporada 2025 lakehouse 6
    linhas: 640 (bronze/cartoes 0, bronze/estatisticas 120, bronze/gols 140, bronze/partidas 60, silver/cartoes 0, silver/estatisticas 120, silver/gols 140, silver/partidas 60)
    aviso: brasileirao.transform.silver: silver/partidas temporada 2025: 60 partidas na temporada, esperado 380
    aviso: brasileirao.transform.silver: silver/estatisticas temporada 2025: nulos em colunas opcionais: posse_de_bola=2, precisao_passes=4
2026-11-12 01:02:37  sucesso    17s  local  f8d1d046  simulacao --destino data_sim --temporada 2025 checar 5
```

Devolvi a partida, lida da versão 33 pelo time travel, e disparei a rodada 6 de novo. Passou inteira, e a tabela ganhou as três linhas de sucesso por cima da falha.

```
$ make status DATA_ROOT=data_sim LIMITE=4
últimas 4 execução(ões) em data_sim/ops/execucoes
2026-11-12 01:06:43  sucesso    18s  local  c27929bd  simulacao --destino data_sim --temporada 2025 checar 6
2026-11-12 01:05:52  sucesso    50s  local  75765682  simulacao --destino data_sim --temporada 2025 gold 6
    linhas: 34538 (gold/artilharia 3394, gold/campanhas 470, ...)
2026-11-12 01:05:23  sucesso    27s  local  e5d0e0bc  simulacao --destino data_sim --temporada 2025 lakehouse 6
    ...
2026-11-12 01:04:13  falha      32s  local  682de55a  simulacao --destino data_sim --temporada 2025 gold 6
    ...
```

No grid do Airflow, da esquerda para a direita, aparecem as rodadas 2 e 3, a rodada 4 com falha e de novo, a rodada 1 no alvo `aws` (a barra alta, de 20 minutos), a rodada 5, a 6 com falha e de novo, e a 7 com falha e de novo.

![Grid da DAG brasileirao_rodada com dez execuções, três delas com o gold vermelho e checagens e registra_execucao em upstream_failed, cada uma seguida da mesma rodada verde](img/grid-observabilidade.png)

## Testes

A DAG tem dois níveis de teste, e o Airflow não entra no ambiente principal.

- `tests/unit/test_orquestracao.py`, na suíte normal (`make test`), testa o que a DAG roda sem importar o Airflow. Executa o script de cada tarefa com um `python` e um `aws` falsos e confere o destino de cada alvo, que a credencial exportada sai antes do perfil, que sem credencial o Spark nem começa e que um alvo desconhecido falha. Também confere que as versões da imagem batem com o `pyproject.toml` e o Makefile.
- `tests/unit/test_execucoes.py` e `tests/integration/test_registro_execucoes.py` conferem o registro em `ops/execucoes` com sucesso e com falha, no pipeline e na simulação, e que uma gravação que falha não derruba a execução nem muda o erro dela. `tests/unit/test_alerta.py` testa o alerta com um SMTP falso, com e sem configuração, na 587 e na 465, e que um envio que falha só vira aviso.
- `orchestration/tests/test_dag.py` (`make test-dag`) carrega a DAG no Airflow 3.4.0 de verdade. Confere que importa sem erro, não tem ciclo, segue a ordem das tarefas com a regra padrão de `upstream_failed`, conhece só os parâmetros `rodada` e `alvo`, recusa valores inválidos e tem o callback do alerta em toda tarefa. Roda num venv à parte (`.venv-airflow`), instalado com as constraints oficiais do Airflow, e no CI é o job `dag`, em paralelo ao `test`.

Separei porque acho que não vale pagar esse peso. O Airflow traz centenas de dependências com versões fixadas pelas constraints dele. No `uv.lock` elas disputariam versão com o PySpark e o Streamlit, e no job `test` somariam a instalação ao tempo que o [tuning](../tuning/README.md) cortou. Num venv próprio, a instalação leva segundos com o cache do uv, e os 11 testes da DAG, cerca de 2 s.
