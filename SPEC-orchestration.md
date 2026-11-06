# Spec: orchestration

> Status: **aprovada para execução**. Substitui o módulo `orchestration-aws` (Step Functions). Tickets OR1 a OR3 em [tasks/todo-fechamento.md](tasks/todo-fechamento.md).

## Objetivo

Fazer o pipeline aguentar dado que muda, rodar agendado no Airflow e avisar quando algo der errado, para que o agendamento tenha um motivo real.

Fora do escopo: MWAA, Step Functions, Prometheus, Grafana, CDC de banco de dados e mexer no gold publicado do app.

## OR1: dado que muda (simulação)

A versão 18 é a mais recente do Kaggle (conferido em 06/11/2026) e termina em 07/12/2025, então **não há temporada real em andamento**. Vamos simular, e todo documento diz que é simulação.

- `brasileirao.simulacao` gera, para a rodada N de 2025, um retrato da fonte só com o que já teria acontecido, mais eventos de `reference/simulacao_eventos.csv`: jogo adiado que aparece depois e placar corrigido.
- **Isolamento:** tudo em `BRASILEIRAO_DATA_ROOT=data_sim`. Teste garante que a simulação recusa `data/` como destino, e `app/dados` nunca é escrito.
- **Mudanças esperadas no pipeline:** opção explícita para aceitar temporada em andamento (hoje `validar_temporada` recusa), e checagens do gold que entendam temporada parcial (`jogos_finais_invalidos` hoje exige a temporada completa). A execução normal continua recusando.
- **Change Data Feed** ligado em `silver/partidas` na simulação; o relatório mostra o que mudou entre rodadas e usa a comparação de versões do OT2.

**Aceite:**
1. Reprocessar as rodadas de 1 a 38, com os eventos, chega ao mesmo gold de 2025 do pipeline normal, salvo as correções injetadas, que são explicadas.
2. O `MERGE` atualiza só as linhas alteradas, com prova em teste.
3. Rodar a mesma rodada de novo não cria versão nova em `silver/partidas`.

## OR2: Airflow

- **Airflow em Docker local**, versão fixada, com `LocalExecutor` e banco próprio em `orchestration/docker-compose.yml`, separado do serving. A imagem traz Java e o projeto.
- **DAG `brasileirao_rodada`**: gera o retrato → bronze/silver → gold → checagens → registra execução. Parâmetros: `rodada` e `alvo` (`local` ou `emr`).
- **Alvo padrão `local`**, sem custo. `emr` só roda por disparo manual, com a infraestrutura de pé e o portão de gasto do AWS.
- Limite conhecido: o agendamento só dispara com a máquina ligada, e a documentação diz isso.

**Aceite:** DAG validada por teste (importa, sem ciclos, parâmetros conhecidos); execução real de pelo menos 3 rodadas seguidas no alvo `local`; falha em uma tarefa impede as seguintes; uma execução real no alvo `emr` se AW2 tiver passado, seguida de `destroy`.

## OR3: observabilidade

- **Tabela `ops/execucoes`** (Delta): id, comando, alvo, início, fim, status, linhas por tabela e avisos. Toda execução do `pipeline.py` grava, inclusive em falha.
- **Alerta:** callback de falha do Airflow envia a um webhook definido por variável de ambiente, sem segredo no repositório. Destino real escolhido com o Romualdo no ticket.
- `make status` mostra as últimas execuções.

**Aceite:** teste cobre execução com sucesso e com falha; falha provocada de verdade gera registro e alerta recebido; demonstração em `docs/orchestration/README.md` com saídas reais e screenshots; falha ao gravar o registro não derruba o pipeline.
