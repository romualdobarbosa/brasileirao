# Spec: otimizacao

> Status: **aprovada para execução** em 06/11/2026, a partir do levantamento de gaps. Tickets OT1 e OT2 em [tasks/todo-fechamento.md](tasks/todo-fechamento.md).

## Objetivo

Medir onde o pipeline e a suíte gastam tempo, atacar o que a medição mostrar e aprender na prática as operações de manutenção e proteção de uma tabela Delta. O Romualdo precisa sair sabendo explicar cada uma delas.

Fora do escopo: cluster, teste de volume artificial, Great Expectations e reescrever transformações sem medição que justifique.

## OT1: tuning medido

Já existe: `shuffle.partitions=4`, `broadcast`, `localCheckpoint` e `docs/explain`. O item é medir, não adicionar config.

- **Linha de base:** `make lakehouse`, `make gold` e `pytest --durations=30`, três execuções cada, reportando a mediana, na mesma máquina. Do CI, o job `test` das últimas execuções via `gh run`.
- **Mudanças:** uma por vez, cada uma com o antes e depois. O que não mostrar ganho medido é revertido, e a tentativa fica registrada.
- **Saída:** `docs/tuning/README.md`, com a tabela de resultados, inclusive o que não funcionou.

**Aceite:**
1. Tabelas gold com conteúdo idêntico antes e depois (comparação por linhas ordenadas de todas as 18).
2. `make test` verde, sem teste removido ou enfraquecido para ganhar tempo.
3. Meta: job `test` do CI pelo menos 25% mais rápido que a linha de base. Se não der, o documento explica por quê, com números, e o ticket fecha assim mesmo.

## OT2: constraints e manutenção do Delta

- **Constraints:** `CHECK` nas tabelas silver e gold em regras que hoje só existem em `qualidade.py` (placar não negativo, rodada válida, cartão válido e equivalentes), aplicadas de forma idempotente por uma função chamada no pipeline. Regras que precisam de join ou agregação continuam em `qualidade.py`. As constraints não substituem as checagens atuais.
- **`make manutencao`:** `OPTIMIZE`, `VACUUM` e Z-Order. Registra arquivos e tamanho antes e depois. Retenção padrão do `VACUUM` mantida, sem desligar a trava de segurança.
- **Time travel:** `make historico TABELA=...` mostra versões, e uma função compara duas versões de uma tabela. Ela é reutilizada em OR1.
- **Saída:** `docs/delta/README.md`, em linguagem de quem está aprendendo, com a saída real dos comandos.

**Aceite:**
1. Teste de integração prova que uma escrita inválida é recusada pela constraint, venha de onde vier.
2. Pipeline rodado duas vezes não falha nem duplica constraint.
3. Teste prova que `VACUUM` remove versões antigas e que, depois dele, a leitura da versão removida falha; a retenção curta só existe no teste.
4. O documento não afirma ganho de velocidade: com 9.165 partidas, o esperado é não haver ganho visível, e ele diz isso. Só entra número medido.
5. **Portão de aprendizado:** a orquestradora explica ao Romualdo o que cada comando fez, usando as saídas reais, antes de marcar o ticket como pronto.
