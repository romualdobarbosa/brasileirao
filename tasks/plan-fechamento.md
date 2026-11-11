# Plano: fechamento do projeto

Specs: [otimizacao](../SPEC-otimizacao.md), [enrichment](../SPEC-enrichment.md), [databricks](../SPEC-databricks.md), [cloud-aws](../SPEC-cloud-aws.md), [orchestration](../SPEC-orchestration.md). Tickets: [todo-fechamento.md](todo-fechamento.md).

Ordem fechada pelo Romualdo em 06/11/2026. **Quando o último ticket fechar, o projeto é declarado pronto.** Fora de vez: cluster/escala, Structured Streaming, serving em produção e Great Expectations.

## Sequência

```
OT1 → OT2 → EN1 → EN2 → DB1 → DB2 → AW1 → AW2 → AW3 → OR1 → OR2 → OR3 → FIM
tuning  Delta   enriquecimento  Databricks   AWS + Terraform   dado que muda, Airflow, observabilidade
```

Cada ticket foi dimensionado para 150–200 mil tokens. É estimativa de planejamento, não medição; o tamanho real entra no relatório para corrigir o resto da fila.

## Como roda

- **Orquestradora:** uma sessão principal, que não implementa. Dispara um subagente de contexto limpo por ticket, em sequência, sem paralelismo, porque o working tree e `data/` são compartilhados.
- **Subagente:** recebe o ticket e as regras abaixo, implementa, marca seus checkboxes e devolve um relatório curto.
- **Conferência:** a orquestradora refaz lint e testes, lê o diff e confere os critérios do ticket por conta própria. Só então commita.
- **Commits:** automáticos, um por ticket, na `main`, sem pedir ok. O push vai junto, porque o CI faz parte da conferência; o ticket só fecha com o CI verde. Precisa de `SSH_AUTH_SOCK` ativo: abrir a sessão principal depois de `eval "$(ssh-agent)" && ssh-add`.
- **Se o ticket estourar:** perto de 200 mil tokens, o subagente para em ponto estável e entrega o que falta. A orquestradora abre um ticket de continuação.

## Regras dos subagentes

- Não commitar. Não editar specs nem este plano; no todo, só marcar a própria tarefa.
- Divergência com a spec ou com os dados: parar e reportar, sem decidir sozinho.
- Rodar `make lint` e `make test` antes de reportar. Reportar falhas como falhas.
- Não remover nem enfraquecer teste ou checagem para o ticket passar.
- **Não afirmar nada que não rodou**, em doc, README ou relatório.
- Não criar conta, não executar nada que consuma crédito e não publicar: isso é portão.
- Texto público novo fica marcado para a orquestradora passar pela `voz-romualdo`.

## Portões: a fila para e chama o Romualdo

| Ticket | O que precisa dele |
|---|---|
| OT2 | Explicação do Delta (aprendizado) antes de fechar |
| EN1 | Criar o projeto no Google Cloud e autorizar; decidir se a sondagem falhar |
| DB2 | Criar a conta no Databricks e enviar os arquivos, com a orquestradora guiando |
| AW1 | `aws login`; decidir se o Free plan bloquear o EMR |
| AW2 | Qualquer gasto acima do teto (o Free plan bloqueou o EMR; o lake vai pro S3 com Spark local) |
| OR2 | Ok para a execução no alvo `aws` |
| OR3 | Escolher o destino do alerta |
| FIM | Revisar os textos públicos e decidir o que entra no currículo |

Fora desses pontos, a fila anda sozinha. Qualquer gasto de dinheiro real, upgrade de plano, ação irreversível fora do escopo ou mudança de escopo também é portão, mesmo sem estar na tabela.

## Riscos conhecidos

| Risco | Tratamento |
|---|---|
| Tabela da Base dos Dados não existe ou é fraca | Sondagem no EN1; padrão é cancelar o módulo |
| `localCheckpoint` falha no serverless | `materializar(df)` por ambiente (DB1) |
| Spark 4.2 local × 4.0 no EMR | Testar com a versão do EMR antes de gastar (AW2) |
| Free plan bloqueia EMR, Glue ou Athena | Sondagem no AW1; fallback definido na spec |
| Créditos expiram em 25/03/2027 | AWS precisa estar concluída antes; conta suspensa depois disso. Outros recursos dessa conta, como os do Sentinela Coins, têm o mesmo prazo |
| Gold não aceita temporada parcial | Previsto no OR1, com a execução normal inalterada |
| Suíte do CI estoura 15 min com os novos testes | OT1 reduz antes; cada ticket acompanha o tempo do job |
