# Tarefas: fechamento do projeto

Plano e regras: [plan-fechamento.md](plan-fechamento.md). Um subagente por ticket, na ordem. 🚧 = portão com o Romualdo.

## OT1: tuning medido. Spec: otimizacao
- [x] Linha de base local e do CI registrada
- [x] Mudanças testadas uma a uma, sem ganho = revertida e registrada
- [x] Gold idêntico nas 18 tabelas; `make test` verde
- [x] `docs/tuning/README.md` com o antes e depois, inclusive o que não funcionou

## OT2: constraints e manutenção do Delta. Spec: otimizacao
- [x] Constraints idempotentes no pipeline, com teste de recusa
- [x] `make manutencao` (OPTIMIZE, VACUUM, Z-Order), com arquivos e tamanho antes e depois
- [x] `make historico` e função de comparação de versões
- [x] `docs/delta/README.md` com saídas reais
- [x] 🚧 Explicação ao Romualdo com as saídas reais

## OT3: custo das constraints na suíte. Decidido com o Romualdo em 09/11/2026
- [x] Medição local por arquivo e por fixture, antes e depois
- [x] Redução sem remover nem enfraquecer teste; recusa das constraints segue testada
- [x] `make test` e `make lint` verdes; job `test` do CI abaixo de 680 s (476 s, run 42683903508)
- [x] `docs/tuning/README.md` ganha a seção com o antes e depois

## EN1: sondagem e entrada do enriquecimento. Spec: enrichment. **Cancelado em 10/11/2026: licença desconhecida**
- [x] 🚧 Romualdo cria o projeto Google e autoriza o acesso local (projeto `brasileirao-numeros`, Sandbox; credencial do pydata-google-auth em ~/.config/pydata/)
- [x] Sondagem registrada: colunas, temporadas, nulos e licença. Se reprovar, 🚧 decisão
- [-] ~~`make download-enriquecimento` congela o arquivo em `raw/`~~
- [-] ~~Limpeza e silver, com mapa de times e fixture de teste~~
- [-] ~~Taxa de casamento ≥ 95%, com avisos do que não casa~~

## EN2: gold e app do enriquecimento. Spec: enrichment. **Cancelado junto com o EN1**
- [-] ~~Tabelas gold novas, com checagens e testes~~
- [-] ~~18 tabelas anteriores inalteradas~~
- [-] ~~`make exportar` e página nova no app~~
- [-] ~~Texto da página marcado para a `voz-romualdo`; README cita fonte e cobertura~~

## DB1: código pronto para o Databricks. Spec: databricks
- [x] `get_spark` com ambiente `databricks`, usando a sessão existente
- [x] Imports de `serving` e `exportar` dentro dos subcomandos
- [x] `materializar(df)` por ambiente, se a sondagem do DB2 pedir; deixar a troca preparada
- [x] Wheel puro sem `pyspark`/`delta-spark` como dependência, e `make wheel`
- [x] Testes unitários do ambiente; local inalterado

## DB2: pipeline rodando no Databricks. Spec: databricks
- [x] 🚧 Romualdo cria a conta e envia CSVs e wheel a um Volume (`/Volumes/workspace/default/brasileirao/{raw,wheels}`; acesso local por token em ~/.databrickscfg)
- [x] Sondar `localCheckpoint`, DeltaTable e `_metadata` num notebook
- [x] Job bronze → silver → gold termina com sucesso
- [x] Contagens batem com o local, e diferenças são explicadas
- [x] `docs/databricks/README.md` com screenshots e limites encontrados

## AW1: sondagem e base em Terraform. Spec: cloud-aws
- [x] Instalar Terraform com versão fixada; `.gitignore` de state e `.terraform`
- [x] 🚧 `aws login` do Romualdo; conferir plano e saldo (usuário IAM romualdobarbosa, AdministratorAccess; plano FREE ativo, US$ 119,95, vence 2027-03-23)
- [x] Sondar EMR Serverless, Glue e Athena no Free plan e destruir. Se bloquear, 🚧 decisão (EMR e Glue Jobs bloqueados; Romualdo escolheu Spark local com lake no S3 em 11/11/2026)
- [x] `infra/`: bootstrap do state, storage, IAM e `cost_guard`
- [x] `apply` e `destroy` testados, listagem sem sobras (só o bucket do state fica)

## AW2: pipeline com o lake no S3. Spec: cloud-aws (revisão de 11/11)
- [x] Ambiente `aws` no `get_spark` (Spark local, `hadoop-aws` fixado, credencial só do ambiente), com testes
- [x] Prova com uma temporada no S3
- [x] Pipeline completo no S3; foto de silver e gold igual à do local; gasto registrado; `destroy`

## AW3: catálogo, CI e documentação da AWS. Spec: cloud-aws
- [x] Credencial por perfil com `credential_process` (renova sozinha); pipeline completo num processo só no S3, foto igual à do local
- [ ] Glue e Athena em Terraform; consulta real de uma classificação conhecida
- [ ] CI: `fmt` e `validate`; `plan` por `workflow_dispatch` com OIDC
- [ ] `docs/aws/README.md`: diagrama, passo a passo, gasto real e screenshots
- [ ] `destroy` final, listagem sem sobras

## OR1: dado que muda. Spec: orchestration
- [ ] `brasileirao.simulacao`, com eventos e isolamento em `data_sim`, com teste
- [ ] Opção explícita de temporada em andamento; execução normal continua recusando
- [ ] Checagens do gold para temporada parcial
- [ ] Change Data Feed em `silver/partidas` e relatório entre rodadas
- [ ] Rodadas 1–38 reproduzem o gold de 2025, salvo correções explicadas

## OR2: Airflow. Spec: orchestration
- [ ] Compose e imagem em `orchestration/`, com versão fixada
- [ ] DAG `brasileirao_rodada` com testes
- [ ] 3 rodadas reais seguidas no alvo `local`; falha interrompe as seguintes
- [ ] 🚧 Execução no alvo `aws`, se AW2 passou, seguida de `destroy`

## OR3: observabilidade. Spec: orchestration
- [ ] `ops/execucoes` gravada em sucesso e falha, sem derrubar o pipeline
- [ ] `make status`
- [ ] 🚧 Destino do alerta escolhido; falha real gera alerta recebido
- [ ] `docs/orchestration/README.md` com saídas reais e screenshots

## FIM: fechamento. Executado pela orquestradora
- [ ] README e `CAPABILITY_MAP.md` batem com o que rodou; toda afirmação tem evidência
- [ ] Textos públicos passam pela `voz-romualdo`; 🚧 Romualdo revisa
- [ ] Conferir `destroy` da AWS, incluindo o bucket do state, e créditos gastos
- [ ] Limpar o que sobrou da nuvem cancelada no Makefile (`deploy`, `carga-remota`, `backup-metabase`), se não tiver uso
- [ ] Fatos enviados à sessão `resume-studio`; 🚧 Romualdo decide o que entra no currículo
- [ ] CI verde; projeto declarado pronto
