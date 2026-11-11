# Sondagem da AWS no Free plan (AW1)

Data: 11/11/2026. Conta 548850331677, usuário IAM `romualdobarbosa` (AdministratorAccess), sessão do `aws login`. Região: `us-east-1`. Tudo feito pela CLI (aws-cli 2.38.0), com nome `brasileirao-sondagem*` e tag `projeto=brasileirao`, e apagado no fim.

## Resultado

| Serviço | Resultado |
|---|---|
| EMR Serverless | **Bloqueado.** `ListApplications`, `CreateApplication` e `emr list-release-labels` respondem `SubscriptionRequiredException: The AWS Access Key Id needs a subscription for the service`, em `us-east-1` e em `us-east-2`. Nenhuma aplicação foi criada, então não há release registrada. |
| Glue Jobs | **Bloqueado.** `CreateJob` responde `AccessDeniedException: Account 548850331677 is denied access` para `glueetl` nas versões 6.0, 5.1 e 5.0 (G.1X, 2 workers) e também para `pythonshell` (0,0625 DPU). Nenhum job foi criado. Leitura (`ListJobs`, `GetCrawlers`) funciona. |
| Glue Data Catalog | **Funciona.** `CreateDatabase` de `brasileirao_sondagem` deu certo. |
| Athena | **Funciona.** `SELECT 1 AS um` num workgroup próprio (`brasileirao-sondagem`, saída em S3, corte de 10 MB por consulta) terminou `SUCCEEDED` em 344 ms, 0 bytes lidos, engine 3, e devolveu `1`. |
| S3 | **Funciona.** Bucket criado com bloqueio público e tag. |
| IAM | **Funciona.** Role `brasileirao-sondagem-glue` com política inline restrita ao bucket da sondagem. |

O bloqueio não vem do IAM: `iam simulate-principal-policy` dá `allowed` para `glue:CreateJob`, `emr-serverless:CreateApplication` e `emr-serverless:ListApplications`, e a conta não está numa organização (`AWSOrganizationsNotInUseException`), então não há SCP. Os erros são da própria conta, o que bate com as restrições do Free plan.

Com EMR e Glue Jobs bloqueados, a spec mandava parar e me deixar decidir. Eu não quis fazer upgrade, porque aí a conta passa a poder cobrar no cartão. Fiquei com o Spark rodando na minha máquina e gravando o lake no S3, com o Terraform criando de verdade o bucket, o IAM, o Glue Catalog e o Athena. O EMR fica de fora, e a doc da AWS explica por quê.

## Passo a passo

1. `aws emr-serverless list-applications` → `SubscriptionRequiredException`.
2. `aws emr-serverless create-application --type SPARK --release-label emr-7.12.0 --maximum-capacity cpu=2vCPU,memory=4GB --auto-stop-configuration enabled=true,idleTimeoutMinutes=5` → `SubscriptionRequiredException` (a chamada é recusada antes de validar a release).
3. Bucket `brasileirao-sondagem-548850331677`, database `brasileirao_sondagem`, workgroup `brasileirao-sondagem`.
4. `SELECT 1` no Athena → `SUCCEEDED`.
5. Script PySpark de 5 linhas no bucket, role do Glue, `glue create-job` → `AccessDeniedException` (glueetl 6.0/5.1/5.0 e pythonshell).
6. Destruição: workgroup (com `--recursive-delete-option`), database, política e role, objetos e bucket.

## Custo

Saldo de créditos antes: US$ 119,95. Depois: US$ 119,95 (consulta de `get-account-plan-state` logo após a limpeza; o saldo pode atualizar com atraso). O único consumo possível é o Athena, com 0 bytes lidos, e meia dúzia de requisições S3.

## Listagem depois da limpeza

```
== us-east-1 / us-east-2
S3:                 sentinela-coins-548850331677-us-east-2-an   (de outro projeto)
Glue databases:     (nenhum)
Athena workgroups:  primary
Glue jobs:          (nenhum)
EMR Serverless:     SubscriptionRequiredException (nada pode existir)
IAM roles brasileirao*: (nenhuma)
Budgets:            "Alerta de 5 dolares", "My Monthly Cost Budget"   (já existiam)
Tag projeto=brasileirao (us-east-1): []
```

## Notas para o `cost_guard`

A conta já tem dois budgets criados antes deste ticket. O preço do AWS Budgets com mais budgets na conta não foi conferido nesta sondagem. A proposta é um único budget do projeto com três notificações (US$ 5, 10 e 15), sem ações, a confirmar no preço antes de criar.

## O que ficou de fora

O `infra/` em Terraform (bootstrap do state, storage, IAM, `cost_guard`) e o `apply`/`destroy` de teste não foram feitos: a sondagem caiu no portão antes. O Terraform 1.17.2 está instalado localmente (binário oficial, SHA256 e assinatura GPG da HashiCorp conferidos) e o `.gitignore` já cobre state e `.terraform/`.
