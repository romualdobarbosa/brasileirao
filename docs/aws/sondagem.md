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

## Base em Terraform (AW1)

Data: 11/11/2026. Terraform 1.17.2, provider `hashicorp/aws` 6.72.0 (o mais recente estável no registry nesse dia), região `us-east-1`, credencial da sessão do `aws login` via `aws configure export-credentials --format env`. Código em [`infra/`](../../infra/README.md). Cada `apply` partiu de um `plan` salvo e conferido.

**Bootstrap:** 7 recursos criados, o bucket `brasileirao-tfstate-548850331677` (versionamento, SSE-S3, bloqueio público, só HTTPS, versões antigas expiram em 30 dias). Fica de pé até o fechamento do módulo.

**Stack principal:** `apply` com 16 recursos criados:

```
bucket_lake       = "brasileirao-lake-548850331677"
bucket_athena     = "brasileirao-athena-548850331677"
glue_database     = "brasileirao"
athena_workgroup  = "brasileirao"
politica_lake_arn = "arn:aws:iam::548850331677:policy/brasileirao-lake"
```

Conferências com a stack de pé:
- `plan` logo depois do `apply`: nenhuma diferença.
- `main/terraform.tfstate` no bucket do state, com lock nativo do S3.
- `SELECT 1 AS um` no workgroup `brasileirao`: `SUCCEEDED`, 0 bytes lidos, resultado gravado em `s3://brasileirao-athena-548850331677/resultados/`.
- `iam simulate-custom-policy` com a política `brasileirao-lake`: `s3:PutObject` permitido no lake e em `resultados/` do bucket do Athena, negado no bucket de outro projeto; `glue:GetTable` negado num database de fora.
- Budget `brasileirao-cost-guard`: US$ 15 por mês, alertas em 5, 10 e 15, `IncludeCredit = False`. Sem tirar o crédito, o custo líquido no Free plan fica em zero e o alarme nunca dispara. Vale para a conta inteira, porque filtrar pela tag pediria ativar a tag de alocação de custo. É o terceiro budget da conta; pela [página de preços](https://aws.amazon.com/aws-cost-management/aws-budgets/pricing/), budget sem ações é gratuito ("You can monitor and receive notifications on your budgets free of charge"); só budgets com ações passam a cobrar depois dos dois primeiros.

**Destroy:** `Apply complete! Resources: 0 added, 0 changed, 16 destroyed.`

Listagem em `us-east-1` depois do `destroy`:

```
S3:                         brasileirao-tfstate-548850331677, sentinela-coins-548850331677-us-east-2-an (de outro projeto)
Glue databases:             (nenhum)
Athena workgroups:          primary
IAM roles brasileirao*:     (nenhuma)
IAM policies brasileirao*:  (nenhuma)
Budgets:                    "Alerta de 5 dolares", "My Monthly Cost Budget" (já existiam)
Tag projeto=brasileirao:    arn:aws:s3:::brasileirao-tfstate-548850331677
Bucket do state:            main/terraform.tfstate (181 bytes, state vazio)
```

Créditos: US$ 119,95 antes e depois (o saldo pode atualizar com atraso). O gasto possível é uma consulta do Athena com 0 bytes lidos e algumas dezenas de requisições S3.
