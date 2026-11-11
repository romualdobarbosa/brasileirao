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

## Pipeline com o lake no S3 (AW2)

Rodei em 11/11/2026 o mesmo código do local, só trocando o ambiente (`BRASILEIRAO_SPARK_ENV=aws`) e a raiz (`BRASILEIRAO_DATA_ROOT=s3a://brasileirao-lake-548850331677`). A stack principal subiu com `plan -out` e `apply` do plano salvo (16 recursos, 67 s) e foi destruída no fim.

### Ambiente `aws` no `get_spark`

É o `local`, com as mesmas configs e o mesmo Delta 4.4.1, mais dois pacotes Maven fixados. O `org.apache.hadoop:hadoop-aws:3.5.0` tem de ser da versão do `hadoop-client-api-3.5.0.jar` que vem no PySpark 4.2.0, e o `software.amazon.awssdk:bundle:2.35.4` é o que o `hadoop-aws` 3.5.0 declara. O Ivy trouxe junto `analyticsaccelerator-s3` 1.3.1 e `wildfly-openssl` 2.2.5.Final. A credencial vem do `EnvironmentVariableCredentialsProvider` do SDK v2, que lê `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` e `AWS_SESSION_TOKEN`, e a região fica fixa em `us-east-1`. Para `s3a://` o Delta usa o `S3SingleDriverLogStore`, que só é seguro com um escritor por vez na mesma tabela, e o pipeline grava uma tabela de cada vez.

Não precisei mudar nada no pipeline. Nenhum trecho monta caminho com `os.path` sobre o lake, e `_metadata.file_path`, `DeltaTable.forPath`, `replaceWhere`, `MERGE` e as constraints funcionaram no S3 como estavam. Só o `foto.py` mudou, para listar as tabelas pelo `FileSystem` do Hadoop quando a raiz é uma URL.

### A credencial dura 15 minutos

Foi o que mais deu trabalho. A credencial que o `aws configure export-credentials` entrega a partir do `aws login` vale 15 min, a CLI só troca por outra quando faltam 5, e o JVM lê a credencial uma vez e não renova. A primeira tentativa do pipeline completo, num processo só, caiu na sexta temporada com `AWSBadRequestException ... Status Code: 400` num `getFileStatus`, que é a resposta do S3 a um token vencido.

Passei a rodar uma temporada por processo. Antes de cada uma, o `infra/credencial.sh` espera a credencial ter pelo menos 6 min de validade (14 no gold) e só então entrega. O script também limpa do ambiente qualquer credencial já exportada, porque com ela lá a CLI devolve a mesma credencial vencida em vez de renovar a sessão (isso travou duas retomadas antes de eu entender).

Comandos, com a stack de pé:

```bash
make raw-aws                      # CSVs de data/raw/ para s3://<lake>/raw/ (3 s)
make lakehouse-aws TEMPORADA=2023 # uma temporada
make lakehouse-aws                # 2003 a 2025, um processo por temporada
make gold-aws
make foto-aws SAIDA=docs/aws/foto_aws.json
```

### Tempos

| Etapa | Tempo |
|---|---:|
| Uma temporada (2023), lake vazio | 390 s no total, contando o download dos jars, 128,1 s da temporada, a checagem de órfãs e as 23 constraints da silver aplicadas pela primeira vez |
| Temporada, de 2003 a 2013 | 63 a 80 s cada |
| Temporada, de 2014 a 2025 | 117 a 134 s cada |
| Soma das 23 temporadas | 2308 s |
| Último trecho (2014 a 2025, 12 processos sem espera de credencial) | 2085 s, uns 174 s por processo |
| Gold, 18 tabelas | 236,9 s de processamento, 731 s contando a espera pela credencial de 14 min e as constraints |
| Foto do lake no S3 | 70 s |

O pipeline completo não rodou de uma vez. Foram quatro trechos (2003 a 2004, 2005 a 2009, 2010 a 2013 e 2014 a 2025) por causa da credencial, e as temporadas de 2003 a 2007 e a de 2023 foram reprocessadas por cima do que já estava no S3, sem duplicar nada, porque a carga usa `MERGE` e `replaceWhere`. O gold rodou três vezes antes da final, duas delas com a silver ainda incompleta. Vale a última, feita sobre a silver inteira.

No local o lakehouse leva de 5 a 9 s por temporada, e aqui leva de 63 a 134. A diferença é a latência do S3 em cada leitura e em cada commit do Delta.

### Comparação com o local

Rodei o mesmo `databricks/foto.py` sobre `./data` e sobre o lake no S3, e as duas fotos estão em [`foto_local.json`](foto_local.json) e [`foto_aws.json`](foto_aws.json). As 22 tabelas (4 da silver e 18 do gold) têm as mesmas linhas e o mesmo sha256. A foto local também é idêntica à do DB2 ([`../databricks/foto_local.json`](../databricks/foto_local.json)), então o local não mudou. A bronze fica fora da foto e tem a diferença que já apareceu no DB2. O `_source_file` traz `s3a://brasileirao-lake-548850331677/raw/...` em vez de `file:/.../data/raw/...`, e o `_ingested_at` muda a cada execução.

### Uso, custo e `destroy`

Antes do `destroy`, o bucket tinha 928 objetos e 11,0 MB (raw com 4 objetos e 4,6 MB, bronze com 283 e 2,7 MB, silver com 239 e 1,8 MB, gold com 402 e 1,8 MB).

Os créditos ficaram em **US$ 119,95** antes e depois. O saldo pode atualizar com atraso, mas o gasto aqui é só de requisições ao S3 e de alguns MB guardados por poucas horas.

O `destroy` usou um plano de destroy salvo e terminou em 12 s com `Resources: 0 added, 0 changed, 16 destroyed.`. Recriar tudo leva uns 70 s (67 s de `apply` e 3 s do raw), mais o pipeline.

Listagem em `us-east-1` depois do `destroy`:

```
S3:                         brasileirao-tfstate-548850331677, sentinela-coins-548850331677-us-east-2-an (de outro projeto)
Glue databases:             (nenhum)
Athena workgroups:          primary
IAM roles brasileirao*:     (nenhuma)
IAM policies brasileirao*:  (nenhuma)
Budgets:                    "Alerta de 5 dolares", "My Monthly Cost Budget" (já existiam)
Tag projeto=brasileirao:    arn:aws:s3:::brasileirao-tfstate-548850331677
Bucket do state:            main/terraform.tfstate (181 bytes)
```

### O que fica para o AW3

Nas tabelas locais, gravadas pelo mesmo código, o `_delta_log` tem `minReaderVersion 1`, sem deletion vectors nem table features de leitura, e o `minWriterVersion` vai de 2 a 3 quando as constraints entram. Isso deve bastar para o Athena ler, mas não conferi o log do S3 antes do `destroy`, então o AW3 começa subindo o lake de novo.
