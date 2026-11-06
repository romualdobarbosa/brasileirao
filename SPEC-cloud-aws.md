# Spec: cloud-aws

> Status: **aprovada para execução, condicionada à sondagem do AW1**. Tickets AW1 a AW3 em [tasks/todo-fechamento.md](tasks/todo-fechamento.md). Substitui as partes correspondentes de `ADDENDUM_TERRAFORM_AWS.md` (fora do git).

## Objetivo

Rodar o mesmo pipeline no EMR Serverless, com toda a infraestrutura em Terraform e consulta do gold no Athena, **sem nenhum dinheiro de verdade**. Vale o ciclo `apply` → roda → `destroy`.

## Conta e regra de custo

Conta no **Free plan**, com US$ 120 de crédito e validade de 139 dias contados de 06/11/2026 (até 25/03/2027). Na documentação da AWS, o Free plan não gera cobrança durante o uso, e a conta é suspensa quando os créditos acabam ou o plano expira, com os dados apagados depois de 90 dias sem upgrade.

**Regras inegociáveis:**
1. **Nunca fazer upgrade para o Paid plan.** Só o Romualdo pode decidir isso, e nenhum ticket presume essa decisão.
2. **Teto de US$ 15 em créditos.** Antes de cada execução real, conferir o consumo; ao bater o teto, parar.
3. Missões de crédito (como a do EC2) não são necessárias e ficam fora do plano.
4. Sem VPC, NAT, EC2, RDS, capacidade pré-inicializada ou qualquer recurso sempre ligado.
5. `terraform destroy` ao fim de cada sessão de uso. Só o bucket do state fica entre sessões, e sai no fechamento do módulo.
6. Credenciais nunca em arquivo versionado. Em CI, só OIDC.
7. AWS Budgets em US$ 5, 10 e 15 serve como alarme, não como trava.

## Decisões

- **EMR Serverless** (`spark`), em `us-east-1`, com `maximum_capacity` baixo, parada automática em 5 min e workers pequenos.
- **Delta** continua o formato, e o Athena lê pelo Glue Catalog. AW3 confirma o suporte com a versão gravada; se a leitura falhar, ver se a causa é um recurso da tabela (como deletion vectors) antes de mudar qualquer coisa.
- **Sem Metabase conectado ao Athena.** O Metabase local já cobre esse lado.
- **Versões:** o projeto usa Spark 4.2.0 e Delta 4.4.1, mas a release encontrada (`emr-spark-8.0.0`) traz Spark 4.0.2 e Delta 4.0.0. AW2 confere a release mais recente e testa o código numa virtualenv com essa versão. Imagem própria em ECR só com aprovação, porque é mais um custo.
- **Empacotamento** com o mesmo cuidado do Databricks: sem `psycopg`/`kagglehub` no caminho do job.

## Sondagem do AW1 (vai ou não vai)

A documentação do Free plan diz que alguns serviços ficam restritos, mas as páginas lidas não citam EMR Serverless, Glue ou Athena. O primeiro passo, com o Romualdo logado, é criar uma aplicação EMR Serverless mínima, conferir Glue e Athena e destruir tudo.

**Se o EMR estiver bloqueado:** tentar Glue Jobs. **Se ambos estiverem:** o ticket para. O Terraform é entregue validado com `plan`, sem execução real, e o README diz exatamente isso. Upgrade só por decisão dele.

## Aceite

1. `terraform fmt -check` e `validate` no CI, sem credencial. `plan` só por `workflow_dispatch`, com OIDC. Nada de `apply` automático.
2. Pipeline completo termina no EMR, com contagens do gold iguais às do local, e toda diferença é explicada.
3. Consulta real no Athena retorna a classificação de uma temporada conhecida.
4. IAM sem `*` em recurso de dados, restrito aos buckets do projeto.
5. Depois do `destroy`, a listagem confirma que nada ficou de pé.
6. README com diagrama, passo a passo, gasto real e a nota de que o volume é pequeno e o Spark está ali para demonstrar a stack.
