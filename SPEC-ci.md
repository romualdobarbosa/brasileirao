# Spec: ci

> Módulo `ci` do [mapa de capacidades](CAPABILITY_MAP.md). Status: **aprovada** em 28/10/2026.

## Objetivo

Rodar lint e testes no GitHub Actions a cada mudança na `main` e em cada PR, com Spark local e Delta, sem token nenhum. É o selo que o recrutador vê no README e a rede de segurança antes do `serving` começar a ler o gold.

Fora do escopo: deploy, checks de Terraform (entram com o `cloud-aws`), build de imagem, publicação de pacote, rodar o pipeline com os dados reais (o CI não tem token do Kaggle).

### Critérios de aceite (histórias)

1. Como dev, dou push na `main` e o workflow roda lint e testes. Se algo quebra, o commit fica vermelho no GitHub.
2. Como dev, abro um PR para a `main` e o mesmo workflow roda no PR.
3. Como visitante do repositório, vejo no topo do README um badge com o status do CI.
4. Um push novo no mesmo branch cancela a execução anterior que ainda estiver rodando.

## Tech Stack

GitHub Actions (runner `ubuntu-latest`), `astral-sh/setup-uv` com cache do `uv.lock`, Python 3.12, `actions/setup-java` (Temurin 21; o Spark 4 roda em Java 17 ou 21), `actions/cache` para os jars do Delta que o `configure_spark_with_delta_pip` baixa do Maven.

## Commands

O workflow chama os mesmos alvos do Makefile que o dev usa local:

```
uv sync --all-groups --locked
make lint
make test
```

## Project Structure

```
.github/workflows/ci.yml   # único workflow
README.md                  # + badge
```

## Desenho do workflow

- **Gatilhos:** `push` na `main` e `pull_request` para a `main`. O push é o gatilho que importa no dia a dia, porque os commits vão direto na `main`.
- **Dois jobs em paralelo:** `lint` (sem Java nem Spark, falha em segundos) e `test` (Java + Spark). Um não espera o outro.
- **Permissões mínimas:** `contents: read` no topo do workflow.
- **Concorrência:** grupo por workflow + ref, com `cancel-in-progress: true`.
- **Timeout:** 15 min no job de teste (hoje a suíte leva ~3min15s local) e 5 min no de lint.
- **Cache:** o `setup-uv` cuida do cache das dependências Python. O cache dos jars do Delta (diretório do Ivy) usa uma chave com a versão do `delta-spark` do `uv.lock`.
- **Actions fixadas** em versão maior (`@v7`, `@v6`…). Exceção: o `astral-sh/setup-uv` não publica tag maior móvel, então fica na tag completa (`@v10.3.3`).

## Boundaries

- **Sempre:** o CI roda exatamente `make lint` e `make test`, sem atalho que o dev não tenha local. Teste nunca depende de rede além do download de dependências.
- **Perguntar antes:** adicionar segredo no repositório, mudar a suíte ou o Makefile pra caber no CI, criar outro workflow.
- **Nunca:** pular teste, colocar `continue-on-error` ou baixar a exigência pra ficar verde.

## Success Criteria

1. `.github/workflows/ci.yml` existe e passa no `actionlint`.
2. Depois do push, a execução na `main` fica verde nos dois jobs, com a suíte completa (209+ testes).
3. A segunda execução usa os caches (uv e jars do Delta), o que se nota no log e no tempo.
4. O README tem o badge, apontando pro workflow certo.

## Decisões

- **Sem matriz de versões:** o projeto fixa Python 3.12 e uma versão do PySpark. Testar várias combinações não agrega nada.
- **Java 21 no CI**, mesmo com o Java 25 local: é a versão LTS suportada oficialmente pelo Spark 4.
- **Sem limite mínimo de cobertura** por enquanto: a cobertura está em 99% e o relatório já sai no log do `make test`.

## Open Questions

Nenhuma.
