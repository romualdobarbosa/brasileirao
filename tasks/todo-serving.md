# Tarefas: serving

Plano: [plan-serving.md](plan-serving.md). Spec: [SPEC-serving.md](../SPEC-serving.md).

Toda tarefa de código só conta como feita com `make lint` e `make test` passando.

---

## Fase 1: Fatia vertical local

### T1: Compose local, `initdb/` e `serving-up` / `serving-down`

**Descrição:** Criar `serving/docker-compose.yml` com `postgres` (17.x, tag fixa) e `metabase` (open source, tag fixa com `linux/arm64`, conferido por `docker manifest inspect`). Os dois publicam porta só em `127.0.0.1` (5432 e 3000), com volume nomeado e `restart: unless-stopped`. O Metabase usa o banco `metabase` do mesmo Postgres como banco de aplicação (`MB_DB_*`) e `JAVA_OPTS` com `-Xmx` fixo. O Caddy fica pra T6.

`initdb/`:
- `01-papeis.sql`: papéis `carga`, `leitor` e `metabase` com `LOGIN` e sem senha; banco `metabase` com dono `metabase`; `REVOKE CONNECT ... FROM PUBLIC` nos dois bancos, para cada papel só conectar onde deve.
- `02-gold.sql`: schemas `gold` e `gold_carga` com dono `carga`; as 8 tabelas de `gold` com tipos explícitos e PK pelo grão do contrato do gold; grants do `leitor` (`USAGE` em `gold`, `SELECT` nas tabelas, `ALTER DEFAULT PRIVILEGES` para tabelas futuras); nada para o `leitor` em `gold_carga`.
- `03-senhas.sh`: `ALTER ROLE ... PASSWORD` para os três papéis, lendo do ambiente.

Os `.sql` precisam aguentar ser aplicados de novo num cluster onde os papéis e o banco `metabase` já existem: o teste do T3 aplica os dois num banco descartável do mesmo Postgres local, e papel é do cluster inteiro. Papéis com bloco `DO` que checa `pg_roles`. `CREATE DATABASE` não roda em bloco `DO` nem em transação: deixar essa instrução isolada no fim do `01-papeis.sql` e documentar que a fixture do T3 cria o banco `metabase` por fora se ele faltar. Se achar saída melhor, perguntar à sessão principal antes.

PKs: `classificacao_por_rodada (temporada, rodada, clube)`, `desempenho_mandante_visitante (temporada, clube, mando)`, `sequencias (temporada, clube, tipo)`, `ranking_historico (clube)`, `disciplina (temporada, clube)`, `artilharia (temporada, atleta)`, `gols_por_minuto (temporada, faixa, lado)`, `resumo_temporada (temporada)`. Tipos: `int` → `integer`, `bigint` → `bigint`, `double` → `double precision`, `string` → `text`, `date` → `date`, conferidos contra o schema real em `data/gold/`.

`serving/.env.example` com todas as variáveis, sem valor real. `.env` e `backups/` no `.gitignore`. Alvos `serving-up` e `serving-down` no Makefile (`docker compose -f serving/docker-compose.yml --env-file .env ...`).

**Critérios de aceite:**
- [x] `make serving-up` sobe os dois containers saudáveis (healthcheck no Postgres); `make serving-down` derruba sem apagar volume
- [x] Num volume novo, `psql` como superusuário mostra os 3 papéis, os 2 bancos, os 2 schemas e as 8 tabelas com PK
- [x] `leitor` conecta em `brasileirao`, faz `SELECT` em `gold.*` e leva erro de permissão em `INSERT`, em `CREATE TABLE gold.x`, em `gold_carga` e ao conectar no banco `metabase`
- [x] `ss -tlnp` (ou `docker port`) mostra 5432 e 3000 só em `127.0.0.1`
- [x] Nenhuma tag `latest`; tag do Metabase com manifest `linux/arm64` (registrar a saída no relatório)
- [x] `.env` fora do git

**Verificação:**
- [x] `make serving-up`, os `psql` acima e `make serving-down`, colados no relatório

**Dependências:** nenhuma

**Arquivos:** `serving/docker-compose.yml`, `serving/.env.example`, `serving/initdb/{01-papeis.sql,02-gold.sql,03-senhas.sh}`, `Makefile`, `.gitignore`

**Tamanho:** M

### T2: Config do Postgres, driver JDBC no Spark, `psycopg` e `tabelas.py`

**Descrição:**
- `config.py`: funções para `BRASILEIRAO_PG_HOST` (padrão `localhost`), `_PORT` (padrão `5432`), `_DB` (padrão `brasileirao`), `_USUARIO` (padrão `carga`) e `_SENHA` (sem padrão: erro claro se faltar). Uma função monta a URL JDBC e as propriedades.
- `utils/spark.py`: `get_spark` ganha `pacotes: list[str] | None`, repassado como `extra_packages` ao `configure_spark_with_delta_pip`.
- `serving/tabelas.py`: `TABELAS`, lista ordenada de `Tabela(nome, colunas, pk)` para as 8 tabelas, e a constante com a coordenada Maven do driver (`org.postgresql:postgresql:42.7.x`, versão exata conferida no Maven Central).
- `uv add "psycopg[binary]"`, com o `uv.lock` atualizado.
- A fixture `spark` de sessão (`tests/conftest.py`) passa a pedir o driver JDBC, já que a sessão é criada uma vez e não aceita pacote depois.

**Critérios de aceite:**
- [x] Teste: os nomes de `TABELAS` são exatamente as chaves do registro de `aggregate/gold.py`, na mesma ordem
- [x] Teste: as colunas de cada `Tabela` batem, em nome e ordem, com o schema que o gold grava (gold montado das fixtures `raw_gold`)
- [x] Teste: a URL JDBC e as propriedades saem certas do ambiente; sem `BRASILEIRAO_PG_SENHA`, erro que diz qual variável falta
- [x] Teste: `get_spark` com pacotes extras repassa a lista (sem precisar baixar nada no teste unitário)
- [x] `make test` continua verde e o tempo da suíte não sobe mais que uns 10 s (subiu 21 s com a montagem extra do gold nos testes do serving; aceito pelo Romualdo em 05/11/2026)

**Verificação:**
- [x] `uv run pytest tests/unit/serving/ tests/unit/test_config.py` e `make test`

**Dependências:** T1 (só pelo DDL, que os testes do T3 vão comparar)

**Arquivos:** `pyproject.toml`, `uv.lock`, `src/brasileirao/config.py`, `src/brasileirao/utils/spark.py`, `src/brasileirao/serving/{__init__,tabelas}.py`, `tests/conftest.py`, `tests/unit/serving/test_tabelas.py` (+ ajustes em `tests/unit/test_config.py`)

**Tamanho:** S

### T3: Carga com troca transacional, CLI `carga`, `make carga` e integração

**Descrição:**
- `serving/postgres.py`: conexão psycopg a partir do config; `trocar_gold(conexao)` (`TRUNCATE` + `INSERT ... SELECT` das 8 tabelas numa transação, identificadores só via `psycopg.sql` e só da lista `TABELAS`); `contar(conexao, schema)`.
- `serving/carga.py`: `carregar(spark)` lê cada tabela gold (erro claro mandando rodar `make gold` se não existir), grava em `gold_carga.<tabela>` via JDBC com `mode("overwrite")`, chama `trocar_gold` e confere as contagens de `gold` contra as do Delta (diferença é falha). Devolve as contagens.
- CLI: subcomando `carga` em `pipeline.py`, que cria a sessão com o driver JDBC. Alvo `make carga` carregando o `.env`.
- Marcador `postgres` registrado no `pyproject.toml`. Fixture que, com `BRASILEIRAO_PG_TESTE_URL` (superusuário), cria um banco descartável, aplica `01-papeis.sql` e `02-gold.sql` via psycopg, define senhas de teste e aponta o `BRASILEIRAO_PG_*` pra ele. Sem a variável, `pytest.skip` com o motivo.
- `.env.example` ganha `BRASILEIRAO_PG_TESTE_URL` apontando pro Postgres local do compose.

**Critérios de aceite (todos em `tests/integration/test_serving.py`, marcados `postgres`):**
- [x] Gold das fixtures → `carregar` → contagens de `gold` iguais às do Delta, e algumas linhas conferidas valor a valor
- [x] `carregar` 2× deixa o conteúdo de `gold` igual ao de 1×
- [x] Falha no meio de `trocar_gold` (ex.: tabela de `gold_carga` com coluna faltando) deixa `gold` exatamente como antes
- [x] O OID das tabelas de `gold` não muda entre duas cargas
- [x] O DDL do `02-gold.sql` (lido do `information_schema`) bate com `TABELAS`: nomes, colunas em ordem e PK
- [x] `leitor`: `SELECT` em `gold` funciona; `INSERT`, `CREATE`, acesso a `gold_carga` e conexão ao banco `metabase` falham
- [x] Sem gold, a carga falha com a mensagem que manda rodar `make gold`
- [x] Sem `BRASILEIRAO_PG_TESTE_URL`, os testes aparecem como pulados com o motivo; `make test` continua verde
- [x] Nos dados reais: `make gold && make serving-up && make carga` deixa as 8 tabelas com as contagens do Delta (colar no relatório)

**Verificação:**
- [x] `BRASILEIRAO_PG_TESTE_URL=... uv run pytest -m postgres` com o compose local de pé, e `make test`

**Dependências:** T2

**Arquivos:** `src/brasileirao/serving/{postgres,carga}.py`, `src/brasileirao/pipeline.py`, `Makefile`, `pyproject.toml`, `serving/.env.example`, `tests/integration/test_serving.py` (+ fixture em `tests/integration/conftest.py`)

**Tamanho:** M

---

## Fase 2: CI, cards e peças de deploy

### T4: Postgres no CI

**Descrição:** O job `test` do `ci.yml` ganha um service container `postgres` (mesma tag do compose), com healthcheck, e `BRASILEIRAO_PG_TESTE_URL` no ambiente do `make test`. A chave do cache do Ivy passa a incluir a versão do driver JDBC além da do `delta-spark`.

**Critérios de aceite:**
- [ ] `actionlint` limpo
- [ ] No CI, os testes `postgres` rodam (não aparecem como pulados) e passam
- [ ] O tempo do job `test` sobe pouco (registrar antes e depois)
- [ ] Nenhum segredo novo no repositório: a senha do Postgres do CI é descartável e fica no próprio workflow

**Verificação:**
- [ ] `actionlint` local. O push e a conferência da execução ficam com a sessão principal (o agente não commita)

**Dependências:** T3

**Arquivos:** `.github/workflows/ci.yml`

**Tamanho:** XS

### T5: SQL dos 12 cards em `docs/serving/dashboard.md`

**Descrição:** Para cada pergunta da seção "Dashboard" da spec: aba, pergunta, tabela, colunas, tipo de gráfico, filtros ligados (`temporada`, `rodada`, `clube`) e o SQL equivalente, rodado no Postgres local como `leitor` contra o gold real. Inclui o cartão de notas (buracos da fonte, W.O. de 2016, Barueri e Grêmio Prudente) como rascunho, marcado para passar pela `voz-romualdo` na Fase 5, e um passo a passo curto de como montar o dashboard e ligar o Public Sharing.

**Critérios de aceite:**
- [ ] Os 12 SQLs rodam como `leitor` sem erro e devolvem linhas para 2025 (e 2019 nos de 2014+)
- [ ] A classificação da última rodada de 2025 no SQL bate com a tabela final do gold
- [ ] Os filtros do Metabase aparecem no SQL como variáveis (`{{temporada}}`, `{{rodada}}`), com o padrão definido na spec
- [ ] Nenhum card usa tabela fora de `gold`

**Verificação:**
- [ ] Saída de cada SQL (primeiras linhas) colada no relatório

**Dependências:** T3

**Arquivos:** `docs/serving/dashboard.md`

**Tamanho:** S

### T6: Caddy, túnel, deploy, backup e `oracle.md`

**Descrição:**
- Compose: serviço `caddy` (2.x, tag fixa) no profile `publico`, portas 80 e 443, volumes de dados e config. `serving/Caddyfile` com `{$DOMINIO}` → `metabase:3000`.
- Makefile:
  - `carga-remota`: abre `ssh -f -N -M -S <socket> -L 15432:127.0.0.1:5432 $(VM_USUARIO)@$(VM_HOST)`, roda a carga com `BRASILEIRAO_PG_PORT=15432` e fecha o túnel com `ssh -S <socket> -O exit` também em falha;
  - `deploy`: `rsync` de `serving/` pra VM (sem `.env` local; o `.env` da VM é criado à mão uma vez) e `docker compose --profile publico up -d` por SSH;
  - `backup-metabase`: `pg_dump` do banco `metabase` pela VM, salvo em `backups/metabase-<data>.sql.gz`.
- `.env.example` ganha `VM_HOST`, `VM_USUARIO` e `DOMINIO`.
- `docs/serving/oracle.md`: passo a passo da Fase 3. Conta e escolha da home region; Pay As You Go e orçamento com o menor alerta aceito; como achar o que gerou custo e parar a VM; VM A1 (conferir o limite Always Free no console, shape sugerido, Ubuntu 24.04 ARM, chave SSH); security list só com 22/80/443; `iptables` da imagem Ubuntu da Oracle; instalação do Docker com o plugin compose; Docker no boot; DuckDNS (subdomínio e IP); criação do `.env` na VM; primeiro `make deploy`; conferência com `dig` antes do Caddy pedir o certificado; checagens de fora (`nc -zv` em 5432, 3000, 80, 443).

**Critérios de aceite:**
- [ ] `docker compose -f serving/docker-compose.yml --profile publico config` válido; sem o profile, o Caddy não sobe
- [ ] `make carga-remota` testado contra `localhost` (com `VM_HOST=localhost`, se houver `sshd` local; senão revisão do alvo e do `trap` na sessão principal): o túnel fecha em sucesso e em falha
- [ ] `make -n deploy` e `make -n backup-metabase` mostram os comandos certos
- [ ] `oracle.md` cobre todos os itens acima, com os comandos prontos pra copiar
- [ ] Nenhum IP, senha ou domínio real versionado

**Verificação:**
- [ ] Saídas do `compose config` e dos `make -n` no relatório

**Dependências:** T3

**Arquivos:** `serving/docker-compose.yml`, `serving/Caddyfile`, `serving/.env.example`, `Makefile`, `docs/serving/oracle.md`

**Tamanho:** M

---

## Fase 3: Nuvem (Romualdo + sessão principal)

### T7: Conta, VM, DNS, deploy e carga remota

**Descrição:** O Romualdo segue o `oracle.md` com a sessão principal acompanhando. O `oracle.md` é corrigido no caminho se algum passo estiver diferente na prática.

**Critérios de aceite:**
- [ ] Conta em Pay As You Go, com orçamento e alerta criados
- [ ] VM A1 dentro do limite Always Free, com SSH por chave
- [ ] `make deploy` sobe Postgres, Metabase e Caddy; certificado válido em `https://<subdominio>.duckdns.org`
- [ ] Metabase configurado: admin com senha forte, cadastro fechado, banco `brasileirao` cadastrado como `leitor` mostrando só o schema `gold`
- [ ] `make carga-remota` abaixo de 3 minutos; contagens iguais às do Delta
- [ ] De fora: 22, 80 e 443 respondem; 5432 e 3000 não
- [ ] `sudo reboot` e tudo volta em até 5 minutos

**Dependências:** T6

## Fase 4: Dashboard (Romualdo)

### T8: Montagem e Public Sharing

**Descrição:** Montar as duas abas pelo `dashboard.md`, com nomes de tabela e coluna amigáveis no modelo de dados. Ligar o Public Sharing só desse dashboard. Rodar o primeiro `make backup-metabase`.

**Critérios de aceite:**
- [ ] As 12 perguntas no dashboard; os filtros de temporada e rodada funcionando no link público
- [ ] O link abre numa janela anônima, sem login
- [ ] A tabela final de 2025 no dashboard bate com o gold
- [ ] Backup em `backups/`, fora do git

**Dependências:** T5, T7

## Fase 5: Fechamento

### T9: Documentação e conferência final

**Descrição:** `docs/serving/README.md` (arquitetura do serving, papéis e segurança, como a carga e a troca funcionam, como rodar local). README da raiz com o link público, um print do dashboard e a seção do serving. Cartão de notas do dashboard revisado. Todo texto público passa pela `voz-romualdo`. Conferência dos 11 critérios de sucesso da spec, com evidência de cada um.

**Critérios de aceite:**
- [ ] Os 11 critérios da spec conferidos, com evidência no relatório
- [ ] Textos revisados pela `voz-romualdo`
- [ ] `make lint` e `make test` verdes; CI verde

**Dependências:** T8

**Arquivos:** `docs/serving/README.md`, `README.md`, `docs/serving/dashboard.md` (cartão de notas), imagem do print em `docs/serving/`
