# Plano de implementação: serving

Spec: [SPEC-serving.md](../SPEC-serving.md). Tarefas detalhadas em [todo-serving.md](todo-serving.md). O plano do módulo anterior está em [plan-gold.md](plan-gold.md).

> **Canceladas as Fases 3 a 5** em 05/11/2026: o cadastro da Oracle no Brasil pede CNPJ. Ver [SPEC-vitrine.md](../SPEC-vitrine.md) e [todo-vitrine.md](todo-vitrine.md). O `docs/serving/oracle.md` foi removido.

## Visão geral

Primeiro uma fatia vertical **toda local**: Postgres + Metabase em Docker Compose na máquina do Romualdo, o pacote `brasileirao.serving` carregando o gold real e o Metabase lendo como `leitor`. Depois entram o Postgres no CI, o SQL dos cards do dashboard (validado no Postgres local) e as peças de deploy (Caddy, túnel, `make deploy`, passo a passo da Oracle). Só então vem a parte na nuvem, que é manual e feita junto com o Romualdo: conta, VM, DNS, deploy e carga remota. Por último, ele monta o dashboard na tela do Metabase e o README ganha o link.

A ordem empurra o que depende da Oracle (capacidade A1, cartão, DNS) pro fim, e tudo o que dá pra testar sem nuvem fica pronto antes.

## Decisões de arquitetura

- **O `initdb/` é a única fonte do schema do Postgres.** `01-papeis.sql` e `02-gold.sql` são SQL puro, sem senha. O `03-senhas.sh` lê as senhas do ambiente e roda `ALTER ROLE ... PASSWORD` (só no entrypoint do container). O teste de integração aplica os mesmos dois `.sql` via psycopg num Postgres vazio e define senhas de teste, então o service container do CI não precisa montar arquivo nenhum (no CI, o service sobe antes do checkout).
- **`POSTGRES_DB=brasileirao`.** O entrypoint roda os `.sql` nesse banco; o `01-papeis.sql` cria o banco `metabase` à parte.
- **`TABELAS` em `serving/tabelas.py` é o registro do lado do Postgres:** nome, colunas em ordem e PK de cada tabela. Um teste garante que os nomes batem com o registro de `aggregate/gold.py` e que as colunas batem com o schema que o gold grava. Outro garante que batem com o DDL do `02-gold.sql` (aplicado num Postgres de teste e lido do `information_schema`).
- **Driver JDBC pelo Ivy.** `get_spark` ganha um parâmetro de pacotes extras, repassado a `configure_spark_with_delta_pip(builder, extra_packages=...)`. Só o subcomando `carga` pede o driver; o resto do pipeline não muda. A versão fica numa constante em `serving/`.
- **Credenciais só por ambiente.** `config.py` lê `BRASILEIRAO_PG_HOST`, `_PORT`, `_DB`, `_USUARIO`, `_SENHA` (o papel `carga`). O Makefile carrega o `.env` da raiz. Os testes usam `BRASILEIRAO_PG_TESTE_URL` (superusuário de um Postgres descartável) e se pulam sem ela.
- **Marcador `postgres` no pytest**, registrado no `pyproject.toml`. `make test` roda tudo; sem Postgres, os testes marcados aparecem como pulados com o motivo.
- **O túnel vive no Makefile** (ou num script curto chamado por ele), com socket de controle do SSH e `trap` para fechar mesmo em falha. A carga não sabe que existe túnel: só recebe `BRASILEIRAO_PG_PORT=15432`.
- **O Compose tem dois modos pelo mesmo arquivo:** sem profile (local: Postgres e Metabase em `127.0.0.1`) e com `--profile publico` (VM: + Caddy nas portas 80 e 443).

## Grafo de dependências

```
T1 compose local + initdb + .env.example + make serving-up/down
 └── T2 config PG, get_spark com pacotes extras, psycopg, tabelas.py
      └── T3 postgres.py + carga.py + CLI carga + make carga + integração   ← checkpoint com dados reais
           ├── T4 Postgres no CI                     (T4, T5, T6 independentes entre si)
           ├── T5 SQL dos 12 cards em docs/serving/dashboard.md
           └── T6 Caddy + carga-remota + deploy + backup + docs/serving/oracle.md
                └── T7 nuvem (Romualdo + principal): conta, VM, DuckDNS, deploy, carga remota, Metabase
                     └── T8 dashboard (Romualdo monta) + Public Sharing
                          └── T9 docs/serving/README.md, README e conferência dos 11 critérios
```

## Lista de tarefas

### Fase 1: Fatia vertical local (agente)
- [ ] T1: Compose local, `initdb/` e alvos `serving-up` / `serving-down`
- [ ] T2: Config do Postgres, driver JDBC no Spark, `psycopg` e `tabelas.py`
- [ ] T3: Carga com troca transacional, CLI `carga`, `make carga` e testes de integração

### Checkpoint: Carga local nos dados reais (revisão com o Romualdo)
- [ ] `make gold && make serving-up && make carga` deixa as 8 tabelas em `gold` com as contagens do Delta
- [ ] `make carga` 2× não muda nada; o `leitor` só lê `gold`
- [ ] O Metabase local (`localhost:3000`) conecta como `leitor` e enxerga as 8 tabelas

### Fase 2: CI, cards e peças de deploy (agente)
- [ ] T4: Postgres como service container no `ci.yml`
- [ ] T5: `docs/serving/dashboard.md` com o SQL dos 12 cards, testado no Postgres local
- [ ] T6: Caddy, `make carga-remota`, `make deploy`, `make backup-metabase` e `docs/serving/oracle.md`

### Checkpoint: Pronto pra nuvem
- [ ] CI verde com os testes `postgres` rodando (e conferir o critério 3 da SPEC-ci: segunda execução usa os caches)
- [ ] `docker compose --profile publico config` válido; Makefile revisado

### Fase 3: Nuvem (Romualdo + sessão principal, sem agente), cancelada
- [ ] T7: Conta Oracle, Pay As You Go e orçamento, VM A1, rede, DuckDNS, deploy, carga remota e configuração inicial do Metabase

### Checkpoint: Infra no ar
- [ ] De fora, só 22, 80 e 443 respondem; certificado válido no DuckDNS
- [ ] `make carga-remota` abaixo de 3 minutos e fecha o túnel em falha
- [ ] `sudo reboot` na VM e tudo volta em até 5 minutos

### Fase 4: Dashboard (Romualdo, com apoio da principal), cancelada
- [ ] T8: Montar o dashboard pelo `dashboard.md`, ligar o Public Sharing e fazer o primeiro `make backup-metabase`

### Fase 5: Fechamento (agente + `voz-romualdo`), cancelada
- [ ] T9: `docs/serving/README.md`, seção do serving e link no README, conferência dos 11 critérios da spec

### Checkpoint: Completo
- [ ] Revisão com o Romualdo antes de fechar o módulo

## Riscos e mitigações

| Risco | Impacto | Mitigação |
|---|---|---|
| "Out of capacity" na A1 em São Paulo e Vinhedo | Alto | Fase 3 vem por último; tudo testado local antes. Tentar em horários diferentes e outros domínios de disponibilidade; Pay As You Go costuma ajudar a conseguir capacidade |
| Limite Always Free menor que o esperado (2 OCPU / 12 GB ou menos) | Médio | Postgres + Metabase cabem em 1 OCPU / 6 GB. `JAVA_OPTS` com `-Xmx` fixo no Metabase (ex.: 2 GB) |
| Imagem do Metabase sem `linux/arm64` na tag escolhida | Médio | T1 confere com `docker manifest inspect` antes de fixar a tag |
| Regras de `iptables` da imagem Ubuntu da Oracle bloqueando 80/443 | Médio | `oracle.md` tem o passo; o checkpoint da Fase 3 testa de fora |
| Let's Encrypt falhar porque o DNS ainda não propagou ou a 80 está fechada | Baixo | Subir o Caddy só depois de `dig` resolver o DuckDNS pro IP da VM e a 80 responder |
| Metabase perder a referência das tabelas numa carga | Médio | Troca por `TRUNCATE` + `INSERT` (as tabelas nunca são dropadas); T3 testa que o OID das tabelas não muda |
| Download do driver JDBC deixando o CI lento ou instável | Baixo | Mesmo cache do Ivy do Delta; T4 inclui a versão do driver na chave do cache |
| Tempo do job de teste no CI subir muito | Baixo | Os testes `postgres` usam o gold das fixtures (poucas linhas). Medir no checkpoint da Fase 2 |

## Paralelização

T4, T5 e T6 só dependem do T3 e não mexem nos mesmos arquivos (`ci.yml`; `docs/serving/dashboard.md`; `serving/` + Makefile + `oracle.md`). Pelo fluxo de uma sessão por fase, rodam em sequência dentro da Fase 2. As Fases 3 e 4 não usam agente: são passos do Romualdo no console da Oracle, no DuckDNS e na tela do Metabase, acompanhados pela sessão principal.

## Open Questions

Nenhuma.
