# Spec: serving

> Módulo `serving` do [mapa de capacidades](CAPABILITY_MAP.md). Status: **Fases 1 e 2 entregues, Fases 3 a 5 canceladas** em 05/11/2026 (o cadastro da Oracle no Brasil pede CNPJ). O que substitui a nuvem está em [SPEC-vitrine.md](SPEC-vitrine.md), e o que foi construído, em [docs/serving/README.md](docs/serving/README.md). O texto abaixo é o plano aprovado em 28/10/2026, inclusive as menções ao `oracle.md`, removido. Depende do contrato do gold em [SPEC-gold.md](SPEC-gold.md).

## Objetivo

Publicar o gold num Postgres e montar em cima dele um dashboard do Metabase com link público, tudo numa VM da Oracle Cloud Always Free (custo zero). É o MVP do projeto: o link que o recrutador abre sem rodar nada.

O que o módulo precisa demonstrar no portfólio:

- **Lakehouse para processar, relacional para servir:** o Spark roda local, e só o resultado vai pro Postgres.
- **Carga do Spark via JDBC** atômica para quem lê: o dashboard nunca vê metade de uma carga.
- **Infra própria e enxuta:** Docker Compose numa VM Linux, com a porta do banco fechada pra internet e a carga passando por túnel SSH.
- **Segurança básica feita direito:** usuário read-only só no schema `gold` para o Metabase, HTTPS com Caddy e só as portas 22, 80 e 443 abertas.
- **Dashboard que responde perguntas**, não só gráficos soltos (ver "Dashboard").

Fora do escopo: Terraform na Oracle (Terraform entra no `cloud-aws`), carga agendada (a carga é manual depois de `make gold`), Base dos Dados, mudanças no gold, alta disponibilidade.

### Critérios de aceite (histórias)

1. Como dev, rodo `make serving-up` e `make carga` e vejo as 8 tabelas gold no Postgres local e o Metabase em `localhost:3000`.
2. Como dev, rodo `make carga-remota` depois de `make gold` e o Postgres da VM recebe o gold novo por túnel SSH, sem a porta 5432 aberta na internet.
3. Como dev, rodo a carga duas vezes e o conteúdo no Postgres é o mesmo de uma vez só.
4. Como recrutador, abro `https://<subdominio>.duckdns.org` (link do README), vejo o dashboard sem login, com cadeado no navegador, e troco a temporada no filtro.
5. Como dev, reinicio a VM e o Postgres, o Metabase e o Caddy voltam sozinhos.

## Tech Stack

| Peça | Versão | Observação |
|---|---|---|
| PostgreSQL | imagem oficial `postgres`, 17.x fixada por tag | um banco `brasileirao` (dados) e um `metabase` (aplicação do Metabase) |
| Metabase | imagem oficial `metabase/metabase` (edição open source), tag fixa | conferir na implementação se a tag tem manifest `linux/arm64` (`docker manifest inspect`) |
| Caddy | imagem oficial `caddy`, 2.x fixada | HTTPS automático com Let's Encrypt |
| Driver JDBC | `org.postgresql:postgresql` 42.7.x, versão exata conferida no Maven Central | entra pela `extra_packages` do `configure_spark_with_delta_pip`, no mesmo cache do Ivy dos jars do Delta |
| `psycopg[binary]` | 3.x | **dependência nova**: roda a troca transacional e as checagens em SQL |
| VM | Oracle `VM.Standard.A1.Flex` (ARM), Ubuntu 24.04 | shape dentro do limite Always Free vigente (ver "Oracle") |
| DNS | DuckDNS | subdomínio grátis apontando pro IP público da VM |

As versões exatas das imagens e do driver são escolhidas e fixadas na implementação (nada de `latest`).

## Commands

```bash
make serving-up        # sobe Postgres + Metabase locais (compose, sem o Caddy)
make serving-down      # derruba os containers locais (volumes ficam)
make carga             # gold (Delta) → Postgres do .env (local por padrão)
make carga-remota      # abre túnel SSH pra VM, roda a carga pela porta local 15432 e fecha o túnel
make deploy            # copia serving/ pra VM por rsync e roda `docker compose --profile publico up -d` lá
make backup-metabase   # pg_dump do banco `metabase` da VM pra backups/ (fora do git)
make test              # inalterado; ganha os testes do serving
make lint              # inalterado
```

`make carga` falha com mensagem clara se o gold não existir ou se o Postgres não responder. Credenciais e o host da VM vêm do `.env` (fora do git), com `.env.example` versionado.

Local e na VM, o compose roda com `docker compose` (no Fedora, pacote `docker-compose`).

## Project Structure

```
src/brasileirao/
├── config.py                  # + leitura das variáveis BRASILEIRAO_PG_*
├── pipeline.py                # CLI ganha o subcomando `carga`
├── utils/spark.py             # get_spark aceita pacotes extras (driver JDBC)
└── serving/
    ├── __init__.py
    ├── carga.py               # lê o gold, grava em gold_carga via JDBC, chama a troca
    ├── postgres.py            # conexão psycopg, troca transacional, contagens
    └── tabelas.py             # lista das 8 tabelas e chave primária de cada uma

serving/                       # tudo o que vai pra VM
├── docker-compose.yml         # postgres, metabase, caddy (profile `publico`)
├── Caddyfile                  # <subdominio>.duckdns.org → metabase:3000
├── .env.example               # senhas e nomes, sem valor real
└── initdb/
    ├── 01-papeis.sql          # papéis carga, leitor e metabase (sem senha); banco metabase
    ├── 02-gold.sql            # schemas gold e gold_carga; DDL das 8 tabelas com PK; grants
    └── 03-senhas.sh           # define as senhas dos papéis a partir do ambiente

docs/serving/
├── README.md                  # arquitetura do serving, segurança, como a carga funciona
├── oracle.md                  # passo a passo: conta, região, VM, rede, firewall, DuckDNS
└── dashboard.md               # perguntas, cards, filtros e como remontar

tests/
├── unit/serving/              # lista de tabelas, montagem da URL JDBC, mensagens de erro
└── integration/test_serving.py  # gold de fixture → Postgres real; marcado `postgres`
```

## Desenho

### Postgres

Dois bancos na mesma instância:

- `brasileirao`: schema `gold` (o que o dashboard lê) e schema `gold_carga` (área de pouso da carga).
- `metabase`: banco da aplicação do Metabase (perguntas, dashboards, usuários). O Metabase **não** usa o H2 embutido, que não é recomendado fora de teste.

Papéis:

| Papel | Pode |
|---|---|
| `carga` | dono de `gold` e `gold_carga`; escreve nos dois |
| `leitor` | `CONNECT` em `brasileirao`, `USAGE` em `gold`, `SELECT` nas tabelas de `gold`. Nada em `gold_carga`, nada no banco `metabase` |
| `metabase` | dono do banco `metabase`; não enxerga `brasileirao` |

O Metabase se conecta ao `brasileirao` como `leitor`.

As 8 tabelas de `gold` são criadas pelo `02-gold.sql`, com tipos explícitos e chave primária pelo grão de cada tabela no contrato do gold (ex.: `classificacao_por_rodada` → `(temporada, rodada, clube)`). O DDL versionado é o contrato do lado do Postgres: se o gold mudar de schema, a carga quebra em vez de mudar a tabela por baixo do dashboard.

### Carga

1. O Spark lê cada tabela gold (Delta) e grava em `gold_carga.<tabela>` via JDBC com `mode("overwrite")`. Essa área é descartável.
2. Uma única transação no psycopg faz, para cada tabela, `TRUNCATE gold.<tabela>` e `INSERT INTO gold.<tabela> SELECT ... FROM gold_carga.<tabela>`. Se qualquer passo falhar, nada muda em `gold`.
3. Depois do commit, confere que a contagem de linhas de cada tabela em `gold` bate com a do Delta. Diferença é falha.

Por que assim: o `TRUNCATE` + `INSERT` numa transação deixa a troca atômica para o Metabase, mantém as tabelas (com o mesmo OID, os grants e as PKs) e não confunde a sincronização do Metabase, que perderia a referência se a tabela fosse dropada e recriada. Com ~17 mil linhas na maior tabela, o custo é desprezível.

### Túnel e rede

- O Postgres publica a porta só em `127.0.0.1` da VM. Fora dela, a 5432 não existe.
- `make carga-remota` abre `ssh -N -L 15432:127.0.0.1:5432` com um socket de controle, roda a carga apontando pra `localhost:15432` e fecha o túnel, mesmo se a carga falhar.
- O Metabase também publica só em `127.0.0.1:3000`. Quem fala com a internet é o Caddy, nas portas 80 e 443.
- Security list da Oracle e firewall da VM: entrada só em 22 (SSH com chave, sem senha), 80 e 443. As imagens Ubuntu da Oracle vêm com regras de `iptables` que bloqueiam 80 e 443 mesmo com a security list aberta; o `oracle.md` cobre isso.
- Todos os serviços com `restart: unless-stopped`, e o Docker habilitado no boot.

### Metabase

- Admin com senha forte; cadastro aberto desligado.
- **Public Sharing** ligado só para o dashboard principal.
- Banco `brasileirao` cadastrado com o usuário `leitor`, mostrando só o schema `gold`.
- Nomes de tabela e coluna amigáveis em português no modelo de dados do Metabase (ex.: `gols_pro` → "Gols pró").

### Oracle

Passos do Romualdo (manuais, guiados pelo `docs/serving/oracle.md`):

1. Criar a conta. A **home region não muda depois**: escolher São Paulo ou Vinhedo, a que tiver capacidade A1 no momento.
2. Subir a VM A1 dentro do limite Always Free vigente. Relatos de 2026 falam em corte de 4 OCPU / 24 GB para **2 OCPU / 12 GB**; conferir no console antes de escolher o shape. Postgres + Metabase cabem em 1 OCPU / 6 GB, e 2 / 12 dá folga.
3. Se der "out of capacity", tentar de novo em outro horário ou outro domínio de disponibilidade.
4. Migrar a conta para **Pay As You Go** e criar um orçamento com o menor valor e o menor limite de alerta que o console aceitar (meta: aviso no primeiro centavo, ex.: orçamento de US$ 1 com alerta em 1%). Conta paga não sofre a recuperação de VM ociosa (95º percentil de CPU abaixo de 20% por 7 dias, pela documentação da Oracle), e os recursos Always Free continuam de graça. O alerta só avisa, não desliga nada: ao receber o e-mail, o Romualdo para ou apaga o recurso que saiu da cota. O `oracle.md` ensina a achar o que gerou custo e a parar a VM pelo console.
5. Criar o subdomínio no DuckDNS apontando pro IP público.

## Dashboard

Um dashboard público, "Brasileirão 2003–2025", com duas abas. Os cards são perguntas do Metabase (query builder quando der, SQL quando precisar), e `docs/serving/dashboard.md` registra cada card com tabela, colunas, tipo de gráfico e o SQL equivalente, pra dar pra remontar do zero.

**Aba "Temporada"**, com filtros `temporada` (padrão 2025) e `rodada` (padrão a última):

| Pergunta | Tabela | Card |
|---|---|---|
| Como estava a tabela nessa rodada? | `classificacao_por_rodada` | tabela com posição, pontos, jogos, V/E/D, saldo, aproveitamento e últimos 5 |
| Como a posição de cada time mudou ao longo do campeonato? | `classificacao_por_rodada` | linha posição × rodada, eixo invertido, filtro de clube |
| Quem foi campeão, vice e quem caiu? | `resumo_temporada` | cartões de número/texto + média de gols |
| Quem fez mais gols? | `artilharia` | top 10 (2014 em diante) |
| Quem rende mais em casa e fora? | `desempenho_mandante_visitante` | barras de aproveitamento por mando |
| Em que minuto saem os gols? | `gols_por_minuto` | barras por faixa, mandante × visitante |
| Quais foram as maiores sequências? | `sequencias` | tabela por tipo (invencibilidade, jejum…) |

**Aba "Histórico"**:

| Pergunta | Tabela | Card |
|---|---|---|
| Quem mais somou pontos desde 2003? | `ranking_historico` | tabela com pontos, títulos e aproveitamento |
| Quem tem mais títulos na era dos pontos corridos? | `ranking_historico` | barras |
| O mando de campo perdeu força? (2020 sem torcida) | `resumo_temporada` | linha de % vitória do mandante por temporada |
| O campeonato ficou com mais ou menos gols? | `resumo_temporada` | linha de média de gols por temporada |
| O jogo ficou mais violento? | `disciplina` | linha de cartões por jogo por temporada (2014 em diante) |

Notas visíveis no dashboard (cartão de texto curto): buracos da fonte (artilharia e cartões só a partir de 2014; W.O. de 2016) e que Barueri e Grêmio Prudente aparecem separados. O texto passa pela `voz-romualdo`.

## Code Style

Igual ao resto do projeto: nomes em português sem acento, I/O separado da lógica, caminhos e credenciais via `config.py`. SQL em arquivos `.sql` versionados ou em constantes no módulo, sempre com parâmetros do psycopg, nunca com f-string de valor. Identificador (nome de tabela) só da lista fixa em `tabelas.py`.

```python
from psycopg import Connection, sql

from brasileirao.serving.tabelas import TABELAS


def trocar_gold(conexao: Connection) -> None:
    """Copia gold_carga → gold numa transação só. Se algo falhar, gold fica como estava."""
    with conexao.transaction():
        for tabela in TABELAS:
            alvo = sql.Identifier("gold", tabela.nome)
            origem = sql.Identifier("gold_carga", tabela.nome)
            colunas = sql.SQL(", ").join(map(sql.Identifier, tabela.colunas))
            conexao.execute(sql.SQL("TRUNCATE {}").format(alvo))
            conexao.execute(
                sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                    alvo, colunas, colunas, origem
                )
            )
```

## Testing Strategy

- **Unitários** (`tests/unit/serving/`): a lista de `TABELAS` bate com as 8 tabelas do gold e com as colunas do contrato; montagem da URL JDBC e das propriedades a partir do ambiente; erro claro sem variável de ambiente ou sem gold.
- **Integração** (`tests/integration/test_serving.py`, marcador `postgres`): Postgres real em container com o `initdb/` do projeto; gold de fixture → `carga` → contagens iguais; carga 2× = 1×; falha no meio da troca deixa `gold` intacto; `leitor` consegue `SELECT` em `gold` e leva erro em `INSERT`, `CREATE`, em `gold_carga` e no banco `metabase`. Pulados quando `BRASILEIRAO_PG_TESTE_URL` não existe, com o motivo na saída do pytest.
- **CI:** o job `test` ganha um service container de Postgres, então os testes `postgres` rodam no GitHub Actions também. É mudança no `ci.yml` (decidido, ver "Decisões").
- **Conferência manual** (checkpoint, não teste): de fora da VM, `nc -zv <ip> 5432` recusa e 80/443 respondem; o certificado é válido; o link público abre numa janela anônima; a VM reiniciada volta com tudo no ar.
- **Cobertura:** continua ≥ 80% em `src/brasileirao/`, sem cair muito dos 99% atuais.

## Boundaries

- **Sempre:** `make lint` e `make test` antes de commit; senha e IP só no `.env`; texto público (README, `docs/serving/`, notas do dashboard) passa pela `voz-romualdo`; a carga só troca o `gold` depois de tudo gravado em `gold_carga`.
- **Perguntar antes:** abrir qualquer porta além de 22, 80 e 443; mudar o schema de tabela gold ou o DDL do `02-gold.sql`; adicionar dependência além do `psycopg`; mexer no `ci.yml`; qualquer ação no console da Oracle ou no DuckDNS (são passos do Romualdo).
- **Nunca:** versionar `.env`, senha, chave SSH, IP da VM ou dump do Metabase; expor a 5432 ou a 3000 na internet; dar ao `leitor` qualquer coisa além de `SELECT` em `gold`; usar imagem com tag `latest`.

## Success Criteria

1. `make serving-up && make carga` local cria as 8 tabelas em `gold` com as mesmas contagens do Delta.
2. Carga 2× deixa o conteúdo idêntico ao de 1× (teste de integração).
3. Falha no meio da troca não altera `gold` (teste de integração).
4. O `leitor` só consegue `SELECT` em `gold` (teste de integração).
5. De fora da VM, só 22, 80 e 443 respondem; a 5432 e a 3000 não.
6. `https://<subdominio>.duckdns.org` abre o dashboard público com certificado válido, sem login, e os filtros de temporada e rodada funcionam.
7. O dashboard responde as 12 perguntas da seção "Dashboard"; a tabela final de 2025 nele bate com a do gold.
8. Depois de `sudo reboot` na VM, o link volta sozinho em até 5 minutos.
9. `make carga-remota` termina em menos de 3 minutos e fecha o túnel mesmo quando falha.
10. `make test` passa (com os testes `postgres` rodando no CI) e `make lint` está limpo.
11. O README ganha o link público, um print do dashboard e a seção do serving, revisados pela `voz-romualdo`.

## Decisões

- **Metabase** em vez de Streamlit, para variar o portfólio.
- **Carga por túnel SSH**, com a 5432 fechada pra internet.
- **HTTPS com Caddy + DuckDNS** (subdomínio com nome, em vez de IP no sslip.io).
- **Dashboard montado na tela do Metabase**, com as perguntas definidas aqui e documentadas em `dashboard.md`. Sem dashboard como código.
- **Troca por `TRUNCATE` + `INSERT` numa transação**, com área de pouso `gold_carga` e DDL versionado.
- **VM criada à mão pelo console**, guiada por `oracle.md`. Terraform fica para a AWS.
- **Carga manual**, sem agendamento: o dado só muda quando a temporada termina.

- **`psycopg` como dependência nova** para a troca transacional e as checagens, em vez de rodar SQL pela JVM do Spark.
- **Postgres no CI** como service container no job `test`: os testes `postgres` rodam a cada push.
- **Compose local com `docker compose`** (pacote `docker-compose` do Fedora), igual ao da VM. Nada de `podman-compose`.
- **Conta Oracle em Pay As You Go** com alerta de orçamento no menor limite possível, para a VM não ser recuperada por ociosidade.

## Open Questions

Nenhuma.
