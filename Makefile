.PHONY: setup download lakehouse gold exportar app explain serving-up serving-down carga carga-remota deploy \
	backup-metabase test lint fmt

# Credenciais e destino do Postgres (fora do git; modelo em serving/.env.example).
ifneq (,$(wildcard .env))
include .env
export
endif

COMPOSE = docker compose -f serving/docker-compose.yml --env-file .env

# VM: VM_HOST, VM_USUARIO e VM_PG_SENHA_CARGA vêm do .env. SSH pode ser trocado pra teste
# (ex.: SSH="ssh -F outro_config").
SSH ?= ssh
VM = $(VM_USUARIO)@$(VM_HOST)
VM_DIR ?= brasileirao
TUNEL_PORTA ?= 15432
TUNEL_SOCKET ?= $(HOME)/.ssh/brasileirao-tunel.sock

setup:
	uv sync --all-groups

download:
	uv run python -m brasileirao.ingestion.download

lakehouse:
ifdef TEMPORADA
	uv run python -m brasileirao.pipeline lakehouse --temporada $(TEMPORADA)
else
	uv run python -m brasileirao.pipeline lakehouse --todas
endif

gold:
	uv run python -m brasileirao.pipeline gold

# gold → app/dados/<tabela>.parquet, que vão pro git e são o que o app lê.
exportar:
	uv run python -m brasileirao.pipeline exportar

app:
	uv run --group app streamlit run app/streamlit_app.py

explain:
	uv run python -m brasileirao.pipeline explain

# Precisa do Postgres de pé (local: `make serving-up`) e do .env.
carga:
	uv run python -m brasileirao.pipeline carga

# Túnel SSH até o Postgres da VM (que só escuta em 127.0.0.1 de lá), carga pela porta local
# e túnel fechado pelo trap, com a carga dando certo ou não. A senha vai pelo ambiente
# (VM_PG_SENHA_CARGA, exportada do .env), pra não aparecer no eco do make.
carga-remota:
	@test -n "$(VM_HOST)" && test -n "$(VM_USUARIO)" && test -n "$$VM_PG_SENHA_CARGA" || \
		{ echo "defina VM_HOST, VM_USUARIO e VM_PG_SENHA_CARGA no .env"; exit 1; }
	trap '$(SSH) -S $(TUNEL_SOCKET) -O exit $(VM) 2>/dev/null' EXIT; \
	$(SSH) -f -N -M -S $(TUNEL_SOCKET) -o ExitOnForwardFailure=yes \
		-L $(TUNEL_PORTA):127.0.0.1:5432 $(VM) && \
	BRASILEIRAO_PG_HOST=127.0.0.1 BRASILEIRAO_PG_PORT=$(TUNEL_PORTA) \
	BRASILEIRAO_PG_SENHA="$$VM_PG_SENHA_CARGA" uv run python -m brasileirao.pipeline carga

# Copia serving/ pra VM (o .env de lá, criado à mão, fica intocado) e sobe com o Caddy.
deploy:
	rsync -av --delete --exclude .env serving/ $(VM):$(VM_DIR)/
	$(SSH) $(VM) 'cd $(VM_DIR) && docker compose --profile publico up -d --wait'

# Dump do banco do Metabase (perguntas, dashboard, usuários) da VM pra backups/ (fora do git).
# Arquivo pela metade (ssh ou pg_dump falhou) é apagado.
backup-metabase:
	@mkdir -p backups
	arquivo=backups/metabase-$$(date +%Y-%m-%d).sql.gz; \
	$(SSH) $(VM) 'set -o pipefail; cd $(VM_DIR) && \
		docker compose exec -T postgres pg_dump -U postgres -d metabase | gzip' > $$arquivo \
		|| { rm -f $$arquivo; exit 1; }; \
	echo "backup em $$arquivo"

serving-up:
	$(COMPOSE) up -d --wait

serving-down:
	$(COMPOSE) down

test:
	uv run --group app pytest --cov=brasileirao --cov-report=term-missing -n 2 --dist loadfile

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .
