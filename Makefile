.PHONY: setup download lakehouse gold exportar app explain manutencao historico serving-up serving-down carga carga-remota deploy \
	backup-metabase test lint fmt wheel raw-aws lakehouse-aws gold-aws foto-aws \
	simulacao-preparar simulacao relatorio-rodada simulacao-preparar-aws \
	airflow-up airflow-up-aws airflow-down airflow-rodada airflow-estado test-dag status

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

# OPTIMIZE, Z-Order e VACUUM com a retenção padrão de 7 dias e a trava ligada.
manutencao:
	uv run python -m brasileirao.pipeline manutencao

# make historico TABELA=silver/partidas [COMPARAR="0 5"]
historico:
	@test -n "$(TABELA)" || { echo "use: make historico TABELA=camada/tabela [COMPARAR=\"A B\"]"; exit 1; }
	uv run python -m brasileirao.pipeline historico $(TABELA) $(if $(COMPARAR),--comparar $(COMPARAR))

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

# Ambiente aws: o mesmo pipeline, com o Spark local e o lake no bucket do infra/main (que precisa
# estar de pé). A credencial vem do perfil AWS_PROFILE, cujo `credential_process` pede à CLI a da
# sessão do `aws login`, e o SDK do Spark roda o comando de novo quando ela vence. Credencial
# exportada no ambiente sai antes, para não passar na frente do perfil.
AWS_PROFILE ?= brasileirao
AWS_ENV = unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_CREDENTIAL_EXPIRATION && \
	export AWS_PROFILE=$(AWS_PROFILE) AWS_REGION=us-east-1 BRASILEIRAO_SPARK_ENV=aws && \
	conta=$$(aws sts get-caller-identity --query Account --output text) && \
	export BRASILEIRAO_DATA_ROOT=s3a://brasileirao-lake-$$conta &&

# Os CSVs de data/raw/ para s3://<lake>/raw/.
raw-aws:
	$(AWS_ENV) aws s3 cp --recursive --exclude "*" --include "*.csv" data/raw/ \
		"s3://brasileirao-lake-$$conta/raw/" --only-show-errors

lakehouse-aws:
ifdef TEMPORADA
	$(AWS_ENV) uv run python -m brasileirao.pipeline lakehouse --temporada $(TEMPORADA)
else
	$(AWS_ENV) uv run python -m brasileirao.pipeline lakehouse --todas
endif

gold-aws:
	$(AWS_ENV) uv run python -m brasileirao.pipeline gold

# make foto-aws SAIDA=foto_aws.json: linhas e sha256 de silver e gold no S3.
foto-aws:
	$(AWS_ENV) uv run python databricks/foto.py "$$BRASILEIRAO_DATA_ROOT" $(or $(SAIDA),foto_aws.json)

# Simulação de temporada em andamento (2025 rodada a rodada) no lake isolado data_sim/.
# make simulacao-preparar [HISTORICO=1], make simulacao RODADA=1 [ATE=38], make relatorio-rodada RODADA=4
simulacao-preparar:
	uv run python -m brasileirao.simulacao preparar $(if $(HISTORICO),--historico)

simulacao:
	@test -n "$(RODADA)" || { echo "use: make simulacao RODADA=N [ATE=M]"; exit 1; }
	uv run python -m brasileirao.simulacao rodada $(RODADA) $(if $(ATE),--ate $(ATE))

relatorio-rodada:
	@test -n "$(RODADA)" || { echo "use: make relatorio-rodada RODADA=N"; exit 1; }
	uv run python -m brasileirao.simulacao relatorio $(RODADA)

# A mesma preparação no lake da simulação no S3 (s3a://<lake>/data_sim), para o alvo aws da DAG.
simulacao-preparar-aws:
	$(AWS_ENV) uv run python -m brasileirao.simulacao --destino "$$BRASILEIRAO_DATA_ROOT/data_sim" \
		preparar $(if $(HISTORICO),--historico)

# Últimas execuções registradas em <lake>/ops/execucoes (o pipeline e a simulação gravam lá).
# make status [DATA_ROOT=data_sim] [LIMITE=20]
DATA_ROOT ?= data
status:
	BRASILEIRAO_DATA_ROOT=$(DATA_ROOT) uv run python -m brasileirao.execucoes $(if $(LIMITE),--limite $(LIMITE))

# Airflow local (orchestration/), com a DAG brasileirao_rodada. O orchestration/.env (fora do
# git) nasce na primeira vez com o seu UID e segredos aleatórios. `down` mantém os volumes
# (banco, logs e cache do Ivy).
AIRFLOW_VERSION = 3.4.0
AIRFLOW = docker compose -f orchestration/docker-compose.yml
AIRFLOW_AWS = $(AIRFLOW) -f orchestration/docker-compose.aws.yml

orchestration/.env:
	@python3 -c 'import base64, os, secrets; print(f"AIRFLOW_UID={os.getuid()}\nAIRFLOW_PORTA=8080\nAIRFLOW_PG_SENHA={secrets.token_urlsafe(24)}\nAIRFLOW_JWT_SECRET={secrets.token_urlsafe(32)}\nAIRFLOW_FERNET_KEY={base64.urlsafe_b64encode(os.urandom(32)).decode()}\n# Alerta de falha por e-mail: copie as ALERTA_* do orchestration/.env.example")' > $@
	@chmod 600 $@
	@echo "criado $@ (fora do git)"

airflow-up: orchestration/.env
	@test -f data/raw/campeonato-brasileiro-full.csv || { echo "falta data/raw: rode make download"; exit 1; }
	mkdir -p data_sim
	$(AIRFLOW) up -d --build --wait

# Portão do AWS: só com a infra do infra/main de pé e o `aws login` feito.
airflow-up-aws: orchestration/.env
	@test -f data/raw/campeonato-brasileiro-full.csv || { echo "falta data/raw: rode make download"; exit 1; }
	mkdir -p data_sim
	$(AIRFLOW_AWS) up -d --build --wait

airflow-down:
	$(AIRFLOW) down

# make airflow-rodada RODADA=4 [ALVO=aws]
airflow-rodada:
	@test -n "$(RODADA)" || { echo "use: make airflow-rodada RODADA=N [ALVO=aws]"; exit 1; }
	$(AIRFLOW) exec airflow-scheduler airflow dags trigger brasileirao_rodada \
		-c '{"rodada": $(RODADA), "alvo": "$(or $(ALVO),local)"}'

# Execuções da DAG e, com RUN=<run_id>, o estado de cada tarefa dela.
airflow-estado:
	$(AIRFLOW) exec airflow-scheduler airflow dags list-runs brasileirao_rodada
	$(if $(RUN),$(AIRFLOW) exec airflow-scheduler airflow tasks states-for-dag-run brasileirao_rodada "$(RUN)")

# Testes da DAG num venv à parte com o Airflow (e as constraints dele), fora do `make test`.
test-dag:
	uv venv --python 3.12 --allow-existing -q .venv-airflow
	uv pip install --python .venv-airflow -q "apache-airflow==$(AIRFLOW_VERSION)" pytest \
		-c https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-3.12.txt
	PYTHONPATH=src .venv-airflow/bin/pytest orchestration/tests

serving-up:
	$(COMPOSE) up -d --wait

serving-down:
	$(COMPOSE) down

test:
	uv run --group app pytest --cov=brasileirao --cov-report=term-missing -n 2 --dist loadfile

# Wheel do pacote em dist/, sem pyspark nem delta-spark nas dependências (para o Databricks).
wheel:
	rm -rf dist
	uv build --wheel

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .
