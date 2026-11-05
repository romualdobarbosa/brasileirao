.PHONY: setup download lakehouse gold explain serving-up serving-down carga test lint fmt

# Credenciais e destino do Postgres (fora do git; modelo em serving/.env.example).
ifneq (,$(wildcard .env))
include .env
export
endif

COMPOSE = docker compose -f serving/docker-compose.yml --env-file .env

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

explain:
	uv run python -m brasileirao.pipeline explain

# Precisa do Postgres de pé (local: `make serving-up`) e do .env.
carga:
	uv run python -m brasileirao.pipeline carga

serving-up:
	$(COMPOSE) up -d --wait

serving-down:
	$(COMPOSE) down

test:
	uv run pytest --cov=brasileirao --cov-report=term-missing

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .
