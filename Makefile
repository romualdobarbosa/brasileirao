.PHONY: setup download lakehouse gold explain test lint fmt

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

test:
	uv run pytest --cov=brasileirao --cov-report=term-missing

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .
