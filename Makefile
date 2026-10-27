.PHONY: setup download test lint fmt

setup:
	uv sync --all-groups

download:
	uv run python -m brasileirao.ingestion.download

test:
	uv run pytest --cov=brasileirao --cov-report=term-missing

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .
