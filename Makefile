help:
	@echo help sync test coverage lint fix

sync:
	uv run sync

test:
	uv run pytest -s -v

coverage:
	uv run pytest -s -v --cov=. --cov-report=html

lint:
	uv run ruff check
	uv run ruff format --check

fix:
	uv run ruff check --fix
	uv run ruff format

.PHONY: help sync test coverage lint fix
