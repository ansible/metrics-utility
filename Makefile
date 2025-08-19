CONTAINER_ENGINE ?= docker

help:
	@echo help sync test coverage lint fix compose clean psql

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

compose:
	${CONTAINER_ENGINE} compose -f tools/docker/docker-compose.yaml up

clean:
	${CONTAINER_ENGINE} compose -f tools/docker/docker-compose.yaml down -v

psql:
	${CONTAINER_ENGINE} compose -f tools/docker/docker-compose.yaml exec postgres psql -U awx

.PHONY: help sync test coverage lint fix compose clean psql
