help:
	@echo help sync test coverage lint fix compose

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
	docker compose -f tools/docker/docker-compose.yaml up

# This will populate the main_hostmetric table in the db with data. The database must be up and running first.
populate_host_metrics:
	docker cp tools/docker/main_hostmetric.sql postgres:/main_hostmetric.sql
	docker exec postgres psql -U myuser -d awx -f /main_hostmetric.sql


.PHONY: help sync test coverage lint fix compose
