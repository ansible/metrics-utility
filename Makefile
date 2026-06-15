COMPOSE_CMD ?= $(shell command -v podman-compose 2>/dev/null || echo "docker compose")
COMPOSE_FILE = tools/docker/docker-compose.yaml

help:
	@echo help sync test coverage lint fix compose compose-pytest compose-env compose-service compose-pytest-svc clean psql

sync:
	uv run sync

test:
	uv run pytest -s -v

coverage:
	uv run pytest -s -v --cov --cov-branch --cov-report=html --cov-report=xml

lint:
	uv run ruff check
	uv run ruff format --check

fix:
	uv run ruff check --fix
	uv run ruff format

compose:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) up

compose-pytest:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) --profile pytest up

compose-env:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) --profile env up -d

compose-service:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) --profile service up

compose-pytest-svc:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) --profile pytest-svc up

clean:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) down -v --rmi local

psql:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) exec postgres psql -U awx


.PHONY: help sync test coverage lint fix compose compose-pytest compose-env compose-service compose-pytest-svc clean psql
