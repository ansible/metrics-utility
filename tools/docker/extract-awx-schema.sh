#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FORCE=false
AWX_DIR=""

for arg in "$@"; do
  case "$arg" in
    --force) FORCE=true ;;
    *) AWX_DIR="$arg" ;;
  esac
done

AWX_DIR="${AWX_DIR:-$(cd "$SCRIPT_DIR/../../.." && pwd)/awx}"

COMPOSE_CMD="${COMPOSE_CMD:-$(command -v podman-compose 2>/dev/null || echo "docker compose")}"

if [ ! -d "$AWX_DIR/.git" ]; then
  echo "Error: AWX repo not found at $AWX_DIR"
  echo "Usage: $0 [AWX_DIR] [--force]"
  exit 1
fi

cd "$AWX_DIR"

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "$current_branch" != "devel" ]; then
  echo "Error: AWX repo is on branch '$current_branch', expected 'devel'"
  if [ "$FORCE" = false ]; then
    echo "Use --force to override"
    exit 1
  fi
  echo "Continuing anyway (--force)"
fi

git fetch origin devel
local_sha="$(git rev-parse HEAD)"
remote_sha="$(git rev-parse origin/devel)"
if [ "$local_sha" != "$remote_sha" ]; then
  echo "Error: AWX repo is not up to date with origin/devel"
  echo "  local:  $local_sha"
  echo "  remote: $remote_sha"
  echo "  Run: git merge --ff-only origin/devel"
  if [ "$FORCE" = false ]; then
    echo "Use --force to override"
    exit 1
  fi
  echo "Continuing anyway (--force)"
fi

cd "$SCRIPT_DIR"

echo "Starting compose postgres..."
$COMPOSE_CMD -f docker-compose.yaml up -d postgres

echo "Waiting for postgres..."
retries=0
until $COMPOSE_CMD -f docker-compose.yaml exec -T postgres pg_isready -U awx 2>/dev/null; do
  retries=$((retries + 1))
  if [ "$retries" -ge 15 ]; then
    echo "Error: postgres did not become ready after 30s"
    exit 1
  fi
  sleep 2
done

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo "Installing AWX dependencies in $AWX_DIR/.venv ..."
cd "$AWX_DIR"
uv venv .venv 2>/dev/null || true
sed '/^uwsgi==/d' requirements/requirements.txt > "$TMPDIR/awx-requirements-filtered.txt"
uv pip install --python .venv/bin/python \
  -r "$TMPDIR/awx-requirements-filtered.txt" \
  -r requirements/requirements_git.txt \
  -r requirements/requirements_dev.txt
uv pip install --python .venv/bin/python -e .

cat > "$TMPDIR/database.py" << 'EOF'
DATABASES = {
    'default': {
        'ATOMIC_REQUESTS': True,
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'awx',
        'USER': 'awx',
        'PASSWORD': 'awx',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}
EOF

echo "Recreating awx database..."
$COMPOSE_CMD -f "$SCRIPT_DIR/docker-compose.yaml" exec -T postgres \
  psql -U awx -d postgres -c "DROP DATABASE IF EXISTS awx;" -c "CREATE DATABASE awx OWNER awx;"

echo "Running AWX migrations..."
# AWX_SETTINGS_DIR: point AWX at our database.py override
# AWX_LOGGING_MODE: avoid file logging to /var/log/tower/
AWX_SETTINGS_DIR="$TMPDIR" \
AWX_LOGGING_MODE=stdout \
.venv/bin/awx-manage migrate --noinput

echo "Extracting schema..."
cd "$SCRIPT_DIR"
$COMPOSE_CMD -f "$SCRIPT_DIR/docker-compose.yaml" exec -T postgres pg_dump -s -U awx awx \
  | python3 "$SCRIPT_DIR/filter_pgdump.py" \
  > latest.sql

echo "Extracting initial data..."
$COMPOSE_CMD -f "$SCRIPT_DIR/docker-compose.yaml" exec -T postgres pg_dump --data-only --disable-triggers -U awx awx \
  | python3 "$SCRIPT_DIR/filter_pgdump.py" --normalize \
  > initial.sql

echo "Done. Schema written to $SCRIPT_DIR/latest.sql"
echo "       Data written to $SCRIPT_DIR/initial.sql"
echo "AWX commit: $(git -C "$AWX_DIR" rev-parse --short HEAD)"
echo "Lines: $(wc -l < latest.sql) (schema), $(wc -l < initial.sql) (data)"
