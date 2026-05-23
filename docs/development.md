# Development environment

## Prerequisites

- Python 3.12 or later
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Docker or Podman (with compose)
- `make`, `git`

Dependencies are managed via `pyproject.toml` (& `uv.lock`).
There is also `setup.cfg` with dependencies but those are only used for the Controller mode.

`uv` is also not required as long as you can manage your own python venv and install dependencies from `pyproject.toml`.


## Clone & install

```bash
git clone https://github.com/ansible/metrics-utility.git
cd metrics-utility
uv sync
```


## Database & minio

Tests and the gather command need a PostgreSQL database and a minio (S3) instance.

### Option A: `make compose` (recommended)

```bash
make compose
```

This starts postgres on port 5432 and minio on ports 9000/9001, imports the mock data, and creates the minio bucket and user. See [docker-compose.yaml](../tools/docker/docker-compose.yaml) for details.

`make clean` tears down the compose environment (including volumes).

To reimport SQL data into an already-running compose postgres (e.g. after updating the SQL files):

```bash
docker compose -f tools/docker/docker-compose.yaml exec postgres bash -c \
  'cat /docker-entrypoint-initdb.d/init-*.sql | psql -U awx -d postgres'
```

### Option B: bare postgres + minio

If you prefer running postgres and minio directly:

**PostgreSQL** - create a database and import the SQL files in order:

```bash
# roles first, then schema, then the rest
cat tools/docker/{roles,latest,functions,conf_setting,main_hostmetric,main_instance,main_jobhostsummary,dab_feature_flags}.sql | psql -U awx
```

The compose environment uses user `myuser` / password `mypassword` / database `awx` / port `5432`.
CI uses user `awx` / password `awx` / database `postgres` (system postgres).

Set `METRICS_UTILITY_DB_*` env vars to match your setup:

* `AWX_PATH` - path to Controller virtualenv, when *not* using `mock_awx`; defaults to `/awx_devel`
* `METRICS_UTILITY_DB_HOST` - host to talk to controller DB when using `mock_awx`
* `METRICS_UTILITY_DB_NAME` - database name (default `awx`)
* `METRICS_UTILITY_DB_USER` - database user (default `myuser`)
* `METRICS_UTILITY_DB_PASSWORD` - database password (default `mypassword`)
* `METRICS_UTILITY_DB_PORT` - database port (default `5432`)

**Minio** - set up a bucket and user:

```bash
mc alias set local http://localhost:9000 minioaccess miniosecret
mc mb --ignore-existing local/metricsutilitys3
mc admin user add local mynewuser mysecretpassword
mc admin policy attach local readwrite --user=mynewuser
mc admin accesskey create local mynewuser --access-key myuseraccesskey --secret-key myusersecretkey
```

Set `METRICS_UTILITY_BUCKET_*` env vars to match (see [gather.md](./gather.md#s3) for the full list).


## Running the utility

In development, `uv run ./manage.py` is the equivalent of the installed `metrics-utility` command:

```bash
alias metrics-utility='uv run ./manage.py'

metrics-utility --help
metrics-utility gather_automation_controller_billing_data --help
```

There are also convenience scripts for development:
- `run-ccsp2-gather` - runs gather with all optional collectors (except those needing Prometheus/service DB) to a local `./out` directory
- `run-s3-gather` - runs gather with S3 target against the compose minio

See [gather.md](./gather.md) for full CLI and environment variable reference.


## Make targets

| Target | Description |
|--------|-------------|
| `make compose` | Start postgres & minio via docker compose |
| `make clean` | Tear down the compose environment |
| `make psql` | Run psql in the postgres container |
| `make test` | Run the full test suite |
| `make coverage` | Run tests with coverage report |
| `make lint` | Run ruff check + format check |
| `make fix` | Run ruff auto-fix + format |

Podman variants: `make pcompose`, `make pclean`, `make ppsql`.


## Running tests

Some tests depend on a running postgres & minio instance - run `make compose` first.

```bash
make test                                    # full test suite
uv run pytest -s -v                          # verbose output
uv run pytest -s -v metrics_utility/test/gather/  # specific directory
uv run pytest -s -v path/to/test_file.py     # specific file
make coverage                                # coverage report (HTML + XML)
```


### Running tests in containers

To run all tests in a container at once:

```bash
docker compose -f tools/docker/docker-compose.yaml --profile=pytest up
```

For an interactive container with python & uv:

```bash
docker compose -f tools/docker/docker-compose.yaml --profile=env up -d
docker exec -it metrics-utility-env /bin/sh
# inside container:
uv run pytest -vv metrics_utility/test/gather/
```

When running in CI mode (e.g. to pass DB settings), use `exec` with `-e` flags:

```bash
docker compose -f tools/docker/docker-compose.yaml exec \
  -e METRICS_UTILITY_DB_NAME=postgres \
  -e METRICS_UTILITY_DB_USER=awx \
  -e METRICS_UTILITY_DB_PASSWORD=awx \
  -e METRICS_UTILITY_DB_HOST=postgres \
  metrics-utility-env bash -c 'uv run pytest -s -v'
```

Replace `docker` with `podman` for Podman.


## CI environment

GitHub Actions CI ([`.github/workflows/pytest.yml`](../.github/workflows/pytest.yml)) runs on bare Ubuntu with:
- system PostgreSQL (user `awx`, database `postgres`)
- minio binary downloaded directly (no container)
- SQL data imported via `cat tools/docker/{roles,latest,...}.sql | sudo su - postgres -c psql`
- same `METRICS_UTILITY_*` env vars as compose, adjusted for localhost
