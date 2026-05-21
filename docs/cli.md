# Running the CLI

The standalone mode is currently used only for development & testing. It does not need a running awx instance (only a running postgres with imported data), and mocks some values otherwise obtained from awx (see [`mock_awx/settings/__init__.py`](../mock_awx/settings/__init__.py)).


### Basic Usage

1. Know the environment
  - In Controller mode:
    - make sure to connect to a running Controller instance,
    - get metrics-utility (map a volume, or git clone),
    - activate the virtual environment (`source /var/lib/awx/venv/awx/bin/activate`),
    - `pip install .` from the `metrics-utility` dir,
    - run utility using `python manage.py ...`.
    - see [`docs/awx.md`](./awx.md) for more
  - In RPM mode:
    - install the right RPM
    - run utility using `metrics-utility ...`.
  - **In standalone mode**:
    - make sure to run `docker compose -f tools/docker/docker-compose.yaml up` if you need the database or minio,
    - or set `METRICS_UTILITY_DB_*` env vars correctly,
    - run utility using `uv run python manage.py ...`.

1. Run `gather_automation_controller_billing_data` - collects metrics from controller db, saves daily tarballs with csv/json inside

1. Pick a time period
  - `--since=12m`
  - and `--until=10m`

1. Use `--help` to see any other params
  - `gather_automation_controller_billing_data` also supports `--dry-run` and `--ship`

1. Set any other necessary environmental variable
  - see [`docs/environment.md`](./environment.md) for a full list of the environment variables
  - see [`docs/old-readme.md`](./old-readme.md) for more usage examples


#### Example gather run

```bash
export METRICS_UTILITY_SHIP_PATH="./out"
export METRICS_UTILITY_SHIP_TARGET="directory"

# collect data
uv run ./manage.py gather_automation_controller_billing_data --ship --until=10m

# collected tarballs somewhere here (by date and instance uuid)
ls out/data/`date +%Y/%m/%d`/
```
