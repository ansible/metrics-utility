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

1. Pick a task (goes right after the previous command)
  - `gather_automation_controller_billing_data` - collects metrics from controller db, saves daily tarballs with csv/json inside
  - `build_report` - builds a XLSX report, either from controller db or collected tarballs

1. Pick a report type (`export METRICS_UTILITY_REPORT_TYPE=...`)
  - `CCSPv2` - uses metrics tarballs to produce a usage report
  - `CCSP` - similar to v2, slightly different aggregation
  - `RENEWAL_GUIDANCE` - uses controller db to produce a renewal guidance report

1. Pick a time period
  - `--since=12m`
  - and `--until=10m` (only with `CCSP` and `CCSPv2`)
  - or `--month=2024-06` (only with `build_report`)

1. Use `--help` to see any other params
  - `build_report` also supports `--ephemeral`, `--force` and `--verbose`
  - `gather_automation_controller_billing_data` also supports `--dry-run` and `--ship`

1. Set any other necessary environmental variable
  - see [`docs/environment.md`](./environment.md) for a full list of the environment variables
  - see [`docs/old-readme.md`](./old-readme.md) for more usage examples


#### Example CCSPv2 run

```bash
# You can use also an env-file but then you must export it with `export UV_ENV_FILE=<your_env_file>`
export METRICS_UTILITY_REPORT_TYPE="CCSPv2"
export METRICS_UTILITY_SHIP_PATH="./test/test_data/"
export METRICS_UTILITY_SHIP_TARGET="directory"

export METRICS_UTILITY_PRICE_PER_NODE=11.55 # in USD
export METRICS_UTILITY_REPORT_COMPANY_NAME="Partner A"
export METRICS_UTILITY_REPORT_EMAIL="email@email.com"
export METRICS_UTILITY_REPORT_END_USER_CITY="Springfield"
export METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME="Customer A"
export METRICS_UTILITY_REPORT_END_USER_COUNTRY="US"
export METRICS_UTILITY_REPORT_END_USER_STATE="TX"
export METRICS_UTILITY_REPORT_H1_HEADING="CCSP NA Direct Reporting Template"
export METRICS_UTILITY_REPORT_PO_NUMBER="123"
export METRICS_UTILITY_REPORT_RHN_LOGIN="test_login"
export METRICS_UTILITY_REPORT_SKU="MCT3752MO"
export METRICS_UTILITY_REPORT_SKU_DESCRIPTION="EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)"

# collect data
uv run ./manage.py gather_automation_controller_billing_data --ship --until=10m --force

# collected tarballs somewhere here (by date and instance uuid)
ls metrics_utility/test/test_data/data/2024/04/*

# build report, overwrite existing if necessary
uv run ./manage.py build_report --month=2024-04 --force

# resulting XLSX
ls metrics_utility/test/test_data/reports/2024/04/
```


#### Example RENEWAL\_GUIDANCE run

```bash
export METRICS_UTILITY_REPORT_TYPE="RENEWAL_GUIDANCE"
export METRICS_UTILITY_SHIP_PATH="./out"
export METRICS_UTILITY_SHIP_TARGET="controller_db"

uv run ./manage.py build_report --since=12months --ephemeral=1month --force
```
