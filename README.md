# AAP metrics-utility

A standalone CLI utility for collecting and reporting metrics from [Ansible Automation Platform (AAP)](https://www.ansible.com/products/automation-platform) Controller instances. This tool allows users to:

- Collect and analyze Controller usage data
- Generate reports (`CCSP`, `CCSPv2`, `RENEWAL_GUIDANCE`)
- Support multiple storage adapters for data persistence (local dir, S3)
- Push metrics data to `console.redhat.com`

## Quick Start

The project can run in 2 modes - real controller, and standalone.

The controller mode is for running inside Controller containers and is more fully documented in [`docs/old-readme.md`](./docs/old-readme.md).

The standalone mode is currently used only for development & testing. It does not need a running awx instance (only a running postgres with imported data), and mocks some values otherwise obtained from awx (see [`mock_awx/settings/__init__.py`](./mock_awx/settings/__init__.py)).

### Prerequisites (standalone)

- **Python 3.11 or later**
- **[uv](https://github.com/astral-sh/uv) package manager** (Install with `pip install uv` if not already installed)
- **Dependencies managed via `pyproject.toml`** (Ensure `uv.lock` is used for consistency)
- **MacOS** `brew install postgresql`, if you need the database or minio

### Installation (standalone)

```bash
# Clone the repository
git clone https://github.com/ansible/metrics-utility.git
cd metrics-utility

# Install dependencies using uv
uv sync
```

For more about the development setup, see the [Developer Setup Guide](./docs/developer_setup.md).

### Tests (standalone)

Run tests using `uv run pytest -s -v`. Some tests depend on a running postgres & minio instance - run `docker compose -f tools/docker/docker-compose.yaml up` to get one.

You can also run pytest inside a container too - to run all tests once, you can `docker compose -f tools/docker/docker-compose.yaml --profile=pytest up`. You use also `podman`.

For more flexibility, use:

```
(host) $ docker compose -f tools/docker/docker-compose.yaml --profile=env up -d  # runs a metrics-utility-env container with python & uv set up
(host) $ docker exec -it metrics-utility-env /bin/sh # (wait for postgres & minio containers to start before running)
(container) $ uv run pytest -vv metrics_utility/test/ccspv_reports/test_complex_CCSP_with_scope.py # 1 test
(container) $ uv run pytest -vv metrics_utility/test/ccspv_reports # all ccsp tests
```

#### Using Docker (in CI mode to be able to run all tests)

```bash
# Ensure SQL data is loaded (only needed once after starting containers)
docker compose -f tools/docker/docker-compose.yaml exec postgres bash -c \
  'cat /docker-entrypoint-initdb.d/init-*.sql | psql -U awx -d postgres'

# Run all gather tests
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v metrics_utility/test/gather/'

# Run a specific gather test
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v metrics_utility/test/gather/test_jobhostsummary_gather.py::test_command'

# Run all tests (not just gather)
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v'
```

#### Using Podman (in CI mode to be able to run all tests)

```bash
# Ensure SQL data is loaded (only needed once after starting containers)
podman compose -f tools/docker/docker-compose.yaml exec postgres bash -c \
  'cat /docker-entrypoint-initdb.d/init-*.sql | psql -U awx -d postgres'

# Run all gather tests
podman compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v metrics_utility/test/gather/'

# Run a specific gather test
podman compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v metrics_utility/test/gather/test_jobhostsummary_gather.py::test_command'

# Run all tests (not just gather)
podman compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v'
```

### Basic Usage

1. Know the environment
  - In Controller mode:
    - make sure to connect to a running Controller instance,
    - get metrics-utility (map a volume, or git clone),
    - activate the virtual environment (`source /var/lib/awx/venv/awx/bin/activate`),
    - `pip install .` from the `metrics-utility` dir,
    - run utility using `python manage.py ...`.
  - In RPM mode:
    - install the right RPM
    - run utility using `metrics-utility ...`.
  - **In standalone mode**:
    - make sure to run `docker compose -f tools/docker/docker-compose.yaml up` if you need the database or minio,
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

1. Set any other necessary environmental variable - see more in [`docs/old-readme.md`](./docs/old-readme.md).
  - `METRICS_UTILITY_BILLING_ACCOUNT_ID`
  - `METRICS_UTILITY_BILLING_PROVIDER`
  - `METRICS_UTILITY_BUCKET_ACCESS_KEY`
  - `METRICS_UTILITY_BUCKET_ENDPOINT`
  - `METRICS_UTILITY_BUCKET_NAME`
  - `METRICS_UTILITY_BUCKET_REGION`
  - `METRICS_UTILITY_BUCKET_SECRET_KEY`
  - `METRICS_UTILITY_CRC_INGRESS_URL`
  - `METRICS_UTILITY_CRC_SSO_URL`
  - `METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS`
  - `METRICS_UTILITY_OPTIONAL_COLLECTORS`
  - `METRICS_UTILITY_ORGANIZATION_FILTER`
  - `METRICS_UTILITY_PRICE_PER_NODE`
  - `METRICS_UTILITY_PROXY_URL`
  - `METRICS_UTILITY_RED_HAT_ORG_ID`
  - `METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER`
  - `METRICS_UTILITY_REPORT_COMPANY_NAME`
  - `METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER`
  - `METRICS_UTILITY_REPORT_EMAIL`
  - `METRICS_UTILITY_REPORT_END_USER_CITY`
  - `METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME`
  - `METRICS_UTILITY_REPORT_END_USER_COUNTRY`
  - `METRICS_UTILITY_REPORT_END_USER_STATE`
  - `METRICS_UTILITY_REPORT_H1_HEADING`
  - `METRICS_UTILITY_REPORT_PO_NUMBER`
  - `METRICS_UTILITY_REPORT_RHN_LOGIN`
  - `METRICS_UTILITY_REPORT_SKU`
  - `METRICS_UTILITY_REPORT_SKU_DESCRIPTION`
  - `METRICS_UTILITY_REPORT_TYPE`
  - `METRICS_UTILITY_SERVICE_ACCOUNT_ID`
  - `METRICS_UTILITY_SERVICE_ACCOUNT_SECRET`
  - `METRICS_UTILITY_SHIP_PATH`
  - `METRICS_UTILITY_SHIP_TARGET`

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

## Documentation

Documentation is available in the [`/docs` directory](./docs).

Please note that this is the upstream documentation for the metrics-utility project. Additional internal downstream documentation, accessible only to the Ansible organization, is maintained separately in the [Ansible Handbook](https://github.com/ansible/handbook/tree/main/The%20Ansible%20Engineering%20Handbook/docs/AAP/Services/Metrics).

As the project grows, more guides and references will be added to the `/docs` folder.

## Version mapping

|metrics-utility version|AAP version|
|-|-|
|0.4.1|2.4\*|
|0.5.0|2.5-20250507|
|0.6.0|2.5.20250924 & 2.6|

## Contributing

Please follow our [Contributor's Guide](./docs/contributing/CONTRIBUTING.md) for details on submitting changes and documentation standards.
