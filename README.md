# metrics-utility

metrics-utility deals with collecting metrics from [Ansible Automation Platform (AAP)](https://www.ansible.com/products/automation-platform) Controller instances.

It provides two interfaces - a [CLI](#cli) and a python [library](#python-library).

Also see below for [dev setup](#developer-setup), and other [docs](#documentation).


### CLI

A `metrics-utility` CLI tool for collecting metrics from Controller, allowing users to:

- Collect Controller usage data from the database, settings, and prometheus
- Support multiple storage adapters for data persistence (local directory, S3)
- Push metrics data to `console.redhat.com`

It can run either standalone (against a specified postgres instance),
or inside the Controller's python virtual environment. The controller mode allows the `config` collector to collect more settings and takes DB connection details from there.

It provides one subcommand:
  - `gather_automation_controller_billing_data`
    - collects data from controller, saves daily tarballs with `.csv` / `.json` inside
    - saves tarballs in specified storage
    - optionally sends to console

Example invocation:

```bash
pip install metrics-utility

# common
export METRICS_UTILITY_SHIP_PATH="./out"
export METRICS_UTILITY_SHIP_TARGET="directory"

# gather data
metrics-utility gather_automation_controller_billing_data --ship --until=10m
ls out/data/`date +%Y/%m/%d`/ # data/<year>/<month>/<day>/<uuid>-<since>-<until>-<index>-<collection>.tar.gz
```

See [docs/cli.md](./docs/cli.md) and [docs/old-readme.md](./docs/old-readme.md) for details on the usage,  
See [docs/environment.md](./docs/environment.md) for a full list of environment variables,  
See [docs/awx.md](./docs/awx.md) for more on running against an awx dev env.


### Python library

The `metrics_utility.library` library provides a lower-level python API exposing the same functionality using these abstractions:

* collectors - functions that collect specific data, from database to a `.csv`, or from elsewhere into a python dict
* csv file splitter - splits large CSV output into multiple files
* db locking helper

The library uses no env variables, and doesn't rely on Controller environment.
The CLI is expected to use the library where possible, but is not limited to it.

Example use:

```python
from metrics_utility.library.collectors.controller import config, main_jobevent
from metrics_utility.library import lock

db = ... # django.db.connection / psycopg 3

with lock('my-unique-key', wait=False, db=db) as acquired:
    if not acquired:
        raise "too bad" # or use wait=True instead

    # dict, will be converted to json
    config_dict = config(db=db).gather()

    # list of .csv filenames; since is included, until is excluded
    job_csvs = main_jobevent(db=db, since=since, until=until).gather()
```

See [library README](./metrics_utility/library/README.md) for details.


## Developer setup

### Prerequisites

- Python 3.12 or later
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Docker compose
- `make`, `git`

Dependencies are managed via `pyproject.toml` (& `uv.lock`).
There is also `setup.cfg` with dependencies but those are only used for the controller mode.

The Docker compose environment is used to provide a quick postgres & minio instances on ports 5432 and 9000/9001, but they can be replaced with local setup. See [docker-compose.yaml](./tools/docker/docker-compose.yaml) for details of the `mc` setup (substitute the `minio` hostname for localhost), and [tools/docker/\*.sql](./tools/docker/) for users & data to import in postgres (start with `roles.sql` and `latest.sql`). (Or don't, and use docker.)

`uv` is also not required as long as you can manage your own python venv and install dependencies from `pyproject.toml`.

Optionally, `uvx pre-commit install` to run ruff checks from a pre-commit hook, defined in [.pre-commit-config.yaml](../.pre-commit-config.yaml). Or you can run `make lint` / `make fix` manually.


### Installation

```bash
# Clone the repository
git clone https://github.com/ansible/metrics-utility.git
cd metrics-utility

# Install dependencies using uv
uv sync
```


### Run

```bash
cd metrics-utility
make compose
```

```bash
cd metrics-utility
uv run ./manage.py --help
uv run ./manage.py gather_automation_controller_billing_data --help
```

`make clean` resets the docker environment,
`make lint` & `make fix` run the linters & formatters,
`make psql` runs psql in the postgres container.


### Tests

Some tests depend on a running postgres & minio instance - run `make compose` to get one.

`make test` runs the full test suite,
`make coverage` produces a coverage report.

Use `uv run pytest -s -v` for running tests with verbose output, also accepts test filenames.

See [docs/tests-compose.md](./docs/tests-compose.md) to run the tests inside the docker compose environment.


## Documentation

More documentation is available in [docs/](./docs/), and elsewhere:

* [CHANGELOG.md](./CHANGELOG.md) - changes between tagged releases
* [LICENSE.md](./LICENSE.md) - the Apache-2.0 license
* [README.md](./README.md) - this README
* [docs/CONTRIBUTING.md](./docs/CONTRIBUTING.md) - Contributor's guide
* [docs/awx.md](./docs/awx.md) - running against awx dev env
* [docs/cli.md](./docs/cli.md) - CLI docs
* [docs/environment.md](./docs/environment.md) - Environment variables
* [docs/old-readme.md](./docs/old-readme.md) - pre-0.5 README, with more examples
* [docs/tests-compose.md](./docs/tests-compose.md) - running tests inside docker compose
* [docs/vcpu.md](./docs/vcpu.md) - docs for the total workers vcpu collector
* [metrics\_utility/library/](./metrics_utility/library/) - library documentation
* [tools/docker/](./tools/docker/) - docker compose environment & mock awx data

Please follow our [Contributor's Guide](./docs/CONTRIBUTING.md) for details on submitting changes and documentation standards.
