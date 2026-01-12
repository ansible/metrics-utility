# metrics-utility

metrics-utility deals with collecting, analyzing and reporting metrics from [Ansible Automation Platform (AAP)](https://www.ansible.com/products/automation-platform) Controller instances.

It provides two interfaces - a [CLI](#cli) and a python [library](#python-library).

Also see below for [dev setup](#developer-setup), and other [docs](#documentation).


### CLI

A `metrics-utility` CLI tool for collecting and reporting metrics from Controller, allowing users to:

- Collect Controller usage data from the database, settings, and prometheus
- Analyze the data and generate `.xlsx` reports
- Support multiple storage adapters for data persistence (local directory, S3)
- Push metrics data to `console.redhat.com`

It can run either standalone (against a specified postgres instance),
or inside the Controller's python virtual environment. The controller mode allows the `config` collector to collect more settings and takes DB connection details from there.

It provides two subcommands:
  - `gather_automation_controller_billing_data`
    - collects data from controller, saves daily tarballs with `.csv` / `.json` inside
    - saves tarballs in specified storage
    - optionally sends to console
  - `build_report`
    - builds a `.xlsx` report
      - 3 report types - `CCSP`, `CCSPv2`, `RENEWAL_GUIDANCE`
      - the ccsp* reports use the collected tarballs as the source
      - the renewal* report reads from controller db

Example invocation:

```bash
pip install metrics-utility

# common
export METRICS_UTILITY_SHIP_PATH="./out"
export METRICS_UTILITY_SHIP_TARGET="directory"

# gather data
metrics-utility gather_automation_controller_billing_data --ship --until=10m
ls out/data/`date +%Y/%m/%d`/ # data/<year>/<month>/<day>/<uuid>-<since>-<until>-<index>-<collection>.tar.gz

# build report
export METRICS_UTILITY_REPORT_TYPE="CCSPv2"

metrics-utility build_report --month=`date +%Y-%m` # year-month
ls out/reports/`date +%Y/%m`/ # reports/<year>/<month>/<type>-<year>-<month>.xlsx
```

See [docs/cli.md](./docs/cli.md) and [docs/old-readme.md](./docs/old-readme.md) for details on the usage,  
See [docs/environment.md](./docs/environment.md) for a full list of environment variables,  
See [docs/awx.md](./docs/awx.md) for more on running against an awx dev env.


### Python library

The `metrics_utility.library` library provides a lower-level python API exposing the same functionality using these abstractions:

* collectors - functions that collect specific data, from database to a `.csv`, or from elsewhere into a python dict
* packagers - packages multiple related `.csv` & `.json` into `.tar.gz` daily tarballs
* extractors - extracts these tarballs, loading specific data into dicts or Pandas dataframe
* rollups - group and aggregate dataframes, compute stats and optionally save them
* reports - builds a xlsx report from a set of dataframes
* storage - unified storage backend for filesystem, s3, segment, crc and db
* instants - associated datetime-related helpers
* tempdir & db locking helpers

The library uses no env variables, and doesn't rely on Controller environment.
The CLI is expected to use the library where possible, but is not limited to it.

Example use:

```python
from metrics_utility.library.collectors.controller import config, main_jobevent
from metrics_utility.library.instants import last_day, this_day
from metrics_utility.library import lock, storage

db = ... # django.db.connection / psycopg 3

dir_storage = storage.StorageDirectory(base_path='./out')

with lock(db=db, key='my-unique-key'):
    # dict, will be converted to json
    config_dict = config(db=db).gather()

    # list of .csv filenames; since is included, until is excluded
    job_csvs = main_jobevent(db=db, since=last_day(), until=this_day()).gather()

# save in storage
dir_storage.put('config.json', dict=config_dict)
for index, file in enumerate(job_csvs):
    dir_storage.put(f'main_jobevent.{index}.csv', filename=file)
    os.remove(file)
```

See [library README](./metrics_utility/library/README.md) for details.  
See [workers/](./workers/) for more library usage examples.


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
uv run ./manage.py build_report --help
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
* [tools/anonymized\_db\_perf\_data/](./tools/anonymized_db_perf_data/) - perf test data for anonymization
* [tools/collections/](./tools/collections/) - scripts for pulling list of collections from galaxy & automation hub
* [tools/docker/](./tools/docker/) - docker compose environment & mock awx data
* [tools/perf/](./tools/perf/) - perf test data generator and scripts for build report
* [tools/testathon/](./tools/testathon/) - data generator for testing

Please follow our [Contributor's Guide](./docs/CONTRIBUTING.md) for details on submitting changes and documentation standards.
