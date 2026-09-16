# Running against a real awx - compose `awx` profile

The compose environment can run a real AWX next to the mock data, using the same postgres.
See [tools/docker/README.md](../tools/docker/README.md#awx) for how the awx container is put together.


### setup

Clone awx next to metrics-utility:

```bash
cd ..
git clone https://github.com/ansible/awx
cd metrics-utility

make compose-awx  # or compose-awx-service, compose-ui, compose-ui-service
```

The first start installs awx python dependencies into a volume, which takes a few minutes.

```bash
open https://localhost:8043/api/v2/  # admin:admin
open https://localhost:8043/api/docs/
```

Jobs run for real - create a project, inventory & job template (or use the UI profile), launch, and the job data ends up in the same postgres metrics-utility collects from.
The first job pulls the `quay.io/ansible/awx-ee` execution environment inside the awx container, which also takes a few minutes.


### metrics-utility in standalone mode

Nothing changes - the compose postgres is the awx database, so run metrics-utility from the host as usual:

```bash
export METRICS_UTILITY_SHIP_TARGET=directory
export METRICS_UTILITY_SHIP_PATH=./out

uv run python manage.py gather_automation_controller_billing_data --dry-run --since=2d
```


### metrics-utility in controller mode

The metrics-utility checkout is mounted into the awx container as `/metrics-utility`,
install it into the awx virtualenv (on `PATH` already) and run it there:

```bash
podman exec -it awx /bin/bash
    # inside the container
    pip install -e /metrics-utility
    cd /metrics-utility

    export METRICS_UTILITY_SHIP_TARGET=directory
    export METRICS_UTILITY_SHIP_PATH=/tmp/out
    python manage.py gather_automation_controller_billing_data --dry-run --since=2d
```

The awx virtualenv is recreated whenever awx requirements change, so the install may need repeating after updating awx.


### update

```bash
cd ../awx
git pull --ff-only origin devel
```

Python code changes restart the awx processes automatically, requirement changes get installed on the next container start.
Git-based requirements (`requirements_git.txt`, `@devel` branches) are only reinstalled when a requirements file changes - to pick up newer commits, remove the venv volume:

```bash
podman rm -f awx && podman volume rm docker_awx_venv
```

New awx migrations are applied on start, [update the schema dump](#extract-schema) to get them into the mock data too.


### psql

```bash
make psql
    select app, max(name) from django_migrations group by app order by app;
```


### extract schema

Automated script using our compose postgres and uv (no awx containers needed):

```bash
tools/docker/extract-awx-schema.sh [AWX_DIR] [--force]
```

Defaults to `../awx` relative to the metrics-utility repo root. Requires the awx repo to be on `devel` and up to date with `origin/devel` (use `--force` to override).

The script strips pg-version-dependent output (version comments, `\restrict`, `transaction_timeout`, named NOT NULL constraints) to keep diffs stable across postgres upgrades.
