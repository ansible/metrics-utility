# Gather

The `gather_automation_controller_billing_data` subcommand collects metrics from Controller:

- Collects Controller usage data from the database, settings, and Prometheus
- Supports multiple storage adapters for data persistence (local directory, S3)
- Can push metrics data to `console.redhat.com`

It saves daily tarballs with `.csv` / `.json` inside, named `<uuid>-<since>-<until>-<index>-<collection>.tar.gz`.

See [collectors.md](./collectors.md) for what data each collector gathers,
and [partitions.md](./partitions.md) for partition pruning analysis on `main_jobevent`.


## Installation

```bash
pip install metrics-utility
```


## Running modes

It can run standalone (against a specified postgres instance) or inside the Controller's python virtual environment. The Controller mode allows the `config` collector to collect more settings and takes DB connection details from there.

- **Standalone** (development & testing):
  - needs a running postgres with imported data (not a running Controller)
  - mocks some values otherwise obtained from Controller (see [`mock_awx/settings/__init__.py`](../mock_awx/settings/__init__.py))
  - see [development.md](./development.md) for setup

- **Controller** (inside Controller's virtualenv):
  - connect to a running Controller instance
  - activate the virtual environment (`source /var/lib/awx/venv/awx/bin/activate`)
  - `pip install .` from the metrics-utility dir
  - see [awx.md](./awx.md) for running against an awx dev env

- **RPM** (installed via RPM):
  - run with `metrics-utility gather_automation_controller_billing_data ...`


## CLI flags

```
gather_automation_controller_billing_data [--ship] [--dry-run] [--force] [--since=DATE] [--until=DATE]
```

- `--ship` - save/send the collected tarballs
- `--dry-run` - collect data without shipping
- `--since` / `--until` - time period selection (absolute dates like `2023-12-21`, or relative like `12m` for 12 months ago, `10m` for 10 minutes ago)
- `--force` - skip the last-gathered check and collect regardless
- `--help` - show all available flags


## Storage adapters

Set `METRICS_UTILITY_SHIP_TARGET` to one of: `directory`, `s3`, `crc`.

### Local directory

```bash
export METRICS_UTILITY_SHIP_TARGET=directory
export METRICS_UTILITY_SHIP_PATH=/path/to/gathered/data

metrics-utility gather_automation_controller_billing_data --ship --until=10m
# tarballs saved under $SHIP_PATH/data/<year>/<month>/<day>/
```

### S3

```bash
export METRICS_UTILITY_SHIP_TARGET=s3
export METRICS_UTILITY_SHIP_PATH=metrics-utility/gathered-data

export METRICS_UTILITY_BUCKET_NAME=metrics-utility
export METRICS_UTILITY_BUCKET_ENDPOINT=<endpoint to your S3>
# export METRICS_UTILITY_BUCKET_REGION="us-east-1"  # for AWS S3

export METRICS_UTILITY_BUCKET_ACCESS_KEY=<access_key>
export METRICS_UTILITY_BUCKET_SECRET_KEY=<secret_key>

metrics-utility gather_automation_controller_billing_data --ship --until=10m
```

### console.redhat.com (CRC)

Pushes data to console.redhat.com. Automatically tracks last collected interval and collects up to a 4-week gap. The `--until=10m` gives time for fresh data to be inserted into Controller's database. Run as a cronjob.

```bash
export METRICS_UTILITY_SHIP_TARGET=crc
export METRICS_UTILITY_SERVICE_ACCOUNT_ID=<service account name>
export METRICS_UTILITY_SERVICE_ACCOUNT_SECRET=<service account secret>
export METRICS_UTILITY_OPTIONAL_COLLECTORS=""

# AWS billing
export METRICS_UTILITY_BILLING_PROVIDER=aws
export METRICS_UTILITY_BILLING_ACCOUNT_ID="<AWS 12-digit customer id>"
export METRICS_UTILITY_RED_HAT_ORG_ID="<Red Hat org id tied to the AWS billing account>"

metrics-utility gather_automation_controller_billing_data --ship --until=10m
```

Inspect what would be sent:
```bash
metrics-utility gather_automation_controller_billing_data --dry-run --since=2023-12-21 --until=2023-12-22
```


## Environment variables

All known metrics-utility environment variables.

### Special variables

* `KUBERNETES_SERVICE_PORT` - used by collectors' `get_install_type` for `k8s` detection
* `container` - used by collectors' `get_install_type` for `oci` detection

### Billing provider params (stored in `config.json`)

* `METRICS_UTILITY_BILLING_ACCOUNT_ID` - `billing_account_id`
* `METRICS_UTILITY_BILLING_PROVIDER` - `billing_provider`
* `METRICS_UTILITY_RED_HAT_ORG_ID` - `red_hat_org_id`

### S3

* `METRICS_UTILITY_BUCKET_ACCESS_KEY` - `bucket_access_key`
* `METRICS_UTILITY_BUCKET_ENDPOINT` - `bucket_endpoint`
* `METRICS_UTILITY_BUCKET_NAME` - `bucket_name`
* `METRICS_UTILITY_BUCKET_REGION` - `bucket_region`
* `METRICS_UTILITY_BUCKET_SECRET_KEY` - `bucket_secret_key`

### CRC

* `METRICS_UTILITY_CRC_INGRESS_URL` - upload URL
* `METRICS_UTILITY_CRC_SSO_URL` - login URL
* `METRICS_UTILITY_PROXY_URL` - upload proxy
* `METRICS_UTILITY_SERVICE_ACCOUNT_ID` - account id
* `METRICS_UTILITY_SERVICE_ACCOUNT_SECRET` - secret

### Gather configuration

* `METRICS_UTILITY_CLUSTER_NAME` - `total_workers_vcpu` collector payload `.cluster_name` (required when that collector is enabled)
* `METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX` - suffix added to the lock name; must be set when multiple cronjobs run to avoid one blocking the others
* `METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR` - [true/false] disable `job_host_summary` collector (default false); useful when multiple cronjobs run to collect data (use together with `METRICS_UTILITY_OPTIONAL_COLLECTORS`)
* `METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES` - [true/false] skip updating last gather info from controller settings (default false); some collectors take a point-in-time sample rather than using since/until
* `METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS` - maximum length of collection interval in days (default 28)
* `METRICS_UTILITY_OPTIONAL_COLLECTORS` - optional collectors, comma-separated list
* `METRICS_UTILITY_PROMETHEUS_URL` - Prometheus base URL
* `METRICS_UTILITY_SHIP_PATH` - directory in local path or S3
* `METRICS_UTILITY_SHIP_TARGET` - one of `directory`, `s3`, `crc`
* `METRICS_UTILITY_USAGE_BASED_METERING_ENABLED` - [true/false] `total_workers_vcpu` collector toggle; when disabled (=false, default), skips Kubernetes worker filtering


