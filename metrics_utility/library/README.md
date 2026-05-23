## `metrics-utility.library`

This is a Python library for metrics-utility, exposing all the functionality in the form of python callables.

It provides collectors, a CSV file splitter, and a helper for database locking.

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


### Abstractions

#### Collector

A collector is a python function which accepts params, gathers data, and returns it in one of the supported formats.

By default, it either returns a python dict (for snapshot collectors like config),
or a pandas DataFrame (for SQL-based collectors).
When used by the CLI gather pipeline, an alternative output adapter is passed in, which writes CSV files instead of returning DataFrames.

It's exported decorated to wrap calls into a collector class, so that param passing can happen separately from `.gather()`.
The wrapper ensures that any calls to `my_collector(db=connection).gather()` do the same thing as an undecorated `my_collector(db=connection)` - this is so that initialization can happen before db locks are acquired.

When a collector accepts timestamp boundaries, they are passed in the form of `since=` and `until=` params, using datetime objects with timezone, where `since` is the first moment of the collected interval (and therefore included), while `until` is the first moment *outside* the collected interval (and therefore excluded) - this is so that we never omit the 1-2 seconds between 23:59:59 and 00:00:00 by accident.

A collector should never depend on anything that's not passed in via params (except for randomness for tempfile names),
should raise an exception when passed invalid values or a bad DB connection, but just return None, or an empty list/dict when no new data is present. (Any logic such as "since the last time" should be implemented *outside* the collector function.)

Files created by collectors are only cleaned up when called by Package, otherwise rely on having been created inside a per-job tempdir, which then gets cleaned up.

Currently supported:

Controller collectors (in `metrics_utility.library.collectors.controller`):
* `config(db, billing_provider_params).gather() -> Dict`
* `config_django(db, billing_provider_params).gather() -> Dict`
* `controller_version_service(db).gather() -> DataFrame`
* `credentials_service(db, since, until).gather() -> DataFrame`
* `execution_environments(db).gather() -> DataFrame`
* `feature_flags_service(db).gather() -> DataFrame`
* `job_host_summary(db, since, until).gather() -> DataFrame`
* `job_host_summary_service(db, since, until).gather() -> DataFrame`
* `main_host(db).gather() -> DataFrame`
* `main_hostmetric(db, since, until).gather() -> DataFrame`
* `main_host_daily(db, since, until).gather() -> DataFrame`
* `main_indirectmanagednodeaudit(db, since, until).gather() -> DataFrame`
* `main_jobevent(db, since, until).gather() -> DataFrame`
* `main_jobevent_service(db, since, until).gather() -> DataFrame`
* `table_metadata(db).gather() -> DataFrame`
* `unified_jobs(db, since, until).gather() -> DataFrame`

Service collectors (in `metrics_utility.library.collectors.service`):
* `task_executions_service(db, since, until).gather() -> DataFrame`

Dashboard collectors (in `metrics_utility.library.collectors.dashboard`):
* `dashboard_jobs(db, since, until) -> Dict`

Other collectors (in `metrics_utility.library.collectors.others`):
* `total_workers_vcpu(cluster_name, metering_enabled, prometheus_url, ca_cert_path, token) -> Dict`


#### CsvFileSplitter

`metrics_utility.library.csv_file_splitter.CsvFileSplitter` handles writing large CSV output to multiple files, splitting when a size threshold is reached. Used internally by collectors when producing CSV output for the CLI gather pipeline.


#### Lock

`metrics_utility.library.lock` provides a database-level advisory lock context manager, used to prevent concurrent gather runs.

```python
from metrics_utility.library import lock

with lock('my-unique-key', wait=False, db=db) as acquired:
    if not acquired:
        raise "too bad" # or use wait=True instead
    # ... do work under lock
```
