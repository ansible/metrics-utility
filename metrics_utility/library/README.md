## `metrics-utility.library`

This is a Python library for metrics-utility, exposing the data collection functionality in the form of python callables.

It provides an abstraction over collectors, plus a segment.com storage backend and a DB locking helper. It is shared with the external metrics-service, which is its main consumer, so it uses no env variables and doesn't rely on the Controller environment - everything is passed in via params.

Anonymization and rollup of the collected data lives in the sibling [`metrics_utility.anonymized_rollups`](../anonymized_rollups/) package.


### Abstractions

#### Collector

Collector is a python function which accepts params, gathers data, and returns it in one of the supported formats.

It either returns a python dict (for snapshot collectors like config),
or a pandas DataFrame (for SQL-based collectors).

It's exported decorated to wrap calls into BaseCollector subclass instances, so that param passing can happen separately from .gather().
The wrapper ensures that any calls to `my_collector(db=connection).gather()` do the same thing as an undecorated `my_collector(db=connection)` - this is so that initialization can happen before db locks are acquired.

When a collector accepts timestamp boundaries, they are passed in in the form of `since=` and `until=` params, using datetime object with timezone, where `since` is the first moment of the collected interval (and therefore included), while `until` is the first moment *outside* the collected interval (and therefore excluded) - this is so that we never omit the 1-2 seconds between 23:59:59 and 00:00:00 by accident.

A collector should never depend on anything that's not passed in via params (except for randomness for tempfile names),
should raise an exception when passed invalid values or a bad DB connection, but just return None, or an empty list/dict when no new data is present. (Any logic such as "since the last time" should be implemented *outside* the collector function.)

Files created by collectors rely on having been created inside a per-job tempdir, which then gets cleaned up by the caller.

Currently supported:

Controller collectors (in `metrics_utility.library.collectors.controller`):
* `config(db, billing_provider_params).gather() -> Dict`
* `config_django(...).gather() -> Dict`
* `controller_version_service(db).gather() -> DataFrame`
* `credentials_service(db).gather() -> DataFrame`
* `execution_environments(db).gather() -> DataFrame`
* `feature_flags_service(db).gather() -> DataFrame`
* `job_host_summary(db, since, until).gather() -> DataFrame`
* `job_host_summary_service(db, since, until).gather() -> DataFrame`
* `main_host(db).gather() -> DataFrame`
* `main_host_daily(db, since, until).gather() -> DataFrame`
* `main_hostmetric(db, since, until).gather() -> DataFrame`
* `main_indirectmanagednodeaudit(db, since, until).gather() -> DataFrame`
* `main_jobevent(db, since, until).gather() -> DataFrame`
* `main_jobevent_service(db, since, until).gather() -> DataFrame`
* `table_metadata(db).gather() -> DataFrame`
* `unified_jobs(db, since, until).gather() -> DataFrame`
* `unified_jobs_dashboard(db, since, until).gather() -> DataFrame`

Dashboard collectors (in `metrics_utility.library.collectors.dashboard`):
* `dashboard_jobs(...)` - plus the `AWXJobType`, `AWXJobHostSummaryType`, `DashboardJobsResultType` types and the `get_min_max_job_id_query` query helper

Service collectors (in `metrics_utility.library.collectors.service`):
* `task_executions_service(...)`

Other collectors (in `metrics_utility.library.collectors.others`):
* `total_workers_vcpu(cluster_name, metering_enabled, prometheus_url, ca_cert_path, token) -> Dict`


#### Storage

`StorageSegment` (in `metrics_utility.library.storage`) provides a put-only interface for pushing data to [segment analytics](https://segment.com/docs/connections/sources/catalog/libraries/server/python/).

```python
from metrics_utility.library.storage import StorageSegment

# debug = bool
# user_id = string, passed to analytics.track
# write_key = segment.com source write key

storage = StorageSegment(
    debug=False,
    user_id='unknown',
    write_key='...',
)
```

The CLI keeps its own storage backends for filesystem, S3 and console.redhat.com under `metrics_utility.automation_controller_billing`.


### Helpers

#### DB locking (`library.lock`)

`lock` (in `metrics_utility.library.lock`) is a context manager wrapping a PostgreSQL advisory lock, used to prevent concurrent collection runs from stepping on each other.

```python
from metrics_utility.library import lock

with lock('my-unique-key', wait=False, db=db) as acquired:
    if not acquired:
        raise 'too bad'  # or use wait=True instead
    ...
```
