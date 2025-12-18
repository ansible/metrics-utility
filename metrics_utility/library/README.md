## `metrics-utility.library`

This is a Python library for metrics-utility, exposing all the functionality in the form of python callables.

It provides an abstraction over collectors, packaging and storage, extraction, rollups, dataframes and reports, as well as helper functions for tempdirs, locking, and datetime handling.


### Abstractions

#### Collector

Collector is python function which accepts params, gathers data, and returns it in one of the supported formats.

It either returns a python dict, which gets serialized into JSON,
or a list of filenames of temporary files it created.

It's exported decorated to wrap calls into BaseCollector subclass instances, so that param passing can happen separately from .gather().
The wrapper ensures that any calls to `my_collector(db=connection).gather()` do the same thing as an undecorated `my_collector(db=connection)` - this is so that initialization can happen before db locks are acquired.

When a collector accepts timestamp boundaries, they are passeda in in the form of `since=` and `until=` params, using datetime object with timezone, where `since` is the first moment of the collected interval (and therefore included), while `until` is the first moment *outside* the collected interval (and therefore excluded) - this is so that we never omit the 1-2 seconds between 23:59:59 and 00:00:00 by accident.

A collector should never depend on anything that's not passed in via params (except for randomness for tempfile names),
should raise an exception when passed invalid values or a bad DB connection, but just return None, or an empty list/dict when no new data is present. (Any logic such as "since the last time" should be implemented *outside* the collector function.)

Files created by collectors are only cleaned up when called by Package, otherwise rely on having been created inside a per-job tempdir (see helpers), which then gets cleaned up.

Currently supported:

Controller collectors (in `metrics_utility.library.collectors.controller`):
* `config(db, billing_provider_params).gather() -> Dict`
* `execution_environments(db, [output_dir]).gather() -> [filenames]`
* `job_host_summary(db, since, until, [output_dir]).gather() -> [filenames]`
* `job_host_summary_service(db, since, until, [output_dir]).gather() -> [filenames]`
* `main_host(db, [output_dir]).gather() -> [filenames]`
* `main_indirectmanagednodeaudit(db, since, until, [output_dir]).gather() -> [filenames]`
* `main_jobevent(db, since, until, [output_dir]).gather() -> [filenames]`
* `main_jobevent_service(db, since, until, [output_dir]).gather() -> [filenames]`
* `unified_jobs(db, since, until, [output_dir]).gather() -> [filenames]`

Other collectors (in `metrics_utility.library.collectors.others`):
* `total_workers_vcpu(cluster_name, metering_enabled, prometheus_url, ca_cert_path, token) -> Dict`


#### Package

When multiple collectors are called, or the same collector is called multiple times, they are independent of each other.
Such artifact can still be stored in Storage, but only independently.

For grouping things together, we have a Package class, which takes a list of initialized collectors, plus configuration for size constraints and naming files, and produces a stream of `.tar.gz` files, each containing:

* `config.json` - produced by the `config` collector, saved in each tarball
* `manifest.json` - produced internally by Package, contains version info for each used collector
* `data_collection_status.csv` - produced internally by Package, contains start/stop & success info for each collector run
* 1 or more `*.json` and `*.csv` files, obtained by running the next collector while there are any - a collector can produce multiple files, ending up accross multiple tarballs

Such tarball can then be passed to a Storage class, and gets cleaned up afterwards.


#### Storage

Storage objects serve to provide a shared interface for various storage modes. Each can be initialized with an appropriate configuration, and can retrieve or save objects from/to long-term storage.

Mainly S3 and local directories are supported,
but the Storage mechanism can also be used to push the data to cloud APIs or to save it in a local DB.

Common API:

* `storage.put(name, ...)` - should upload to storage, and retry/raise on failure.
    * `storage.put(name, dict=data)` - uploads a dict, likely as json data, or a .json file
    * `storage.put(name, filename=path)` - uploads a local file (by name)
    * `storage.put(name, fileobj=handle)` - uploads an opened local file or a compatible object (by a file-like handle)
* `storage.get(name)` - (context manager) should download from storage into a temporary file, yield the temporary filename, and remove the file again.

Also supported - `exists(name) -> Bool`, `remove(name)`, `glob(pattern) -> [filenames]`.

Implemented storage classes:

```
# StorageDirectory - local directory structure under base_path
#
# base_path = METRICS_UTILITY_SHIP_PATH

StorageDirectory(
    base_path='./',
)
```

```
# StorageS3 - S3 or minio
#
# bucket = METRICS_UTILITY_BUCKET_NAME
# endpoint = METRICS_UTILITY_BUCKET_ENDPOINT
# region = METRICS_UTILITY_BUCKET_REGION
# access_key = METRICS_UTILITY_BUCKET_ACCESS_KEY
# secret_key = METRICS_UTILITY_BUCKET_SECRET_KEY

StorageS3(
    bucket='name',
    endpoint='http://localhost:9000', # or 'https://s3.us-east.example.com'
    region='us-east-1', # optional
    access_key='...',
    secret_key='...',
)
```

```
# StorageSegment - segment analytics (put-only)
#
# debug = bool
# user_id = string, passed to analytics.track
# write_key = https://segment.com/docs/connections/sources/catalog/libraries/server/python/#getting-started

StorageSegment(
    debug=False,
    user_id='unknown',
    write_key='...',
)
```

```
# StorageCRC - console.redhat.com, using service accounts (put-only)
#
# client_id = METRICS_UTILITY_SERVICE_ACCOUNT_ID
# client_secret = METRICS_UTILITY_SERVICE_ACCOUNT_SECRET
# ingress_url = METRICS_UTILITY_CRC_INGRESS_URL
# proxy_url = METRICS_UTILITY_PROXY_URL
# sso_url = METRICS_UTILITY_CRC_SSO_URL
# verify_cert_path = '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'

StorageCRC(
    client_id='00000000-0000-0000-0000-000000000000',
    client_secret='...',
    ingress_url='https://console.redhat.com/api/ingress/v1/upload',
    proxy_url=None,
    sso_url='https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token',
    verify_cert_path='/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
)
```

```
# StorageCRCMutual - console.redhat.com, using mutual tls (put-only)
#
# ingress_url = METRICS_UTILITY_CRC_INGRESS_URL
# proxy_url = METRICS_UTILITY_PROXY_URL
# session_cert = ('/etc/pki/consumer/cert.pem', '/etc/pki/consumer/key.pem')
# verify_cert_path = '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'

StorageCRCMutual(
    ingress_url='https://console.redhat.com/api/ingress/v1/upload',
    proxy_url=None,
    session_cert=('/etc/pki/consumer/cert.pem', '/etc/pki/consumer/key.pem'),
    verify_cert_path='/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
)
```


#### Extractors

The opposite of `Package`, an extractor can take a set of files (obtained from storage.get), and read a set of dataframes from them, optionally filtered to select a subset of dataframes to load.

The returned dataframes are raw, but compatible with the `add_*` methods of our named Dataframe classes.


#### Dataframes

A pandas dataframe object with extras - a dataframe always knows about its fields and indexes even when empty,
has an `add_csv` method that accepts pre-rollup dataframes, has a `group` method to convert them to post-rollup dataframes,
has an `add_parquet` method that accepts rollup dataframes, has a `regroup` method to reaggregate,
and a `to_csv` / `to_parquet` / `to_json` set of methods to convert to storable artifacts again.

A rollup is the process of building a dataframe from raw csv files, and saving the grouped/aggregated result back into a parquet file.


#### Reports

Reports are predefined classes which take a set of dataframes, along with additional config, and create a XLSX file with a specific report. ReportCCSP, ReportCCSPv2 and ReportRenewalGuidance are implemented.

The xlsx file can again be passed to storage.


### Helpers

#### Datetime helpers (`library.instants`)

The `instants` module provides helper functions for working with datetime values. All functions return `datetime.datetime` objects with timezone set to UTC. These helpers are designed to work with the collector convention where `since` is the first moment of the collected interval (inclusive), while `until` is the first moment outside the interval (exclusive).

Available helpers:

* `now()` - current moment in UTC
* `this_minute()`, `this_hour`, `this_day`, `this_week`, `this_month` - start of the current time period
* `last_hour(relative_to=this_hour())`, `last_day`, `last_week`, `last_month` - start of the previous time period (relative to the provided datetime or current time)
* `minutes_ago(n, relative_to=this_minute())`, `hours_ago`, `days_ago`, `weeks_ago`, `months_ago` - start of the time period n periods ago
* `iso(dt)` - convert datetime to ISO 8601 string format

Example usage:

```python
from metrics_utility.library.instants import now, days_ago

# Get data for the last 30 days
since = days_ago(30)
until = now()
```
