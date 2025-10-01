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

* `StorageDirectory(base_path='./')`
* `StorageS3(...bucket, auth, server...)`
* `StorageCRC(...server, auth...)`

`storage.put(name, data)` - should upload to storage, and retry/raise on failure.
`storage.get(name)` - (context manager) should download from storage into a temporary file, yield the temporary filename, and remove the file again.

The supported `data` formats would still be a local file, an array of local files (crc), or JSON/dict data (crc).

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


### helpers

TODO lock temp date
