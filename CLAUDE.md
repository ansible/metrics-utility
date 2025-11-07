Most of the repo is a python CLI command - `metrics-utility`, aka `manage.py` is the entrypoint for that.
It's based on Django, provides 2 subcommands - `gather_automation_controller_billing_data` & `build_report`.

Gather collects data from a Controller (other product) DB, needs a running controller db (a development version is started by `make compose`), and produces tarballs with collected csvs, optionally sending them to a remote service, or local or S3 storage.
Build reads these tarballs for a given time interval, and produces reports based on that. `CCSP` & `CCSPv2` are the main reports, `RENEWAL_GUIDANCE` is another, but that one reads directly from the DB. The reports are XLSX.
(There will be a third commmand - rollup, to deal with *dataframe* and *rollup* logic, allowing to save pre-processed data without doing reports.)

Some of the CLI logic is shared with an external service, and thus moved into `metrics_utility/library/`. Code inside the library should not import anything from outside the library (other than external dependencies), and it should not read any environment variables, use function params instead. (One exception being a collector reading the environment to determine oci/k8s/other.)

The codebase supports both psycopg2 and psycopg (=version 3) (whatever Django connection returns), by wrapping some of the cursor handling differences in library.collectors.util `_copy_table`, and by never importing `psycopg` directly without a try:catch. Keep that compatibility by doing the same if psycopg is needed. The v2 fallback doesn't have to be perfect, just compatible enough not to break.

Tests can be run via `uv run pytest...`, or `make test`. Tests can rely on the mock database & minio provided by `make compose`, with mock data from `tools/docker/*.sql`. Tests should never compare floats directly, use `pytest.approx` for that.
Tests live in `metrics_utility/test/`, with tests for `metrics_utility/library/` under `metrics_utility/test/library/`.

When running ad-hoc python one-liners to test some of the code, only the library parts can be directly imported. To import anything other than the library, first import `prepare` from `metrics_utility`, and call `prepare()`, after which other `metrics_utility` imports will work.
The CLI would originally run only inside controller's virtualenv, reusing its dependencies, django config/settings, etc. For development/testing the `prepare` function ensures we either have that virtualenv, or changes import paths so that our code in `mock_awx/` is used instead. Any dependencies in `setup.cfg` are for when running inside controller, any dependencies in `pyproject.toml` are for development/testing - and also for the library.

If needed, there is more info in `README.md`, `metrics_utility/library/README.md`, `docs/old-readme.md`, and `docs/developer_setup.md`.
