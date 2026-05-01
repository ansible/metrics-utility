# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync                  # Install dependencies
make compose             # Start postgres + minio via docker-compose (required for tests)
make test                # Run full test suite
make coverage            # Run tests with HTML coverage report
make lint                # Check with ruff (E, W, I rules)
make fix                 # Auto-fix and format with ruff
make clean               # Tear down docker compose
make psql                # psql into the running postgres container
```

Single test or specific test function:
```bash
uv run pytest metrics_utility/test/test_helpers.py -xvs
uv run pytest metrics_utility/test/test_helpers.py::test_sanitize_json_with_nan -xvs
uv run pytest -k "test_ccsp" -xvs
```

Run the CLI directly:
```bash
uv run ./manage.py gather_automation_controller_billing_data --help
uv run ./manage.py build_report --help
```

## Architecture

metrics-utility is a Python CLI that collects metrics from AAP Controller instances and produces billing/usage reports. It runs either standalone (against a standalone postgres instance) or inside Controller's Python virtualenv.

### Two top-level commands

**`gather_automation_controller_billing_data`** — Reads from Controller's DB (and optionally Prometheus), groups records into daily `.tar.gz` tarballs containing `.csv`/`.json` files, and saves them to a storage backend (local directory, S3, or CRC).

**`build_report`** — Loads tarballs from a storage backend, runs pandas-based transformations and deduplication, and writes an `.xlsx` report. Three report types: `CCSP`, `CCSPv2`, `RENEWAL_GUIDANCE`.

### Entry point and Django bootstrap

```
manage.py / metrics_utility:manage()
  └─► metrics_utility/__init__.py::prepare()
        - Attempts to import AWX modules; falls back to mock_awx if not in Controller venv
        - Sets DJANGO_SETTINGS_MODULE
  └─► metrics_utility/management_utility.py::ManagementUtility.execute()
        - Custom Django management utility
        - Catches MetricsException for clean user-facing errors
```

### Key module areas

| Path | Purpose |
|------|---------|
| `metrics_utility/management/commands/` | CLI command implementations (gather + build_report) |
| `metrics_utility/automation_controller_billing/` | All gather/build business logic: collectors, dataframe transformations, dedup, packaging, report saving |
| `metrics_utility/library/` | Reusable lower-level abstractions: storage backends, dataframe helpers, extractor, report generation |
| `metrics_utility/base/` | Abstract base classes: `BaseCollector`, `BasePackage`, `Collection`, `CollectionCSV`, `CollectionJSON` |
| `metrics_utility/anonymized_rollups/` | Anonymization of sensitive metrics before shipping |

### Gather data flow

```
Collector.gather()
  ├── ConfigCollector, JobHostSummaryCollector, MainJobEventCollector, …
  ├── Package  →  daily .tar.gz
  └── Storage  →  directory | S3 | CRC
                  (optional: ship to console.redhat.com)
```

### Build report data flow

```
Extractor  (loads tarballs from directory | S3 | controller_db)
  ├── Dataframe transformations (pandas)
  ├── Deduplicator (factory pattern, strategy-based)
  └── Report  →  .xlsx (ReportCCSP / ReportCCSPv2 / ReportRenewalGuidance)
                  saved via Storage backend
```

### Storage backends

`StorageDirectory`, `StorageS3`, `StorageCRC`, `StorageSegment` all share a common interface defined in `metrics_utility/library/`. The active backend is chosen at runtime via `METRICS_UTILITY_SHIP_TARGET`.

## Configuration

All runtime configuration comes from environment variables. See `docs/environment.md` for the full list. Key variables:

| Variable | Purpose |
|----------|---------|
| `METRICS_UTILITY_SHIP_TARGET` | `directory`, `s3`, `crc` (gather) or `controller_db` (build) |
| `METRICS_UTILITY_SHIP_PATH` | Local path or S3 bucket path |
| `METRICS_UTILITY_REPORT_TYPE` | `CCSPv2`, `CCSP`, or `RENEWAL_GUIDANCE` |
| `AWX_PATH` | Path to Controller venv (default `/awx_devel`); when found, AWX modules are imported directly |

Validation logic for these variables lives in `metrics_utility/management/validation.py`.

## Test structure

Tests live in `metrics_utility/test/`. Integration tests (gather, extract) require `make compose` to be running. Key fixtures in `conftest.py`:

- `fixed_now` — deterministic timezone-aware datetime
- `setup_processed_dataframe` — mocked dataframe for report tests
- `cleanup` — file cleanup between tests

Snapshot/golden-file tests are under `test/snapshot_tests/` and `test/test_data/` holds CSV, JSON, and XLSX fixtures.

## Code style

- Line length: 150 characters
- Single quotes
- Ruff enforces E, W, and I (import sorting) rules
- Pre-commit hooks run ruff automatically (`uvx pre-commit install` to enable)
