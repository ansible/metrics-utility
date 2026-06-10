# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`metrics-utility` collects, aggregates, and reports metrics from Ansible Automation Platform (AAP) Controller instances. It exposes both a CLI tool (`metrics-utility`) and a Python library interface.

---

## Development Commands

This project uses `uv` as the package manager.

```bash
uv sync                   # Install dependencies
make compose              # Start postgres + minio via Docker (required for integration tests)
make test                 # Run full pytest suite
make coverage             # Run tests and generate HTML coverage report
make lint                 # Run ruff checks and format validation
make fix                  # Auto-fix ruff issues and reformat code
make clean                # Reset docker environment
```

Run a single test file or test:
```bash
uv run pytest metrics_utility/test/path/to/test_file.py -s -v
uv run pytest -k "test_name" -s -v
```

Pre-commit hooks:
```bash
uvx pre-commit install    # Install hooks (ruff check/format + custom logger check)
```

---

## Architecture

### Two Interfaces

**CLI** — entry point `metrics_utility:manage()` dispatches two management commands:
- `gather_automation_controller_billing_data` — queries the Controller DB, settings, and Prometheus; packages daily tarballs (CSV/JSON) to a configured storage backend.
- `build_report` — reads tarballs and generates XLSX reports (CCSP, CCSPv2, RENEWAL_GUIDANCE).

**Library** — `metrics_utility.library` provides a standalone Python API that works against any postgres instance without AWX/Controller dependencies or environment variables.

### Dual Mode

The CLI supports two runtime modes, auto-detected at startup:
- **Controller mode**: AWX Django modules are importable (via `AWX_PATH`); additional config details come from Django settings.
- **Standalone mode**: No AWX dependency; connection via `METRICS_UTILITY_DB_*` env vars.

### Key Packages

| Package | Role |
|---|---|
| `metrics_utility/library/collectors/` | Data collectors for Controller DB, Prometheus, vCPU metrics — decorated functions returning dicts or DataFrames |
| `metrics_utility/library/storage/` | Unified `put()`/`get()` interface over filesystem, S3/Minio, console.redhat.com, and Segment Analytics |
| `metrics_utility/library/dataframes/` | Pandas DataFrame subclasses with built-in schema, field/index awareness, CSV import, and aggregation |
| `metrics_utility/library/package.py` | Packages collector output into dated tarballs |
| `metrics_utility/library/extractors.py` | Extracts tarballs back into DataFrames |
| `metrics_utility/library/reports.py` | Builds XLSX reports from extracted DataFrames |
| `metrics_utility/automation_controller_billing/` | Higher-level CLI orchestration: collectors, report builders, dedup, extraction, dataframe engine |
| `metrics_utility/anonymized_rollups/` | Anonymization and rollup aggregation |
| `metrics_utility/management/commands/` | Django-style management command handlers (gather, build_report) |

### Tarball Format

Each daily tarball contains:
- `config.json` — snapshot config data
- `manifest.json` — version metadata
- `data_collection_status.csv` — per-collector success/timing
- `*.csv` / `*.json` — collected metric data

### Time Interval Convention

- `since` = first moment of interval (inclusive)
- `until` = first moment outside interval (exclusive)
- All datetimes must be UTC-aware.

Helper: `metrics_utility/library/instants.py`

---

## Code Style

- **Linter**: Ruff — 150-character line length, single quotes, sorted imports.
- **Logger**: Always import from `metrics_utility.logger` — enforced by pre-commit hook. Do not use the stdlib `logging` module directly.
- **Python**: 3.12+ required.

---

## Testing Infrastructure

Integration tests require running postgres and Minio (S3-compatible). Start them with `make compose` before running the full suite.

- Fixtures: `metrics_utility/test/conftest.py`
- Mock AWX modules for standalone testing: `mock_awx/`
- DB schema: `tools/docker/*.sql`
