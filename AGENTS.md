# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `metrics-utility`, a standalone CLI tool for collecting and reporting metrics from Ansible Automation Platform (AAP) Controller instances. The project supports data collection, report generation (CCSP, CCSPv2, RENEWAL_GUIDANCE), and multiple storage adapters (local directory, S3).

## Architecture

### Core Components

- **Base collector framework** (`metrics_utility/base/`): Abstract classes for data collection with tarball packaging and shipping
- **Automation Controller Billing** (`metrics_utility/automation_controller_billing/`): AAP-specific collectors, extractors, and report generators
- **Management commands** (`metrics_utility/management/commands/`): Django-style CLI commands for gather and build operations
- **Mock AWX** (`mock_awx/`): Standalone mode configuration for development/testing without running AWX

### Data Flow

1. **Data Collection**: Collectors extract metrics from Controller DB or existing tarballs
2. **Packaging**: Data is packaged into daily tarballs with CSV/JSON inside
3. **Report Generation**: Builds XLSX reports from collected data
4. **Storage**: Supports local directory and S3 storage adapters

### Key Modules

- **Extractors** (`extract/`): Pull data from Controller DB, S3, or directories
- **Dataframe Engines** (`dataframe_engine/`): Process and transform collected data
- **Deduplication** (`dedup/`): Handle data deduplication for CCSP reports
- **Report Generators** (`report/`): Create XLSX output files
- **Packaging** (`package/`): Handle tarball creation and storage

## Development Commands

### Environment Setup
```bash
# Install dependencies
uv sync

# Start Docker services (postgres + minio for testing)
make compose
# or: docker compose -f tools/docker/docker-compose.yaml up
```

### Testing
```bash
# Run all tests
make test
# or: uv run pytest -s -v

# Run specific test
uv run pytest -s -v metrics_utility/test/path/to/test.py

# Run tests with coverage
make coverage
# or: uv run pytest -s -v --cov=. --cov-report=html

# Run gather tests (requires Docker services)
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c 'uv run pytest -s -v metrics_utility/test/gather/'
```

### Code Quality
```bash
# Lint and format check
make lint
# or: uv run ruff check && uv run ruff format --check

# Auto-fix linting and formatting
make fix
# or: uv run ruff check --fix && uv run ruff format
```

### Main CLI Commands
```bash
# Data collection
uv run python manage.py gather_automation_controller_billing_data [options]

# Report building
uv run python manage.py build_report [options]
```

## Running Modes

### Standalone Mode (Development)
- Uses `mock_awx/settings/` for configuration
- Requires Docker postgres + minio services
- Run commands with `uv run python manage.py`

### Controller Mode (Production)
- Runs inside Controller containers
- Uses actual AWX database and settings
- Activate venv: `source /var/lib/awx/venv/awx/bin/activate`
- Run commands with `python manage.py`

## Environment Variables

Key environment variables for configuration:
- `METRICS_UTILITY_REPORT_TYPE`: CCSP, CCSPv2, or RENEWAL_GUIDANCE
- `METRICS_UTILITY_SHIP_TARGET`: directory, s3, or controller_db
- `METRICS_UTILITY_SHIP_PATH`: Output path for reports/data
- Database and S3 credentials as needed

## Testing Strategy

- **Unit tests**: Individual component testing
- **Functional tests**: End-to-end collector workflows
- **Integration tests**: Full gather+build cycles
- **Snapshot tests**: Report output validation with golden files
- **Docker-based testing**: Full environment testing with postgres/minio

The project uses extensive test data in `metrics_utility/test/test_data/` and snapshot testing for report validation.