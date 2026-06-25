# Dashboard Collection Performance Tests

End-to-end performance benchmarks for the automation dashboard data collection pipeline.

| Script | What it measures |
|--------|-----------------|
| `fill_data.py` | Populates the AWX database with synthetic test data |
| `benchmark_dashboard_collection.py` | Internal benchmark — calls `_collect_data()` directly in Python |
| `benchmark_dashboard_api.py` | API benchmark — triggers tasks via `POST /api/v1/tasks/schedule_immediate/` |

---

## Prerequisites

These scripts live in `metrics-utility` but import from `metrics-service`. They expect
the metrics-service checkout at `../metrics-service` (sibling repos).

**postgres must be running** (from metrics-utility or metrics-service):

```bash
# Option A — shared compose (from metrics-utility)
make compose

# Option B — from metrics-service
cd ../metrics-service && make compose
```

**For the API benchmark, also start the web server and dispatcherd:**

```bash
# Terminal 1 — web server (from metrics-service)
cd ../metrics-service
.venv/bin/python manage.py runserver

# Terminal 2 — dispatcherd (from metrics-service)
cd ../metrics-service
.venv/bin/python manage.py run_dispatcherd --workers 4
```

**First-time setup (migrations + superuser, from metrics-service):**

```bash
cd ../metrics-service
.venv/bin/python manage.py migrate
.venv/bin/python manage.py metrics_service init-service-id
.venv/bin/python manage.py metrics_service init-system-tasks

DJANGO_SUPERUSER_PASSWORD=<password> \
  .venv/bin/python manage.py createsuperuser \
  --username <username> --email <email> --noinput
```

---

## Shared environment variables

Set these once for the whole session before running any benchmark:

```bash
# metrics-service DB
export METRICS_SERVICE_DATABASES__default__ENGINE=django.db.backends.postgresql
export METRICS_SERVICE_DATABASES__default__HOST=localhost
export METRICS_SERVICE_DATABASES__default__PORT=<port>
export METRICS_SERVICE_DATABASES__default__USER=<metrics-service-db-user>
export METRICS_SERVICE_DATABASES__default__PASSWORD=<metrics-service-db-password>
export METRICS_SERVICE_DATABASES__default__NAME=<metrics-service-db-name>

# AWX DB (used by both metrics-service and fill_data.py)
export METRICS_SERVICE_DATABASES__awx__HOST=localhost
export METRICS_SERVICE_DATABASES__awx__PORT=<port>
export METRICS_SERVICE_DATABASES__awx__USER=<awx-db-user>
export METRICS_SERVICE_DATABASES__awx__PASSWORD=<awx-db-password>
export METRICS_SERVICE_DATABASES__awx__NAME=<awx-db-name>

# fill_data.py AWX connection (same DB, different env var prefix)
export METRICS_UTILITY_DB_HOST=localhost
export METRICS_UTILITY_DB_PORT=<port>
export METRICS_UTILITY_DB_NAME=<awx-db-name>
export METRICS_UTILITY_DB_USER=<awx-db-user>
export METRICS_UTILITY_DB_PASSWORD=<awx-db-password>

# General
export METRICS_SERVICE_LOG_LEVEL=WARNING
export METRICS_SERVICE_FEATURE__DASHBOARD_COLLECTION=true
```

If `metrics-service` is not checked out as a sibling of `metrics-utility` (at `../metrics-service`
relative to the repo root), also set `METRICS_SERVICE_PATH` or adjust the
`project_root` in each script.

---

## 1 — Filling test data (`fill_data.py`)

Runs `fill_perf_db_data.py` once for every day in the given period and cleans existing AWX data first.

### Scale presets

| Scale | Jobs/day | Hosts | `main_unifiedjob` | `main_jobhostsummary` | `main_unifiedjob_credentials` |
|------:|---------:|------:|------------------:|---------------------:|-----------------------------:|
| 1     |      100 |     5 |             9,100 |               45,500 |                       36,400 |
| 2     |      500 |     5 |            45,500 |              227,500 |                      182,000 |
| 3     |    1,100 |     5 |           100,100 |              500,500 |                      400,400 |
| 4     |      500 |    50 |            45,500 |            2,275,000 |                      182,000 |

> Row counts measured over the default 90-day period (`2024-01-01 → 2024-03-31`).

### Usage

```bash
# Scale preset (recommended)
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 2

# Custom period with a scale preset
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 2 \
  --period-start 2024-01-01 \
  --period-end   2024-06-30

# Fully custom (no scale preset)
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --period-start 2024-01-01 \
  --period-end   2024-03-31 \
  --job-count    750 \
  --host-count   10 \
  --task-count   30
```

### Arguments

| Argument             | Default      | Description                                                     |
|----------------------|--------------|-----------------------------------------------------------------|
| `--scale`            | —            | Preset 1–4 (overrides `--job-count`, `--host-count`, `--task-count`) |
| `--period-start`     | `2024-01-01` | First day to fill                                               |
| `--period-end`       | `2024-03-31` | Last day to fill                                                |
| `--job-count`        | `500`        | Jobs per day (ignored when `--scale` is set)                    |
| `--host-count`       | `5`          | Hosts (ignored when `--scale` is set)                           |
| `--task-count`       | `50`         | Tasks per job (ignored when `--scale` is set)                   |

### Verify row counts

```bash
docker exec <postgres-container> psql -U <user> -d <db> \
  -c "SELECT COUNT(*) FROM main_unifiedjob;"

docker exec <postgres-container> psql -U <user> -d <db> \
  -c "SELECT COUNT(*) FROM main_jobhostsummary;"
```

---

## 2 — Internal benchmark (`benchmark_dashboard_collection.py`)

Calls `_collect_data()` directly in Python — no HTTP or dispatcherd layer.

Two phases:
- **Phase 1 — Initial backfill:** full collection from `TEST_SINCE` to `TEST_UNTIL`
- **Phase 2 — Incremental sync:** deletes the last day of data and re-syncs it in 4 × 6h windows

### Run at each scale

```bash
# Scale 1
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 1

../metrics-service/.venv/bin/python \
  tools/dashboard_perf/benchmark_dashboard_collection.py \
  | tee /tmp/results_scale1_internal.txt

# Scale 2
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 2

../metrics-service/.venv/bin/python \
  tools/dashboard_perf/benchmark_dashboard_collection.py \
  | tee /tmp/results_scale2_internal.txt

# Scale 3
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 3

../metrics-service/.venv/bin/python \
  tools/dashboard_perf/benchmark_dashboard_collection.py \
  | tee /tmp/results_scale3_internal.txt

# Scale 4
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 4

../metrics-service/.venv/bin/python \
  tools/dashboard_perf/benchmark_dashboard_collection.py \
  | tee /tmp/results_scale4_internal.txt
```

> Output files are written to `/tmp/` to avoid committing large artifacts.

### Parameters

| Variable            | Default      | Description                                      |
|---------------------|--------------|--------------------------------------------------|
| `TEST_SINCE`        | `2024-01-01` | Start of the initial backfill                    |
| `TEST_UNTIL`        | `2024-03-31` | End of the initial backfill                      |
| `INCREMENTAL_START` | `TEST_UNTIL` | Records ≥ this date are deleted before Phase 2   |
| `INCREMENT_HOURS`   | `6`          | Window size for each incremental run             |
| `INCREMENT_COUNT`   | `4`          | Number of incremental runs                       |
| `DB_NAME`           | `awx`        | AWX database alias                               |

### Expected output

```
================================================================================
  Automation Dashboard Collection Performance Test
  Phase 1 (backfill):    2024-01-01 → 2024-03-31
  Delete from:           2024-03-31 onwards
  Phase 2 (incremental): 4 run(s) × 6h  (from 2024-03-31)
  DB alias:       awx
================================================================================

Phase 1: Initial data collection (full backfill)
  Range: 2024-01-01T00:00:00+00:00 → 2024-03-31T00:00:00+00:00
  Duration:            XX.XXs
  Status:              success
  Jobs collected:      XXXX
  JobData rows in DB:  XX,XXX
  Memory after:        XXX.X MB

Deleting JobData with awx_modified >= 2024-03-31 ...
  Deleted: XXX records   Remaining: XX,XXX

Phase 2: Incremental collection — 4 runs × 6h window
  ...

================================================================================
  Final Results
================================================================================
  Initial backfill:      XX.XXs
  Incremental runs:      X.Xs (X.X min)
  Total:                 XX.Xs (X.X min)

  Baseline memory:   XXX.X MB
  Peak memory:       XXX.X MB (RSS, sampled every 50 ms)
  Delta:             XX.X MB
```

---

## 3 — API benchmark (`benchmark_dashboard_api.py`)

Triggers tasks via `POST /api/v1/tasks/schedule_immediate/` and polls until completion.
Duration is measured from `started_at` to `completed_at` — directly comparable to the
internal benchmark. Wall time (printed separately) includes the dispatcherd queue wait.

Three phases, each on a clean `JobData` table:

| Phase | Window |
|------:|--------|
| 1 — one month | `TEST_SINCE` → `TEST_SINCE + 30 days` |
| 2 — one week  | `TEST_UNTIL - 7 days` → `TEST_UNTIL`  |
| 3 — one day   | `TEST_UNTIL - 1 day`  → `TEST_UNTIL`  |

### Run at each scale

```bash
# Scale 1
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 1

BASE_URL=http://localhost:8000/api \
BENCHMARK_USER=<username> \
PASSWORD=<password> \
  ../metrics-service/.venv/bin/python \
  tools/dashboard_perf/benchmark_dashboard_api.py \
  | tee /tmp/results_scale1_api.txt

# Scale 2
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 2

BASE_URL=http://localhost:8000/api \
BENCHMARK_USER=<username> \
PASSWORD=<password> \
  ../metrics-service/.venv/bin/python \
  tools/dashboard_perf/benchmark_dashboard_api.py \
  | tee /tmp/results_scale2_api.txt

# Scale 3
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 3

BASE_URL=http://localhost:8000/api \
BENCHMARK_USER=<username> \
PASSWORD=<password> \
  ../metrics-service/.venv/bin/python \
  tools/dashboard_perf/benchmark_dashboard_api.py \
  | tee /tmp/results_scale3_api.txt

# Scale 4
../metrics-service/.venv/bin/python \
  tools/dashboard_perf/fill_data.py \
  --scale 4

BASE_URL=http://localhost:8000/api \
BENCHMARK_USER=<username> \
PASSWORD=<password> \
  ../metrics-service/.venv/bin/python \
  tools/dashboard_perf/benchmark_dashboard_api.py \
  | tee /tmp/results_scale4_api.txt
```

> Output files are written to `/tmp/` to avoid committing large artifacts.

### Parameters

| Variable      | Default      | Description                                                 |
|---------------|--------------|-------------------------------------------------------------|
| `BASE_URL`    | `http://localhost:8000/api` | metrics-service API base URL          |
| `BENCHMARK_USER` | `superadmin` | Admin username                                        |
| `PASSWORD`    | —            | Password for the above user                                 |
| `METRICS_URL` | —            | Prometheus `/metrics` URL (optional, for server-side stats) |
| `TEST_SINCE`  | `2024-01-01` | Start of the test period                                    |
| `TEST_UNTIL`  | `2024-03-31` | End of the test period                                      |
| `DB_NAME`     | `awx`        | AWX database alias                                          |

### Expected output

```
================================================================================
  Automation Dashboard Collection API Benchmark
  Test period:  2024-01-01 → 2024-03-31
  Phase 1 (month):  2024-01-01 → 2024-01-31
  Phase 2 (week):   2024-03-24 → 2024-03-31
  Phase 3 (day):    2024-03-30 → 2024-03-31
  Target:    http://localhost:8000/api
  User:      <username>
================================================================================

Phase 1: One month collection
  Range: 2024-01-01 → 2024-01-31
  Duration (task):     X.XXs
  Duration (wall):     XX.XXs  (includes queue wait)
  JobData rows in DB:  X,XXX

Phase 2: One week collection ...
Phase 3: One day collection ...

================================================================================
  Final Results
================================================================================

  Phase                          Window                    Task time
  ------------------------------ ------------------------ ----------
  Phase 1 — one month            2024-01-01 → 2024-01-31     X.XXs
  Phase 2 — one week             2024-03-24 → 2024-03-31     X.XXs
  Phase 3 — one day              2024-03-30 → 2024-03-31     X.XXs

  Total wall time:  XX.Xs (X.X min)  (includes queue waits)
```

---

## Troubleshooting

- **postgres not running** — `make compose` (from either repo)
- **`JobData` table missing** — run `python manage.py migrate`
- **Feature flag error** — verify `METRICS_SERVICE_FEATURE__DASHBOARD_COLLECTION=true`
- **`fill_data.py` — scripts directory not found** — ensure `metrics-service` is checked out at `../metrics-service`
- **401 Unauthorized (API benchmark)** — wrong `BENCHMARK_USER` / `PASSWORD`; user must be a superuser
- **Task stays pending (API benchmark)** — dispatcherd is not running
- **Wall time >> task time (API benchmark)** — normal; includes the dispatcherd queue wait (~20s per task locally)
- **Verbose output** — set `METRICS_SERVICE_LOG_LEVEL=INFO`

