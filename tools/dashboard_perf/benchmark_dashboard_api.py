#!/usr/bin/env python
"""
API performance benchmark for the automation dashboard collection pipeline.

Mirrors benchmark_dashboard_collection.py but triggers each task via the
HTTP API (POST /api/v1/tasks/schedule_immediate/) instead of calling Python
directly.

Three phases, each clears JobData and collects a different window size:
  Phase 1 — One month:  collect TEST_SINCE → TEST_SINCE + 1 month
  Phase 2 — One week:   collect TEST_UNTIL - 7 days → TEST_UNTIL
  Phase 3 — One day:    collect TEST_UNTIL - 1 day  → TEST_UNTIL

Duration is measured from started_at to completed_at on the TaskExecution
record — actual execution time, directly comparable to the internal benchmark.

Memory reporting note
---------------------
The Prometheus /metrics endpoint reflects the web process only, not the
dispatcherd workers that run the collectors.  The RSS figure is a health
check, not directly comparable to the internal benchmark's peak memory.

Environment variables
---------------------
BASE_URL            Base URL of the metrics-service API  (default: http://localhost:18002/api)
BENCHMARK_USER      Admin username                        (default: superadmin)
PASSWORD            Password for the above user
METRICS_URL         Prometheus /metrics endpoint URL      (optional)
TEST_SINCE          ISO-8601 start of the test period   (default: 2024-01-01)
TEST_UNTIL          ISO-8601 end of the test period     (default: 2024-03-31)
DB_NAME             AWX database alias (default: awx)

Usage:
    BASE_URL=http://localhost:18002/api \\
    BENCHMARK_USER=superadmin \\
    PASSWORD=<password> \\
    METRICS_URL=http://localhost:18002/metrics \\
        .venv/bin/python \\
        metrics_service/tools/dashboard_collection_performance_tests/benchmark_dashboard_api.py
"""

# ruff: noqa: E402
import os
import sys
import time

from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests

from requests.auth import HTTPBasicAuth


# ---------------------------------------------------------------------------
# Django bootstrap — needed only for the JobData delete step
# ---------------------------------------------------------------------------
project_root = (Path(__file__).parent.parent.parent.parent / 'metrics-service').resolve()
sys.path.insert(0, str(project_root))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metrics_service.settings')

import django


django.setup()

from apps.dashboard_reports.models import JobData, TemplateMetadata


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get('BASE_URL', 'http://localhost:18002/api').rstrip('/')
USERNAME = os.environ.get('BENCHMARK_USER', 'superadmin')
PASSWORD = os.environ.get('PASSWORD', '')
METRICS_URL = os.environ.get('METRICS_URL', '')
DB_NAME = os.environ.get('DB_NAME', 'awx')

until_str = os.environ.get('TEST_UNTIL', '2024-03-31')
until = datetime.fromisoformat(until_str).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

since_str = os.environ.get('TEST_SINCE', '2024-01-01')
since = datetime.fromisoformat(since_str).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

# Derived phase windows
MONTH_SINCE = since
MONTH_UNTIL = since + timedelta(days=30)
WEEK_SINCE = until - timedelta(days=7)
WEEK_UNTIL = until
DAY_SINCE = until - timedelta(days=1)
DAY_UNTIL = until

POLL_INTERVAL = 1.0  # seconds between status checks
POLL_TIMEOUT = 3600  # seconds before giving up on a task

AUTH = HTTPBasicAuth(USERNAME, PASSWORD)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def trigger_task(function_name: str, task_data: dict | None = None) -> str:
    """POST to schedule_immediate and return the task ID."""
    payload = {
        'function_name': function_name,
        'task_data': task_data or {},
        'name': f'benchmark: {function_name}',
    }
    resp = requests.post(
        f'{BASE_URL}/v1/tasks/schedule_immediate/',
        json=payload,
        auth=AUTH,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()['task_id']


def wait_for_task(task_id: str) -> float:
    """Poll until task reaches a terminal state. Returns execution seconds (started_at → completed_at)."""
    deadline = time.perf_counter() + POLL_TIMEOUT
    while time.perf_counter() < deadline:
        resp = requests.get(f'{BASE_URL}/v1/tasks/{task_id}/', auth=AUTH, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        status = data.get('status')
        if status == 'completed':
            started_at = datetime.fromisoformat(data['started_at'])
            completed_at = datetime.fromisoformat(data['completed_at'])
            return (completed_at - started_at).total_seconds()
        if status in ('failed', 'cancelled'):
            raise RuntimeError(f'Task {task_id} {status}: {data.get("error", "")}')
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f'Task {task_id} did not complete within {POLL_TIMEOUT}s')


def run_phase(label: str, phase_since: datetime, phase_until: datetime) -> float:
    """Clear JobData, trigger a collection for the given window, return task duration."""
    print(f'\n{label}')
    print(f'  Range: {phase_since.date()} → {phase_until.date()}')

    JobData.objects.all().delete()
    TemplateMetadata.objects.all().delete()

    wall_start = time.perf_counter()
    task_id = trigger_task(
        'collect_dashboard_reports_data',
        {
            'since': phase_since.isoformat(),
            'until': phase_until.isoformat(),
            'database': DB_NAME,
        },
    )
    task_elapsed = wait_for_task(task_id)
    wall_elapsed = time.perf_counter() - wall_start

    job_data_count = JobData.objects.count()
    print(f'  Duration (task):     {task_elapsed:.2f}s')
    print(f'  Duration (wall):     {wall_elapsed:.2f}s  (includes queue wait)')
    print(f'  JobData rows in DB:  {job_data_count:,}')
    return task_elapsed


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------


def read_prometheus_metrics(url: str) -> dict:
    """Fetch /metrics and parse into {metric_name: float}."""
    if not url:
        return {}
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
    except Exception:
        return {}
    result = {}
    for line in resp.text.splitlines():
        if line.startswith('#') or not line.strip():
            continue
        parts = line.split()
        if len(parts) >= 2:
            result[parts[0]] = float(parts[-1])
    return result


def prometheus_delta(before: dict, after: dict, key: str) -> float:
    """Return how much a Prometheus counter increased between two snapshots."""
    return after.get(key, 0) - before.get(key, 0)


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


def main() -> None:
    print(f'\n{"=" * 80}')
    print('  Automation Dashboard Collection API Benchmark')
    print(f'  Test period:  {since.date()} → {until.date()}')
    print(f'  Phase 1 (month):  {MONTH_SINCE.date()} → {MONTH_UNTIL.date()}')
    print(f'  Phase 2 (week):   {WEEK_SINCE.date()} → {WEEK_UNTIL.date()}')
    print(f'  Phase 3 (day):    {DAY_SINCE.date()} → {DAY_UNTIL.date()}')
    print(f'  Target:    {BASE_URL}')
    print(f'  User:      {USERNAME}')
    print(f'  Prometheus: {METRICS_URL or "(not configured)"}')
    print(f'{"=" * 80}\n')

    # Verify connectivity
    print('Verifying connectivity...')
    resp = requests.get(f'{BASE_URL}/v1/', auth=AUTH, timeout=10)
    if not resp.ok:
        print(f'  ERROR: {BASE_URL}/v1/ returned {resp.status_code}')
        raise SystemExit(1)
    print('  OK')

    metrics_before = read_prometheus_metrics(METRICS_URL)
    overall_start = time.perf_counter()

    month_elapsed = run_phase('Phase 1: One month collection', MONTH_SINCE, MONTH_UNTIL)
    week_elapsed = run_phase('Phase 2: One week collection', WEEK_SINCE, WEEK_UNTIL)
    day_elapsed = run_phase('Phase 3: One day collection', DAY_SINCE, DAY_UNTIL)

    total_wall = time.perf_counter() - overall_start
    metrics_after = read_prometheus_metrics(METRICS_URL)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f'\n{"=" * 80}')
    print('  Final Results')
    print(f'{"=" * 80}\n')
    print(f'  {"Phase":<30} {"Window":<24} {"Task time":>10}')
    print(f'  {"-" * 30} {"-" * 24} {"-" * 10}')
    print(f'  {"Phase 1 — one month":<30} {str(MONTH_SINCE.date()) + " → " + str(MONTH_UNTIL.date()):<24} {month_elapsed:>9.2f}s')
    print(f'  {"Phase 2 — one week":<30} {str(WEEK_SINCE.date()) + " → " + str(WEEK_UNTIL.date()):<24} {week_elapsed:>9.2f}s')
    print(f'  {"Phase 3 — one day":<30} {str(DAY_SINCE.date()) + " → " + str(DAY_UNTIL.date()):<24} {day_elapsed:>9.2f}s')
    print()
    print(f'  Total wall time:  {total_wall:.1f}s ({total_wall / 60:.1f} min)  (includes queue waits)')
    print()

    if metrics_before and metrics_after:
        print('  Server-side Metrics (Prometheus — web process only)')
        print('  Note: RSS is the web process, not dispatcherd workers.')
        cpu = prometheus_delta(metrics_before, metrics_after, 'process_cpu_seconds_total')
        rss_mb = metrics_after.get('process_resident_memory_bytes', 0) / 1024 / 1024
        print(f'    CPU time used (web process): {cpu:.3f}s')
        print(f'    RSS memory (web process):    {rss_mb:.1f} MB')
        print()


if __name__ == '__main__':
    main()
