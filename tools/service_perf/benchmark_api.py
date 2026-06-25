#!/usr/bin/env python
"""
API performance benchmark for the metrics-service collection pipeline.

Runs the same three pipeline phases as collection_rollup_benchmark_hourly.py,
but triggers each task via the HTTP API rather than calling Python directly:

  Phase 1 — Snapshot collectors: 4 collectors run once (collect_snapshot_metrics)
  Phase 2 — Hourly collectors:   24 hours × 4 collectors (collect_hourly_metrics)
  Phase 3 — Daily rollup:        1 task (daily_metrics_rollup)

Each task is triggered via POST /api/v1/tasks/schedule_immediate/ and polled
until completion. Duration is measured from started_at to completed_at on the
TaskExecution record, so it reflects actual execution time and is directly
comparable to the internal benchmark numbers.

Memory reporting difference
---------------------------
The internal benchmark uses psutil to sample peak RSS of the Python process
that is directly executing the collectors (polled every 50 ms). Here, the
collectors run inside dispatcherd worker processes — separate from the
metrics-service web process. The Prometheus /metrics endpoint only reflects
the web process, so the RSS figure reported below is NOT comparable to the
internal benchmark's peak memory. It is included as a general health check
(confirming the web process is not leaking memory), not as a like-for-like
measurement.

Usage:
    # Port-forward the metrics-service pod first:
    #   kubectl port-forward -n aap26-next <pod> 18002:8000 &
    # Then run:
    BASE_URL=http://localhost:18002/api \\
    BENCHMARK_USER=superadmin \\
    PASSWORD=superadmin123 \\
    METRICS_URL=http://localhost:18002/metrics \\
    TEST_DATE=2024-01-25 \\
        python metrics_service/tools/performance_tests/benchmark_api.py

Environment variables:
    BASE_URL       Base URL of the metrics-service API (default: http://localhost:18002/api)
    BENCHMARK_USER Admin username (default: superadmin)
    PASSWORD       Password for the above user
    METRICS_URL    URL of the pod's Prometheus /metrics endpoint (optional)
    TEST_DATE      Date to collect for, YYYY-MM-DD (default: 2024-01-25)
"""

# ruff: noqa: T201
import os
import time

from datetime import datetime

import requests

from requests.auth import HTTPBasicAuth


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get('BASE_URL', 'http://localhost:18002/api').rstrip('/')
USERNAME = os.environ.get('BENCHMARK_USER', 'superadmin')
PASSWORD = os.environ.get('PASSWORD', '')
METRICS_URL = os.environ.get('METRICS_URL', '')
TEST_DATE = os.environ.get('TEST_DATE', '2024-01-25')

SNAPSHOT_COLLECTORS = [
    'execution_environments',
    'config',
    'controller_version_service',
    'table_metadata',
]

HOURLY_COLLECTORS = [
    'job_host_summary_service',
    'unified_jobs',
    'credentials_service',
    'main_jobevent_service',
]

POLL_INTERVAL = 1.0  # seconds between status checks
POLL_TIMEOUT = 600  # seconds before giving up on a task

AUTH = HTTPBasicAuth(USERNAME, PASSWORD)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def trigger_task(function_name, task_data=None):
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


def wait_for_task(task_id):
    """Poll until task reaches a terminal state. Returns actual execution seconds (started_at → completed_at)."""
    deadline = time.perf_counter() + POLL_TIMEOUT
    while time.perf_counter() < deadline:
        resp = requests.get(f'{BASE_URL}/v1/tasks/{task_id}/', auth=AUTH, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        status = data.get('status')
        if status == 'completed':
            started_at = datetime.fromisoformat(data['started_at'].replace('Z', '+00:00'))
            completed_at = datetime.fromisoformat(data['completed_at'].replace('Z', '+00:00'))
            return (completed_at - started_at).total_seconds()
        if status in ('failed', 'cancelled'):
            raise RuntimeError(f'Task {task_id} {status}')
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f'Task {task_id} did not complete within {POLL_TIMEOUT}s')


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------


def read_prometheus_metrics(url):
    """Fetch /metrics and parse into a plain dict of {metric_name: float}."""
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


def prometheus_delta(before, after, key):
    """Return how much a Prometheus counter increased between two snapshots."""
    return after.get(key, 0) - before.get(key, 0)


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


def main():  # noqa: PLR0915
    print('=' * 70)
    print('Metrics Service API Pipeline Benchmark')
    print('=' * 70)
    print(f'  Target    : {BASE_URL}')
    print(f'  User      : {USERNAME}')
    print(f'  Test date : {TEST_DATE}')
    print(f'  Prometheus: {METRICS_URL or "(not configured — set METRICS_URL for server-side metrics)"}')

    # Verify connectivity
    print('\nVerifying connectivity...')
    resp = requests.get(f'{BASE_URL}/v1/', auth=AUTH, timeout=10)
    if not resp.ok:
        print(f'  ERROR: {BASE_URL}/v1/ returned {resp.status_code}')
        raise SystemExit(1)
    print('  OK')

    metrics_before = read_prometheus_metrics(METRICS_URL)
    overall_start = time.perf_counter()

    # Phase 1 — Snapshot collectors
    print('\n' + '=' * 70)
    print('Phase 1: Snapshot collectors (run once each)')
    print('=' * 70)
    print(f'  {"Collector":<35} {"Duration":>10}')
    print(f'  {"---------":<35} {"--------":>10}')

    snapshot_start = time.perf_counter()
    for collector in SNAPSHOT_COLLECTORS:
        task_id = trigger_task('collect_snapshot_metrics', {'collector_type': collector})
        elapsed = wait_for_task(task_id)
        print(f'  {collector:<35} {elapsed:>8.2f}s')

    snapshot_elapsed = time.perf_counter() - snapshot_start
    print(f'\n  Total: {snapshot_elapsed:.1f}s')

    # Phase 2 — Hourly collection
    print('\n' + '=' * 70)
    print('Phase 2: Hourly collectors (24 hours x 4 collectors)')
    print('=' * 70)
    print(f'  {"Hour":<6} {"Collector":<35} {"Duration":>10}')
    print(f'  {"----":<6} {"---------":<35} {"--------":>10}')

    hourly_start = time.perf_counter()
    for hour in range(24):
        hour_ts = f'{TEST_DATE}T{hour:02d}:00:00Z'
        for collector in HOURLY_COLLECTORS:
            task_id = trigger_task(
                'collect_hourly_metrics',
                {'collector_type': collector, 'hour_timestamp': hour_ts},
            )
            elapsed = wait_for_task(task_id)
            print(f'  {hour:<6} {collector:<35} {elapsed:>8.2f}s')

    hourly_elapsed = time.perf_counter() - hourly_start
    print(f'\n  Total: {hourly_elapsed:.1f}s ({hourly_elapsed / 60:.1f} min)')

    # Phase 3 — Daily rollup
    print('\n' + '=' * 70)
    print('Phase 3: Daily rollup')
    print('=' * 70)
    task_id = trigger_task('daily_metrics_rollup', {'summary_date': TEST_DATE})
    rollup_elapsed = wait_for_task(task_id)
    print(f'  Duration: {rollup_elapsed:.2f}s')

    total_elapsed = time.perf_counter() - overall_start
    metrics_after = read_prometheus_metrics(METRICS_URL)

    # Summary
    print('\n' + '=' * 70)
    print('Summary')
    print('=' * 70)
    print(f'  Snapshot collectors: {snapshot_elapsed:>8.2f}s')
    print(f'  Hourly collection  : {hourly_elapsed:>8.1f}s ({hourly_elapsed / 60:.1f} min)')
    print(f'  Daily rollup       : {rollup_elapsed:>8.2f}s')
    print(f'  Total              : {total_elapsed:>8.1f}s ({total_elapsed / 60:.1f} min)')

    if metrics_before and metrics_after:
        print('\n' + '=' * 70)
        print('Server-side Metrics (Prometheus — web process only)')
        print('=' * 70)
        print('  Note: RSS here is the web process, not the dispatcherd workers')
        print('  that ran the collectors. Not comparable to internal benchmark peak memory.')
        cpu = prometheus_delta(metrics_before, metrics_after, 'process_cpu_seconds_total')
        rss_mb = metrics_after.get('process_resident_memory_bytes', 0) / 1024 / 1024
        print(f'  CPU time used (web process) : {cpu:>8.3f}s')
        print(f'  RSS memory (web process)    : {rss_mb:>8.1f} MB')

    print()


if __name__ == '__main__':
    main()
