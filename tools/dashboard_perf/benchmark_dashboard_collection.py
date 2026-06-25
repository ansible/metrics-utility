#!/usr/bin/env python
"""
Performance test for metrics-service automation dashboard data collection.

Mirrors the structure of benchmark_internal_collection_rollup_benchmark_hourly.py
but targets the dashboard_reports collection pipeline:

  Phase 1 – Initial backfill
      collect_dashboard_reports_initial_data   (full date range, once)

  Phase 2 – Incremental collection
      collect_dashboard_reports_data           (once per configured increment window)

Environment variables
---------------------
TEST_SINCE          ISO-8601 start of the initial backfill (default: 2024-01-01)
TEST_UNTIL          ISO-8601 end of the initial backfill (default: 2024-03-31)
INCREMENTAL_START   ISO-8601 date — records >= this date are deleted after the backfill
                    so Phase 2 re-syncs them incrementally (default: TEST_UNTIL = last day)
INCREMENT_HOURS     Window size for each incremental run in hours (default: 6)
INCREMENT_COUNT     Number of incremental runs to simulate (default: 4)
DB_NAME             AWX database alias to use (default: awx)

Example
-------
TEST_SINCE=2024-01-01 TEST_UNTIL=2024-04-01 INCREMENT_HOURS=6 INCREMENT_COUNT=8 \\
    python metrics_service/tools/dashboard_collection_performance_tests/benchmark_dashboard_collection.py
"""

# ruff: noqa: T201, E402
import contextlib
import os
import sys
import threading
import time

from datetime import UTC, datetime, timedelta
from pathlib import Path

import psutil


# ---------------------------------------------------------------------------
# Django bootstrap — must happen before any app imports
# ---------------------------------------------------------------------------
project_root = (Path(__file__).parent.parent.parent.parent / 'metrics-service').resolve()
sys.path.insert(0, str(project_root))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metrics_service.settings')

import django


django.setup()

# ---------------------------------------------------------------------------
# Application imports (after Django setup)
# ---------------------------------------------------------------------------
from apps.dashboard_reports.models import JobData, TemplateMetadata
from apps.dashboard_reports.tasks import _collect_data


# ---------------------------------------------------------------------------
# Memory helpers
# ---------------------------------------------------------------------------


def get_memory_mb(process: psutil.Process) -> float:
    """Return current RSS in MB."""
    return process.memory_info().rss / 1024 / 1024


class PeakMemoryMonitor:
    """Measure peak RSS during task execution by polling in a background thread."""

    def __init__(self, process: psutil.Process, interval: float = 0.05) -> None:
        self._process = process
        self._interval = interval
        self._peak = 0.0
        self._stop = threading.Event()

    def __enter__(self) -> 'PeakMemoryMonitor':
        self._peak = get_memory_mb(self._process)
        self._stop.clear()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        self._stop.set()
        self._thread.join()

    def _poll(self) -> None:
        while not self._stop.is_set():
            self._peak = max(self._peak, get_memory_mb(self._process))
            time.sleep(self._interval)

    @property
    def peak_mb(self) -> float:
        """Return the highest observed RSS in MB."""
        return self._peak


# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------


def run_initial_phase(
    since: datetime,
    until: datetime,
    db_name: str,
    process: psutil.Process,
    peak_memory_mb: float,
) -> tuple[float, float, dict]:
    """Phase 1: Full historical backfill via _collect_data."""
    print('Phase 1: Initial data collection (full backfill)')
    print(f'  Range: {since.isoformat()} → {until.isoformat()}')

    start_ts = time.time()
    with PeakMemoryMonitor(process) as monitor:
        result = _collect_data(
            task_name='collect_dashboard_reports_initial_data',
            since=since.isoformat(),
            until=until.isoformat(),
            database=db_name,
        )
    duration = time.time() - start_ts
    peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)

    error = result.get('error', False)
    status = 'error' if error else 'success'
    job_count = result.get('data', {}).get('job_count', '?')
    job_data_count = JobData.objects.count()

    print(f'  Duration:            {duration:.2f}s')
    print(f'  Status:              {status}')
    print(f'  Jobs collected:      {job_count}')
    print(f'  JobData rows in DB:  {job_data_count:,}')
    print(f'  Memory after:        {get_memory_mb(process):.1f} MB')

    if error:
        print(f'  Error:               {result.get("message", "")}')
        print()
        sys.exit(1)
    print()

    return duration, peak_memory_mb, result


def run_incremental_phase(
    until: datetime,
    increment_hours: int,
    increment_count: int,
    db_name: str,
    process: psutil.Process,
    peak_memory_mb: float,
    baseline_memory_mb: float,
) -> tuple[float, float]:
    """Phase 2: Simulate recurring incremental collections."""
    print(f'Phase 2: Incremental collection — {increment_count} runs × {increment_hours}h window')
    print(f'  {"Run":<5} {"Since":>28} {"Until":>28} {"Jobs":>8} {"Duration":>11} {"Memory MB":>11}')

    total_start = time.time()
    run_durations = []
    failed_runs = []
    current_since = until  # first incremental run starts at the end of the initial backfill

    for i in range(increment_count):
        run_since = current_since
        run_until = current_since + timedelta(hours=increment_hours)

        run_start = time.time()
        try:
            with PeakMemoryMonitor(process) as monitor:
                result = _collect_data(
                    task_name='collect_dashboard_reports_data',
                    since=run_since.isoformat(),
                    until=run_until.isoformat(),
                    database=db_name,
                )
            run_duration = time.time() - run_start
            peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)

            error = result.get('error', False)
            job_count = result.get('data', {}).get('job_count', '?')
            current_memory = get_memory_mb(process)
            run_durations.append(run_duration)

            err_flag = ' *' if error else ''
            if error:
                failed_runs.append(i)
                print(
                    f'  {i:>4}   {run_since.isoformat():>28} {run_until.isoformat():>28}'
                    f' {str(job_count):>8} {run_duration:>10.2f}s {get_memory_mb(process):>10.1f}{err_flag}'
                )
                print(f'  Error: {result.get("message", "unknown error")}')
                sys.exit(1)

            print(
                f'  {i:>4}   {run_since.isoformat():>28} {run_until.isoformat():>28}'
                f' {str(job_count):>8} {run_duration:>10.2f}s {current_memory:>10.1f}{err_flag}'
            )
        except Exception as exc:
            run_duration = time.time() - run_start
            failed_runs.append(i)
            run_durations.append(run_duration)
            print(f'  {i:>4}   Error after {run_duration:.2f}s: {exc}')
            sys.exit(1)

        current_since = run_until  # next run starts where this one ended

    total_duration = time.time() - total_start
    job_data_count = JobData.objects.count()

    print()
    print('  Incremental Summary:')
    print(f'    Total duration:    {total_duration:.1f}s ({total_duration / 60:.1f} min)')
    print(f'    Runs completed:    {increment_count - len(failed_runs)} / {increment_count}')
    print(f'    JobData rows in DB:{job_data_count:,}')
    if run_durations:
        print(f'    Slowest run:       {max(run_durations):.2f}s (run #{run_durations.index(max(run_durations))})')
        print(f'    Fastest run:       {min(run_durations):.2f}s (run #{run_durations.index(min(run_durations))})')
    if failed_runs:
        print(f'    Failed runs:       {len(failed_runs)} (* above) — runs {failed_runs}')
    print()

    return total_duration, peak_memory_mb


# ---------------------------------------------------------------------------
# Source table helpers
# ---------------------------------------------------------------------------

_TABLE_COUNT_QUERIES: dict[str, str] = {
    'main_unifiedjob': 'SELECT COUNT(*) FROM main_unifiedjob',
    'main_job': 'SELECT COUNT(*) FROM main_job',
    'main_jobhostsummary': 'SELECT COUNT(*) FROM main_jobhostsummary',
    'main_host': 'SELECT COUNT(*) FROM main_host',
    'main_unifiedjobtemplate': 'SELECT COUNT(*) FROM main_unifiedjobtemplate',
    'main_organization': 'SELECT COUNT(*) FROM main_organization',
    'main_label': 'SELECT COUNT(*) FROM main_label',
    'main_unifiedjob_labels': 'SELECT COUNT(*) FROM main_unifiedjob_labels',
}


def _count(cursor, table: str) -> int:
    cursor.execute(_TABLE_COUNT_QUERIES[table])
    return cursor.fetchone()[0]


def print_source_table_counts(since: datetime, until: datetime, db_name: str) -> None:
    """Print row counts for all AWX source tables relevant to dashboard collection."""
    from django.db import connections

    print('  Source Table Counts (AWX DB):')
    print(f'    {"Table":<32} {"Total":>12}  In test range')
    print(f'    {"-" * 32} {"-" * 12}  {"─" * 13}')

    try:
        with connections[db_name].cursor() as cursor:
            total_jobs = _count(cursor, 'main_unifiedjob')
            cursor.execute(
                'SELECT COUNT(*) FROM main_unifiedjob WHERE finished >= %s AND finished < %s',
                [since, until],
            )
            jobs_in_range = cursor.fetchone()[0]

            print(f'    {"main_unifiedjob":<32} {total_jobs:>12,}  {jobs_in_range:,}')
            print(f'    {"main_job":<32} {_count(cursor, "main_job"):>12,}')
            print(f'    {"main_jobhostsummary":<32} {_count(cursor, "main_jobhostsummary"):>12,}')
            print(f'    {"main_host":<32} {_count(cursor, "main_host"):>12,}')
            print(f'    {"main_unifiedjobtemplate":<32} {_count(cursor, "main_unifiedjobtemplate"):>12,}')
            print(f'    {"main_organization":<32} {_count(cursor, "main_organization"):>12,}')
            print(f'    {"main_label":<32} {_count(cursor, "main_label"):>12,}')
            print(f'    {"main_unifiedjob_labels":<32} {_count(cursor, "main_unifiedjob_labels"):>12,}')
    except Exception as exc:
        print(f'    (Could not query AWX tables: {exc})')
    print()


def print_output_table_sizes() -> None:
    """Print row counts and on-disk sizes for all dashboard output tables."""
    from django.db import connection

    output_tables = [
        ('JobData', 'dashboard_job_data'),
        ('JobLabel', 'dashboard_job_data_label'),
        ('JobHostSummary', 'dashboard_job_data_host_summary'),
        ('TemplateMetadata', 'dashboard_template_metadata'),
    ]

    print('  Output Table Sizes:')
    print(f'    {"Model":<30} {"Rows":>10}  Total size')
    print(f'    {"-" * 30} {"-" * 10}  {"─" * 12}')

    with connection.cursor() as cursor:
        for label, table in output_tables:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM {table}')  # noqa: S608
                rows = cursor.fetchone()[0]
                cursor.execute(
                    'SELECT pg_size_pretty(pg_total_relation_size(%s))',
                    [table],
                )
                size = cursor.fetchone()[0]
                print(f'    {label:<30} {rows:>10,}  {size}')
            except Exception as exc:
                print(f'    {label:<30} {"?":>10}  (error: {exc})')
    print()


# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------


def print_final_summary(
    initial_duration: float,
    incremental_duration: float,
    baseline_memory_mb: float,
    peak_memory_mb: float,
    process: psutil.Process,
    since: datetime,
    until: datetime,
    db_name: str,
) -> None:
    """Print the complete benchmark results."""
    total_duration = initial_duration + incremental_duration

    print(f'{"=" * 80}')
    print('  Final Results')
    print(f'{"=" * 80}\n')
    print(f'  Initial backfill:      {initial_duration:.2f}s')
    print(f'  Incremental runs:      {incremental_duration:.1f}s ({incremental_duration / 60:.1f} min)')
    print(f'  Total:                 {total_duration:.1f}s ({total_duration / 60:.1f} min)')
    print()
    print(f'  Baseline memory:   {baseline_memory_mb:.1f} MB')
    print(f'  Peak memory:       {peak_memory_mb:.1f} MB (RSS, sampled every 50 ms)')
    print(f'  Delta:             {peak_memory_mb - baseline_memory_mb:.1f} MB')
    print()

    print_output_table_sizes()
    print_source_table_counts(since, until, db_name)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_dashboard_collection_benchmark() -> None:
    """Run the full automation dashboard collection performance benchmark."""
    # --- Config from environment -------------------------------------------
    db_name = os.environ.get('DB_NAME', 'awx')
    increment_hours = int(os.environ.get('INCREMENT_HOURS', '6'))
    increment_count = int(os.environ.get('INCREMENT_COUNT', '4'))

    until_str = os.environ.get('TEST_UNTIL', '2024-03-31')
    until = datetime.fromisoformat(until_str).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

    since_str = os.environ.get('TEST_SINCE', '2024-01-01')
    since = datetime.fromisoformat(since_str).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

    # Start of the incremental phase — defaults to TEST_UNTIL (last day of the backfill).
    # Override with INCREMENTAL_START to choose a different cutoff date.
    incremental_start_str = os.environ.get('INCREMENTAL_START', until_str)
    incremental_start = datetime.fromisoformat(incremental_start_str).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

    # --- Header ------------------------------------------------------------
    print(f'\n{"=" * 80}')
    print('  Automation Dashboard Collection Performance Test')
    print(f'  Phase 1 (backfill):    {since.date()} → {until.date()}')
    print(f'  Delete from:           {incremental_start.date()} onwards')
    print(f'  Phase 2 (incremental): {increment_count} run(s) × {increment_hours}h  (from {incremental_start.date()})')
    print(f'  DB alias:       {db_name}')
    print(f'{"=" * 80}\n')

    # --- Clean up existing data -------------------------------------------
    print('Cleaning existing dashboard data...')
    JobData.objects.all().delete()
    TemplateMetadata.objects.all().delete()
    print('Done\n')

    # --- Warm-up -----------------------------------------------------------
    print('Warm-up: throwaway collector call to initialise DB connections / caches...')
    warmup_since = until - timedelta(minutes=5)
    with contextlib.suppress(Exception):
        _collect_data(
            task_name='warmup',
            since=warmup_since.isoformat(),
            until=until.isoformat(),
            database=db_name,
        )
    JobData.objects.all().delete()
    TemplateMetadata.objects.all().delete()
    print('Done\n')

    # --- Baseline memory ---------------------------------------------------
    process = psutil.Process()
    baseline_memory_mb = get_memory_mb(process)
    peak_memory_mb = baseline_memory_mb

    # --- Phase 1: Initial backfill (1 month) ------------------------------
    initial_duration, peak_memory_mb, _ = run_initial_phase(
        since=since,
        until=until,
        db_name=db_name,
        process=process,
        peak_memory_mb=peak_memory_mb,
    )

    # --- Delete data from incremental_start onwards -----------------------
    print(f'Deleting JobData with awx_modified >= {incremental_start.date()} to prepare incremental phase...')
    deleted_count = JobData.objects.filter(awx_modified__gte=incremental_start).count()
    JobData.objects.filter(awx_modified__gte=incremental_start).delete()
    remaining = JobData.objects.count()
    print(f'  Deleted: {deleted_count:,} records   Remaining: {remaining:,}\n')

    # --- Phase 2: Incremental runs ----------------------------------------
    incremental_duration, peak_memory_mb = run_incremental_phase(
        until=incremental_start,
        increment_hours=increment_hours,
        increment_count=increment_count,
        db_name=db_name,
        process=process,
        peak_memory_mb=peak_memory_mb,
        baseline_memory_mb=baseline_memory_mb,
    )

    # --- Final summary ----------------------------------------------------
    print_final_summary(
        initial_duration=initial_duration,
        incremental_duration=incremental_duration,
        baseline_memory_mb=baseline_memory_mb,
        peak_memory_mb=peak_memory_mb,
        process=process,
        since=since,
        until=until,
        db_name=db_name,
    )


if __name__ == '__main__':
    run_dashboard_collection_benchmark()
