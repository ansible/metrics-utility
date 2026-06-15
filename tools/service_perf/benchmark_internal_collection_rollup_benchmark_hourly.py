#!/usr/bin/env python
"""
Performance test for metrics-service collection and rollup.

Runs all snapshot collectors once, then runs the hourly collectors once per hour for
24 hours, then triggers the daily rollup.

Snapshot collectors (run once):
    execution_environments, config, controller_version_service, table_metadata

Hourly collectors (run 24x):
    job_host_summary_service, unified_jobs, credentials_service, main_jobevent_service
    (note: main_jobevent_service is disabled in production by default)

"""

# ruff: noqa: T201, E402
import contextlib
import os
import sys
import threading
import time

from datetime import datetime, timedelta
from pathlib import Path

import psutil


# Setup Django
project_root = (Path(__file__).parent.parent.parent.parent / 'metrics-service').resolve()
sys.path.insert(0, str(project_root))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metrics_service.settings')

import django


django.setup()

from apps.tasks.collectors import (
    collect_hourly_metrics,
    collect_snapshot_metrics,
    daily_metrics_rollup,
)
from apps.tasks.models import DailyMetricsSummary, HourlyMetricsCollection


SNAPSHOT_COLLECTOR_TYPES = [
    'execution_environments',
    'config',
    'controller_version_service',
    'table_metadata',
]

HOURLY_COLLECTOR_TYPES = [
    'job_host_summary_service',
    'unified_jobs',
    'credentials_service',
    'main_jobevent_service',  # disabled in production by default; included for perf testing
]


def get_memory_mb(process):
    """Return current RSS in MB."""
    return process.memory_info().rss / 1024 / 1024


class PeakMemoryMonitor:
    """Measures peak RSS memory during task execution by polling in a background thread."""

    def __init__(self, process, interval=0.05):
        self._process = process
        self._interval = interval
        self._peak = 0.0
        self._stop = threading.Event()

    def __enter__(self):
        self._peak = get_memory_mb(self._process)
        self._stop.clear()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args):
        self._stop.set()
        self._thread.join()

    def _poll(self):
        while not self._stop.is_set():
            self._peak = max(self._peak, get_memory_mb(self._process))
            time.sleep(self._interval)

    @property
    def peak_mb(self):
        return self._peak


def run_snapshot_phase(process, peak_memory_mb):
    """Phase 1: Run all snapshot collectors once."""
    print('Phase 1: Snapshot collectors — run once')
    snapshot_start = time.time()
    for collector_type in SNAPSHOT_COLLECTOR_TYPES:
        try:
            with PeakMemoryMonitor(process) as monitor:
                collect_snapshot_metrics(collector_type=collector_type, database='awx')
            peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)
            print(f'  {collector_type}: {get_memory_mb(process):.1f} MB')
        except Exception as e:
            print(f'  Error ({collector_type}): {e}')
    snapshot_duration = time.time() - snapshot_start
    print(f'  Total duration: {snapshot_duration:.2f}s\n')
    return snapshot_duration, peak_memory_mb


def run_hourly_phase(test_date, process, peak_memory_mb, baseline_memory_mb):
    """Phase 2: Run hourly collectors for each hour in a 24-hour period."""
    from django.db.models import Count, Sum

    collector_totals = dict.fromkeys(HOURLY_COLLECTOR_TYPES, 0.0)
    collector_peak_memory = dict.fromkeys(HOURLY_COLLECTOR_TYPES, baseline_memory_mb)
    hour_timings, failed_hours = [], []

    col_w = 24
    header_cols = ''.join(f'{name:>{col_w}}' for name in HOURLY_COLLECTOR_TYPES)
    print('Phase 2: Hourly collectors — 24 hours')
    print(f'  {"Hour":<6}{header_cols} {"Total":>10} {"Memory MB":>11}')

    hourly_collection_start = time.time()

    for hour in range(24):
        hour_timestamp = (test_date + timedelta(hours=hour)).isoformat()
        hour_start = time.time()
        hour_collector_times = {}
        hour_had_error = False

        for collector_type in HOURLY_COLLECTOR_TYPES:
            collector_start = time.time()
            try:
                with PeakMemoryMonitor(process) as monitor:
                    collect_hourly_metrics(collector_type=collector_type, hour_timestamp=hour_timestamp, database='awx')
            except Exception as e:
                hour_had_error = True
                failed_hours.append(f'{collector_type}:hour_{hour}')
                print(f'  Error at hour {hour}, {collector_type}: {e}')
            collector_duration = time.time() - collector_start
            hour_collector_times[collector_type] = collector_duration
            collector_totals[collector_type] += collector_duration
            collector_peak_memory[collector_type] = max(collector_peak_memory[collector_type], monitor.peak_mb)
            peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)

        hour_total = time.time() - hour_start
        current_memory = get_memory_mb(process)
        hour_timings.append(hour_total)

        time_cols = ''.join(f'{hour_collector_times.get(name, 0):>{col_w - 1}.2f}s' for name in HOURLY_COLLECTOR_TYPES)
        err_flag = ' *' if hour_had_error else ''
        print(f'  {hour:>4}   {time_cols} {hour_total:>9.2f}s {current_memory:>10.1f}{err_flag}')

    hourly_collection_duration = time.time() - hourly_collection_start

    stats = HourlyMetricsCollection.objects.aggregate(total_size=Sum('data_size_bytes'), count=Count('id'))
    total_size = stats['total_size'] or 0
    collections_count = stats['count']

    print()
    print('  Hourly Collection Summary:')
    print(f'    Total duration: {hourly_collection_duration:.1f}s ({hourly_collection_duration / 60:.1f} min)')
    print(f'    Collections created: {collections_count}')
    print(f'    Total data size: {total_size / 1024 / 1024:.2f} MB')
    for name, total in collector_totals.items():
        print(f'    {name} total: {total:.1f}s')
    if hour_timings:
        print(f'    Slowest hour: {max(hour_timings):.2f}s (hour {hour_timings.index(max(hour_timings))})')
        print(f'    Fastest hour: {min(hour_timings):.2f}s (hour {hour_timings.index(min(hour_timings))})')
    if failed_hours:
        print(f'    Failed collections: {len(failed_hours)} (* in table above)')
        for failure in failed_hours:
            print(f'      - {failure}')
    print()

    return hourly_collection_duration, peak_memory_mb, collector_totals, collector_peak_memory


def run_rollup_phase(test_date, process, peak_memory_mb):
    """Phase 3: Run daily rollup."""
    print('Phase 3: Daily rollup')

    rollup_start = time.time()
    rollup_duration = 0.0

    try:
        with PeakMemoryMonitor(process) as monitor:
            result = daily_metrics_rollup(summary_date=test_date.date().isoformat())
        rollup_duration = time.time() - rollup_start
        peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)

        summaries = DailyMetricsSummary.objects.filter(summary_date=test_date.date())

        print(f'  Duration: {rollup_duration:.2f}s')
        print(f'  Status: {result.get("status")}')
        print(f'  Summaries created: {summaries.count()}')
        print(f'  Memory after rollup: {get_memory_mb(process):.1f} MB')
        print()

    except Exception as e:
        rollup_duration = time.time() - rollup_start
        print(f'  Rollup failed after {rollup_duration:.2f}s: {e}')
        peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)
        print()

    return rollup_duration, peak_memory_mb


def print_final_summary(
    snapshot_duration,
    hourly_collection_duration,
    rollup_duration,
    collector_totals,
    collector_peak_memory,
    baseline_memory_mb,
    peak_memory_mb,
    process,
    test_date,
):
    """Print the final benchmark results."""
    total_duration = snapshot_duration + hourly_collection_duration + rollup_duration

    print(f'{"=" * 80}')
    print('  Final Results')
    print(f'{"=" * 80}\n')
    print(f'  Snapshot collectors:  {snapshot_duration:.2f}s')
    print(f'  Hourly collection:    {hourly_collection_duration:.1f}s ({hourly_collection_duration / 60:.1f} min)')
    for name, total in collector_totals.items():
        print(f'    {name}: {total:.1f}s total, peak {collector_peak_memory[name]:.1f} MB')
    print(f'  Rollup:               {rollup_duration:.2f}s, {get_memory_mb(process):.1f} MB after')
    print(f'  Total:                {total_duration:.1f}s ({total_duration / 60:.1f} min)')
    print()
    print(f'  Baseline memory: {baseline_memory_mb:.1f} MB')
    print(f'  Peak memory:     {peak_memory_mb:.1f} MB (RSS, sampled every 50ms during execution)')
    print(f'  Delta:           {peak_memory_mb - baseline_memory_mb:.1f} MB')
    print()

    # Output table sizes
    from django.core import serializers
    from django.db.models import Count

    hourly_count = HourlyMetricsCollection.objects.aggregate(count=Count('id'))['count']
    daily_count = DailyMetricsSummary.objects.aggregate(count=Count('id'))['count']

    hourly_json = serializers.serialize('json', HourlyMetricsCollection.objects.all())
    daily_json = serializers.serialize('json', DailyMetricsSummary.objects.all())

    hourly_size_mb = len(hourly_json.encode()) / 1024 / 1024
    daily_size_mb = len(daily_json.encode()) / 1024 / 1024

    print('  Output Table Sizes:')
    print(f'    HourlyMetricsCollection: {hourly_count} rows, {hourly_size_mb:.2f} MB')
    print(f'    DailyMetricsSummary:     {daily_count} rows, {daily_size_mb:.2f} MB')
    print()

    print_source_table_counts(test_date)


_TABLE_COUNT_QUERIES = {
    'main_jobevent': 'SELECT COUNT(*) FROM main_jobevent',
    'main_jobhostsummary': 'SELECT COUNT(*) FROM main_jobhostsummary',
    'main_host': 'SELECT COUNT(*) FROM main_host',
    'main_unifiedjob': 'SELECT COUNT(*) FROM main_unifiedjob',
    'main_job': 'SELECT COUNT(*) FROM main_job',
    'main_unifiedjobtemplate': 'SELECT COUNT(*) FROM main_unifiedjobtemplate',
    'main_inventory': 'SELECT COUNT(*) FROM main_inventory',
    'main_organization': 'SELECT COUNT(*) FROM main_organization',
    'main_credential': 'SELECT COUNT(*) FROM main_credential',
    'main_credentialtype': 'SELECT COUNT(*) FROM main_credentialtype',
    'main_unifiedjob_credentials': 'SELECT COUNT(*) FROM main_unifiedjob_credentials',
    'main_executionenvironment': 'SELECT COUNT(*) FROM main_executionenvironment',
}


def _count(cursor, table):
    cursor.execute(_TABLE_COUNT_QUERIES[table])
    return cursor.fetchone()[0]


def print_source_table_counts(test_date):
    """Print row counts for all AWX source tables touched by collectors."""
    from django.db import connections

    with connections['awx'].cursor() as cursor:
        total_events = _count(cursor, 'main_jobevent')
        cursor.execute(
            'SELECT COUNT(*) FROM main_jobevent WHERE job_created >= %s AND job_created < %s',
            [test_date, test_date + timedelta(days=1)],
        )
        events_on_date = cursor.fetchone()[0]

        print('  Source Table Counts (AWX DB):')
        print(f'    {"Table":<32} {"Total":>12}  On test date')
        print(f'    {"-" * 32} {"-" * 12}  ------------')
        print(f'    {"main_jobevent":<32} {total_events:>12,}  {events_on_date:,}')
        print(f'    {"main_jobhostsummary":<32} {_count(cursor, "main_jobhostsummary"):>12,}')
        print(f'    {"main_host":<32} {_count(cursor, "main_host"):>12,}')
        print(f'    {"main_unifiedjob":<32} {_count(cursor, "main_unifiedjob"):>12,}')
        print(f'    {"main_job":<32} {_count(cursor, "main_job"):>12,}')
        print(f'    {"main_unifiedjobtemplate":<32} {_count(cursor, "main_unifiedjobtemplate"):>12,}')
        print(f'    {"main_inventory":<32} {_count(cursor, "main_inventory"):>12,}')
        print(f'    {"main_organization":<32} {_count(cursor, "main_organization"):>12,}')
        print(f'    {"main_credential":<32} {_count(cursor, "main_credential"):>12,}')
        print(f'    {"main_credentialtype":<32} {_count(cursor, "main_credentialtype"):>12,}')
        print(f'    {"main_unifiedjob_credentials":<32} {_count(cursor, "main_unifiedjob_credentials"):>12,}')
        print(f'    {"main_executionenvironment":<32} {_count(cursor, "main_executionenvironment"):>12,}')
    print()


def run_collection_rollup_benchmark():
    test_date_str = os.environ.get('TEST_DATE', '2024-01-25')
    test_date = datetime.fromisoformat(test_date_str).replace(hour=0, minute=0, second=0, microsecond=0)

    print(f'\n{"=" * 80}')
    print('  Metrics Collection & Rollup Performance Test')
    print(f'  Test Date: {test_date.date()}')
    print(f'{"=" * 80}\n')

    print('Cleaning old collections...')
    HourlyMetricsCollection.objects.all().delete()
    DailyMetricsSummary.objects.all().delete()
    print('Done\n')

    print('Warm-up: running throwaway collector call to initialize DB connections and caches...')
    with contextlib.suppress(Exception):
        collect_snapshot_metrics(collector_type='config', database='awx')
    HourlyMetricsCollection.objects.all().delete()
    print('Done\n')

    process = psutil.Process()
    baseline_memory_mb = get_memory_mb(process)
    peak_memory_mb = baseline_memory_mb

    snapshot_duration, peak_memory_mb = run_snapshot_phase(process, peak_memory_mb)

    hourly_collection_duration, peak_memory_mb, collector_totals, collector_peak_memory = run_hourly_phase(
        test_date,
        process,
        peak_memory_mb,
        baseline_memory_mb,
    )

    rollup_duration, peak_memory_mb = run_rollup_phase(test_date, process, peak_memory_mb)

    print_final_summary(
        snapshot_duration,
        hourly_collection_duration,
        rollup_duration,
        collector_totals,
        collector_peak_memory,
        baseline_memory_mb,
        peak_memory_mb,
        process,
        test_date,
    )


if __name__ == '__main__':
    run_collection_rollup_benchmark()
