# ruff: noqa: E402
import os
import sys
import threading
import time

from datetime import UTC, date, datetime, timedelta
from pathlib import Path


project_root = (Path(__file__).parent.parent.parent.parent / 'metrics-service').resolve()
sys.path.insert(0, str(project_root))

import django


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metrics_service.settings')
django.setup()

import psutil

from apps.tasks.collectors import collect_hourly_metrics, collect_snapshot_metrics, daily_metrics_rollup
from apps.tasks.models import DailyMetricsSummary, HourlyMetricsCollection
from django.core import serializers
from django.db.models import Count


_TABLE_COUNT_QUERIES = {
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


def print_source_table_counts(test_date_str, collector_durations=None):
    """Print row counts for all AWX source tables touched by collectors."""
    from django.db import connections

    parsed = date.fromisoformat(test_date_str)
    day_start = datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    with connections['awx'].cursor() as cursor:
        cursor.execute(
            'SELECT COUNT(*) FROM main_unifiedjob WHERE finished >= %s AND finished < %s',
            [day_start, day_end],
        )
        unifiedjob_on_date = cursor.fetchone()[0]

        total_unifiedjob = _count(cursor, 'main_unifiedjob')
        total_jobhostsummary = _count(cursor, 'main_jobhostsummary')
        total_unifiedjob_credentials = _count(cursor, 'main_unifiedjob_credentials')

        # Derive on-test-date counts from fixed ratios (confirmed in generator: each job
        # gets the same host_count summaries and the same credential set)
        hosts_per_job = round(total_jobhostsummary / total_unifiedjob) if total_unifiedjob else 0
        creds_per_job = round(total_unifiedjob_credentials / total_unifiedjob) if total_unifiedjob else 0
        jobhostsummary_on_date = unifiedjob_on_date * hosts_per_job
        credentials_on_date = unifiedjob_on_date * creds_per_job

        print('  Source Table Counts (AWX DB):')
        print(f'    {"Table":<32} {"Total (all time)":>16}  {"Test date total":>15}')
        print(f'    {"-" * 32} {"-" * 16}  {"-" * 15}')
        print(f'    {"main_unifiedjob":<32} {total_unifiedjob:>16,}  {unifiedjob_on_date:>15,}')
        print(f'    {"main_jobhostsummary":<32} {total_jobhostsummary:>16,}  {jobhostsummary_on_date:>15,} ({hosts_per_job} per job)')
        print(f'    {"main_unifiedjob_credentials":<32} {total_unifiedjob_credentials:>16,}  {credentials_on_date:>15,} ({creds_per_job} per job)')
        for table in [
            'main_host',
            'main_job',
            'main_unifiedjobtemplate',
            'main_inventory',
            'main_organization',
            'main_credential',
            'main_credentialtype',
            'main_executionenvironment',
        ]:
            print(f'    {table:<32} {_count(cursor, table):>16,}')

        print()
        print('  Jobs finished per hour on test date (main_unifiedjob.finished):')
        print(f'    {"Hour":>6}  {"Jobs":>8}')
        print(f'    {"----":>6}  {"----":>8}')
        for hour in range(24):
            hour_start = day_start + timedelta(hours=hour)
            hour_end = hour_start + timedelta(hours=1)
            cursor.execute(
                'SELECT COUNT(*) FROM main_unifiedjob WHERE finished >= %s AND finished < %s',
                [hour_start, hour_end],
            )
            print(f'    {hour:>6}  {cursor.fetchone()[0]:>8,}')

    if collector_durations:
        # Mapping from collector name to the rows it processes on the test date
        collector_rows = {
            'job_host_summary_service': (jobhostsummary_on_date, 'main_jobhostsummary rows'),
            'unified_jobs': (unifiedjob_on_date, 'main_unifiedjob rows'),
            'credentials_service': (credentials_on_date, 'main_unifiedjob_credentials rows'),
        }
        print()
        print('  Per-Collector Summary (24 hours total):')
        print(f'    {"Collector":<35} {"Total duration":>15}  {"Rows processed":>20}')
        print(f'    {"---------":<35} {"--------------":>15}  {"-------------------":>20}')
        for collector, duration in collector_durations.items():
            rows, label = collector_rows.get(collector, (0, 'rows'))
            print(f'    {collector:<35} {duration:>13.2f}s  {rows:>15,} ({label})')
    print()


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


TEST_DATE = '2024-01-25'

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
]

process = psutil.Process()
baseline_memory_mb = get_memory_mb(process)
peak_memory_mb = baseline_memory_mb

# Capture starting IDs so output table sizes reflect only rows written in this run
starting_hourly_id = HourlyMetricsCollection.objects.order_by('-id').values_list('id', flat=True).first() or 0
starting_daily_id = DailyMetricsSummary.objects.order_by('-id').values_list('id', flat=True).first() or 0

print('=' * 70)
print('Metrics Service Pipeline Benchmark (manage.py)')
print('=' * 70)
print(f'  Test date       : {TEST_DATE}')
print(f'  Baseline memory : {baseline_memory_mb:.1f} MB')

# Phase 1 — Snapshot collectors
print('\n' + '=' * 70)
print('Phase 1: Snapshot collectors (run once each)')
print('=' * 70)
print(f'  {"Collector":<35} {"Duration":>10} {"Peak MB":>10}')
print(f'  {"---------":<35} {"--------":>10} {"-------":>10}')

snapshot_start = time.perf_counter()
for collector in SNAPSHOT_COLLECTORS:
    start = time.perf_counter()
    with PeakMemoryMonitor(process) as monitor:
        collect_snapshot_metrics(collector_type=collector)
    elapsed = time.perf_counter() - start
    peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)
    print(f'  {collector:<35} {elapsed:>8.2f}s {monitor.peak_mb:>9.1f}M')

snapshot_elapsed = time.perf_counter() - snapshot_start
print(f'\n  Total: {snapshot_elapsed:.1f}s')

# Phase 2 — Hourly collection
print('\n' + '=' * 70)
print('Phase 2: Hourly collectors (24 hours x 3 collectors)')
print('=' * 70)
print(f'  {"Hour":<6} {"Collector":<35} {"Duration":>10} {"Peak MB":>10}')
print(f'  {"----":<6} {"---------":<35} {"--------":>10} {"-------":>10}')

collector_durations = dict.fromkeys(HOURLY_COLLECTORS, 0.0)

hourly_start = time.perf_counter()
for hour in range(24):
    hour_ts = f'{TEST_DATE}T{hour:02d}:00:00Z'
    for collector in HOURLY_COLLECTORS:
        start = time.perf_counter()
        with PeakMemoryMonitor(process) as monitor:
            collect_hourly_metrics(collector_type=collector, hour_timestamp=hour_ts)
        elapsed = time.perf_counter() - start
        collector_durations[collector] += elapsed
        peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)
        print(f'  {hour:<6} {collector:<35} {elapsed:>8.2f}s {monitor.peak_mb:>9.1f}M')

hourly_elapsed = time.perf_counter() - hourly_start
print(f'\n  Total: {hourly_elapsed:.1f}s ({hourly_elapsed / 60:.1f} min)')

# Phase 3 — Daily rollup
print('\n' + '=' * 70)
print('Phase 3: Daily rollup')
print('=' * 70)
start = time.perf_counter()
with PeakMemoryMonitor(process) as monitor:
    daily_metrics_rollup(summary_date=TEST_DATE)
rollup_elapsed = time.perf_counter() - start
peak_memory_mb = max(peak_memory_mb, monitor.peak_mb)
print(f'  Duration: {rollup_elapsed:.2f}s')
print(f'  Peak memory: {monitor.peak_mb:.1f} MB')

total_elapsed = snapshot_elapsed + hourly_elapsed + rollup_elapsed

# Summary
print('\n' + '=' * 70)
print('Summary')
print('=' * 70)
print(f'  Snapshot collectors: {snapshot_elapsed:>8.2f}s')
print(f'  Hourly collection  : {hourly_elapsed:>8.1f}s ({hourly_elapsed / 60:.1f} min)')
print(f'  Daily rollup       : {rollup_elapsed:>8.2f}s')
print(f'  Total              : {total_elapsed:>8.1f}s ({total_elapsed / 60:.1f} min)')
print()
print(f'  Baseline memory: {baseline_memory_mb:.1f} MB')
print(f'  Peak memory:     {peak_memory_mb:.1f} MB (RSS, sampled every 50ms during execution)')
print(f'  Delta:           {peak_memory_mb - baseline_memory_mb:.1f} MB')
print()

hourly_qs = HourlyMetricsCollection.objects.filter(id__gt=starting_hourly_id)
daily_qs = DailyMetricsSummary.objects.filter(id__gt=starting_daily_id)
hourly_count = hourly_qs.aggregate(count=Count('id'))['count']
daily_count = daily_qs.aggregate(count=Count('id'))['count']
hourly_size_mb = len(serializers.serialize('json', hourly_qs).encode()) / 1024 / 1024
daily_size_mb = len(serializers.serialize('json', daily_qs).encode()) / 1024 / 1024

print('  Output Table Sizes:')
print(f'    HourlyMetricsCollection: {hourly_count} rows, {hourly_size_mb:.2f} MB')
print(f'    DailyMetricsSummary:     {daily_count} rows, {daily_size_mb:.2f} MB')
print()

print_source_table_counts(TEST_DATE, collector_durations)
