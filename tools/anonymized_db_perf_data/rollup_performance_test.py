#!/usr/bin/env python3
"""
Test anonymized rollup performance by running each task separately.

Measures time and memory for:
- Each collector (execution_environments, unified_jobs, job_host_summary, main_jobevent)
- Rollup computation

Usage:
    python rollup_performance_test.py
"""

import sys
import time

from datetime import datetime
from pathlib import Path


# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Initialize Django
from metrics_utility import prepare  # noqa: E402


prepare()

from django.db import connection  # noqa: E402

# Import rollup computation
from metrics_utility.anonymized_rollups.anonymized_rollups import (  # noqa: E402
    compute_anonymized_rollup_from_raw_data,
)

# Import collectors
from metrics_utility.library.collectors.controller import (  # noqa: E402
    execution_environments,
    job_host_summary_service,
    main_jobevent_service,
    unified_jobs,
)


# Try to import psutil for memory tracking
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("Warning: psutil not available. Install with 'pip install psutil' for memory tracking.")


def run_task(name, func):
    """Run a task and return timing/memory metrics."""
    print(f'\n{"=" * 60}')
    print(f'Testing: {name}')
    print(f'{"=" * 60}')

    # Measure memory before
    mem_before = psutil.Process().memory_info().rss / (1024 * 1024) if PSUTIL_AVAILABLE else None

    # Run task and measure time
    start_time = time.time()
    try:
        result = func()
    except Exception as e:
        print(f'ERROR: {e}')
        import traceback

        traceback.print_exc()
        return None, None
    elapsed = time.time() - start_time

    # Measure memory after
    mem_after = psutil.Process().memory_info().rss / (1024 * 1024) if PSUTIL_AVAILABLE else None

    # Build metrics
    metrics = {'name': name, 'elapsed_seconds': round(elapsed, 2)}
    if mem_before and mem_after:
        metrics['memory_mb'] = round(mem_after - mem_before, 2)

    # Print results
    print(f'✓ Completed in {metrics["elapsed_seconds"]} seconds')
    if 'memory_mb' in metrics:
        print(f'  Memory used: {metrics["memory_mb"]} MB')

    return metrics, result


def main():
    """Run all performance tests."""
    import argparse

    parser = argparse.ArgumentParser(description='Run anonymized rollup performance tests on individual tasks')
    parser.add_argument('--since', type=str, default='2024-01-01', help='Start date (YYYY-MM-DD, default: 2024-01-01)')
    parser.add_argument('--until', type=str, default='2024-02-01', help='End date (YYYY-MM-DD, default: 2024-02-01)')
    args = parser.parse_args()

    # Configuration
    since = datetime.strptime(args.since, '%Y-%m-%d')
    until = datetime.strptime(args.until, '%Y-%m-%d')
    output_dir = Path(__file__).parent / 'out'
    output_dir.mkdir(exist_ok=True)

    print('\n' + '=' * 60)
    print('ANONYMIZED ROLLUP PERFORMANCE TEST')
    print('=' * 60)
    print(f'Date range: {since} to {until}')
    print(f'Output directory: {output_dir}')
    print(f'Memory tracking: {"ENABLED" if PSUTIL_AVAILABLE else "DISABLED (install psutil)"}')

    # Disable parallel workers to avoid shared memory issues with large datasets
    cursor = connection.cursor()
    cursor.execute('SET max_parallel_workers_per_gather = 0;')
    print('Note: Disabled parallel workers to avoid shared memory issues')

    all_metrics = []

    # Test collectors
    print('\n' + '=' * 60)
    print('PHASE 1: Testing Collectors')
    print('=' * 60)

    collector_data = {}

    # Test execution_environments
    metrics, data = run_task('execution_environments', lambda: execution_environments(db=connection).gather())
    if metrics:
        all_metrics.append(metrics)
        collector_data['execution_environments'] = data

    # Test unified_jobs
    metrics, data = run_task('unified_jobs', lambda: unified_jobs(db=connection, since=since, until=until).gather())
    if metrics:
        all_metrics.append(metrics)
        collector_data['unified_jobs'] = data

    # Test job_host_summary_service
    metrics, data = run_task('job_host_summary_service', lambda: job_host_summary_service(db=connection, since=since, until=until).gather())
    if metrics:
        all_metrics.append(metrics)
        collector_data['job_host_summary_service'] = data

    # Test main_jobevent_service
    metrics, data = run_task('main_jobevent_service', lambda: main_jobevent_service(db=connection, since=since, until=until).gather())
    if metrics:
        all_metrics.append(metrics)
        collector_data['main_jobevent_service'] = data

    # Test rollup computation
    print('\n' + '=' * 60)
    print('PHASE 2: Testing Rollup Computation')
    print('=' * 60)

    input_data = {
        'execution_environments': collector_data['execution_environments'],
        'unified_jobs': collector_data['unified_jobs'],
        'job_host_summary': collector_data['job_host_summary_service'],
        'main_jobevent': collector_data['main_jobevent_service'],
    }

    metrics, _ = run_task(
        'Rollup Computation',
        lambda: compute_anonymized_rollup_from_raw_data(
            input_data, salt='', since=since, until=until, base_path=str(output_dir), save_rollups=True, save_rollups_packed=False
        ),
    )
    if metrics:
        all_metrics.append(metrics)

    # Get database counts
    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM main_host;')
    host_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM main_job;')
    job_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM main_jobevent;')
    event_count = cursor.fetchone()[0]

    # Print summary
    print('\n' + '=' * 60)
    print('SUMMARY')
    print('=' * 60)

    total_time = sum(m['elapsed_seconds'] for m in all_metrics)

    print(f'\n{"Task":<30} {"Time (s)":<12} {"Memory (MB)":<12}')
    print('-' * 60)
    for m in all_metrics:
        mem_str = f'{m["memory_mb"]}' if 'memory_mb' in m else 'N/A'
        print(f'{m["name"]:<30} {m["elapsed_seconds"]:<12} {mem_str:<12}')

    print('-' * 60)
    print(f'{"TOTAL":<30} {total_time:<12}')

    # Write metrics to log file
    log_file = output_dir / 'performance_metrics.log'
    with open(log_file, 'w') as f:
        f.write('ANONYMIZED ROLLUP PERFORMANCE TEST - INDIVIDUAL TASKS\n')
        f.write('=' * 60 + '\n')
        f.write(f'Date: {datetime.now()}\n')
        f.write(f'Date range: {since} to {until}\n\n')

        f.write(f'{"Task":<30} {"Time (s)":<12} {"Memory (MB)":<12}\n')
        f.write('-' * 60 + '\n')
        for m in all_metrics:
            mem_str = f'{m["memory_mb"]}' if 'memory_mb' in m else 'N/A'
            f.write(f'{m["name"]:<30} {m["elapsed_seconds"]:<12} {mem_str:<12}\n')

        f.write('-' * 60 + '\n')
        f.write(f'{"TOTAL":<30} {total_time:<12}\n')

    # Write markdown report
    test_date = datetime.now().strftime('%Y-%m-%d')
    md_file = output_dir / f'perf_test_individual_tasks_{test_date}.md'
    with open(md_file, 'w') as f:
        f.write('# Performance Test Results - Individual Tasks\n\n')
        f.write(f'**Test Date:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')

        f.write('## Dataset Information\n\n')
        f.write(f'- **Test Period:** {since.strftime("%Y-%m-%d")} to {until.strftime("%Y-%m-%d")}\n')
        f.write(f'- **Total Jobs:** {job_count:,}\n')
        f.write(f'- **Total Hosts:** {host_count:,}\n')
        f.write(f'- **Total Events:** {event_count:,}\n\n')

        f.write('## Performance Results\n\n')
        f.write('### Individual Task Performance\n\n')
        f.write('| Task | Duration | Memory Used |\n')
        f.write('|------|----------|-------------|\n')
        for m in all_metrics:
            mem_str = f'{m["memory_mb"]:.2f} MB' if 'memory_mb' in m else 'N/A'
            f.write(f'| {m["name"]} | {m["elapsed_seconds"]:.2f}s | {mem_str} |\n')

        f.write('\n### Summary\n\n')
        total_mem = sum(m.get('memory_mb', 0) for m in all_metrics)
        f.write(f'- **Total Duration:** {total_time:.2f}s ({total_time / 60:.2f} minutes)\n')
        if PSUTIL_AVAILABLE:
            f.write(f'- **Total Memory Used:** {total_mem:.2f} MB\n')
        f.write(f'- **Memory Tracking:** {"Enabled" if PSUTIL_AVAILABLE else "Disabled (psutil not installed)"}\n')

    print('\n✓ Performance test completed!')
    print(f'Results saved to: {output_dir}')
    print(f'Performance log: {log_file}')
    print(f'Markdown report: {md_file}')


if __name__ == '__main__':
    main()
