#!/usr/bin/env python3
"""
Performance test for collectors: jobs, jobhostsummary (old/new), and events (old/new).

Tests different time ranges and compares old vs new collector performance.
Includes EXPLAIN ANALYZE for events collectors to verify partition pruning.

NOTE: Run this with venv activated:
  source .venv/bin/activate
  python tools/anonymized_db_perf_data/test_collectors_performance.py
"""

import sys
import time

from datetime import datetime, timedelta
from pathlib import Path


# Add project root to path so we can import metrics_utility
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Initialize Django (requires venv to be activated)
from metrics_utility import prepare  # noqa: E402


prepare()

from django.db import connection  # noqa: E402

from metrics_utility.library.collectors.controller.job_host_summary import job_host_summary  # noqa: E402
from metrics_utility.library.collectors.controller.job_host_summary_service import job_host_summary_service  # noqa: E402
from metrics_utility.library.collectors.controller.main_jobevent import main_jobevent  # noqa: E402
from metrics_utility.library.collectors.controller.main_jobevent_service import main_jobevent_service  # noqa: E402
from metrics_utility.library.collectors.controller.unified_jobs import unified_jobs  # noqa: E402


def count_csv_rows(file_paths):
    """Count total rows across all CSV files (excluding headers)."""
    total_rows = 0
    for file_path in file_paths:
        try:
            with open(file_path, 'r') as f:
                rows = sum(1 for line in f) - 1  # Subtract header
                total_rows += rows
        except Exception:
            pass
    return total_rows


def explain_main_jobevent_old(db, since, until):
    """Run EXPLAIN ANALYZE on the OLD main_jobevent collector query."""
    where = ' AND '.join(
        [
            f"main_jobhostsummary.modified >= '{since.isoformat()}'",
            f"main_jobhostsummary.modified < '{until.isoformat()}'",
        ]
    )

    query = f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        WITH job_scope AS (
            SELECT
                main_jobhostsummary.id AS main_jobhostsummary_id,
                main_jobhostsummary.created AS main_jobhostsummary_created,
                main_jobhostsummary.modified AS main_jobhostsummary_modified,
                main_unifiedjob.created AS job_created,
                main_jobhostsummary.job_id AS job_id,
                main_jobhostsummary.host_name
            FROM main_jobhostsummary
            JOIN main_unifiedjob ON main_unifiedjob.id = main_jobhostsummary.job_id
            WHERE {where}
        )
        SELECT
            job_scope.main_jobhostsummary_id,
            main_jobevent.id,
            main_jobevent.job_created
        FROM main_jobevent
        JOIN job_scope ON
            job_scope.job_created = main_jobevent.job_created
            AND job_scope.job_id = main_jobevent.job_id
            AND job_scope.host_name = main_jobevent.host_name
        WHERE main_jobevent.event IN (
            'runner_on_ok', 'runner_on_failed', 'runner_on_unreachable',
            'runner_on_skipped', 'runner_retry', 'runner_on_async_ok',
            'runner_item_on_ok', 'runner_item_on_failed', 'runner_item_on_skipped'
        )
    """

    with db.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()[0]  # JSON result

    # Parse the explain output
    plan = result[0]['Plan']
    planning_time = result[0].get('Planning Time', 0)
    execution_time = result[0].get('Execution Time', 0)

    # Check for partition scans
    partitions_scanned = count_partitions_in_plan(plan)

    return {
        'planning_time': f'{planning_time:.3f}ms',
        'execution_time': f'{execution_time:.3f}ms',
        'partitions_scanned': partitions_scanned,
        'full_plan': result[0],
    }


def explain_main_jobevent_service(db, since, until):
    """Run EXPLAIN ANALYZE on the NEW main_jobevent_service collector query."""
    # First, get the jobs (same as the collector does)
    jobs_query = """
        SELECT uj.id AS job_id, uj.created AS job_created
        FROM main_unifiedjob uj
        WHERE uj.finished >= %(since)s AND uj.finished < %(until)s
    """

    with db.cursor() as cursor:
        cursor.execute(jobs_query, {'since': since, 'until': until})
        jobs = cursor.fetchall()

    if not jobs:
        return {
            'planning_time': 'N/A',
            'execution_time': 'N/A',
            'partitions_scanned': 0,
            'note': 'No jobs in range',
        }

    # Extract job_ids and hour boundaries (same logic as collector)
    job_ids_set = set(job_id for job_id, _ in jobs)
    hour_boundaries = set()
    for job_id, job_created in jobs:
        if job_created:
            hour_start = job_created.replace(minute=0, second=0, microsecond=0)
            hour_boundaries.add(hour_start)

    sorted_hours = sorted(hour_boundaries)

    # Group consecutive hours into ranges
    ranges = []
    if sorted_hours:
        range_start = sorted_hours[0]
        range_end = sorted_hours[0] + timedelta(hours=1)

        for hour in sorted_hours[1:]:
            if hour == range_end:
                range_end = hour + timedelta(hours=1)
            else:
                ranges.append((range_start, range_end))
                range_start = hour
                range_end = hour + timedelta(hours=1)
        ranges.append((range_start, range_end))

    # Build WHERE clause
    or_clauses = []
    for range_start, range_end in ranges:
        or_clauses.append(f"(e.job_created >= '{range_start.isoformat()}'::timestamptz AND e.job_created < '{range_end.isoformat()}'::timestamptz)")

    timestamp_where_clause = ' OR '.join(or_clauses) if or_clauses else 'FALSE'

    if job_ids_set:
        job_ids_str = ','.join(str(job_id) for job_id in job_ids_set)
        job_id_where_clause = f'e.job_id IN ({job_ids_str})'
    else:
        job_id_where_clause = 'FALSE'

    where_clause = f'({timestamp_where_clause}) AND ({job_id_where_clause})'

    # Simplified query for EXPLAIN (just essential fields)
    query = f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT e.id, e.job_created, e.job_id
        FROM main_jobevent e
        WHERE {where_clause}
    """

    with db.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()[0]

    # Parse the explain output
    plan = result[0]['Plan']
    planning_time = result[0].get('Planning Time', 0)
    execution_time = result[0].get('Execution Time', 0)

    # Check for partition scans
    partitions_scanned = count_partitions_in_plan(plan)

    return {
        'planning_time': f'{planning_time:.3f}ms',
        'execution_time': f'{execution_time:.3f}ms',
        'partitions_scanned': partitions_scanned,
        'hour_ranges': len(ranges),
        'full_plan': result[0],
    }


def count_partitions_in_plan(plan, count=0):
    """Recursively count partition scans in the query plan."""
    if isinstance(plan, dict):
        # Check for partition-specific nodes
        relation_name = plan.get('Relation Name', '')

        # Count if this is a partition scan
        if 'main_jobevent_' in relation_name:  # Partition naming pattern
            count += 1

        # Recurse into plans
        if 'Plans' in plan:
            for subplan in plan['Plans']:
                count = count_partitions_in_plan(subplan, count)

    return count


def run_collector_test(collector_func, collector_name, since, until, explain_analyze=False):
    """
    Test a single collector and measure execution time.

    Args:
        collector_func: The collector function to test
        collector_name: Name of the collector for display
        since: Start datetime for data range
        until: End datetime for data range
        explain_analyze: If True, run EXPLAIN ANALYZE on the query (events collectors only)

    Returns:
        dict with test results
    """
    print(f'\n{"=" * 70}')
    print(f'Testing: {collector_name}')
    print(f'{"=" * 70}')
    print(f'Time range: {since} to {until}')
    print(f'Duration: {until - since}')

    # Check expected data counts
    with connection.cursor() as cursor:
        # Count jobs
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM main_unifiedjob
            WHERE (created >= %s AND created < %s)
               OR (finished >= %s AND finished < %s)
        """,
            (since, until, since, until),
        )
        job_count = cursor.fetchone()[0]

        # Count job host summaries
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM main_jobhostsummary
            WHERE modified >= %s AND modified < %s
        """,
            (since, until),
        )
        jhs_count = cursor.fetchone()[0]

        # Count job events
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM main_jobevent je
            JOIN main_jobhostsummary jhs ON jhs.job_id = je.job_id
            WHERE jhs.modified >= %s AND jhs.modified < %s
        """,
            (since, until),
        )
        event_count = cursor.fetchone()[0]

    print('Expected data in range:')
    print(f'  - Jobs: {job_count:,}')
    print(f'  - Job Host Summaries: {jhs_count:,}')
    print(f'  - Job Events: {event_count:,}')

    explain_results = None
    # Run EXPLAIN ANALYZE if requested (for events collectors)
    if explain_analyze and 'event' in collector_name.lower():
        print(f'\n--- EXPLAIN ANALYZE for {collector_name} ---')
        try:
            # Build the actual query that the collector would run
            if 'main_jobevent_service (NEW)' in collector_name:
                # For NEW collector, we need to execute the complex logic
                explain_results = explain_main_jobevent_service(connection, since, until)
            elif 'main_jobevent (OLD)' in collector_name:
                # For OLD collector, build the query directly
                explain_results = explain_main_jobevent_old(connection, since, until)

            if explain_results:
                print('✓ EXPLAIN ANALYZE completed')
                print(f'  Query planning time: {explain_results.get("planning_time", "N/A")}')
                print(f'  Query execution time: {explain_results.get("execution_time", "N/A")}')

                # Check for partition pruning
                if 'partitions_scanned' in explain_results:
                    print(f'  Partitions scanned: {explain_results["partitions_scanned"]}')

        except Exception as e:
            print(f'Could not run EXPLAIN ANALYZE: {e}')
            import traceback

            traceback.print_exc()

    # Time the collector
    print('\nRunning collector...')
    start_time = time.time()

    try:
        # Run the collector - it returns a CollectorClass instance
        collector_instance = collector_func(db=connection, since=since, until=until, output_dir=None)

        # Call gather() to actually execute the collector
        result = collector_instance.gather()

        elapsed = time.time() - start_time

        # Count rows in generated CSV files
        row_count = count_csv_rows(result) if result else 0

        print('✓ Collector completed successfully')
        print(f'  Execution time: {elapsed:.3f} seconds')
        print(f'  CSV files generated: {len(result) if result else 0}')
        print(f'  Total rows in CSV: {row_count:,}')

        # Show file details
        if result and len(result) > 0:
            print('\n  Generated files:')
            for i, file_path in enumerate(result):
                try:
                    with open(file_path, 'r') as f:
                        file_rows = sum(1 for line in f) - 1
                    print(f'    {i + 1}. {Path(file_path).name} ({file_rows:,} rows)')
                except Exception as e:
                    print(f'    {i + 1}. {Path(file_path).name} (could not read: {e})')

        return {
            'success': True,
            'collector': collector_name,
            'elapsed': elapsed,
            'file_count': len(result) if result else 0,
            'row_count': row_count,
            'expected_jobs': job_count,
            'expected_jhs': jhs_count,
            'expected_events': event_count,
            'explain_results': explain_results,
        }

    except Exception as e:
        elapsed = time.time() - start_time
        print(f'✗ Collector failed: {e}')
        print(f'  Time until failure: {elapsed:.3f} seconds')

        import traceback

        traceback.print_exc()

        return {
            'success': False,
            'collector': collector_name,
            'elapsed': elapsed,
            'error': str(e),
        }


def find_data_range():
    """Find the actual date range of data in the database."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT MIN(created), MAX(created)
            FROM main_unifiedjob
            WHERE created IS NOT NULL
        """)
        result = cursor.fetchone()
        return result[0], result[1]


def main():
    """Main entry point."""
    print('\n' + '=' * 70)
    print('COMPREHENSIVE COLLECTOR PERFORMANCE TEST')
    print('=' * 70)
    print('\nTesting collectors:')
    print('  1. unified_jobs (jobs collector)')
    print('  2. job_host_summary (OLD jobhostsummary collector)')
    print('  3. job_host_summary_service (NEW jobhostsummary collector)')
    print('  4. main_jobevent (OLD events collector)')
    print('  5. main_jobevent_service (NEW events collector)')

    # Find actual data range
    min_date, max_date = find_data_range()
    print(f'\nData available from {min_date} to {max_date}')

    # Define test time ranges
    # Use January 2024 as base since that's where test data is
    time_ranges = [
        {
            'name': '1 month (Jan 2024)',
            'since': datetime(2024, 1, 1, 0, 0, 0),
            'until': datetime(2024, 2, 1, 0, 0, 0),
        },
        {
            'name': '1 week',
            'since': datetime(2024, 1, 1, 0, 0, 0),
            'until': datetime(2024, 1, 8, 0, 0, 0),
        },
        {
            'name': '1 day',
            'since': datetime(2024, 1, 1, 0, 0, 0),
            'until': datetime(2024, 1, 2, 0, 0, 0),
        },
        {
            'name': '1 hour',
            'since': datetime(2024, 1, 1, 0, 0, 0),
            'until': datetime(2024, 1, 1, 1, 0, 0),
        },
    ]

    # Collectors to test
    collectors = [
        {'func': unified_jobs, 'name': 'unified_jobs', 'explain': False},
        {'func': job_host_summary, 'name': 'job_host_summary (OLD)', 'explain': False},
        {'func': job_host_summary_service, 'name': 'job_host_summary_service (NEW)', 'explain': False},
        {'func': main_jobevent, 'name': 'main_jobevent (OLD)', 'explain': True},
        {'func': main_jobevent_service, 'name': 'main_jobevent_service (NEW)', 'explain': True},
    ]

    # Store all results
    all_results = []

    # Test each time range
    for time_range in time_ranges:
        print(f'\n\n{"#" * 70}')
        print(f'# TIME RANGE: {time_range["name"]}')
        print(f'{"#" * 70}')

        for collector in collectors:
            result = run_collector_test(
                collector['func'], collector['name'], time_range['since'], time_range['until'], explain_analyze=collector['explain']
            )
            result['time_range'] = time_range['name']
            all_results.append(result)

    # Print summary
    print(f'\n\n{"=" * 70}')
    print('PERFORMANCE SUMMARY')
    print(f'{"=" * 70}')

    # Group by time range
    for time_range in time_ranges:
        print(f'\n{time_range["name"]}:')
        print(f'{"  Collector":<45} {"Time (s)":<12} {"Rows":<12} {"Status"}')
        print('  ' + '-' * 66)

        range_results = [r for r in all_results if r.get('time_range') == time_range['name']]
        for result in range_results:
            if result['success']:
                status = '✓ PASS'
                rows = f'{result["row_count"]:,}'
            else:
                status = '✗ FAIL'
                rows = 'N/A'

            print(f'  {result["collector"]:<45} {result["elapsed"]:<12.3f} {rows:<12} {status}')

    # Compare old vs new collectors
    print(f'\n\n{"=" * 70}')
    print('OLD vs NEW COLLECTOR COMPARISON')
    print(f'{"=" * 70}')

    comparisons = [
        ('job_host_summary (OLD)', 'job_host_summary_service (NEW)', 'Job Host Summary'),
        ('main_jobevent (OLD)', 'main_jobevent_service (NEW)', 'Job Events'),
    ]

    for old_name, new_name, category in comparisons:
        print(f'\n{category}:')
        for time_range in time_ranges:
            old_result = next((r for r in all_results if r.get('collector') == old_name and r.get('time_range') == time_range['name']), None)
            new_result = next((r for r in all_results if r.get('collector') == new_name and r.get('time_range') == time_range['name']), None)

            if old_result and new_result and old_result['success'] and new_result['success']:
                old_time = old_result['elapsed']
                new_time = new_result['elapsed']
                speedup = old_time / new_time if new_time > 0 else 0
                diff = old_time - new_time

                if speedup > 1:
                    comparison = f'NEW is {speedup:.2f}x faster (saved {diff:.3f}s)'
                elif speedup < 1:
                    comparison = f'OLD is {1 / speedup:.2f}x faster (NEW slower by {-diff:.3f}s)'
                else:
                    comparison = 'Same performance'

                print(f'  {time_range["name"]:<20} OLD: {old_time:.3f}s  NEW: {new_time:.3f}s  → {comparison}')

    # Identify bottlenecks
    print(f'\n\n{"=" * 70}')
    print('BOTTLENECK ANALYSIS')
    print(f'{"=" * 70}')

    for time_range in time_ranges:
        range_results = [r for r in all_results if r.get('time_range') == time_range['name'] and r.get('success')]
        if range_results:
            slowest = max(range_results, key=lambda r: r['elapsed'])
            fastest = min(range_results, key=lambda r: r['elapsed'])

            print(f'\n{time_range["name"]}:')
            print(f'  Slowest: {slowest["collector"]} ({slowest["elapsed"]:.3f}s)')
            print(f'  Fastest: {fastest["collector"]} ({fastest["elapsed"]:.3f}s)')
            if slowest['elapsed'] > 0:
                print(f'  Speed difference: {slowest["elapsed"] / fastest["elapsed"]:.2f}x')

    # Save results to markdown file
    print(f'\n{"=" * 70}')


if __name__ == '__main__':
    main()
