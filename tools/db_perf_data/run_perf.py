#!/usr/bin/env python3
"""Run anonymized rollup computation on performance test data.

This script computes anonymized rollups from existing data in the database.
Use fill_perf_db_data.py to generate test data first if needed.
Use clean_all_data.py to clean the database before generating new data.
"""

import argparse
import json
import os
import sys

from datetime import datetime
from pathlib import Path


try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


# Add current directory to path for imports
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))

# Add metrics_utility to path and activate venv if available
metrics_utility_path = current_dir.parent.parent
sys.path.insert(0, str(metrics_utility_path))

# Check for virtual environment and use it
venv_path = metrics_utility_path / '.venv'
if venv_path.exists():
    # Activate venv by updating PATH and VIRTUAL_ENV
    os.environ['VIRTUAL_ENV'] = str(venv_path)
    os.environ['PATH'] = f'{venv_path / "bin"}:{os.environ.get("PATH", "")}'
    # Add venv site-packages to sys.path

    site_packages = list(venv_path.glob('lib/python*/site-packages'))
    if site_packages:
        sys.path.insert(0, str(site_packages[0]))

from metrics_utility import prepare  # noqa: E402
from metrics_utility.anonymized_rollups.compute_anonymized_rollup import compute_anonymized_rollup  # noqa: E402


# Initialize Django and database connection
prepare()
from django.db import connection  # noqa: E402


def print_counts():
    """Print the count of hosts, jobs, job host summaries, and job events in the database."""
    print('\n=== Database counts ===')
    cursor = connection.cursor()

    cursor.execute('SELECT COUNT(*) FROM main_host;')
    host_count = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM main_job;')
    job_count = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM main_jobhostsummary;')
    jhs_count = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM main_jobevent;')
    event_count = cursor.fetchone()[0]

    print(f'Total hosts: {host_count}')
    print(f'Total jobs: {job_count}')
    print(f'Total job host summaries: {jhs_count}')
    print(f'Total job events: {event_count}')
    print()


def main():
    """Main function to run performance tests on existing data."""
    parser = argparse.ArgumentParser(description='Run anonymized rollup computation performance tests on existing database data')
    parser.add_argument(
        '--since',
        type=str,
        default='2024-01-01',
        help='Start date for rollup computation (YYYY-MM-DD, default: 2024-01-01)',
    )
    parser.add_argument(
        '--until',
        type=str,
        default='2024-02-01',
        help='End date for rollup computation (YYYY-MM-DD, default: 2024-02-01)',
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory for results (default: ./out)',
    )
    args = parser.parse_args()

    # Output in same directory as script and create /out subdir
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent / 'out'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse dates
    since = datetime.strptime(args.since, '%Y-%m-%d')
    until = datetime.strptime(args.until, '%Y-%m-%d')

    print('=' * 60)
    print('Running anonymized rollup performance test')
    print('=' * 60)
    print(f'Date range: {since} to {until}')
    print(f'Output directory: {output_dir}')

    print_counts()

    time_start = datetime.now()

    # Get memory usage before computation
    if PSUTIL_AVAILABLE:
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)  # Convert to MB

    # Configuration
    save_rollups = True
    save_rollups_packed = False  # False = CSV files only, True = tarball

    try:
        json_data = compute_anonymized_rollup(
            db=connection,
            salt='',
            since=since,
            until=until,
            ship_path=str(output_dir),
            save_rollups=save_rollups,
            save_rollups_packed=save_rollups_packed,
        )

        # Get memory usage after computation
        if PSUTIL_AVAILABLE:
            memory_after = process.memory_info().rss / (1024 * 1024)  # Convert to MB
            memory_consumed = memory_after - memory_before

        # save into anonymized.json
        with open(output_dir / 'anonymized.json', 'w') as f:
            json.dump(json_data, f, indent=4)

        print('\n✓ Rollup computation completed!')
        print('\nOutput files:')
        print(f'  - JSON: {output_dir}/anonymized.json')
        print(f'  - CSVs: {output_dir}/rollups/')

        # Print memory statistics
        if PSUTIL_AVAILABLE:
            print('\n=== Memory Usage ===')
            print(f'Memory before: {memory_before:.2f} MB')
            print(f'Memory after: {memory_after:.2f} MB')
            print(f'Memory consumed: {memory_consumed:.2f} MB')
        else:
            print('\n(Install psutil for memory tracking: pip install psutil)')
    except Exception as e:
        print(f'✗ Failed: {e}')
        import traceback

        traceback.print_exc()
        sys.exit(1)

    time_end = datetime.now()
    total_minutes = (time_end - time_start).total_seconds() / 60
    print(f'Total minutes = {total_minutes:.2f}')


if __name__ == '__main__':
    main()
