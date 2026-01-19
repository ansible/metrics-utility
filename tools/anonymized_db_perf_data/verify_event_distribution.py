#!/usr/bin/env python3
"""Verify that events are evenly distributed across days in January 2024.

This script checks that the generated test data has roughly ~10M events per day
by joining events with jobs and filtering by job.finished date (as Milan specified).
"""

import os
import sys

from pathlib import Path


# Add current directory to path for imports
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))

# Add metrics_utility to path and activate venv if available
metrics_utility_path = current_dir.parent.parent
sys.path.insert(0, str(metrics_utility_path))

# Check for virtual environment and use it
venv_path = metrics_utility_path / '.venv'
if venv_path.exists():
    os.environ['VIRTUAL_ENV'] = str(venv_path)
    os.environ['PATH'] = f'{venv_path / "bin"}:{os.environ.get("PATH", "")}'
    site_packages = list(venv_path.glob('lib/python*/site-packages'))
    if site_packages:
        sys.path.insert(0, str(site_packages[0]))

from metrics_utility import prepare  # noqa: E402


prepare()

from django.db import connection  # noqa: E402


def verify_distribution():
    """Check event distribution across January 2024."""
    cursor = connection.cursor()

    # Total events
    cursor.execute('SELECT COUNT(*) FROM main_jobevent;')
    total_events = cursor.fetchone()[0]
    print(f'Total events in database: {total_events:,}')

    # Events per day (joined with jobs, filtered by job finished date)
    print('\n=== Events per day (by job.finished date) ===')
    cursor.execute("""
        SELECT
            DATE(uj.finished) as day,
            COUNT(e.id) as event_count
        FROM main_jobevent e
        JOIN main_job j ON e.job_id = j.unifiedjob_ptr_id
        JOIN main_unifiedjob uj ON j.unifiedjob_ptr_id = uj.id
        WHERE uj.finished >= '2024-01-01' AND uj.finished < '2024-02-01'
        GROUP BY DATE(uj.finished)
        ORDER BY day;
    """)

    results = cursor.fetchall()
    if not results:
        print('No events found!')
        return

    print(f'\n{"Date":<12} {"Events":>12} {"% of Total":>12}')
    print('-' * 40)

    total_counted = 0
    for day, count in results:
        percentage = (count / total_events) * 100
        print(f'{day}  {count:>12,}  {percentage:>11.1f}%')
        total_counted += count

    avg_per_day = total_counted / len(results)
    print('-' * 40)
    print(f'{"Average/day:":<12} {avg_per_day:>12,.0f}')
    print(f'{"Total counted:":<12} {total_counted:>12,}')
    print(f'{"Expected ~10M/day:":<12} {"✓" if 8_000_000 <= avg_per_day <= 12_000_000 else "✗"}')


if __name__ == '__main__':
    verify_distribution()
