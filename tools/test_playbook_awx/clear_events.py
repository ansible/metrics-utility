#!/usr/bin/env python
"""
Clear job events from the AWX database.

Deleting events is safe — AWX's own cleanup_jobs command does the same thing
(drops entire partitions). main_jobevent has no foreign key dependents.
The only effect is that job stdout output becomes unavailable in the AWX UI
for affected jobs.

By default, deletes ALL events. Use --job-id to target specific jobs,
or --before to only delete events older than a given date.

Uses the AWX container's awx-manage dbshell for execution.

Usage:
  python clear_events.py                          # delete all events
  python clear_events.py --before 2026-07-24      # delete events before date
  python clear_events.py --job-id 33              # delete events for job 33
  python clear_events.py --job-id 33 --job-id 34  # delete events for jobs 33,34
  python clear_events.py --dry-run                # show counts only
"""

import argparse
import subprocess
import sys


DEFAULT_CONTAINER = 'tools_awx_1'


def run_sql(container: str, sql: str) -> str:
    result = subprocess.run(
        ['docker', 'exec', container, 'awx-manage', 'dbshell', '--', '-c', sql],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f'SQL error: {result.stderr.strip()}', file=sys.stderr)
    return result.stdout.strip()


def get_counts(container: str) -> None:
    out = run_sql(
        container,
        """
        SELECT 'main_jobevent' AS tbl, count(*) FROM main_jobevent
        UNION ALL
        SELECT 'main_jobhostsummary', count(*) FROM main_jobhostsummary
        UNION ALL
        SELECT 'main_unifiedjob', count(*) FROM main_unifiedjob
        ORDER BY tbl;
    """,
    )
    print(out)


def get_partitions(container: str) -> str:
    return run_sql(
        container,
        """
        SELECT child.relname AS partition,
               pg_size_pretty(pg_relation_size(child.oid)) AS size
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE parent.relname = 'main_jobevent'
        ORDER BY child.relname;
    """,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description='Clear job events from AWX database.')
    parser.add_argument('--container', default=DEFAULT_CONTAINER, help=f'AWX container (default: {DEFAULT_CONTAINER})')
    parser.add_argument('--job-id', type=int, action='append', dest='job_ids', help='Delete events for specific job ID(s) only')
    parser.add_argument('--before', type=str, help='Delete events created before this date (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true', help='Show counts and partitions without deleting')
    args = parser.parse_args()

    print('Current state:')
    get_counts(args.container)
    print()
    print('Partitions:')
    print(get_partitions(args.container))
    print()

    if args.dry_run:
        return 0

    if args.job_ids:
        ids_str = ','.join(str(j) for j in args.job_ids)
        print(f'Deleting events for job_id IN ({ids_str})...')
        out = run_sql(args.container, f'DELETE FROM main_jobevent WHERE job_id IN ({ids_str});')
        print(out if out else 'Done')

    elif args.before:
        print(f'Deleting events created before {args.before}...')
        out = run_sql(args.container, f"DELETE FROM main_jobevent WHERE created < '{args.before}'::timestamptz;")
        print(out if out else 'Done')

    else:
        print('Deleting ALL events...')
        out = run_sql(args.container, 'DELETE FROM main_jobevent;')
        print(out if out else 'Done')

    print()
    print('After:')
    get_counts(args.container)

    return 0


if __name__ == '__main__':
    sys.exit(main())
