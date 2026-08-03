#!/usr/bin/env python
"""
Collect job events from the real AWX database using main_jobevent_service
and save the raw DataFrame to out/collected_events.csv.

Defaults to the last 24 hours and the AWX docker-compose dev DB (port 5441).
The DB password must be supplied via --db-password or METRICS_UTILITY_DB_PASSWORD
environment variable.

Usage:
  python collect_events.py --db-password awxpass
  python collect_events.py --since "2025-07-23" --until "2025-07-24"
  python collect_events.py --db-port 5432 --db-password awx
"""

import argparse
import os
import sys
import time

from datetime import UTC, datetime, timedelta
from pathlib import Path


# Bootstrap metrics-utility
current_dir = Path(__file__).resolve().parent
metrics_utility_path = current_dir.parent.parent
sys.path.insert(0, str(metrics_utility_path))

# AWX DB defaults (real AWX dev DB, not the mock DB)
_DB_DEFAULTS = {
    'METRICS_UTILITY_DB_HOST': 'localhost',
    'METRICS_UTILITY_DB_PORT': '5441',
    'METRICS_UTILITY_DB_NAME': 'awx',
    'METRICS_UTILITY_DB_USER': 'awx',
    'METRICS_UTILITY_DB_PASSWORD': None,
}


def parse_datetime(dt_str: str) -> datetime:
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(dt_str, fmt).replace(tzinfo=UTC)
        except ValueError:
            pass
    raise ValueError(f'Cannot parse datetime: {dt_str!r}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS')


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Collect job events from AWX DB and save to CSV.',
    )
    parser.add_argument('--since', type=str, help='Start datetime (UTC). Default: 24 hours ago')
    parser.add_argument('--until', type=str, help='End datetime (UTC). Default: now')
    parser.add_argument('--max-events', type=int, default=200_000, help='Max event rows (default: 200000, 0=unlimited)')
    parser.add_argument('--max-jobs', type=int, default=1_000, help='Max jobs (default: 1000, 0=unlimited)')
    parser.add_argument('--db-host', default=None)
    parser.add_argument('--db-port', default=None)
    parser.add_argument('--db-name', default=None)
    parser.add_argument('--db-user', default=None)
    parser.add_argument('--db-password', default=None)
    default_output = str(current_dir / 'out' / 'collected_events.csv')
    parser.add_argument('-o', '--output', default=default_output, help=f'Output CSV path (default: {default_output})')
    args = parser.parse_args()

    # Set DB env vars before Django bootstrap
    cli_overrides = {
        'METRICS_UTILITY_DB_HOST': args.db_host,
        'METRICS_UTILITY_DB_PORT': args.db_port,
        'METRICS_UTILITY_DB_NAME': args.db_name,
        'METRICS_UTILITY_DB_USER': args.db_user,
        'METRICS_UTILITY_DB_PASSWORD': args.db_password,
    }
    for env_var, default in _DB_DEFAULTS.items():
        if cli_overrides[env_var] is not None:
            os.environ[env_var] = cli_overrides[env_var]
        elif env_var not in os.environ:
            if default is None:
                print(f'Error: {env_var} is required. Set it via environment or --db-password.', file=sys.stderr)
                return 2
            os.environ[env_var] = default

    from metrics_utility import prepare

    prepare()

    from django.db import connection

    from metrics_utility.library.collectors.controller import main_jobevent_service

    now = datetime.now(tz=UTC)
    since = parse_datetime(args.since) if args.since else now - timedelta(hours=24)
    until = parse_datetime(args.until) if args.until else now

    db_host = os.environ.get('METRICS_UTILITY_DB_HOST', '?')
    db_port = os.environ.get('METRICS_UTILITY_DB_PORT', '?')
    db_name = os.environ.get('METRICS_UTILITY_DB_NAME', '?')
    print(f'AWX DB : postgresql://{db_host}:{db_port}/{db_name}')
    print(f'Since  : {since}')
    print(f'Until  : {until}')
    print(f'Output : {args.output}')
    print()

    row_limit = args.max_events if args.max_events != 0 else None
    job_limit = args.max_jobs if args.max_jobs != 0 else None

    print('Collecting events...')
    t0 = time.time()
    collector = main_jobevent_service(
        db=connection,
        since=since,
        until=until,
        row_limit=row_limit,
        job_limit=job_limit,
    )
    df = collector.gather()
    elapsed = time.time() - t0

    if df is None or df.empty:
        print(f'No events found ({elapsed:.2f}s)')
        return 0

    print(f'Collected {len(df):,} events in {elapsed:.2f}s')
    print(f'Columns: {", ".join(df.columns)}')
    print()

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    df.to_csv(args.output, index=False)
    print(f'Saved to {args.output}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
