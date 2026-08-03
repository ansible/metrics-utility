#!/usr/bin/env python
"""
Run the anonymized-tests pipeline against the real AWX development database.

Thin wrapper around ``tools/anonymized_tests/run.py`` that injects
METRICS_UTILITY_DB_* environment variables so the mock_awx Django settings
connect to the real AWX PostgreSQL database (the one running real Controller
data), NOT the metrics-utility local mock DB with seeded test data.

All run.py arguments (--since, --until, --no-events, --max-events,
--max-jobs) are forwarded unchanged.

Defaults match the AWX docker-compose dev setup (awx/tools/docker-compose):
  host=localhost  port=5441  name=awx  user=awx  password=<from compose>

Override via CLI flags or the corresponding environment variables
(METRICS_UTILITY_DB_HOST, etc.) — CLI flags take precedence.

Usage:
  python run_on_awx_db.py [DB options] [-- run.py options]

Examples:
  python run_on_awx_db.py
  python run_on_awx_db.py -- --since 2025-06-13 --until 2025-06-14
  python run_on_awx_db.py --db-host 10.0.0.5 --db-port 15432 -- --no-events
  python run_on_awx_db.py --db-name automationcontroller --db-user automationcontroller
"""

import os
import subprocess
import sys

from datetime import UTC, datetime, timedelta
from pathlib import Path


def main() -> int:
    # Defaults match awx/tools/docker-compose/_sources/docker-compose.yml
    # (the real AWX dev DB, not the metrics-utility mock DB on port 5432).
    db_flags = {
        '--db-host': ('METRICS_UTILITY_DB_HOST', 'localhost'),
        '--db-port': ('METRICS_UTILITY_DB_PORT', '5441'),
        '--db-name': ('METRICS_UTILITY_DB_NAME', 'awx'),
        '--db-user': ('METRICS_UTILITY_DB_USER', 'awx'),
        '--db-password': ('METRICS_UTILITY_DB_PASSWORD', 'ZIeeKvuiyiXioAlvQUWn'),
    }

    db_values: dict[str, str] = {}
    run_py_args: list[str] = []

    args = sys.argv[1:]
    i = 0
    passthrough = False
    while i < len(args):
        if args[i] == '--':
            passthrough = True
            i += 1
            continue
        if passthrough:
            run_py_args.append(args[i])
            i += 1
            continue
        matched = False
        for flag, (env_var, _) in db_flags.items():
            if args[i] == flag:
                if i + 1 >= len(args):
                    print(f'Error: {flag} requires a value', file=sys.stderr)
                    return 2
                db_values[env_var] = args[i + 1]
                matched = True
                i += 2
                break
        if not matched:
            run_py_args.append(args[i])
            i += 1

    env = os.environ.copy()
    for _flag, (env_var, default) in db_flags.items():
        if env_var in db_values:
            env[env_var] = db_values[env_var]
        elif env_var not in env:
            env[env_var] = default

    # Default to last 24 hours if --since/--until not provided by the user.
    has_since = any(a == '--since' for a in run_py_args)
    has_until = any(a == '--until' for a in run_py_args)
    if not has_since and not has_until:
        now = datetime.now(tz=UTC)
        since = (now - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        until = now.strftime('%Y-%m-%d %H:%M:%S')
        run_py_args = ['--since', since, '--until', until, *run_py_args]

    run_py = Path(__file__).resolve().parent.parent / 'anonymized_tests' / 'run.py'
    if not run_py.exists():
        print(f'Error: {run_py} not found', file=sys.stderr)
        return 1

    db_host = env.get('METRICS_UTILITY_DB_HOST', '?')
    db_port = env.get('METRICS_UTILITY_DB_PORT', '?')
    db_name = env.get('METRICS_UTILITY_DB_NAME', '?')
    db_user = env.get('METRICS_UTILITY_DB_USER', '?')
    print(f'AWX DB: postgresql://{db_user}@{db_host}:{db_port}/{db_name}')
    print(f'run.py: {run_py}')
    print(f'args  : {" ".join(run_py_args)}')
    print()

    # Run in this script's directory so run.py's ./out/ lands in test_playbook_awx/out/
    script_dir = str(Path(__file__).resolve().parent)
    return subprocess.call([sys.executable, str(run_py), *run_py_args], env=env, cwd=script_dir)


if __name__ == '__main__':
    sys.exit(main())
