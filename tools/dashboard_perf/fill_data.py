#!/usr/bin/env python
"""
Fill AWX database with performance test data for the dashboard collection benchmark.

Runs fill_perf_db_data.py once for every day in the given period.
Cleans existing AWX data before filling.

Scale presets (--scale 1-4):
  1 —  100 jobs/day,  5 hosts, 50 tasks
  2 —  500 jobs/day,  5 hosts, 50 tasks
  3 — 1100 jobs/day,  5 hosts, 50 tasks
  4 —  500 jobs/day, 50 hosts, 50 tasks

Scale reference table (90-day period, 2024-01-01 → 2024-03-31):
  Scale  Jobs/day  Hosts  main_unifiedjob  main_jobhostsummary  main_unifiedjob_credentials
  -----  --------  -----  ---------------  -------------------  ---------------------------
  1           100      5            9,100               45,500                       36,400
  2           500      5           45,500              227,500                      182,000
  3          1100      5          100,100              500,500                      400,400
  4           500     50           45,500            2,275,000                      182,000

See FILL_DATA.md for full usage instructions and examples.
"""

# ruff: noqa: T201, S603, S607
import argparse
import os
import subprocess
import sys
import time

from datetime import date, timedelta
from pathlib import Path


_SCALES: dict[int, dict[str, int]] = {
    1: {'job_count': 100, 'host_count': 5, 'task_count': 50},
    2: {'job_count': 500, 'host_count': 5, 'task_count': 50},
    3: {'job_count': 1100, 'host_count': 5, 'task_count': 50},
    4: {'job_count': 500, 'host_count': 50, 'task_count': 50},
}


def _resolve_metrics_utility_path() -> Path:
    """Return the metrics-utility checkout directory."""
    env_path = os.environ.get('METRICS_UTILITY_PATH')
    if env_path:
        return Path(env_path).resolve()
    return Path(__file__).parent.parent.parent.resolve()


def _find_python(mu_path: Path) -> Path:
    """Return the Python interpreter inside metrics-utility venv, or fall back."""
    venv_python = mu_path / '.venv' / 'bin' / 'python'
    return venv_python if venv_python.exists() else Path(sys.executable)


def _run(cmd: list[str], cwd: Path, env: dict) -> None:
    """Run a subprocess command and exit on failure."""
    print(f'$ {" ".join(str(c) for c in cmd)}')
    start = time.time()
    result = subprocess.run(cmd, cwd=cwd, env=env)
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f'  ERROR: exited with code {result.returncode}')
        sys.exit(result.returncode)
    print(f'  → done in {elapsed:.1f}s\n')


def main() -> None:  # noqa: PLR0915
    """Fill the AWX DB for each day in the given period."""
    parser = argparse.ArgumentParser(
        description='Fill AWX DB for a date range.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--period-start', default='2024-01-01', metavar='YYYY-MM-DD', help='First day to fill (default: 2024-01-01)')
    parser.add_argument('--period-end', default='2024-03-31', metavar='YYYY-MM-DD', help='Last day to fill (default: 2024-03-31)')
    parser.add_argument(
        '--scale',
        type=int,
        choices=[1, 2, 3, 4],
        default=None,
        help='Scale preset 1-4 (overrides --job-count/--host-count/--task-count)',
    )
    parser.add_argument('--job-count', type=int, default=500, help='Jobs per day — ignored when --scale is set (default: 500)')
    parser.add_argument('--host-count', type=int, default=5, help='Hosts — ignored when --scale is set (default: 5)')
    parser.add_argument('--task-count', type=int, default=50, help='Tasks per job — ignored when --scale is set (default: 50)')
    args = parser.parse_args()

    period_start = date.fromisoformat(args.period_start)
    period_end = date.fromisoformat(args.period_end)

    if period_start > period_end:
        print(f'ERROR: --period-start ({period_start}) must be <= --period-end ({period_end})')
        sys.exit(1)

    if args.scale is not None:
        preset = _SCALES[args.scale]
        job_count = preset['job_count']
        host_count = preset['host_count']
        task_count = preset['task_count']
        scale_label = f'scale {args.scale}'
    else:
        job_count = args.job_count
        host_count = args.host_count
        task_count = args.task_count
        scale_label = 'custom'

    mu_path = _resolve_metrics_utility_path()
    scripts_dir = mu_path / 'tools' / 'anonymized_db_perf_data'
    python = _find_python(mu_path)
    env = os.environ.copy()

    db_host = env.get('METRICS_UTILITY_DB_HOST', 'localhost')
    db_name = env.get('METRICS_UTILITY_DB_NAME', '')
    db_user = env.get('METRICS_UTILITY_DB_USER', '')
    db_port = env.get('METRICS_UTILITY_DB_PORT', '5432')

    days = (period_end - period_start).days + 1

    print('=' * 60)
    print('  Dashboard Benchmark — Data Fill')
    print('=' * 60)
    print(f'  Scale:      {scale_label}')
    print(f'  Period:     {period_start} → {period_end}  ({days} day(s))')
    print(f'  Job-count:  {job_count} per day')
    print(f'  Host-count: {host_count}')
    print(f'  Task-count: {task_count}')
    print(f'  AWX DB:     {db_user}@{db_host}:{db_port}/{db_name}')
    print(f'  mu path:    {mu_path}')
    print()

    if not scripts_dir.exists():
        print(f'ERROR: {scripts_dir} not found — set METRICS_UTILITY_PATH')
        sys.exit(1)

    # Step 1 — clean
    print('Step 1: Cleaning existing AWX data...')
    _run([str(python), 'clean_all_data.py', '--force'], cwd=scripts_dir, env=env)

    # Step 2 — fill one run per day
    print(f'Step 2: Filling {days} day(s)...')
    current = period_start
    while current <= period_end:
        print(f'  {current.isoformat()}')
        _run(
            [
                str(python),
                'fill_perf_db_data.py',
                f'--date={current.isoformat()}',
                f'--job-count={job_count}',
                f'--host-count={host_count}',
                f'--task-count={task_count}',
                '--no-events',
            ],
            cwd=scripts_dir,
            env=env,
        )
        current += timedelta(days=1)

    print('=' * 60)
    print(f'  Done — {days} day(s) filled.')
    print('=' * 60)


if __name__ == '__main__':
    main()
