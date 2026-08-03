#!/usr/bin/env python
"""
End-to-end pipeline: clean slate → run playbook → collect → anonymize.

Steps:
  1. clear_events.py     — wipe all events from the AWX DB
  2. run_rich_playbook.py — deploy + run rich_playbook.yml on AWX
  3. run_on_awx_db.py     — run the full metrics-service pipeline
                            (wipes + recreates out/, produces out/anonymized_rollup.json + out/segment/)
  4. collect_events.py    — dump raw events to out/collected_events.csv

Timestamps are captured around step 2 so that steps 3–4 scope to exactly
the window in which the playbook ran.

All AWX connection defaults (URL, container, DB) match the docker-compose
dev setup and can be overridden via CLI flags.

Usage:
  python run_all_on_awx_db.py
  python run_all_on_awx_db.py --hosts 10
  python run_all_on_awx_db.py --skip-clear
"""

import argparse
import subprocess
import sys
import time

from datetime import UTC, datetime, timedelta
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


def banner(step: int, title: str) -> None:
    print()
    print('=' * 70)
    print(f'  Step {step}: {title}')
    print('=' * 70)
    print()


def run_script(name: str, args: list[str]) -> int:
    script = SCRIPT_DIR / name
    cmd = [sys.executable, str(script), *args]
    print(f'$ {" ".join(cmd)}')
    print()
    return subprocess.call(cmd)


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Run the full clear → playbook → collect → anonymize pipeline.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--url', default='https://localhost:8043', help='AWX base URL')
    parser.add_argument('--user', default='admin', help='AWX username')
    parser.add_argument('--password', default='admin', help='AWX password')
    parser.add_argument('--container', default='tools_awx_1', help='AWX container name')
    parser.add_argument('--hosts', type=int, default=5, help='Number of localhost hosts for rich playbook')
    parser.add_argument('--skip-clear', action='store_true', help='Skip step 1 (clear events)')
    parser.add_argument('--skip-playbook', action='store_true', help='Skip step 2 (run rich playbook)')
    args = parser.parse_args()

    total_start = time.time()

    # ------------------------------------------------------------------
    # Step 1: Clear events
    # ------------------------------------------------------------------
    if not args.skip_clear:
        banner(1, 'Clear events')
        rc = run_script('clear_events.py', ['--container', args.container])
        if rc != 0:
            print(f'clear_events.py failed (rc={rc})', file=sys.stderr)
            return rc
    else:
        print('\nStep 1: Clear events — SKIPPED (--skip-clear)')

    # ------------------------------------------------------------------
    # Step 2: Run rich playbook
    # ------------------------------------------------------------------
    # Capture a window slightly wider than the actual run so we don't
    # miss events due to clock skew between host and AWX container.
    since = datetime.now(tz=UTC) - timedelta(seconds=5)

    if not args.skip_playbook:
        banner(2, 'Run rich playbook')
        rc = run_script(
            'run_rich_playbook.py',
            [
                '--url',
                args.url,
                '--user',
                args.user,
                '--password',
                args.password,
                '--container',
                args.container,
                '--hosts',
                str(args.hosts),
            ],
        )
        if rc != 0:
            print(f'run_rich_playbook.py failed (rc={rc})', file=sys.stderr)
            return rc
    else:
        print('\nStep 2: Run rich playbook — SKIPPED (--skip-playbook)')

    until = datetime.now(tz=UTC) + timedelta(seconds=5)

    since_str = since.strftime('%Y-%m-%d %H:%M:%S')
    until_str = until.strftime('%Y-%m-%d %H:%M:%S')
    print(f'\nTime window for collection: {since_str} → {until_str}')

    # ------------------------------------------------------------------
    # Step 3: Run metrics-service pipeline (run_on_awx_db.py)
    #         This wipes and recreates out/, so it must run before
    #         collect_events which saves into out/.
    # ------------------------------------------------------------------
    banner(3, 'Run metrics-service pipeline (anonymized report)')
    rc = run_script(
        'run_on_awx_db.py',
        [
            '--',
            '--since',
            since_str,
            '--until',
            until_str,
        ],
    )
    if rc != 0:
        print(f'run_on_awx_db.py failed (rc={rc})', file=sys.stderr)
        return rc

    # ------------------------------------------------------------------
    # Step 4: Collect events (raw CSV into out/)
    # ------------------------------------------------------------------
    banner(4, 'Collect events')
    rc = run_script(
        'collect_events.py',
        [
            '--since',
            since_str,
            '--until',
            until_str,
        ],
    )
    if rc != 0:
        print(f'collect_events.py failed (rc={rc})', file=sys.stderr)
        return rc

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    total_elapsed = time.time() - total_start
    print()
    print('=' * 70)
    print('  Pipeline complete')
    print('=' * 70)
    print()
    print(f'  Time window : {since_str} → {until_str}')
    print(f'  Total time  : {total_elapsed:.1f}s')
    print()
    print('  Outputs:')
    print(f'    Raw events   : {SCRIPT_DIR / "out" / "collected_events.csv"}')
    print(f'    Anonymized   : {SCRIPT_DIR / "out" / "anonymized_rollup.json"}')
    print(f'    Segment      : {SCRIPT_DIR / "out" / "segment" / ""}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
