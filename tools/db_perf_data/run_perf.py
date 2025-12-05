#!/usr/bin/env python3
"""Run anonymized rollup computation on performance test data."""

import argparse
import json
import os
import sys

from datetime import datetime
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
from fill_perf_db_data import fill_perf_db_data  # noqa: E402


def main():
    """Main function to generate data and run rollup computation."""
    parser = argparse.ArgumentParser(description='Generate performance test data and compute anonymized rollups')
    parser.add_argument('--reuse-data', action='store_true', help='Skip data generation and reuse existing data')
    parser.add_argument('--host-count', type=int, default=100, help='Number of hosts to create (default: 100)')
    parser.add_argument('--job-count', type=int, default=10, help='Number of jobs to create (default: 10)')
    parser.add_argument('--task-count', type=int, default=50, help='Number of tasks per job (default: 50)')
    parser.add_argument('--template-count', type=int, default=10, help='Number of job templates to create (default: 10)')
    args = parser.parse_args()

    # Output in same directory as script and create /out subdir
    output_dir = Path(__file__).parent / 'out'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Performance test data dates (January 2024)
    since = datetime(2024, 1, 1, 0, 0, 0)
    until = datetime(2024, 2, 1, 0, 0, 0)

    # Generate performance test data if not reusing
    if not args.reuse_data:
        print('=' * 60)
        print('STEP 1: Generating performance test data')
        print('=' * 60)
        fill_perf_db_data(
            host_count=args.host_count,
            job_count=args.job_count,
            task_count=args.task_count,
            template_count=args.template_count,
        )
        print('\n✓ Data generation completed!\n')
    else:
        print('Reusing existing data (--reuse-data flag set)\n')

    print('=' * 60)
    print('STEP 2: Computing anonymized rollups')
    print('=' * 60)
    print(f'Date range: {since} to {until}')
    print(f'Output directory: {output_dir}')

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

        # save into anonymized.json
        with open(output_dir / 'anonymized.json', 'w') as f:
            json.dump(json_data, f, indent=4)

        print('\n✓ Rollup computation completed!')
        print('\nOutput files:')
        print(f'  - JSON: {output_dir}/anonymized.json')
        print(f'  - CSVs: {output_dir}/rollups/')
    except Exception as e:
        print(f'✗ Failed: {e}')
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
