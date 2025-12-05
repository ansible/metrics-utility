#!/usr/bin/env python3
"""Clean all performance test data from the database."""

import argparse
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
    # Activate venv by updating PATH and VIRTUAL_ENV
    os.environ['VIRTUAL_ENV'] = str(venv_path)
    os.environ['PATH'] = f'{venv_path / "bin"}:{os.environ.get("PATH", "")}'
    # Add venv site-packages to sys.path
    site_packages = list(venv_path.glob('lib/python*/site-packages'))
    if site_packages:
        sys.path.insert(0, str(site_packages[0]))

from metrics_utility import prepare  # noqa: E402


# Initialize Django and database connection
prepare()

from helpers import delete_all  # noqa: E402


def main():
    """Main function to clean all data from database."""
    parser = argparse.ArgumentParser(description='Clean all performance test data from the database')
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force deletion without confirmation prompt',
    )
    args = parser.parse_args()

    if not args.force:
        print('WARNING: This will delete ALL performance test data from the database!')
        print('This includes:')
        print('  - Job events')
        print('  - Job host summaries')
        print('  - Jobs')
        print('  - Job templates')
        print('  - Projects')
        print('  - Hosts')
        print('  - Inventories')
        print('  - Organizations')
        print('  - Execution environments')
        print()
        response = input('Are you sure you want to continue? (yes/no): ')
        if response.lower() != 'yes':
            print('Aborted.')
            return

    delete_all()
    print('\n✓ All performance test data has been cleaned!')


if __name__ == '__main__':
    main()
