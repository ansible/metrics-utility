#!/usr/bin/env python3
"""Run anonymized rollup computation on performance test data."""

import json
import os
import sys

from datetime import datetime
from pathlib import Path


# Add metrics_utility to path and activate venv if available
metrics_utility_path = Path(__file__).resolve().parent.parent.parent
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


# Output in same directory as script and create /out subdir
output_dir = Path(__file__).parent / 'out'
output_dir.mkdir(parents=True, exist_ok=True)

# Performance test data dates (January 2024)
since = datetime(2024, 1, 1, 0, 0, 0)
until = datetime(2024, 2, 1, 0, 0, 0)

print('Running anonymized rollup computation...')
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

    print(f'\nRollup CSV files saved to: {output_dir}/rollups/')
except Exception as e:
    print(f'✗ Failed: {e}')
    import traceback

    traceback.print_exc()
    sys.exit(1)
