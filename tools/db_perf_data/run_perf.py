#!/usr/bin/env python3
"""Run anonymized rollup computation on performance test data."""

import sys

from datetime import datetime
from pathlib import Path


# Add metrics_utility to path
metrics_utility_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(metrics_utility_path))

from metrics_utility import prepare  # noqa: E402
from metrics_utility.anonymized_rollups.compute_anonymized_rollup import compute_anonymized_rollup  # noqa: E402


# Initialize Django and database connection
prepare()
from django.db import connection  # noqa: E402


# Output in same directory as script
output_dir = Path(__file__).parent

# Performance test data dates (January 2024)
since = datetime(2024, 1, 1, 0, 0, 0)
until = datetime(2024, 2, 1, 0, 0, 0)

print('Running anonymized rollup computation...')
print(f'Output: {output_dir / "anonymized.json"}')

try:
    json_data = compute_anonymized_rollup(
        db=connection,
        salt='',
        since=since,
        until=until,
        ship_path=str(output_dir),
        save_rollups=False,
    )
    print('✓ Completed successfully!')
except Exception as e:
    print(f'✗ Failed: {e}')
    import traceback

    traceback.print_exc()
    sys.exit(1)
