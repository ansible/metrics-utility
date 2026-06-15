#!/usr/bin/env python
# ruff: noqa: E402, T201
"""
Dump HourlyMetricsCollection records to JSON files.

This script exports all HourlyMetricsCollection records to individual JSON files,
split into data and metadata files:
- {name}.data.json - Contains the raw_data field
- {name}.metadata.json - Contains all other fields (excluding raw_data)

Files are named after the collector type and timestamps.

Usage:
    uv run scripts/dump_hourly.py
    uv run scripts/dump_hourly.py --output-dir ./dumps
    uv run scripts/dump_hourly.py --status collected
"""

import argparse
import json
import os
import sys

from datetime import datetime
from pathlib import Path

# Initialize Django
import django


# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent.parent / 'metrics-service'
sys.path.insert(0, str(project_root))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metrics_service.settings')
django.setup()

# Import after Django setup
from apps.tasks.models import HourlyMetricsCollection


def sanitize_filename(name: str) -> str:
    """
    Sanitize a string for use in filenames.

    Args:
        name: String to sanitize

    Returns:
        str: Sanitized filename-safe string
    """
    return name.replace('/', '_').replace(':', '-').replace(' ', '_')


def generate_filename(collection: HourlyMetricsCollection) -> str:
    """
    Generate filename for a collection record.

    For hourly collections: {collector_type}_{since}_{until}.json
    For daily/snapshot collections: {collector_type}_{date}.json

    Args:
        collection: HourlyMetricsCollection instance

    Returns:
        str: Generated filename
    """
    collector_type = collection.collector_type
    params = collection.collection_parameters or {}

    # Check if this is an hourly collection (has since/until)
    if 'since' in params and 'until' in params:
        # Hourly collection
        since = params['since']
        until = params['until']

        # Parse ISO timestamps and format for filename
        try:
            since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
            until_dt = datetime.fromisoformat(until.replace('Z', '+00:00'))
            since_str = since_dt.strftime('%Y%m%d_%H%M%S')
            until_str = until_dt.strftime('%Y%m%d_%H%M%S')
            filename = f'{collector_type}_{since_str}_{until_str}.json'
        except (ValueError, AttributeError):
            # Fallback if parsing fails
            since_str = sanitize_filename(str(since))
            until_str = sanitize_filename(str(until))
            filename = f'{collector_type}_{since_str}_{until_str}.json'

    # Check if this is a daily/snapshot collection (has collection_time)
    elif 'collection_time' in params:
        collection_time = params['collection_time']

        # Parse date and format for filename
        try:
            date_dt = datetime.fromisoformat(str(collection_time))
            date_str = date_dt.strftime('%Y%m%d')
            filename = f'{collector_type}_{date_str}.json'
        except (ValueError, AttributeError):
            # Fallback if parsing fails
            date_str = sanitize_filename(str(collection_time))
            filename = f'{collector_type}_{date_str}.json'

    # Fallback: use collection_timestamp
    else:
        timestamp_str = collection.collection_timestamp.strftime('%Y%m%d_%H%M%S')
        filename = f'{collector_type}_{timestamp_str}.json'

    return filename


def dump_collections(output_dir: Path, status_filter: str | None = None) -> None:
    """
    Dump all HourlyMetricsCollection records to JSON files.

    Creates two files per record:
    - {filename}.data.json - Contains raw_data field
    - {filename}.metadata.json - Contains all other fields (excluding raw_data)

    Args:
        output_dir: Directory to write JSON files to
        status_filter: Optional status to filter by (e.g., 'collected', 'failed')
    """
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Query collections
    queryset = HourlyMetricsCollection.objects.all()
    if status_filter:
        queryset = queryset.filter(status=status_filter)

    # Order by collector_type and collection_timestamp for consistent output
    queryset = queryset.order_by('collector_type', 'collection_timestamp')

    total_count = queryset.count()
    print(f'Found {total_count} HourlyMetricsCollection records to dump')

    if total_count == 0:
        print('No records found. Exiting.')
        return

    # Process each collection
    dumped_count = 0
    for collection in queryset:
        try:
            filename = generate_filename(collection)
            base_name = filename.replace('.json', '')

            # Write raw_data to .data.json file
            data_filepath = output_dir / f'{base_name}.data.json'
            with open(data_filepath, 'w') as f:
                json.dump(collection.raw_data, f, indent=2, default=str)

            # Write metadata to .metadata.json file (all fields except raw_data)
            metadata = {
                'id': collection.id,
                'collector_type': collection.collector_type,
                'collection_timestamp': str(collection.collection_timestamp),
                'status': collection.status,
                'collection_parameters': collection.collection_parameters,
                'data_size_bytes': collection.data_size_bytes,
                'error_message': collection.error_message,
                'task_execution_id': collection.task_execution_id,
                'created': str(collection.created),
                'modified': str(collection.modified),
            }
            metadata_filepath = output_dir / f'{base_name}.metadata.json'
            with open(metadata_filepath, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)

            dumped_count += 1
            print(f'[{dumped_count}/{total_count}] Dumped: {base_name}.{{data,metadata}}.json')

        except Exception as e:
            print(f'Error dumping collection {collection.id}: {e}', file=sys.stderr)
            continue

    print(f'\nSuccessfully dumped {dumped_count}/{total_count} collections to {output_dir}')


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Dump HourlyMetricsCollection raw_data to JSON files')
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('./hourly_dumps'),
        help='Output directory for JSON files (default: ./hourly_dumps)',
    )
    parser.add_argument(
        '--status',
        type=str,
        choices=['pending', 'collected', 'failed', 'processed'],
        help='Filter by status (default: all)',
    )

    args = parser.parse_args()

    dump_collections(output_dir=args.output_dir, status_filter=args.status)


if __name__ == '__main__':
    main()
