#!/usr/bin/env python
"""
Script to collect data from anonymized collectors (except events) for a given time range.

Usage:
    python run_no_events.py [--since YYYY-MM-DD HH:MM:SS] [--until YYYY-MM-DD HH:MM:SS] [--batches N]

If since/until not provided, uses default from test_from_gather_to_json.py:
    since = datetime(2025, 6, 13, 0, 0, 0)
    until = datetime(2025, 6, 14, 0, 0, 0)

If batches is provided, divides since-until into N subintervals.
If batches is not provided, splits since-until by hourly collections.

Snapshot collectors (execution_environments, table_metadata, controller_version_service)
do not need since-until parameter and will run once.

Output files:

- Final report in ./out/anonymized_rollup_no_events.json
- Segment chunks in ./out/segment/
- Rollup batches in ./out/batches/
"""

import argparse
import json
import os
import shutil

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd

# Initialize Django before importing Django components
from metrics_utility import prepare


prepare()

from django.db import connection  # noqa: E402

from metrics_utility.anonymized_rollups.anonymized_rollups import (  # noqa: E402
    compute_anonymized_rollup_from_raw_data,
)
from metrics_utility.library.collectors.controller import (  # noqa: E402
    controller_version_service,
    credentials_service,
    execution_environments,
    job_host_summary_service,
    table_metadata,
    unified_jobs,
)
from metrics_utility.library.storage.segment import StorageSegment  # noqa: E402


# Collectors to run (excluding events/main_jobevent_service)
COLLECTORS = {
    'unified_jobs': {
        'func': unified_jobs,
        'needs_since_until': True,
    },
    'job_host_summary_service': {
        'func': job_host_summary_service,
        'needs_since_until': True,
    },
    'credentials_service': {
        'func': credentials_service,
        'needs_since_until': True,
    },
    'execution_environments': {
        'func': execution_environments,
        'needs_since_until': False,  # snapshot collector
    },
    'table_metadata': {
        'func': table_metadata,
        'needs_since_until': False,  # snapshot collector
    },
    'controller_version_service': {
        'func': controller_version_service,
        'needs_since_until': False,  # snapshot collector
    },
}

# Default since-until from test_from_gather_to_json.py
DEFAULT_SINCE = datetime(2025, 6, 13, 0, 0, 0)
DEFAULT_UNTIL = datetime(2025, 6, 14, 0, 0, 0)


def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string in format 'YYYY-MM-DD HH:MM:SS'."""
    try:
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        # Try just date
        return datetime.strptime(dt_str, '%Y-%m-%d')


def split_time_range(since: datetime, until: datetime, batches: int = None) -> List[Tuple[datetime, datetime]]:
    """
    Split time range into subintervals.

    If batches is provided, divides since-until into N equal subintervals.
    If batches is not provided, splits by hourly collections.

    Args:
        since: Start datetime
        until: End datetime
        batches: Number of batches (optional)

    Returns:
        List of (since, until) tuples
    """
    if batches:
        # Divide into N equal subintervals
        total_duration = until - since
        interval_duration = total_duration / batches

        intervals = []
        current_since = since
        for i in range(batches):
            current_until = since + (i + 1) * interval_duration
            if i == batches - 1:
                # Last interval should end exactly at 'until'
                current_until = until
            intervals.append((current_since, current_until))
            current_since = current_until

        return intervals
    else:
        # Split by hourly collections
        intervals = []
        current_since = since
        while current_since < until:
            current_until = min(current_since + timedelta(hours=1), until)
            intervals.append((current_since, current_until))
            current_since = current_until

        return intervals


def collect_data_for_collector(
    collector_name: str,
    collector_info: Dict,
    db,
    time_intervals: List[Tuple[datetime, datetime]] = None,
) -> List[pd.DataFrame]:
    """
    Collect data for a single collector.

    Args:
        collector_name: Name of the collector
        collector_info: Dict with 'func' and 'needs_since_until' keys
        db: Database connection
        time_intervals: List of (since, until) tuples (only for time-series collectors)

    Returns:
        List of dataframes (one per batch/interval, or one for snapshot collectors)
    """
    collector_func = collector_info['func']
    needs_since_until = collector_info['needs_since_until']

    dataframes = []

    if needs_since_until:
        # Time-series collector: run for each interval
        if not time_intervals:
            raise ValueError(f'Time intervals required for collector {collector_name}')

        for since, until in time_intervals:
            try:
                df = collector_func(db=db, since=since, until=until).gather()
                if df is not None:
                    dataframes.append(df)
                else:
                    # Return empty dataframe if None
                    dataframes.append(pd.DataFrame())
            except Exception as e:
                print(f'  Error collecting {collector_name} for interval {since} to {until}: {e}')
                dataframes.append(pd.DataFrame())
    else:
        # Snapshot collector: run once
        try:
            df = collector_func(db=db).gather()
            if df is not None:
                dataframes.append(df)
            else:
                dataframes.append(pd.DataFrame())
        except Exception as e:
            print(f'  Error collecting {collector_name}: {e}')
            dataframes.append(pd.DataFrame())

    return dataframes


def count_rows(df: pd.DataFrame) -> int:
    """Count rows in a dataframe."""
    if df is None or df.empty:
        return 0
    return len(df)


def main():
    parser = argparse.ArgumentParser(
        description='Collect data from anonymized collectors (except events)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default time range
  python run_no_events.py

  # Custom time range
  python run_no_events.py --since "2025-06-13 00:00:00" --until "2025-06-14 00:00:00"

  # Split into 4 batches
  python run_no_events.py --since "2025-06-13 00:00:00" --until "2025-06-14 00:00:00" --batches 4

  # Hourly split (default when batches not provided)
  python run_no_events.py --since "2025-06-13 00:00:00" --until "2025-06-14 00:00:00"
        """,
    )
    parser.add_argument(
        '--since',
        type=str,
        help='Start datetime (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)',
    )
    parser.add_argument(
        '--until',
        type=str,
        help='End datetime (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)',
    )
    parser.add_argument(
        '--batches',
        type=int,
        help='Number of batches to divide the time range into',
    )

    args = parser.parse_args()

    # Clean up ./out directory at the start (recursively remove all files and folders)
    out_dir = './out'
    if os.path.exists(out_dir):
        print(f'Cleaning up {out_dir} directory (removing all files and folders recursively)...')
        shutil.rmtree(out_dir, ignore_errors=True)
        print(f'✓ Removed {out_dir} and all its contents')
    print()

    # Parse since/until or use defaults
    if args.since:
        since = parse_datetime(args.since)
    else:
        since = DEFAULT_SINCE

    if args.until:
        until = parse_datetime(args.until)
    else:
        until = DEFAULT_UNTIL

    if since >= until:
        raise ValueError(f'since ({since}) must be before until ({until})')

    # Split time range
    time_intervals = split_time_range(since, until, args.batches)

    print(f'Collecting data from {since} to {until}')
    if args.batches:
        print(f'Split into {args.batches} batches')
    else:
        print(f'Split into {len(time_intervals)} hourly intervals')
    print()

    # Get database connection
    db = connection

    # Collect data for each collector
    results: Dict[str, List[pd.DataFrame]] = {}

    for collector_name, collector_info in COLLECTORS.items():
        print(f'Collecting {collector_name}...')

        if collector_info['needs_since_until']:
            # Time-series collector
            dataframes = collect_data_for_collector(
                collector_name,
                collector_info,
                db,
                time_intervals,
            )
        else:
            # Snapshot collector
            dataframes = collect_data_for_collector(
                collector_name,
                collector_info,
                db,
            )

        results[collector_name] = dataframes
        print(f'  Collected {len(dataframes)} dataframe(s)')

    print()
    print('=' * 70)
    print('COLLECTION RESULTS')
    print('=' * 70)
    print()

    # Print row counts for each collector and batch
    for collector_name, dataframes in results.items():
        print(f'{collector_name}:')

        if COLLECTORS[collector_name]['needs_since_until']:
            # Time-series collector: show per batch
            total_rows = 0
            for i, df in enumerate(dataframes):
                rows = count_rows(df)
                total_rows += rows
                if args.batches:
                    print(f'  Batch {i + 1}/{len(dataframes)}: {rows:,} rows')
                else:
                    # Show time interval for hourly splits
                    since_interval, until_interval = time_intervals[i]
                    print(f'  Hour {i + 1} ({since_interval.strftime("%Y-%m-%d %H:%M")} - {until_interval.strftime("%H:%M")}): {rows:,} rows')
            print(f'  Total: {total_rows:,} rows')
        else:
            # Snapshot collector: show single result
            rows = count_rows(dataframes[0]) if dataframes else 0
            print(f'  Rows: {rows:,}')

        print()

    # Compute anonymized rollups from collected dataframes
    print()
    print('=' * 70)
    print('COMPUTING ANONYMIZED ROLLUPS')
    print('=' * 70)
    print()

    # Map collector names to input_data keys expected by compute_anonymized_rollup_from_raw_data
    collector_to_input_key = {
        'unified_jobs': 'unified_jobs',
        'job_host_summary_service': 'job_host_summary',
        'credentials_service': 'credentials',
        'execution_environments': 'execution_environments',
        'table_metadata': 'table_metadata',
        'controller_version_service': 'controller_version',
    }

    # Prepare input_data dict
    input_data = {
        'main_jobevent': [],  # Empty since we're not collecting events
    }

    # Add collected dataframes to input_data
    for collector_name, dataframes in results.items():
        input_key = collector_to_input_key.get(collector_name)
        if input_key:
            # Filter out None dataframes, but keep empty dataframes (they're valid)
            valid_dataframes = [df for df in dataframes if df is not None]
            if valid_dataframes:
                input_data[input_key] = valid_dataframes
            else:
                # If no valid dataframes, pass a single empty dataframe
                input_data[input_key] = [pd.DataFrame()]

    # Compute anonymized rollup
    salt = 'salt'  # Default salt, could be made configurable
    print('Computing anonymized rollup from collected data...')
    try:
        json_data = compute_anonymized_rollup_from_raw_data(input_data, salt)
        print('✓ Anonymized rollup computed successfully')
    except Exception as e:
        print(f'✗ Error computing anonymized rollup: {e}')
        import traceback

        traceback.print_exc()
        return results

    # Save final JSON
    json_path = './out/anonymized_rollup_no_events.json'

    # Create the directory
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        json.dump(json_data, f, indent=4)

    print(f'✓ Final JSON saved to: {json_path}')
    print()

    # Split data into Segment chunks
    print('=' * 70)
    print('SPLITTING DATA INTO SEGMENT CHUNKS')
    print('=' * 70)
    print()

    storage_segment = StorageSegment()
    chunks = storage_segment._split_into_chunks(json_data, storage_segment.REGULAR_MESSAGE_LIMIT)

    print(f'Total chunks created: {len(chunks)}')
    print(f'Message size limit: {storage_segment.REGULAR_MESSAGE_LIMIT} bytes ({storage_segment.REGULAR_MESSAGE_LIMIT / 1024:.1f} KB)')
    print()

    segment_dir = './out/segment'
    os.makedirs(segment_dir, exist_ok=True)

    for i, chunk in enumerate(chunks, 1):
        chunk_size = storage_segment._calculate_size(chunk)
        chunk_json = json.dumps(chunk, indent=4)
        chunk_path = f'{segment_dir}/chunk_{i:03d}_of_{len(chunks):03d}.json'
        chunk_key = list(chunk.keys())[0] if chunk else 'unknown'

        with open(chunk_path, 'w') as f:
            f.write(chunk_json)

        print(f'Chunk {i}/{len(chunks)}: {chunk_key} - {chunk_size} bytes ({chunk_size / 1024:.1f} KB) - saved to {chunk_path}')

        if isinstance(chunk[chunk_key], list):
            print(f'  └─ Contains {len(chunk[chunk_key])} items in {chunk_key}')

    print()
    print('=' * 70)
    print()

    return results


if __name__ == '__main__':
    main()
