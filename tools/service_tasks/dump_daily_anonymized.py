#!/usr/bin/env python
# ruff: noqa: E402, T201

"""
Dump DailyMetricsSummary and AnonymizedMetricsPayload records to JSON files.

This script exports DailyMetricsSummary and AnonymizedMetricsPayload records to individual JSON files,
split into data and metadata files:

DailyMetricsSummary:
- daily_summary_{YYYYMMDD}.data.json - Contains the aggregated_metrics field
- daily_summary_{YYYYMMDD}.metadata.json - Contains all other fields (excluding aggregated_metrics)

AnonymizedMetricsPayload:
- anonymized_{YYYYMMDD}.data.json - Contains the anonymized_data field
- anonymized_{YYYYMMDD}.metadata.json - Contains all other fields (excluding anonymized_data)

Usage:
    uv run scripts/dump_daily_anonymized.py
    uv run scripts/dump_daily_anonymized.py --output-dir ./dumps
    uv run scripts/dump_daily_anonymized.py --status aggregated
    uv run scripts/dump_daily_anonymized.py --type daily
    uv run scripts/dump_daily_anonymized.py --type anonymized
"""

import argparse
import json
import os
import sys

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
from apps.tasks.models import AnonymizedMetricsPayload, DailyMetricsSummary


def generate_daily_summary_filename(summary: DailyMetricsSummary) -> str:
    """
    Generate filename for a DailyMetricsSummary record.

    Format: daily_summary_{YYYYMMDD}.json

    Args:
        summary: DailyMetricsSummary instance

    Returns:
        str: Generated filename
    """
    date_str = summary.summary_date.strftime('%Y%m%d')
    return f'daily_summary_{date_str}.json'


def generate_anonymized_payload_filename(payload: AnonymizedMetricsPayload) -> str:
    """
    Generate filename for an AnonymizedMetricsPayload record.

    Format: anonymized_{YYYYMMDD}.json

    Args:
        payload: AnonymizedMetricsPayload instance

    Returns:
        str: Generated filename
    """
    date_str = payload.summary_date.strftime('%Y%m%d')
    return f'anonymized_{date_str}.json'


def dump_daily_summaries(output_dir: Path, status_filter: str | None = None) -> None:
    """
    Dump all DailyMetricsSummary records to JSON files.

    Creates two files per record:
    - {filename}.data.json - Contains aggregated_metrics field
    - {filename}.metadata.json - Contains all other fields (excluding aggregated_metrics)

    Args:
        output_dir: Directory to write JSON files to
        status_filter: Optional status to filter by (e.g., 'aggregated', 'failed')
    """
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Query summaries
    queryset = DailyMetricsSummary.objects.all()
    if status_filter:
        queryset = queryset.filter(status=status_filter)

    # Order by summary_date for consistent output
    queryset = queryset.order_by('summary_date')

    total_count = queryset.count()
    print(f'Found {total_count} DailyMetricsSummary records to dump')

    if total_count == 0:
        print('No records found.')
        return

    # Process each summary
    dumped_count = 0
    for summary in queryset:
        try:
            filename = generate_daily_summary_filename(summary)
            base_name = filename.replace('.json', '')

            # Write aggregated_metrics to .data.json file
            data_filepath = output_dir / f'{base_name}.data.json'
            with open(data_filepath, 'w') as f:
                json.dump(summary.aggregated_metrics, f, indent=2, default=str)

            # Write metadata to .metadata.json file (all fields except aggregated_metrics)
            metadata = {
                'id': summary.id,
                'summary_date': str(summary.summary_date),
                'status': summary.status,
                'config_data': summary.config_data,
                'hourly_collection_ids': summary.hourly_collection_ids,
                'hourly_collections_count': summary.hourly_collections_count,
                'missing_hours': summary.missing_hours,
                'aggregation_completed_at': str(summary.aggregation_completed_at) if summary.aggregation_completed_at else None,
                'error_message': summary.error_message,
                'rollup_task_execution_id': summary.rollup_task_execution_id,
                'created': str(summary.created),
                'modified': str(summary.modified),
            }
            metadata_filepath = output_dir / f'{base_name}.metadata.json'
            with open(metadata_filepath, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)

            dumped_count += 1
            print(f'[{dumped_count}/{total_count}] Dumped: {base_name}.{{data,metadata}}.json')

        except Exception as e:
            print(f'Error dumping summary {summary.id}: {e}', file=sys.stderr)
            continue

    print(f'\nSuccessfully dumped {dumped_count}/{total_count} summaries to {output_dir}')


def dump_anonymized_payloads(output_dir: Path, status_filter: str | None = None) -> None:
    """
    Dump all AnonymizedMetricsPayload records to JSON files.

    Creates two files per record:
    - {filename}.data.json - Contains anonymized_data field
    - {filename}.metadata.json - Contains all other fields (excluding anonymized_data)

    Args:
        output_dir: Directory to write JSON files to
        status_filter: Optional status to filter by (e.g., 'sent', 'failed')
    """
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Query payloads
    queryset = AnonymizedMetricsPayload.objects.all()
    if status_filter:
        queryset = queryset.filter(status=status_filter)

    # Order by summary_date for consistent output
    queryset = queryset.order_by('summary_date')

    total_count = queryset.count()
    print(f'Found {total_count} AnonymizedMetricsPayload records to dump')

    if total_count == 0:
        print('No records found.')
        return

    # Process each payload
    dumped_count = 0
    for payload in queryset:
        try:
            filename = generate_anonymized_payload_filename(payload)
            base_name = filename.replace('.json', '')

            # Write anonymized_data to .data.json file
            data_filepath = output_dir / f'{base_name}.data.json'
            with open(data_filepath, 'w') as f:
                json.dump(payload.anonymized_data, f, indent=2, default=str)

            # Write metadata to .metadata.json file (all fields except anonymized_data)
            metadata = {
                'id': payload.id,
                'summary_date': str(payload.summary_date),
                'status': payload.status,
                'retry_count': payload.retry_count,
                'max_retries': payload.max_retries,
                'segment_event_name': payload.segment_event_name,
                'segment_user_id': payload.segment_user_id,
                'segment_message_id': payload.segment_message_id,
                'sent_at': str(payload.sent_at) if payload.sent_at else None,
                'daily_summary_id': payload.daily_summary_id,
                'error_message': payload.error_message,
                'created': str(payload.created),
                'modified': str(payload.modified),
            }
            metadata_filepath = output_dir / f'{base_name}.metadata.json'
            with open(metadata_filepath, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)

            dumped_count += 1
            print(f'[{dumped_count}/{total_count}] Dumped: {base_name}.{{data,metadata}}.json')

        except Exception as e:
            print(f'Error dumping payload {payload.id}: {e}', file=sys.stderr)
            continue

    print(f'\nSuccessfully dumped {dumped_count}/{total_count} payloads to {output_dir}')


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Dump DailyMetricsSummary and AnonymizedMetricsPayload data to JSON files')
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('./daily_dumps'),
        help='Output directory for JSON files (default: ./daily_dumps)',
    )
    parser.add_argument(
        '--status',
        type=str,
        help="Filter by status (e.g., 'aggregated', 'sent', 'failed')",
    )
    parser.add_argument(
        '--type',
        type=str,
        choices=['daily', 'anonymized', 'both'],
        default='both',
        help="Type of data to dump: 'daily' (DailyMetricsSummary), 'anonymized' (AnonymizedMetricsPayload), or 'both' (default: both)",
    )

    args = parser.parse_args()

    # Dump based on type selection
    if args.type in ['daily', 'both']:
        print('\n=== Dumping DailyMetricsSummary records ===')
        dump_daily_summaries(output_dir=args.output_dir, status_filter=args.status)

    if args.type in ['anonymized', 'both']:
        print('\n=== Dumping AnonymizedMetricsPayload records ===')
        dump_anonymized_payloads(output_dir=args.output_dir, status_filter=args.status)


if __name__ == '__main__':
    main()
