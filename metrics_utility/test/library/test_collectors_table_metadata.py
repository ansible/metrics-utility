import pandas as pd

from django.db import connection

from metrics_utility.library.collectors.controller.table_metadata import table_metadata


def test_table_metadata_gather():
    """Test table_metadata collector - prints statistics for validation during development."""
    # Run the collector directly
    collector_instance = table_metadata(db=connection)
    df = collector_instance.gather()

    # Print gathered data
    print('\n' + '=' * 80)
    print('Table Metadata Gathered:')
    print('=' * 80)
    print(df.to_string())
    print('\n' + '-' * 80)
    print('Summary:')
    print('-' * 80)

    # Helper function to format bytes
    def format_bytes(bytes_val):
        if bytes_val is None or pd.isna(bytes_val):
            return 'N/A'
        bytes_val = float(bytes_val)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f'{bytes_val:.2f} {unit}'
            bytes_val /= 1024.0
        return f'{bytes_val:.2f} PB'

    # Print formatted summary
    for _, row in df.iterrows():
        table_name = row['tablename']
        estimated_rows = row['estimated_row_count']
        total_size = row.get('total_size_bytes')
        table_size = row.get('table_size_bytes')
        indexes_size = row.get('indexes_size_bytes')

        print(f'\nTable: {table_name}')
        if pd.isna(estimated_rows):
            print('  Estimated Rows:  N/A')
        elif estimated_rows < 0:
            print(f'  Estimated Rows:  {int(estimated_rows)} (no statistics collected)')
        else:
            print(f'  Estimated Rows:  {int(estimated_rows):,}')

        if total_size is not None and not pd.isna(total_size):
            print(f'  Total Size:      {format_bytes(total_size)} ({int(total_size):,} bytes)')
        if table_size is not None and not pd.isna(table_size):
            print(f'  Table Size:       {format_bytes(table_size)} ({int(table_size):,} bytes)')
        if indexes_size is not None and not pd.isna(indexes_size):
            print(f'  Indexes Size:    {format_bytes(indexes_size)} ({int(indexes_size):,} bytes)')

    print('=' * 80 + '\n')
