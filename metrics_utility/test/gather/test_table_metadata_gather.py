import pandas as pd
import pytest

from django.db import connection

from metrics_utility.library.collectors.controller.table_metadata import table_metadata


@pytest.mark.filterwarnings('ignore::ResourceWarning')
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
    
    # Print formatted summary
    for _, row in df.iterrows():
        table_name = row['tablename']
        estimated_rows = row['estimated_row_count']
        
        print(f'\nTable: {table_name}')
        if pd.isna(estimated_rows):
            print(f'  Estimated Rows:  N/A')
        elif estimated_rows < 0:
            print(f'  Estimated Rows:  {int(estimated_rows)} (no statistics collected)')
        else:
            print(f'  Estimated Rows:  {int(estimated_rows):,}')
    
    print('=' * 80 + '\n')
