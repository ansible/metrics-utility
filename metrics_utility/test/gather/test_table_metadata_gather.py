import pandas as pd
import pytest

from django.db import connection

from metrics_utility.library.collectors.controller.table_metadata import table_metadata


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_table_metadata_gather():
    """Test table_metadata collector gathers metadata with correct field names."""
    # Run the collector directly
    collector_instance = table_metadata(db=connection)
    df = collector_instance.gather()

    # Assert that we got a DataFrame
    assert df is not None, 'table_metadata returned None'
    assert len(df) > 0, 'table_metadata returned empty DataFrame'

    # Print gathered data
    print('\n' + '=' * 80)
    print('Table Metadata Gathered:')
    print('=' * 80)
    print(df.to_string())
    print('\n' + '-' * 80)
    print('Summary:')
    print('-' * 80)
    
    # Print formatted summary with human-readable sizes
    for _, row in df.iterrows():
        table_name = row['tablename']
        total_size = row['total_size_bytes']
        table_size = row['table_size_bytes']
        indexes_size = row['indexes_size_bytes']
        estimated_rows = row['estimated_row_count']
        
        # Convert bytes to human-readable format
        def format_bytes(bytes_val):
            if bytes_val is None or pd.isna(bytes_val):
                return 'N/A'
            bytes_val = float(bytes_val)
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if bytes_val < 1024.0:
                    return f'{bytes_val:.2f} {unit}'
                bytes_val /= 1024.0
            return f'{bytes_val:.2f} PB'
        
        print(f'\nTable: {table_name}')
        if not pd.isna(total_size):
            print(f'  Total Size:      {format_bytes(total_size)} ({int(total_size):,} bytes)')
        else:
            print(f'  Total Size:      N/A')
        if not pd.isna(table_size):
            print(f'  Table Size:      {format_bytes(table_size)} ({int(table_size):,} bytes)')
        else:
            print(f'  Table Size:      N/A')
        if not pd.isna(indexes_size):
            print(f'  Indexes Size:    {format_bytes(indexes_size)} ({int(indexes_size):,} bytes)')
        else:
            print(f'  Indexes Size:    N/A')
        if pd.isna(estimated_rows):
            print(f'  Estimated Rows:  N/A')
        elif estimated_rows < 0:
            print(f'  Estimated Rows:  {int(estimated_rows)} (no statistics collected)')
        else:
            print(f'  Estimated Rows:  {int(estimated_rows):,}')
        print(f'  Last Analyze:    {row["last_analyze"]}')
        print(f'  Last Vacuum:      {row["last_vacuum"]}')
    
    print('=' * 80 + '\n')

    # Expected column names
    expected_columns = {
        'schemaname',
        'tablename',
        'total_size_bytes',
        'table_size_bytes',
        'indexes_size_bytes',
        'estimated_row_count',
        'last_analyze',
        'last_vacuum',
    }

    # Assert all expected columns are present
    actual_columns = set(df.columns)
    assert actual_columns == expected_columns, (
        f'Column mismatch:\n'
        f'Expected: {expected_columns}\n'
        f'Actual:   {actual_columns}\n'
        f'Missing:  {expected_columns - actual_columns}\n'
        f'Extra:    {actual_columns - expected_columns}'
    )

    # Assert we have rows for the expected tables
    expected_tables = {'main_jobevent', 'main_unifiedjob', 'main_jobhostsummary'}
    actual_tables = set(df['tablename'].unique())
    
    # Check that we found at least one table
    assert len(actual_tables) > 0, 'No tables found in metadata'
    
    # Verify that all found tables are from our expected set
    # (Not all tables may exist in test DB, but any that do should be from our list)
    unexpected_tables = actual_tables - expected_tables
    assert len(unexpected_tables) == 0, (
        f'Unexpected tables found: {unexpected_tables}\n'
        f'Expected only tables from: {expected_tables}'
    )
    
    # Verify we found at least one of the expected tables
    found_expected_tables = actual_tables & expected_tables
    assert len(found_expected_tables) > 0, (
        f'None of the expected tables found. Expected: {expected_tables}, Found: {actual_tables}'
    )

    # Assert schema is 'public' for all rows
    assert (df['schemaname'] == 'public').all(), 'All tables should be in public schema'

    # Assert that size columns are numeric (they should be integers or floats)
    size_columns = ['total_size_bytes', 'table_size_bytes', 'indexes_size_bytes']
    for col in size_columns:
        assert df[col].dtype in ['int64', 'float64', 'Int64'], (
            f'Column {col} should be numeric, got {df[col].dtype}'
        )

    # Assert that estimated_row_count is numeric (can be None/NaN)
    assert df['estimated_row_count'].dtype in ['int64', 'float64', 'Int64'], (
        f'Column estimated_row_count should be numeric, got {df["estimated_row_count"].dtype}'
    )
