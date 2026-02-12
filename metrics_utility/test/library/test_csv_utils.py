"""Tests for csv_utils module."""

import os
import tempfile

import pandas as pd

from metrics_utility.library.csv_utils import dataframe_to_csv_files


def test_dataframe_to_csv_files_empty():
    """Test that empty DataFrames create a file with headers only."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create empty DataFrame with schema
        df = pd.DataFrame(columns=['col1', 'col2', 'col3'])

        files = dataframe_to_csv_files(df, 'test_table', tmpdir)

        assert len(files) == 1
        assert os.path.exists(files[0])
        assert 'test_table_table.csv' in files[0]

        # Verify file has headers only
        with open(files[0], 'r') as f:
            content = f.read()
            lines = content.strip().split('\n')
            assert len(lines) == 1
            assert lines[0] == 'col1,col2,col3'


def test_dataframe_to_csv_files_none_input():
    """Test that None input is handled gracefully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        df = None

        # Should not crash, should create empty file
        files = dataframe_to_csv_files(df, 'test_table', tmpdir)

        assert len(files) == 1
        assert os.path.exists(files[0])


def test_dataframe_to_csv_files_small():
    """Test that small DataFrames create a single file without split suffix."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create small DataFrame
        df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'value': [10.5, 20.3, 30.7]})

        files = dataframe_to_csv_files(df, 'small_table', tmpdir)

        # Should create single file without _split suffix
        assert len(files) == 1
        assert os.path.exists(files[0])
        assert 'small_table_table.csv' in files[0]
        assert '_split' not in files[0]

        # Verify content
        result_df = pd.read_csv(files[0])
        assert len(result_df) == 3
        assert list(result_df.columns) == ['id', 'name', 'value']
        assert result_df['id'].tolist() == [1, 2, 3]


def test_dataframe_to_csv_files_large_splitting():
    """Test that large DataFrames are split into multiple files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create DataFrame large enough to trigger splitting
        # Create 1000 rows with reasonably sized data
        df = pd.DataFrame(
            {
                'id': range(1000),
                'name': [f'user_{i:05d}' for i in range(1000)],
                'description': ['This is a longer description field that takes up more space'] * 1000,
                'value': [i * 1.5 for i in range(1000)],
                'timestamp': pd.date_range('2024-01-01', periods=1000, freq='H'),
            }
        )

        # Use small max_file_size to force splitting
        files = dataframe_to_csv_files(df, 'large_table', tmpdir, max_file_size=10000)

        # Should create multiple files with _split suffix
        assert len(files) > 1

        for file in files:
            assert os.path.exists(file)
            assert '_split' in file

        # Verify all files have headers
        for file in files:
            result_df = pd.read_csv(file)
            assert list(result_df.columns) == ['id', 'name', 'description', 'value', 'timestamp']

        # Verify total row count matches when concatenated
        all_dfs = [pd.read_csv(f) for f in files]
        combined_df = pd.concat(all_dfs, ignore_index=True)
        assert len(combined_df) == 1000


def test_dataframe_to_csv_files_custom_output_dir():
    """Test that files are created in the specified output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        custom_dir = os.path.join(tmpdir, 'custom', 'subdir')
        os.makedirs(custom_dir, exist_ok=True)

        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})

        files = dataframe_to_csv_files(df, 'test', custom_dir)

        assert len(files) == 1
        assert custom_dir in files[0]
        assert os.path.exists(files[0])


def test_dataframe_to_csv_files_preserves_data_types():
    """Test that various pandas data types are properly written to CSV."""
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pd.DataFrame(
            {
                'int_col': [1, 2, 3],
                'float_col': [1.1, 2.2, 3.3],
                'str_col': ['a', 'b', 'c'],
                'bool_col': [True, False, True],
                'datetime_col': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            }
        )

        files = dataframe_to_csv_files(df, 'types_test', tmpdir)

        # Read back and verify
        result_df = pd.read_csv(files[0])

        assert len(result_df) == 3
        assert result_df['int_col'].tolist() == [1, 2, 3]
        assert result_df['str_col'].tolist() == ['a', 'b', 'c']
        assert result_df['bool_col'].tolist() == [True, False, True]


def test_dataframe_to_csv_files_with_nulls():
    """Test that DataFrames with null values are handled correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', None, 'Charlie'], 'value': [10.5, 20.3, None]})

        files = dataframe_to_csv_files(df, 'nulls_test', tmpdir)

        # Read back and verify nulls are preserved
        result_df = pd.read_csv(files[0])

        assert len(result_df) == 3
        assert pd.isna(result_df.loc[1, 'name'])
        assert pd.isna(result_df.loc[2, 'value'])


def test_dataframe_to_csv_files_special_characters():
    """Test that DataFrames with special characters in data are properly escaped."""
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pd.DataFrame({'text': ['normal', 'with,comma', 'with"quote', 'with\nnewline'], 'value': [1, 2, 3, 4]})

        files = dataframe_to_csv_files(df, 'special_chars', tmpdir)

        # Read back and verify special characters are preserved
        result_df = pd.read_csv(files[0])

        assert len(result_df) == 4
        assert result_df['text'].tolist() == ['normal', 'with,comma', 'with"quote', 'with\nnewline']


def test_dataframe_to_csv_files_table_name_formatting():
    """Test that table names are correctly formatted in file names."""
    with tempfile.TemporaryDirectory() as tmpdir:
        df = pd.DataFrame({'col': [1, 2, 3]})

        # Test various table name formats
        test_cases = [
            ('main_executionenvironment', 'main_executionenvironment_table.csv'),
            ('unified_jobs', 'unified_jobs_table.csv'),
            ('job_host_summary', 'job_host_summary_table.csv'),
        ]

        for table_name, expected_filename in test_cases:
            files = dataframe_to_csv_files(df, table_name, tmpdir)
            assert len(files) == 1
            assert files[0].endswith(expected_filename)
