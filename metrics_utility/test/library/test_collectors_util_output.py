"""Test suite for collector output classes."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.library.collectors.util import (
    CollectionOutput,
    DataframeOutput,
    DictOutput,
)


class TestDictOutput:
    """Test DictOutput class."""

    def test_dict_returns_valid_dict(self):
        """Test that dict method returns dict unchanged."""
        output = DictOutput()
        test_dict = {'key': 'value', 'number': 42}
        result = output.dict(test_dict)
        assert result == test_dict

    def test_dict_returns_none_for_none(self):
        """Test that dict method returns None for None input."""
        output = DictOutput()
        result = output.dict(None)
        assert result is None

    def test_dict_raises_for_list(self):
        """Test that dict method raises exception for list input."""
        output = DictOutput()
        with pytest.raises(Exception, match='data must be a dict, or None'):
            output.dict(['item1', 'item2'])

    def test_dict_raises_for_string(self):
        """Test that dict method raises exception for string input."""
        output = DictOutput()
        with pytest.raises(Exception, match='data must be a dict, or None'):
            output.dict('string')

    def test_dict_raises_for_tuple(self):
        """Test that dict method raises exception for tuple input."""
        output = DictOutput()
        with pytest.raises(Exception, match='data must be a dict, or None'):
            output.dict(('tuple', 'data'))


class TestDataframeOutput:
    """Test DataframeOutput class."""

    def test_sql_returns_dataframe(self):
        """Test that sql method returns pandas DataFrame."""
        # Create mock database and cursor
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # Setup cursor to return data
        mock_cursor.description = [('col1',), ('col2',)]
        mock_cursor.fetchall.return_value = [('val1', 'val2'), ('val3', 'val4')]

        output = DataframeOutput()
        result = output.sql(mock_db, 'SELECT col1, col2 FROM test')

        assert isinstance(result, pd.DataFrame)

    def test_sql_with_valid_query(self):
        """Test that sql method executes query correctly."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('id',)]
        mock_cursor.fetchall.return_value = [(1,), (2,)]

        output = DataframeOutput()
        query = 'SELECT id FROM users'
        output.sql(mock_db, query)

        mock_cursor.execute.assert_called_once_with(query)

    def test_sql_returns_correct_columns(self):
        """Test that DataFrame has correct columns from cursor description."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('name',), ('age',), ('email',)]
        mock_cursor.fetchall.return_value = [('Alice', 30, 'alice@example.com')]

        output = DataframeOutput()
        result = output.sql(mock_db, 'SELECT * FROM users')

        assert list(result.columns) == ['name', 'age', 'email']

    def test_sql_returns_correct_data(self):
        """Test that DataFrame contains correct row data."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('id',), ('value',)]
        mock_cursor.fetchall.return_value = [(1, 'a'), (2, 'b'), (3, 'c')]

        output = DataframeOutput()
        result = output.sql(mock_db, 'SELECT * FROM test')

        assert len(result) == 3
        assert result.iloc[0]['id'] == 1
        assert result.iloc[0]['value'] == 'a'
        assert result.iloc[2]['id'] == 3
        assert result.iloc[2]['value'] == 'c'

    def test_sql_empty_result(self):
        """Test that empty query result returns empty DataFrame."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('col1',), ('col2',)]
        mock_cursor.fetchall.return_value = []

        output = DataframeOutput()
        result = output.sql(mock_db, 'SELECT * FROM empty_table')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ['col1', 'col2']


class TestCollectionOutput:
    """Test CollectionOutput class."""

    def test_init_stores_full_path(self):
        """Test that __init__ stores full_path."""
        test_path = '/tmp/test_path'
        output = CollectionOutput(test_path)
        assert output.full_path == test_path

    def test_dict_inherits_from_dictoutput(self):
        """Test that dict method inherits from DictOutput."""
        output = CollectionOutput('/tmp/test')
        test_dict = {'key': 'value'}
        result = output.dict(test_dict)
        assert result == test_dict

    def test_files_returns_valid_list(self):
        """Test that files method returns list unchanged."""
        output = CollectionOutput('/tmp/test')
        test_list = ['file1.csv', 'file2.csv']
        result = output.files(test_list)
        assert result == test_list

    def test_files_returns_none_for_none(self):
        """Test that files method returns None for None input."""
        output = CollectionOutput('/tmp/test')
        result = output.files(None)
        assert result is None

    def test_files_raises_for_dict(self):
        """Test that files method raises exception for dict input."""
        output = CollectionOutput('/tmp/test')
        with pytest.raises(Exception, match='filenames must be a list, or None'):
            output.files({'key': 'value'})

    def test_files_raises_for_string(self):
        """Test that files method raises exception for string input."""
        output = CollectionOutput('/tmp/test')
        with pytest.raises(Exception, match='filenames must be a list, or None'):
            output.files('string')

    def test_sql_creates_csv_files(self, tmp_path):
        """Test that sql method creates CSV files."""
        # Create mock database and cursor with copy support
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        # Mock data reading
        mock_copy.read.side_effect = [b'col1,col2\nval1,val2\n', None]

        output = CollectionOutput(str(tmp_path))
        result = output.sql(mock_db, 'SELECT * FROM test')

        # Should call cursor.copy with COPY command
        assert mock_cursor.copy.called
        copy_call_arg = mock_cursor.copy.call_args[0][0]
        assert 'COPY' in copy_call_arg
        assert 'TO STDOUT' in copy_call_arg
        assert 'CSV HEADER' in copy_call_arg

        # Should return list of files
        assert isinstance(result, list)

    def test_sql_uses_full_path(self, tmp_path):
        """Test that sql method uses self.full_path for file location."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        mock_copy.read.return_value = None

        test_path = str(tmp_path)
        output = CollectionOutput(test_path)

        # Mock tempfile.mktemp to verify it's called with correct dir
        with patch('tempfile.mktemp') as mock_mktemp:
            mock_mktemp.return_value = f'{test_path}/test_file'
            output.sql(mock_db, 'SELECT * FROM test')
            mock_mktemp.assert_called_once_with(dir=test_path)

    def test_as_dict_calls_collector(self):
        """Test that as_dict calls collector.gather with output=self."""
        mock_collector = MagicMock()
        mock_collector.gather.return_value = {'result': 'data'}

        output = CollectionOutput('/tmp/test')
        result = output.as_dict(mock_collector)

        mock_collector.gather.assert_called_once_with(output=output)
        assert result == {'result': 'data'}

    def test_as_dict_returns_dict(self):
        """Test that as_dict returns dict from collector."""
        mock_collector = MagicMock()
        test_data = {'key1': 'value1', 'key2': 'value2'}
        mock_collector.gather.return_value = test_data

        output = CollectionOutput('/tmp/test')
        result = output.as_dict(mock_collector)

        assert result == test_data

    def test_as_files_calls_collector(self):
        """Test that as_files calls collector.gather with output=self."""
        mock_collector = MagicMock()
        mock_collector.gather.return_value = ['file1.csv', 'file2.csv']

        output = CollectionOutput('/tmp/test')
        result = output.as_files(mock_collector)

        mock_collector.gather.assert_called_once_with(output=output)
        assert result == ['file1.csv', 'file2.csv']

    def test_as_files_returns_list(self):
        """Test that as_files returns list from collector."""
        mock_collector = MagicMock()
        test_files = ['/tmp/file1.csv', '/tmp/file2.csv', '/tmp/file3.csv']
        mock_collector.gather.return_value = test_files

        output = CollectionOutput('/tmp/test')
        result = output.as_files(mock_collector)

        assert result == test_files
