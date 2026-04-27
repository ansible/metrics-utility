"""Test suite for collector output classes."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.library.collectors.util import (
    CollectionOutput,
    DataframeOutput,
    DictOutput,
    _batch_copy_table_files,
    get_batch_size,
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

        # Mock tempfile.mkdtemp to verify it's called with correct dir.
        # Return test_path itself so os.path.join(tmpdir, 'data') resolves to a
        # writable path without needing a real subdirectory.
        with patch('tempfile.mkdtemp') as mock_mkdtemp:
            mock_mkdtemp.return_value = test_path
            output.sql(mock_db, 'SELECT * FROM test')
            mock_mkdtemp.assert_called_once_with(dir=test_path)

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


class TestGetBatchSize:
    """Test get_batch_size utility function."""

    def test_returns_zero_when_not_set(self):
        with patch.dict('os.environ', {}, clear=True):
            assert get_batch_size() == 0

    def test_returns_configured_value(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_BATCH_SIZE': '100000'}):
            assert get_batch_size() == 100000

    def test_returns_zero_for_negative(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_BATCH_SIZE': '-1'}):
            assert get_batch_size() == 0

    def test_returns_zero_for_non_integer(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_BATCH_SIZE': 'bad'}):
            assert get_batch_size() == 0

    def test_returns_zero_explicitly(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_BATCH_SIZE': '0'}):
            assert get_batch_size() == 0


class TestBatchCopyTableFiles:
    """Test _batch_copy_table_files function."""

    def test_raises_on_zero_batch_size(self, tmp_path):
        mock_db = MagicMock()
        filespec = str(tmp_path / 'data')
        with pytest.raises(ValueError, match='batch_size must be > 0'):
            _batch_copy_table_files(mock_db, lambda s, e: '', '', 0, filespec)

    def test_raises_on_negative_batch_size(self, tmp_path):
        mock_db = MagicMock()
        filespec = str(tmp_path / 'data')
        with pytest.raises(ValueError, match='batch_size must be > 0'):
            _batch_copy_table_files(mock_db, lambda s, e: '', '', -1, filespec)

    def test_returns_empty_file_when_no_rows(self, tmp_path):
        """When min/max returns NULL the function still returns the (empty) file list."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (None, None)

        filespec = str(tmp_path / 'data')
        result = _batch_copy_table_files(mock_db, lambda s, e: 'SELECT 1', 'SELECT MIN(id), MAX(id)', 1000, filespec)

        # CsvFileSplitter always creates at least one (empty) file even with no data
        assert isinstance(result, list)
        assert len(result) == 1

    def test_executes_correct_number_of_batches(self, tmp_path):
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        # IDs 1..300 with batch_size=100 → 3 batches
        mock_cursor.fetchone.return_value = (1, 300)
        mock_copy.read.return_value = None  # no data, just structure

        query_calls = []

        def track_query(s, e):
            query_calls.append((s, e))
            return f'SELECT * WHERE id >= {s} AND id < {e}'

        filespec = str(tmp_path / 'data')
        _batch_copy_table_files(mock_db, track_query, 'SELECT MIN(id), MAX(id)', 100, filespec)

        assert query_calls == [(1, 101), (101, 201), (201, 301)]

    def test_first_batch_uses_csv_header(self, tmp_path):
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.fetchone.return_value = (1, 200)
        mock_copy.read.return_value = None

        filespec = str(tmp_path / 'data')
        _batch_copy_table_files(mock_db, lambda s, e: 'SELECT 1', 'SELECT MIN(id), MAX(id)', 100, filespec)

        copy_calls = mock_cursor.copy.call_args_list
        assert 'WITH CSV HEADER' in copy_calls[0][0][0]
        assert 'WITH CSV HEADER' not in copy_calls[1][0][0]
        assert 'WITH CSV' in copy_calls[1][0][0]


class TestDataframeBatchSql:
    """Test DataframeOutput.batch_sql returns concatenated DataFrame."""

    def _make_mock_db(self, min_id, max_id, rows_per_batch):
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (min_id, max_id)
        mock_cursor.description = [('id',), ('val',)]
        mock_cursor.fetchall.return_value = [(i, f'v{i}') for i in range(rows_per_batch)]
        return mock_db

    def test_returns_dataframe_with_all_batches(self):
        mock_db = self._make_mock_db(1, 200, 5)
        output = DataframeOutput()
        result = output.batch_sql(mock_db, lambda s, e: f'SELECT * WHERE id>={s}', 'SELECT MIN(id), MAX(id)', 100)
        assert isinstance(result, pd.DataFrame)
        # 2 batches × 5 rows each
        assert len(result) == 10

    def test_returns_empty_dataframe_when_no_rows(self):
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (None, None)

        output = DataframeOutput()
        result = output.batch_sql(mock_db, lambda s, e: 'SELECT 1', 'SELECT MIN(id), MAX(id)', 100)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestCollectionOutputBatchSql:
    """Test CollectionOutput.batch_sql writes CSV batches."""

    def test_batch_sql_calls_batch_copy(self, tmp_path):
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.fetchone.return_value = (None, None)  # no data

        output = CollectionOutput(str(tmp_path))
        result = output.batch_sql(
            mock_db,
            query_fn=lambda s, e: f'SELECT * WHERE id >= {s}',
            min_max_query='SELECT MIN(id), MAX(id) FROM t',
            batch_size=1000,
        )
        assert isinstance(result, list)

    def test_batch_sql_uses_full_path(self, tmp_path):
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (None, None)

        test_path = str(tmp_path)
        output = CollectionOutput(test_path)

        with patch('tempfile.mkdtemp') as mock_mkdtemp:
            mock_mkdtemp.return_value = test_path
            output.batch_sql(mock_db, lambda s, e: 'SELECT 1', 'SELECT MIN(id), MAX(id)', 1000)
            mock_mkdtemp.assert_called_once_with(dir=test_path)
