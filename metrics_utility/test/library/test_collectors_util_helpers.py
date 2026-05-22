"""Test suite for collector utility helper functions."""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.library.collectors.util import (
    _copy_table_files,
    _copy_table_pandas,
    date_where,
    ensure_functions,
)
from metrics_utility.test.util import utcdt


class TestEnsureFunctions:
    """Test ensure_functions helper."""

    def test_executes_yaml_json_functions(self):
        """Test that SQL for custom functions is executed."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        ensure_functions(mock_db)

        # Verify cursor.execute was called once
        assert mock_cursor.execute.call_count == 1

        # Verify the SQL contains function definitions
        sql_arg = mock_cursor.execute.call_args[0][0]
        assert 'metrics_utility_parse_yaml_field' in sql_arg
        assert 'metrics_utility_is_valid_json' in sql_arg

    def test_creates_parse_yaml_field(self):
        """Test that parse_yaml_field function is created."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        ensure_functions(mock_db)

        sql_arg = mock_cursor.execute.call_args[0][0]
        assert 'CREATE OR REPLACE FUNCTION metrics_utility_parse_yaml_field' in sql_arg
        assert 'RETURNS text' in sql_arg

    def test_creates_is_valid_json(self):
        """Test that is_valid_json function is created."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        ensure_functions(mock_db)

        sql_arg = mock_cursor.execute.call_args[0][0]
        assert 'CREATE OR REPLACE FUNCTION metrics_utility_is_valid_json' in sql_arg
        assert 'returns boolean' in sql_arg

    def test_cursor_cleanup(self):
        """Test that cursor context manager is used properly."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        ensure_functions(mock_db)

        # Verify context manager was used
        mock_db.cursor.assert_called_once()
        mock_db.cursor.return_value.__enter__.assert_called_once()
        mock_db.cursor.return_value.__exit__.assert_called_once()


class TestCopyTableFiles:
    """Test _copy_table_files function."""

    def test_creates_csv_files(self, tmp_path):
        """Test that CSV files are created."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate data reading
        csv_data = b'id,name\n1,Alice\n2,Bob\n'
        mock_copy.read.side_effect = [csv_data, None]

        filespec = str(tmp_path / 'test_output')
        result = _copy_table_files(mock_db, 'SELECT * FROM users', filespec)

        # Should return list of files
        assert isinstance(result, list)

    def test_uses_csv_splitter(self, tmp_path):
        """Test that CsvFileSplitter is used correctly."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        mock_copy.read.return_value = None

        with patch('metrics_utility.library.collectors.util.CsvFileSplitter') as MockSplitter:
            mock_splitter_instance = MagicMock()
            MockSplitter.return_value = mock_splitter_instance
            mock_splitter_instance.file_list.return_value = ['file1.csv']

            filespec = str(tmp_path / 'test')
            _copy_table_files(mock_db, 'SELECT id FROM test', filespec)

            # Verify CsvFileSplitter was created with filespec
            MockSplitter.assert_called_once_with(filespec=filespec)

            # Verify file_list was called with keep_empty=True
            mock_splitter_instance.file_list.assert_called_once_with(keep_empty=True)

    def test_returns_file_list(self, tmp_path):
        """Test that function returns file list from CsvFileSplitter."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        mock_copy.read.return_value = None

        with patch('metrics_utility.library.collectors.util.CsvFileSplitter') as MockSplitter:
            expected_files = ['file1.csv', 'file2.csv', 'file3.csv']
            mock_splitter_instance = MagicMock()
            MockSplitter.return_value = mock_splitter_instance
            mock_splitter_instance.file_list.return_value = expected_files

            result = _copy_table_files(mock_db, 'SELECT * FROM test', '/tmp/nowrites')

            assert result == expected_files

    def test_copy_query_format(self, tmp_path):
        """Test that COPY query is formatted correctly."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        mock_copy.read.return_value = None

        query = 'SELECT id, name FROM users WHERE active = true'
        _copy_table_files(mock_db, query, str(tmp_path / 'out'))

        # Verify cursor.copy was called with correct COPY command
        copy_call_arg = mock_cursor.copy.call_args[0][0]
        assert copy_call_arg == f'COPY ({query}) TO STDOUT WITH CSV HEADER'

    def test_keeps_empty_files(self, tmp_path):
        """Test that keep_empty=True is passed to file_list."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_copy = MagicMock()

        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
        mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)

        mock_copy.read.return_value = None

        with patch('metrics_utility.library.collectors.util.CsvFileSplitter') as MockSplitter:
            mock_splitter_instance = MagicMock()
            MockSplitter.return_value = mock_splitter_instance
            mock_splitter_instance.file_list.return_value = []

            _copy_table_files(mock_db, 'SELECT * FROM empty', '/tmp/nowrites')

            # Verify keep_empty=True was passed
            mock_splitter_instance.file_list.assert_called_once_with(keep_empty=True)


class TestCopyTablePandas:
    """Test _copy_table_pandas function."""

    def test_returns_dataframe(self):
        """Test that function returns pandas DataFrame."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('col1',), ('col2',)]
        mock_cursor.fetchall.return_value = [('val1', 'val2')]

        result = _copy_table_pandas(mock_db, 'SELECT * FROM test')

        assert isinstance(result, pd.DataFrame)

    def test_correct_column_names(self):
        """Test that DataFrame has correct column names from cursor.description."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('id',), ('username',), ('email',)]
        mock_cursor.fetchall.return_value = [(1, 'alice', 'alice@example.com')]

        result = _copy_table_pandas(mock_db, 'SELECT * FROM users')

        assert list(result.columns) == ['id', 'username', 'email']

    def test_correct_row_data(self):
        """Test that DataFrame contains correct row data from cursor.fetchall."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('id',), ('value',)]
        mock_cursor.fetchall.return_value = [
            (1, 'first'),
            (2, 'second'),
            (3, 'third'),
        ]

        result = _copy_table_pandas(mock_db, 'SELECT * FROM test')

        assert len(result) == 3
        assert result.iloc[0]['id'] == 1
        assert result.iloc[0]['value'] == 'first'
        assert result.iloc[1]['id'] == 2
        assert result.iloc[1]['value'] == 'second'
        assert result.iloc[2]['id'] == 3
        assert result.iloc[2]['value'] == 'third'

    def test_empty_result(self):
        """Test that empty query result returns empty DataFrame."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('col1',), ('col2',)]
        mock_cursor.fetchall.return_value = []

        result = _copy_table_pandas(mock_db, 'SELECT * FROM empty_table')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ['col1', 'col2']

    def test_cursor_cleanup(self):
        """Test that cursor context manager is used properly."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_cursor.description = [('id',)]
        mock_cursor.fetchall.return_value = [(1,)]

        _copy_table_pandas(mock_db, 'SELECT id FROM test')

        # Verify context manager was used
        mock_db.cursor.assert_called_once()
        mock_db.cursor.return_value.__enter__.assert_called_once()
        mock_db.cursor.return_value.__exit__.assert_called_once()


class TestDateWhere:
    """Test date_where function."""

    def test_both_since_and_until(self):
        """Test date_where with both since and until produces range condition."""
        since = utcdt('2024-01-01')
        until = utcdt('2024-12-31T23:59:59')

        result = date_where('created_at', since, until)

        assert 'created_at >=' in result
        assert 'AND created_at <' in result
        assert since.isoformat() in result
        assert until.isoformat() in result

    def test_only_since(self):
        """Test date_where with only since produces >= condition."""
        since = utcdt('2024-06-01T12:00:00')

        result = date_where('modified_date', since, None)

        assert 'modified_date >=' in result
        assert since.isoformat() in result
        assert 'AND' not in result

    def test_only_until(self):
        """Test date_where with only until produces < condition."""
        until = utcdt('2024-03-15T18:30:00')

        result = date_where('timestamp', None, until)

        assert 'timestamp <' in result
        assert until.isoformat() in result
        assert '>=' not in result

    def test_neither_since_nor_until(self):
        """Test date_where with neither since nor until returns 'true'."""
        result = date_where('date_field', None, None)

        assert result == 'true'

    def test_rejects_naive_since(self):
        with pytest.raises(ValueError, match='since must be timezone-aware'):
            date_where('field', datetime(2024, 1, 1), None)

    def test_rejects_naive_until(self):
        with pytest.raises(ValueError, match='until must be timezone-aware'):
            date_where('field', None, datetime(2024, 1, 1))

    def test_rejects_non_datetime_since(self):
        with pytest.raises(TypeError, match='since must be a datetime, got str'):
            date_where('field', '2024-01-01', None)

    def test_rejects_non_datetime_until(self):
        with pytest.raises(TypeError, match='until must be a datetime, got date'):
            date_where('field', None, date(2024, 1, 1))

    def test_dotted_table_column_reference(self):
        """Test that table.column references work correctly (not broken by quoting)."""
        since = utcdt('2024-01-01')
        until = utcdt('2024-12-31')

        result = date_where('main_host.created', since, until)

        assert 'main_host.created >=' in result
        assert 'main_host.created <' in result
        # must NOT be quoted as "main_host.created" — PostgreSQL would treat that as a single identifier
        assert '"main_host.created"' not in result
