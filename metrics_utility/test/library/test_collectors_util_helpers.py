"""Test suite for collector utility helper functions."""

from datetime import date, datetime

import pandas as pd
import pytest

from metrics_utility.library.collectors.util import (
    _copy_table_files,
    _copy_table_pandas,
    date_where,
    ensure_functions,
)
from metrics_utility.test.util import mock_copy_db, mock_cursor_db, utcdt


class TestEnsureFunctions:
    """Test ensure_functions helper."""

    def test_executes_yaml_json_functions(self):
        """Test that SQL for custom functions is executed."""
        mock_db, mock_cursor = mock_cursor_db()

        ensure_functions(mock_db)

        assert mock_cursor.execute.call_count == 1

        sql_arg = mock_cursor.execute.call_args[0][0]
        assert 'metrics_utility_parse_yaml_field' in sql_arg
        assert 'metrics_utility_is_valid_json' in sql_arg

    def test_creates_parse_yaml_field(self):
        """Test that parse_yaml_field function is created."""
        mock_db, mock_cursor = mock_cursor_db()

        ensure_functions(mock_db)

        sql_arg = mock_cursor.execute.call_args[0][0]
        assert 'CREATE OR REPLACE FUNCTION metrics_utility_parse_yaml_field' in sql_arg
        assert 'RETURNS text' in sql_arg

    def test_creates_is_valid_json(self):
        """Test that is_valid_json function is created."""
        mock_db, mock_cursor = mock_cursor_db()

        ensure_functions(mock_db)

        sql_arg = mock_cursor.execute.call_args[0][0]
        assert 'CREATE OR REPLACE FUNCTION metrics_utility_is_valid_json' in sql_arg
        assert 'returns boolean' in sql_arg

    def test_cursor_cleanup(self):
        """Test that cursor context manager is used properly."""
        mock_db, mock_cursor = mock_cursor_db()

        ensure_functions(mock_db)

        mock_db.cursor.assert_called_once()
        mock_db.cursor.return_value.__enter__.assert_called_once()
        mock_db.cursor.return_value.__exit__.assert_called_once()


class TestCopyTableFiles:
    """Test _copy_table_files function."""

    def test_writes_csv_content(self, tmp_path):
        """Test that CSV data from the db is written to a file."""
        csv_data = b'id,name\n1,Alice\n2,Bob\n'
        mock_db, _ = mock_copy_db([csv_data])

        result = _copy_table_files(mock_db, 'SELECT * FROM users', str(tmp_path / 'out'))

        assert len(result) == 1
        with open(result[0]) as f:
            assert f.read() == 'id,name\n1,Alice\n2,Bob\n'

    def test_file_at_expected_path(self, tmp_path):
        """Test that output file is created at the filespec path."""
        mock_db, _ = mock_copy_db([b'col\nval\n'])

        filespec = str(tmp_path / 'test')
        result = _copy_table_files(mock_db, 'SELECT 1', filespec)

        assert len(result) == 1
        assert result[0] == filespec

    def test_multi_chunk_reads(self, tmp_path):
        """Test that multiple read() chunks are concatenated into a single output."""
        chunk1 = b'id,name\n1,Alice\n'
        chunk2 = b'2,Bob\n3,Charlie\n'
        mock_db, _ = mock_copy_db([chunk1, chunk2])

        result = _copy_table_files(mock_db, 'SELECT * FROM users', str(tmp_path / 'out'))

        assert len(result) == 1
        with open(result[0]) as f:
            assert f.read() == 'id,name\n1,Alice\n2,Bob\n3,Charlie\n'

    def test_copy_query_format(self, tmp_path):
        """Test that COPY query is formatted correctly."""
        mock_db, mock_cursor = mock_copy_db([])

        query = 'SELECT id, name FROM users WHERE active = true'
        _copy_table_files(mock_db, query, str(tmp_path / 'out'))

        copy_call_arg = mock_cursor.copy.call_args[0][0]
        assert copy_call_arg == f'COPY ({query}) TO STDOUT WITH CSV HEADER'

    def test_keeps_empty_files(self, tmp_path):
        """Test that header-only output still produces a file (keep_empty=True)."""
        mock_db, _ = mock_copy_db([b'col1,col2\n'])

        result = _copy_table_files(mock_db, 'SELECT * FROM empty', str(tmp_path / 'out'))

        assert len(result) == 1
        with open(result[0]) as f:
            assert f.read() == 'col1,col2\n'


class TestCopyTablePandas:
    """Test _copy_table_pandas function."""

    def test_returns_dataframe(self):
        """Test that function returns pandas DataFrame."""
        mock_db, mock_cursor = mock_cursor_db()
        mock_cursor.description = [('col1',), ('col2',)]
        mock_cursor.fetchall.return_value = [('val1', 'val2')]

        result = _copy_table_pandas(mock_db, 'SELECT * FROM test')

        assert isinstance(result, pd.DataFrame)

    def test_correct_column_names(self):
        """Test that DataFrame has correct column names from cursor.description."""
        mock_db, mock_cursor = mock_cursor_db()
        mock_cursor.description = [('id',), ('username',), ('email',)]
        mock_cursor.fetchall.return_value = [(1, 'alice', 'alice@example.com')]

        result = _copy_table_pandas(mock_db, 'SELECT * FROM users')

        assert list(result.columns) == ['id', 'username', 'email']

    def test_correct_row_data(self):
        """Test that DataFrame contains correct row data from cursor.fetchall."""
        mock_db, mock_cursor = mock_cursor_db()
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
        mock_db, mock_cursor = mock_cursor_db()
        mock_cursor.description = [('col1',), ('col2',)]
        mock_cursor.fetchall.return_value = []

        result = _copy_table_pandas(mock_db, 'SELECT * FROM empty_table')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ['col1', 'col2']

    def test_cursor_cleanup(self):
        """Test that cursor context manager is used properly."""
        mock_db, mock_cursor = mock_cursor_db()
        mock_cursor.description = [('id',)]
        mock_cursor.fetchall.return_value = [(1,)]

        _copy_table_pandas(mock_db, 'SELECT id FROM test')

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
