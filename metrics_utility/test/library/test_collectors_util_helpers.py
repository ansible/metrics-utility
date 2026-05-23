"""Test suite for collector utility helper functions."""

from datetime import date, datetime

import pandas as pd
import pytest

from metrics_utility.gather.output import _copy_table_files
from metrics_utility.library.collectors.util import (
    _copy_table_pandas,
    date_where,
    ensure_functions,
)
from metrics_utility.test.util import mock_copy_db, mock_cursor_db, utcdt


# ensure_functions


def test_ensure_functions_executes_yaml_json_functions():
    mock_db, mock_cursor = mock_cursor_db()

    ensure_functions(mock_db)

    assert mock_cursor.execute.call_count == 1

    sql_arg = mock_cursor.execute.call_args[0][0]
    assert 'metrics_utility_parse_yaml_field' in sql_arg
    assert 'metrics_utility_is_valid_json' in sql_arg


def test_ensure_functions_creates_parse_yaml_field():
    mock_db, mock_cursor = mock_cursor_db()

    ensure_functions(mock_db)

    sql_arg = mock_cursor.execute.call_args[0][0]
    assert 'CREATE OR REPLACE FUNCTION metrics_utility_parse_yaml_field' in sql_arg
    assert 'RETURNS text' in sql_arg


def test_ensure_functions_creates_is_valid_json():
    mock_db, mock_cursor = mock_cursor_db()

    ensure_functions(mock_db)

    sql_arg = mock_cursor.execute.call_args[0][0]
    assert 'CREATE OR REPLACE FUNCTION metrics_utility_is_valid_json' in sql_arg
    assert 'returns boolean' in sql_arg


def test_ensure_functions_cursor_cleanup():
    mock_db, _mock_cursor = mock_cursor_db()

    ensure_functions(mock_db)

    mock_db.cursor.assert_called_once()
    mock_db.cursor.return_value.__enter__.assert_called_once()
    mock_db.cursor.return_value.__exit__.assert_called_once()


# _copy_table_files


def test_copy_table_files_writes_csv_content(tmp_path):
    csv_data = b'id,name\n1,Alice\n2,Bob\n'
    mock_db, _ = mock_copy_db([csv_data])

    result = _copy_table_files(mock_db, 'SELECT * FROM users', str(tmp_path / 'out'))

    assert len(result) == 1
    with open(result[0]) as f:
        assert f.read() == 'id,name\n1,Alice\n2,Bob\n'


def test_copy_table_files_file_at_expected_path(tmp_path):
    mock_db, _ = mock_copy_db([b'col\nval\n'])

    filespec = str(tmp_path / 'test')
    result = _copy_table_files(mock_db, 'SELECT 1', filespec)

    assert len(result) == 1
    assert result[0] == filespec


def test_copy_table_files_multi_chunk_reads(tmp_path):
    chunk1 = b'id,name\n1,Alice\n'
    chunk2 = b'2,Bob\n3,Charlie\n'
    mock_db, _ = mock_copy_db([chunk1, chunk2])

    result = _copy_table_files(mock_db, 'SELECT * FROM users', str(tmp_path / 'out'))

    assert len(result) == 1
    with open(result[0]) as f:
        assert f.read() == 'id,name\n1,Alice\n2,Bob\n3,Charlie\n'


def test_copy_table_files_copy_query_format(tmp_path):
    mock_db, mock_cursor = mock_copy_db([])

    query = 'SELECT id, name FROM users WHERE active = true'
    _copy_table_files(mock_db, query, str(tmp_path / 'out'))

    copy_call_arg = mock_cursor.copy.call_args[0][0]
    assert copy_call_arg == f'COPY ({query}) TO STDOUT WITH CSV HEADER'


def test_copy_table_files_keeps_empty_files(tmp_path):
    mock_db, _ = mock_copy_db([b'col1,col2\n'])

    result = _copy_table_files(mock_db, 'SELECT * FROM empty', str(tmp_path / 'out'))

    assert len(result) == 1
    with open(result[0]) as f:
        assert f.read() == 'col1,col2\n'


# _copy_table_pandas


def test_copy_table_pandas_returns_dataframe():
    mock_db, mock_cursor = mock_cursor_db()
    mock_cursor.description = [('col1',), ('col2',)]
    mock_cursor.fetchall.return_value = [('val1', 'val2')]

    result = _copy_table_pandas(mock_db, 'SELECT * FROM test')

    assert isinstance(result, pd.DataFrame)


def test_copy_table_pandas_correct_column_names():
    mock_db, mock_cursor = mock_cursor_db()
    mock_cursor.description = [('id',), ('username',), ('email',)]
    mock_cursor.fetchall.return_value = [(1, 'alice', 'alice@example.com')]

    result = _copy_table_pandas(mock_db, 'SELECT * FROM users')

    assert list(result.columns) == ['id', 'username', 'email']


def test_copy_table_pandas_correct_row_data():
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


def test_copy_table_pandas_empty_result():
    mock_db, mock_cursor = mock_cursor_db()
    mock_cursor.description = [('col1',), ('col2',)]
    mock_cursor.fetchall.return_value = []

    result = _copy_table_pandas(mock_db, 'SELECT * FROM empty_table')

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
    assert list(result.columns) == ['col1', 'col2']


def test_copy_table_pandas_cursor_cleanup():
    mock_db, mock_cursor = mock_cursor_db()
    mock_cursor.description = [('id',)]
    mock_cursor.fetchall.return_value = [(1,)]

    _copy_table_pandas(mock_db, 'SELECT id FROM test')

    mock_db.cursor.assert_called_once()
    mock_db.cursor.return_value.__enter__.assert_called_once()
    mock_db.cursor.return_value.__exit__.assert_called_once()


# date_where


def test_date_where_both_since_and_until():
    since = utcdt('2024-01-01')
    until = utcdt('2024-12-31T23:59:59')

    result = date_where('created_at', since, until)

    assert 'created_at >=' in result
    assert 'AND created_at <' in result
    assert since.isoformat() in result
    assert until.isoformat() in result


def test_date_where_only_since():
    since = utcdt('2024-06-01T12:00:00')

    result = date_where('modified_date', since, None)

    assert 'modified_date >=' in result
    assert since.isoformat() in result
    assert 'AND' not in result


def test_date_where_only_until():
    until = utcdt('2024-03-15T18:30:00')

    result = date_where('timestamp', None, until)

    assert 'timestamp <' in result
    assert until.isoformat() in result
    assert '>=' not in result


def test_date_where_neither_since_nor_until():
    result = date_where('date_field', None, None)

    assert result == 'true'


def test_date_where_rejects_naive_since():
    with pytest.raises(ValueError, match='since must be timezone-aware'):
        date_where('field', datetime(2024, 1, 1), None)


def test_date_where_rejects_naive_until():
    with pytest.raises(ValueError, match='until must be timezone-aware'):
        date_where('field', None, datetime(2024, 1, 1))


def test_date_where_rejects_non_datetime_since():
    with pytest.raises(TypeError, match='since must be a datetime, got str'):
        date_where('field', '2024-01-01', None)


def test_date_where_rejects_non_datetime_until():
    with pytest.raises(TypeError, match='until must be a datetime, got date'):
        date_where('field', None, date(2024, 1, 1))


def test_date_where_dotted_table_column_reference():
    since = utcdt('2024-01-01')
    until = utcdt('2024-12-31')

    result = date_where('main_host.created', since, until)

    assert 'main_host.created >=' in result
    assert 'main_host.created <' in result
    # must NOT be quoted as "main_host.created" — PostgreSQL would treat that as a single identifier
    assert '"main_host.created"' not in result
