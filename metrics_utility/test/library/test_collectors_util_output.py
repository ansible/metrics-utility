"""Test suite for DictOutput and DataframeOutput."""

import pandas as pd
import pytest

from metrics_utility.library.collectors.util import (
    DataframeOutput,
    DictOutput,
)
from metrics_utility.test.util import mock_cursor_db


# DictOutput


def test_dict_output_returns_valid_dict():
    output = DictOutput()
    test_dict = {'key': 'value', 'number': 42}
    result = output.dict(test_dict)
    assert result == test_dict


def test_dict_output_returns_none_for_none():
    output = DictOutput()
    result = output.dict(None)
    assert result is None


def test_dict_output_raises_for_list():
    output = DictOutput()
    with pytest.raises(Exception, match='data must be a dict, or None'):
        output.dict(['item1', 'item2'])


def test_dict_output_raises_for_string():
    output = DictOutput()
    with pytest.raises(Exception, match='data must be a dict, or None'):
        output.dict('string')


def test_dict_output_raises_for_tuple():
    output = DictOutput()
    with pytest.raises(Exception, match='data must be a dict, or None'):
        output.dict(('tuple', 'data'))


# DataframeOutput


def test_dataframe_output_sql_returns_dataframe():
    mock_db, mock_cursor = mock_cursor_db()

    mock_cursor.description = [('col1',), ('col2',)]
    mock_cursor.fetchall.return_value = [('val1', 'val2'), ('val3', 'val4')]

    output = DataframeOutput()
    result = output.sql(mock_db, 'SELECT col1, col2 FROM test')

    assert isinstance(result, pd.DataFrame)


def test_dataframe_output_sql_with_valid_query():
    mock_db, mock_cursor = mock_cursor_db()

    mock_cursor.description = [('id',)]
    mock_cursor.fetchall.return_value = [(1,), (2,)]

    output = DataframeOutput()
    query = 'SELECT id FROM users'
    output.sql(mock_db, query)

    mock_cursor.execute.assert_called_once_with(query)


def test_dataframe_output_sql_returns_correct_columns():
    mock_db, mock_cursor = mock_cursor_db()

    mock_cursor.description = [('name',), ('age',), ('email',)]
    mock_cursor.fetchall.return_value = [('Alice', 30, 'alice@example.com')]

    output = DataframeOutput()
    result = output.sql(mock_db, 'SELECT * FROM users')

    assert list(result.columns) == ['name', 'age', 'email']


def test_dataframe_output_sql_returns_correct_data():
    mock_db, mock_cursor = mock_cursor_db()

    mock_cursor.description = [('id',), ('value',)]
    mock_cursor.fetchall.return_value = [(1, 'a'), (2, 'b'), (3, 'c')]

    output = DataframeOutput()
    result = output.sql(mock_db, 'SELECT * FROM test')

    assert len(result) == 3
    assert result.iloc[0]['id'] == 1
    assert result.iloc[0]['value'] == 'a'
    assert result.iloc[2]['id'] == 3
    assert result.iloc[2]['value'] == 'c'


def test_dataframe_output_sql_empty_result():
    mock_db, mock_cursor = mock_cursor_db()

    mock_cursor.description = [('col1',), ('col2',)]
    mock_cursor.fetchall.return_value = []

    output = DataframeOutput()
    result = output.sql(mock_db, 'SELECT * FROM empty_table')

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
    assert list(result.columns) == ['col1', 'col2']
