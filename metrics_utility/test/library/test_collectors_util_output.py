"""Test suite for collector output classes."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.library.collectors.util import (
    CollectionOutput,
    DataframeOutput,
    DictOutput,
)
from metrics_utility.test.util import mock_copy_db, mock_cursor_db


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


# CollectionOutput


def test_collection_output_init_stores_full_path():
    test_path = '/tmp/nowrites'
    output = CollectionOutput(test_path)
    assert output.full_path == test_path


def test_collection_output_dict_inherits_from_dictoutput():
    output = CollectionOutput('/tmp/nowrites')
    test_dict = {'key': 'value'}
    result = output.dict(test_dict)
    assert result == test_dict


def test_collection_output_files_returns_valid_list():
    output = CollectionOutput('/tmp/nowrites')
    test_list = ['file1.csv', 'file2.csv']
    result = output.files(test_list)
    assert result == test_list


def test_collection_output_files_returns_none_for_none():
    output = CollectionOutput('/tmp/nowrites')
    result = output.files(None)
    assert result is None


def test_collection_output_files_raises_for_dict():
    output = CollectionOutput('/tmp/nowrites')
    with pytest.raises(Exception, match='filenames must be a list, or None'):
        output.files({'key': 'value'})


def test_collection_output_files_raises_for_string():
    output = CollectionOutput('/tmp/nowrites')
    with pytest.raises(Exception, match='filenames must be a list, or None'):
        output.files('string')


def test_collection_output_sql_creates_csv_files(tmp_path):
    mock_db, mock_cursor = mock_copy_db([b'col1,col2\nval1,val2\n'])

    output = CollectionOutput(str(tmp_path))
    result = output.sql(mock_db, 'SELECT * FROM test')

    assert mock_cursor.copy.called
    copy_call_arg = mock_cursor.copy.call_args[0][0]
    assert 'COPY' in copy_call_arg
    assert 'TO STDOUT' in copy_call_arg
    assert 'CSV HEADER' in copy_call_arg

    assert isinstance(result, list)


def test_collection_output_sql_uses_full_path(tmp_path):
    mock_db, _ = mock_copy_db([])

    test_path = str(tmp_path)
    output = CollectionOutput(test_path)

    with patch('tempfile.mktemp') as mock_mktemp:
        mock_mktemp.return_value = f'{test_path}/test_file'
        output.sql(mock_db, 'SELECT * FROM test')
        mock_mktemp.assert_called_once_with(dir=test_path)


def test_collection_output_as_dict_calls_collector():
    mock_collector = MagicMock()
    mock_collector.gather.return_value = {'result': 'data'}

    output = CollectionOutput('/tmp/nowrites')
    result = output.as_dict(mock_collector)

    mock_collector.gather.assert_called_once_with(output=output)
    assert result == {'result': 'data'}


def test_collection_output_as_dict_returns_dict():
    mock_collector = MagicMock()
    test_data = {'key1': 'value1', 'key2': 'value2'}
    mock_collector.gather.return_value = test_data

    output = CollectionOutput('/tmp/nowrites')
    result = output.as_dict(mock_collector)

    assert result == test_data


def test_collection_output_as_files_calls_collector():
    mock_collector = MagicMock()
    mock_collector.gather.return_value = ['file1.csv', 'file2.csv']

    output = CollectionOutput('/tmp/nowrites')
    result = output.as_files(mock_collector)

    mock_collector.gather.assert_called_once_with(output=output)
    assert result == ['file1.csv', 'file2.csv']


def test_collection_output_as_files_returns_list():
    mock_collector = MagicMock()
    test_files = ['/tmp/nowrites/file1.csv', '/tmp/nowrites/file2.csv', '/tmp/nowrites/file3.csv']
    mock_collector.gather.return_value = test_files

    output = CollectionOutput('/tmp/nowrites')
    result = output.as_files(mock_collector)

    assert result == test_files
