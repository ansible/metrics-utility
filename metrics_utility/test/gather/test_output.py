"""Test suite for CollectionOutput."""

from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.gather.output import CollectionOutput
from metrics_utility.test.util import mock_copy_db


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
