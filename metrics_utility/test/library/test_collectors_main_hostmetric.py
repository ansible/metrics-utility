from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.main_hostmetric import main_hostmetric


def test_main_hostmetric_basic():
    mock_db = MagicMock()

    instance = main_hostmetric(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_hostmetric_calls_copy_table(mock_copy_pandas):
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({'hostname': ['h1'], 'host_id': [0]})

    instance = main_hostmetric(db=mock_db)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_hostmetric_query_structure(mock_copy_pandas):
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_hostmetric(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'main_hostmetric' in query
    assert 'main_host' in query
    assert 'LEFT JOIN main_host ON main_host.name = main_hostmetric.hostname' in query
    assert 'hostname' in query
    assert 'host_id' in query
    assert 'first_automation' in query
    assert 'last_automation' in query
    assert 'automated_counter' in query
    assert 'deleted_counter' in query
    assert 'last_deleted' in query
    assert 'ansible_product_serial' in query
    assert 'ansible_machine_id' in query
    assert 'ansible_host_variable' in query
    assert 'ansible_connection_variable' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_hostmetric_uses_yaml_json_functions(mock_copy_pandas):
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_hostmetric(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'metrics_utility_is_valid_json' in query
    assert 'metrics_utility_parse_yaml_field' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_hostmetric_filters_by_last_automation(mock_copy_pandas):
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    from metrics_utility.test.util import utcdt

    since = utcdt('2025-06-06')
    until = utcdt('2025-06-13')

    instance = main_hostmetric(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'main_hostmetric.last_automation' in query
    assert since.isoformat() in query
    assert until.isoformat() in query
