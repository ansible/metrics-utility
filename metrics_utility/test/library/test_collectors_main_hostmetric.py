from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.main_hostmetric import _host_metric_query, main_hostmetric
from metrics_utility.library.collectors.util import CollectionOutput


SINCE = datetime(2025, 6, 13, 0, 0, 0, tzinfo=UTC)
UNTIL = datetime(2025, 6, 14, 0, 0, 0, tzinfo=UTC)


def test_main_hostmetric_basic():
    """The collector exposes the standard gather()/kwargs interface."""
    mock_db = MagicMock()

    instance = main_hostmetric(db=mock_db, since=SINCE)

    assert hasattr(instance, 'gather')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == SINCE


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_hostmetric_calls_copy_table(mock_copy_pandas):
    """gather() fetches via _copy_table_pandas and returns a DataFrame."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({'hostname': ['a'], 'host_id': [0]})

    result = main_hostmetric(db=mock_db, since=SINCE).gather()

    mock_copy_pandas.assert_called_once()
    db_arg, query, params = mock_copy_pandas.call_args[0]
    assert db_arg == mock_db
    assert 'main_hostmetric' in query
    assert 'LEFT JOIN main_host' in query
    assert 'LIMIT %s' in query
    assert SINCE.isoformat() in query  # since interpolated via date_where
    assert params == [10000]  # only the page-size LIMIT is bound
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_hostmetric_query_columns(mock_copy_pandas):
    """The SQL selects all expected host_metric/host columns."""
    mock_copy_pandas.return_value = pd.DataFrame()

    main_hostmetric(db=MagicMock(), since=SINCE).gather()

    query = mock_copy_pandas.call_args[0][1]
    for column in [
        'hostname',
        'host_id',
        'first_automation',
        'last_automation',
        'automated_counter',
        'deleted_counter',
        'last_deleted',
        'deleted',
        'ansible_product_serial',
        'ansible_machine_id',
        'ansible_host_variable',
        'ansible_connection_variable',
    ]:
        assert column in query


@patch('metrics_utility.library.collectors.controller.main_hostmetric.PAGE_SIZE', 2)
@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_hostmetric_keyset_pagination(mock_copy_pandas):
    """A full page triggers another query carrying the last row as a keyset marker."""
    page1 = pd.DataFrame({'hostname': ['a', 'b'], 'host_id': [1, 2]})
    page2 = pd.DataFrame({'hostname': ['c'], 'host_id': [3]})
    mock_copy_pandas.side_effect = [page1, page2]

    result = main_hostmetric(db=MagicMock(), since=SINCE).gather()

    assert list(result['hostname']) == ['a', 'b', 'c']
    assert mock_copy_pandas.call_count == 2

    # first page: no marker, only the page-size LIMIT is bound
    first_params = mock_copy_pandas.call_args_list[0][0][2]
    assert first_params == [2]

    # second page: keyset marker from last row of page1 (hostname='b', host_id=2)
    second_query = mock_copy_pandas.call_args_list[1][0][1]
    second_params = mock_copy_pandas.call_args_list[1][0][2]
    assert 'main_hostmetric.hostname > %s' in second_query
    assert second_params == ['b', 'b', 2, 2]


@patch('metrics_utility.library.collectors.util._copy_table_files')
def test_main_hostmetric_csv_output_single_copy(mock_copy_files, tmp_path):
    """The CSV (gather) path streams via a single COPY: no LIMIT, no keyset marker."""
    mock_copy_files.return_value = ['host_metric.csv']

    output = CollectionOutput(str(tmp_path))
    result = main_hostmetric(db=MagicMock(), since=SINCE, until=UNTIL, output=output).gather()

    assert result == ['host_metric.csv']
    mock_copy_files.assert_called_once()

    _db, query, _filespec, params = mock_copy_files.call_args[0]
    assert 'LIMIT' not in query
    assert 'hostname > %s' not in query
    assert SINCE.isoformat() in query
    assert UNTIL.isoformat() in query
    assert params == []


def test_host_metric_query_since_only():
    """since-only filter (via date_where), as used by the Renewal Guidance report."""
    query, params = _host_metric_query(since=SINCE)

    assert f"main_hostmetric.last_automation >= '{SINCE.isoformat()}'" in query
    assert 'main_hostmetric.last_automation <' not in query
    assert 'LIMIT' not in query
    assert params == []


def test_host_metric_query_full_params_order():
    """since/until interpolated via date_where; marker and limit bound in placeholder order."""
    query, params = _host_metric_query(since=SINCE, until=UNTIL, marker=('host9', 5), limit=10000)

    assert SINCE.isoformat() in query
    assert UNTIL.isoformat() in query
    assert 'main_hostmetric.hostname > %s' in query
    assert 'LIMIT %s' in query
    assert params == ['host9', 'host9', 5, 10000]


def test_host_metric_query_no_filters():
    """No bounds produces WHERE true and no bound params."""
    query, params = _host_metric_query()

    assert 'WHERE true' in query
    assert params == []
