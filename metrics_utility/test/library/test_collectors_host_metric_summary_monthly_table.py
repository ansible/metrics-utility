from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.awx.host_metric_summary_monthly_table import (
    host_metric_summary_monthly_table,
)


def test_host_metric_summary_monthly_table_basic():
    """Test host_metric_summary_monthly_table collector basic functionality."""
    mock_db = MagicMock()

    instance = host_metric_summary_monthly_table(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_host_metric_summary_monthly_table_calls_copy_table(mock_copy_pandas):
    """Test that host_metric_summary_monthly_table calls copy_table."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2]})

    instance = host_metric_summary_monthly_table(db=mock_db)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_host_metric_summary_monthly_table_query_structure(mock_copy_pandas):
    """Test that the SQL query references expected table."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = host_metric_summary_monthly_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'main_hostmetricsummarymonthly' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_host_metric_summary_monthly_table_query_columns(mock_copy_pandas):
    """Test that the SQL query selects all 7 expected columns."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = host_metric_summary_monthly_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'id' in query
    assert 'date' in query
    assert 'license_capacity' in query
    assert 'license_consumed' in query
    assert 'hosts_added' in query
    assert 'hosts_deleted' in query
    assert 'indirectly_managed_hosts' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_host_metric_summary_monthly_table_order_by(mock_copy_pandas):
    """Test that the query orders by id ASC."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = host_metric_summary_monthly_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'ORDER BY' in query
    assert 'id ASC' in query
