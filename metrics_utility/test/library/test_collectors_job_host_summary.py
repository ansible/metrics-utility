from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.job_host_summary import job_host_summary
from metrics_utility.test.util import utcdt


def test_job_host_summary_basic():
    """Test job_host_summary collector basic functionality."""
    mock_db = MagicMock()
    since = utcdt('2024-01-01')
    until = utcdt('2024-01-31T23:59:59')

    instance = job_host_summary(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_calls_copy_table(mock_copy_pandas):
    """Test that job_host_summary calls copy_table with correct parameters."""
    mock_db = MagicMock()
    since = utcdt('2024-01-01')
    until = utcdt('2024-01-31T23:59:59')
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2], 'host_id': [10, 20]})

    instance = job_host_summary(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_query_contains_time_range(mock_copy_pandas):
    """Test that the query includes the time range."""
    mock_db = MagicMock()
    since = utcdt('2024-01-01')
    until = utcdt('2024-01-31T23:59:59')
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Query should contain time boundaries
    assert '2024-01-01' in query
    assert '2024-01-31' in query
    assert 'main_jobhostsummary.modified >=' in query
    assert 'main_jobhostsummary.modified <' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_query_structure(mock_copy_pandas):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should have CTE and expected tables
    assert 'WITH' in query
    assert 'filtered_hosts' in query
    assert 'hosts_variables' in query
    assert 'main_jobhostsummary' in query
    assert 'main_job' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_isoformat(mock_copy_pandas):
    """Test that datetime objects are converted to isoformat in query."""
    mock_db = MagicMock()
    since = utcdt('2024-06-15T12:30:45')
    until = utcdt('2024-06-16T14:45:30')
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should contain isoformat timestamps
    assert '2024-06-15T12:30:45+00:00' in query
    assert '2024-06-16T14:45:30+00:00' in query
