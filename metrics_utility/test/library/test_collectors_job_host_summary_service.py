import datetime

from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.job_host_summary_service import job_host_summary_service


def test_job_host_summary_service_basic():
    """Test job_host_summary_service collector basic functionality."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)

    instance = job_host_summary_service(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_service_calls_copy_table(mock_copy_pandas):
    """Test that job_host_summary_service calls copy_table with correct parameters."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2], 'host_id': [10, 20], 'job_id': [100, 101]})

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_service_query_contains_time_range(mock_copy_pandas):
    """Test that the query includes the time range."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 3, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 3, 15, 18, 30, 0, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Query should contain time boundaries (uses mu.finished)
    assert '2024-03-01' in query
    assert '2024-03-15' in query
    assert 'mu.finished >=' in query
    assert 'mu.finished <' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_service_query_structure(mock_copy_pandas):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should have CTEs for filtering
    assert 'WITH' in query
    assert 'filtered_jobs' in query
    assert 'filtered_hosts' in query
    assert 'hosts_variables' in query

    # Should query expected tables
    assert 'main_jobhostsummary' in query
    assert 'main_job' in query
    assert 'main_unifiedjob' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_service_filters_by_finished_jobs(mock_copy_pandas):
    """Test that query filters jobs by finished timestamp."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should filter by finished timestamp in the CTE
    assert 'finished IS NOT NULL' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_service_doesnt_yaml_json_functions(mock_copy_pandas):
    """Test that query doesn't use metrics_utility SQL helper functions."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should use helper functions for parsing YAML/JSON
    assert 'metrics_utility_is_valid_json' not in query
    assert 'metrics_utility_parse_yaml_field' not in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_service_orders_by_finished(mock_copy_pandas):
    """Test that query orders results by job finished time."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should order by finished timestamp
    assert 'ORDER BY' in query
    assert 'finished' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_job_host_summary_service_isoformat(mock_copy_pandas):
    """Test that datetime objects are converted to isoformat in query."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 7, 20, 8, 15, 30, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 7, 21, 16, 45, 0, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should contain isoformat timestamps
    assert '2024-07-20T08:15:30+00:00' in query
    assert '2024-07-21T16:45:00+00:00' in query
