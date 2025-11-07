import datetime

from unittest.mock import MagicMock, patch

from metrics_utility.library.collectors.controller.job_host_summary import job_host_summary


def test_job_host_summary_basic():
    """Test job_host_summary collector basic functionality."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)

    instance = job_host_summary(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


def test_job_host_summary_with_output_dir():
    """Test job_host_summary with custom output_dir."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    output_dir = '/tmp/test_output'

    instance = job_host_summary(db=mock_db, since=since, until=until, output_dir=output_dir)

    assert instance.kwargs['output_dir'] == output_dir


@patch('metrics_utility.library.collectors.controller.job_host_summary.copy_table')
def test_job_host_summary_calls_copy_table(mock_copy_table):
    """Test that job_host_summary calls copy_table with correct parameters."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = ['/tmp/main_jobhostsummary_table.csv']

    instance = job_host_summary(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_table.assert_called_once()
    call_args = mock_copy_table.call_args

    assert call_args[1]['db'] == mock_db
    assert call_args[1]['table'] == 'main_jobhostsummary'
    assert call_args[1]['prepend_query'] is True
    assert 'query' in call_args[1]
    assert result == ['/tmp/main_jobhostsummary_table.csv']


@patch('metrics_utility.library.collectors.controller.job_host_summary.copy_table')
def test_job_host_summary_query_contains_time_range(mock_copy_table):
    """Test that the query includes the time range."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Query should contain time boundaries
    assert '2024-01-01' in query
    assert '2024-01-31' in query
    assert 'main_jobhostsummary.modified >=' in query
    assert 'main_jobhostsummary.modified <' in query


@patch('metrics_utility.library.collectors.controller.job_host_summary.copy_table')
def test_job_host_summary_query_structure(mock_copy_table):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should have CTE and expected tables
    assert 'WITH' in query
    assert 'filtered_hosts' in query
    assert 'hosts_variables' in query
    assert 'main_jobhostsummary' in query
    assert 'main_job' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query


@patch('metrics_utility.library.collectors.controller.job_host_summary.copy_table')
def test_job_host_summary_isoformat(mock_copy_table):
    """Test that datetime objects are converted to isoformat in query."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 6, 15, 12, 30, 45, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 6, 16, 14, 45, 30, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should contain isoformat timestamps
    assert '2024-06-15T12:30:45+00:00' in query
    assert '2024-06-16T14:45:30+00:00' in query
