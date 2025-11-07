import datetime

from unittest.mock import MagicMock, patch

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


def test_job_host_summary_service_with_output_dir():
    """Test job_host_summary_service with custom output_dir."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    output_dir = '/tmp/test_output'

    instance = job_host_summary_service(db=mock_db, since=since, until=until, output_dir=output_dir)

    assert instance.kwargs['output_dir'] == output_dir


@patch('metrics_utility.library.collectors.controller.job_host_summary_service.copy_table')
def test_job_host_summary_service_calls_copy_table(mock_copy_table):
    """Test that job_host_summary_service calls copy_table with correct parameters."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = ['/tmp/main_jobhostsummary_table.csv']

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_table.assert_called_once()
    call_args = mock_copy_table.call_args

    assert call_args[1]['db'] == mock_db
    assert call_args[1]['table'] == 'main_jobhostsummary'
    assert call_args[1]['prepend_query'] is True
    assert 'query' in call_args[1]
    assert result == ['/tmp/main_jobhostsummary_table.csv']


@patch('metrics_utility.library.collectors.controller.job_host_summary_service.copy_table')
def test_job_host_summary_service_query_contains_time_range(mock_copy_table):
    """Test that the query includes the time range."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 3, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 3, 15, 18, 30, 0, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Query should contain time boundaries (uses mu.finished)
    assert '2024-03-01' in query
    assert '2024-03-15' in query
    assert 'mu.finished >=' in query
    assert 'mu.finished <' in query


@patch('metrics_utility.library.collectors.controller.job_host_summary_service.copy_table')
def test_job_host_summary_service_query_structure(mock_copy_table):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

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


@patch('metrics_utility.library.collectors.controller.job_host_summary_service.copy_table')
def test_job_host_summary_service_filters_by_finished_jobs(mock_copy_table):
    """Test that query filters jobs by finished timestamp."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should filter by finished timestamp in the CTE
    assert 'finished IS NOT NULL' in query


@patch('metrics_utility.library.collectors.controller.job_host_summary_service.copy_table')
def test_job_host_summary_service_uses_yaml_json_functions(mock_copy_table):
    """Test that query uses metrics_utility helper functions."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should use helper functions for parsing YAML/JSON
    assert 'metrics_utility_is_valid_json' in query
    assert 'metrics_utility_parse_yaml_field' in query


@patch('metrics_utility.library.collectors.controller.job_host_summary_service.copy_table')
def test_job_host_summary_service_orders_by_finished(mock_copy_table):
    """Test that query orders results by job finished time."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should order by finished timestamp
    assert 'ORDER BY' in query
    assert 'finished' in query


@patch('metrics_utility.library.collectors.controller.job_host_summary_service.copy_table')
def test_job_host_summary_service_isoformat(mock_copy_table):
    """Test that datetime objects are converted to isoformat in query."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 7, 20, 8, 15, 30, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 7, 21, 16, 45, 0, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = job_host_summary_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should contain isoformat timestamps
    assert '2024-07-20T08:15:30+00:00' in query
    assert '2024-07-21T16:45:00+00:00' in query
