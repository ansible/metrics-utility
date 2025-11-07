import datetime

from unittest.mock import MagicMock, patch

from metrics_utility.library.collectors.controller.unified_jobs import unified_jobs


def test_unified_jobs_basic():
    """Test unified_jobs collector basic functionality."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = unified_jobs(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


def test_unified_jobs_with_output_dir():
    """Test unified_jobs with custom output_dir."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    output_dir = '/tmp/test_output'

    instance = unified_jobs(db=mock_db, since=since, until=until, output_dir=output_dir)

    assert instance.kwargs['output_dir'] == output_dir


@patch('metrics_utility.library.collectors.controller.unified_jobs.copy_table')
def test_unified_jobs_calls_copy_table(mock_copy_table):
    """Test that unified_jobs calls copy_table."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = ['/tmp/unified_jobs_table.csv']

    instance = unified_jobs(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_table.assert_called_once()
    call_args = mock_copy_table.call_args

    assert call_args[1]['db'] == mock_db
    assert call_args[1]['table'] == 'unified_jobs'
    assert 'query' in call_args[1]
    assert result == ['/tmp/unified_jobs_table.csv']


@patch('metrics_utility.library.collectors.controller.unified_jobs.copy_table')
def test_unified_jobs_query_contains_time_range(mock_copy_table):
    """Test that the query includes the time range with OR logic."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 6, 2, 14, 30, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Query should contain time boundaries for both created and finished
    assert '2024-06-01' in query
    assert '2024-06-02' in query
    assert 'main_unifiedjob.created >=' in query
    assert 'main_unifiedjob.created <' in query
    assert 'main_unifiedjob.finished >=' in query
    assert 'main_unifiedjob.finished <' in query


@patch('metrics_utility.library.collectors.controller.unified_jobs.copy_table')
def test_unified_jobs_uses_or_logic(mock_copy_table):
    """Test that query uses OR logic for created/finished timestamps."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should use OR to capture jobs created OR finished in the time window
    assert ' OR ' in query


@patch('metrics_utility.library.collectors.controller.unified_jobs.copy_table')
def test_unified_jobs_query_structure(mock_copy_table):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should query expected tables
    assert 'main_unifiedjob' in query
    assert 'main_unifiedjobtemplate' in query
    assert 'django_content_type' in query
    assert 'main_job' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query
    assert 'main_executionenvironment' in query


@patch('metrics_utility.library.collectors.controller.unified_jobs.copy_table')
def test_unified_jobs_excludes_sync_jobs(mock_copy_table):
    """Test that query excludes sync launch type."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should exclude sync jobs
    assert "launch_type != 'sync'" in query or "launch_type <> 'sync'" in query


@patch('metrics_utility.library.collectors.controller.unified_jobs.copy_table')
def test_unified_jobs_includes_execution_environment(mock_copy_table):
    """Test that query includes execution environment information."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should include execution environment image
    assert 'execution_environment_image' in query
