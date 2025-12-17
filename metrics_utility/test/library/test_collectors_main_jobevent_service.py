import datetime

from unittest.mock import MagicMock, patch

from metrics_utility.library.collectors.controller.main_jobevent_service import main_jobevent_service


def test_main_jobevent_service_basic():
    """Test main_jobevent_service collector basic functionality."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_jobevent_service(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


def test_main_jobevent_service_with_output_dir():
    """Test main_jobevent_service with custom output_dir."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    output_dir = '/tmp/test_output'

    instance = main_jobevent_service(db=mock_db, since=since, until=until, output_dir=output_dir)

    assert instance.kwargs['output_dir'] == output_dir


@patch('metrics_utility.library.collectors.controller.main_jobevent_service.copy_table')
def test_main_jobevent_service_no_jobs_returns_none(mock_copy_table):
    """Test that collector returns empty CSV with headers when no jobs are found."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # No jobs found
    mock_cursor.fetchall.return_value = []
    mock_copy_table.return_value = ['/tmp/main_jobevent_table.csv']

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    # Should still call copy_table to generate CSV with headers (even if 0 rows)
    mock_copy_table.assert_called_once()

    # Verify the query has FALSE conditions (returns 0 rows but maintains schema)
    call_args = mock_copy_table.call_args
    query = call_args[1]['query']
    assert 'FALSE' in query  # Should have FALSE for empty job set

    # Should return CSV file path
    assert result == ['/tmp/main_jobevent_table.csv']


@patch('metrics_utility.library.collectors.controller.main_jobevent_service.copy_table')
def test_main_jobevent_service_with_jobs_calls_copy_table(mock_copy_table):
    """Test that collector calls copy_table when jobs are found."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    # Configure mock cursor to simulate psycopg3 (no copy_expert method)
    del mock_cursor.copy_expert
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Mock jobs
    job_created1 = datetime.datetime(2024, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)
    job_created2 = datetime.datetime(2024, 1, 16, 14, 45, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(100, job_created1), (101, job_created2)]

    mock_copy_table.return_value = ['/tmp/main_jobevent_table.csv']

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    # Should call copy_table
    mock_copy_table.assert_called_once()
    call_args = mock_copy_table.call_args

    assert call_args[1]['db'] == mock_db
    assert call_args[1]['table'] == 'main_jobevent'
    assert 'query' in call_args[1]
    assert result == ['/tmp/main_jobevent_table.csv']


@patch('metrics_utility.library.collectors.controller.main_jobevent_service.copy_table')
def test_main_jobevent_service_query_structure(mock_copy_table):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    # Configure mock cursor to simulate psycopg3 (no copy_expert method)
    del mock_cursor.copy_expert
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    job_created = datetime.datetime(2024, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(100, job_created)]
    mock_copy_table.return_value = []

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should query expected tables
    assert 'main_jobevent' in query
    assert 'main_unifiedjob' in query

    # Should have event_data JSON extraction
    assert 'event_data' in query
    assert 'task_action' in query
    assert 'resolved_action' in query
    assert 'duration' in query
    assert 'warnings' in query
    assert 'deprecations' in query


@patch('metrics_utility.library.collectors.controller.main_jobevent_service.copy_table')
def test_main_jobevent_service_builds_temp_table_and_hourly_ranges(mock_copy_table):
    """Test that query uses job_id IN clause and builds hourly timestamp ranges."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    job_created1 = datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=datetime.timezone.utc)
    job_created2 = datetime.datetime(2024, 1, 16, 14, 45, 30, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(100, job_created1), (200, job_created2)]
    mock_copy_table.return_value = []

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should use direct job_id IN clause (no temp table for read-only replica compatibility)
    assert 'e.job_id IN (' in query
    assert '100' in query or '200' in query  # Should contain job IDs

    # Should have hourly timestamp ranges (truncated to hour boundaries)
    # Job 1 at 10:30:45 -> hour range 10:00:00 to 11:00:00
    assert '2024-01-15T10:00:00+00:00' in query
    assert '2024-01-15T11:00:00+00:00' in query

    # Job 2 at 14:45:30 -> hour range 14:00:00 to 15:00:00
    assert '2024-01-16T14:00:00+00:00' in query
    assert '2024-01-16T15:00:00+00:00' in query

    # Should have OR clause for multiple hour ranges
    assert ' OR ' in query

    # Verify only the initial jobs query was executed (no temp table operations)
    assert mock_cursor.execute.call_count == 1

    # Check that no temp table operations were called
    execute_calls = [str(call[0][0]) for call in mock_cursor.execute.call_args_list]
    assert not any('temp_jobevent_service_jobs' in call for call in execute_calls)


@patch('metrics_utility.library.collectors.controller.main_jobevent_service.copy_table')
def test_main_jobevent_service_initial_query_parameters(mock_copy_table):
    """Test that initial jobs query uses correct parameters."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = []

    since = datetime.datetime(2024, 3, 1, 8, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 3, 2, 20, 0, tzinfo=datetime.timezone.utc)

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    # Check that execute was called with correct parameters
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args

    # Should pass since and until as parameters
    params = call_args[0][1]
    assert params['since'] == since
    assert params['until'] == until


@patch('metrics_utility.library.collectors.controller.main_jobevent_service.copy_table')
def test_main_jobevent_service_playbook_stats_handling(mock_copy_table):
    """Test that query handles playbook_on_stats event specially."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    # Configure mock cursor to simulate psycopg3 (no copy_expert method)
    del mock_cursor.copy_expert
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    job_created = datetime.datetime(2024, 1, 15, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(100, job_created)]
    mock_copy_table.return_value = []

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should have CASE statement for playbook_on_stats
    assert 'playbook_on_stats' in query
    assert 'CASE' in query
    assert 'artifact_data' in query
