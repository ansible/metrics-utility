from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.main_jobevent_service import main_jobevent_service
from metrics_utility.test.util import mock_cursor_db, utcdt


def test_main_jobevent_service_basic():
    """Test main_jobevent_service collector basic functionality."""
    mock_db = MagicMock()
    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')

    instance = main_jobevent_service(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_no_jobs_returns_none(mock_copy_pandas):
    """Test that collector returns empty CSV with headers when no jobs are found."""
    mock_db, mock_cursor = mock_cursor_db()

    # No jobs found
    mock_cursor.fetchall.return_value = []
    mock_copy_pandas.return_value = pd.DataFrame()

    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    # Should still call copy_table to generate DataFrame (even if 0 rows)
    mock_copy_pandas.assert_called_once()

    # Verify the query has FALSE conditions (returns 0 rows but maintains schema)
    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]
    assert 'FALSE' in query  # Should have FALSE for empty job set

    # Should return DataFrame
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_with_jobs_calls_copy_table(mock_copy_pandas):
    """Test that collector calls copy_table when jobs are found."""
    mock_db, mock_cursor = mock_cursor_db()

    # Mock jobs
    job_created1 = utcdt('2024-01-15T10:30:00')
    job_created2 = utcdt('2024-01-16T14:45:00')
    mock_cursor.fetchall.return_value = [(100, job_created1), (101, job_created2)]

    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2, 3], 'job_id': [100, 100, 101]})

    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    # Should call copy_table
    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_query_structure(mock_copy_pandas):
    """Test that the SQL query has expected structure."""
    mock_db, mock_cursor = mock_cursor_db()

    job_created = utcdt('2024-01-15T10:30:00')
    mock_cursor.fetchall.return_value = [(100, job_created)]
    mock_copy_pandas.return_value = pd.DataFrame()

    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

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

    # Should have ansible_version from unified_job
    assert 'uj.ansible_version' in query or 'ansible_version' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_builds_temp_table_and_hourly_ranges(mock_copy_pandas):
    """Test that query uses job_id IN clause and builds hourly timestamp ranges."""
    mock_db, mock_cursor = mock_cursor_db()

    job_created1 = utcdt('2024-01-15T10:30:45')
    job_created2 = utcdt('2024-01-16T14:45:30')
    mock_cursor.fetchall.return_value = [(100, job_created1), (200, job_created2)]
    mock_copy_pandas.return_value = pd.DataFrame()

    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

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


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_null_job_created(mock_copy_pandas):
    """Test that jobs with NULL job_created are skipped in hour boundary calculation."""
    mock_db, mock_cursor = mock_cursor_db()

    job_created = utcdt('2024-01-15T10:30:00')
    mock_cursor.fetchall.return_value = [(100, job_created), (200, None)]
    mock_copy_pandas.return_value = pd.DataFrame()

    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Both job IDs should be in the IN clause
    assert '100' in query
    assert '200' in query

    # Only job 100's hour boundary should appear (job 200 has NULL created)
    assert '2024-01-15T10:00:00+00:00' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_initial_query_parameters(mock_copy_pandas):
    """Test that initial jobs query uses correct parameters."""
    mock_db, mock_cursor = mock_cursor_db()

    mock_cursor.fetchall.return_value = []

    since = utcdt('2024-03-01T08:00:00')
    until = utcdt('2024-03-02T20:00:00')

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    # Check that execute was called with correct parameters
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args

    # Should pass since and until as parameters
    params = call_args[0][1]
    assert params['since'] == since
    assert params['until'] == until


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_playbook_stats_handling(mock_copy_pandas):
    """Test that query handles playbook_on_stats event specially."""
    mock_db, mock_cursor = mock_cursor_db()

    job_created = utcdt('2024-01-15')
    mock_cursor.fetchall.return_value = [(100, job_created)]
    mock_copy_pandas.return_value = pd.DataFrame()

    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should have CASE statement for playbook_on_stats
    assert 'playbook_on_stats' in query
    assert 'CASE' in query
    assert 'artifact_data' in query
