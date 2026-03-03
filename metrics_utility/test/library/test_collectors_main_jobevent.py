import datetime

from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.main_jobevent import main_jobevent


def test_main_jobevent_basic():
    """Test main_jobevent collector basic functionality."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_jobevent(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_calls_copy_table(mock_copy_pandas):
    """Test that main_jobevent calls copy_table."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2, 3], 'event': ['ok', 'failed', 'ok']})

    instance = main_jobevent(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_query_contains_time_range(mock_copy_pandas):
    """Test that the query includes the time range."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 5, 1, 8, 0, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 5, 2, 20, 30, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Query should contain time boundaries (uses main_jobhostsummary.modified)
    assert '2024-05-01' in query
    assert '2024-05-02' in query
    assert 'main_jobhostsummary.modified >=' in query
    assert 'main_jobhostsummary.modified <' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_query_structure(mock_copy_pandas):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should have CTE
    assert 'WITH' in query
    assert 'job_scope' in query

    # Should query expected tables
    assert 'main_jobevent' in query
    assert 'main_jobhostsummary' in query
    assert 'main_unifiedjob' in query

    # Should have event_data field with JSON extraction
    assert 'event_data' in query
    assert 'task_action' in query
    assert 'resolved_action' in query
    assert 'duration' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_filters_event_types(mock_copy_pandas):
    """Test that query filters for specific event types."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should filter for specific event types
    assert 'runner_on_ok' in query
    assert 'runner_on_failed' in query
    assert 'runner_on_unreachable' in query
    assert 'runner_on_skipped' in query
    assert 'runner_retry' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_unicode_escape_handling(mock_copy_pandas):
    """Test that query handles unicode escapes in event_data."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should have replace() for unicode handling
    assert 'replace' in query
    assert r'\u' in query
