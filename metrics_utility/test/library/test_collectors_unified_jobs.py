import datetime

from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.unified_jobs import unified_jobs


def test_unified_jobs_basic():
    """Test unified_jobs collector basic functionality."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)

    instance = unified_jobs(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_jobs_calls_copy_table(mock_copy_pandas):
    """Test that unified_jobs calls copy_table."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2], 'name': ['job1', 'job2']})

    instance = unified_jobs(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_jobs_query_contains_time_range(mock_copy_pandas):
    """Test that the query includes the time range for finished timestamp."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 6, 2, 14, 30, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Query should contain time boundaries for finished timestamp
    assert '2024-06-01' in query
    assert '2024-06-02' in query
    assert 'main_unifiedjob.finished >=' in query
    assert 'main_unifiedjob.finished <' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_jobs_uses_finished_filter(mock_copy_pandas):
    """Test that query filters by finished timestamp only."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should filter by finished timestamp only (no OR logic)
    assert 'main_unifiedjob.finished >=' in query
    assert 'main_unifiedjob.finished <' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_jobs_query_structure(mock_copy_pandas):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should query expected tables
    assert 'main_unifiedjob' in query
    assert 'main_unifiedjobtemplate' in query
    assert 'django_content_type' in query
    assert 'main_job' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query
    assert 'main_executionenvironment' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_jobs_includes_all_jobs(mock_copy_pandas):
    """Test that query includes all jobs including sync jobs."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should include all jobs (no sync exclusion)
    assert "launch_type != 'sync'" not in query
    assert "launch_type <> 'sync'" not in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_jobs_includes_execution_environment(mock_copy_pandas):
    """Test that query includes execution environment information."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = unified_jobs(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should include execution environment image
    assert 'execution_environment_image' in query
