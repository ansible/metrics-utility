"""
Unit tests for the task_executions_service collector.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.library.collectors.service.task_executions_service import task_executions_service


def test_task_executions_service_basic():
    """Collector has the expected interface produced by the @collector decorator."""
    mock_db = MagicMock()

    instance = task_executions_service(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_task_executions_service_calls_copy_table(mock_copy_pandas):
    """Collector calls _copy_table_pandas with the correct DB and a valid SQL query."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({
        'started_at': [datetime(2025, 6, 13, 1, 0, 0, tzinfo=timezone.utc)],
        'completed_at': [datetime(2025, 6, 13, 1, 0, 5, tzinfo=timezone.utc)],
        'collector_type': ['unified_jobs'],
    })

    since = datetime(2025, 6, 13, 0, 0, 0, tzinfo=timezone.utc)
    until = datetime(2025, 6, 14, 0, 0, 0, tzinfo=timezone.utc)

    instance = task_executions_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_task_executions_service_query_structure(mock_copy_pandas):
    """SQL query references tasks_taskexecution with the expected columns and filters."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    since = datetime(2025, 6, 13, 0, 0, 0, tzinfo=timezone.utc)
    until = datetime(2025, 6, 14, 0, 0, 0, tzinfo=timezone.utc)

    instance = task_executions_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'tasks_taskexecution' in query
    assert 'started_at' in query
    assert 'completed_at' in query
    assert "result_data->'collector_type'" in query
    assert "result_data->'collector_type' IS NOT NULL" in query
    assert '2025-06-13' in query
    assert '2025-06-14' in query

    # Old join-based tables must not appear
    assert 'tasks_hourlymetricscollection' not in query
    assert 'UNION ALL' not in query
    assert 'daily_metrics_rollup' not in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_task_executions_service_returns_empty_dataframe_on_error(mock_copy_pandas):
    """
    Collector returns an empty DataFrame when the underlying DB query raises an
    exception (e.g. when the tasks_* tables do not exist in the test database).
    """
    mock_db = MagicMock()
    mock_copy_pandas.side_effect = Exception('relation "tasks_taskexecution" does not exist')

    since = datetime(2025, 6, 13, 0, 0, 0, tzinfo=timezone.utc)
    until = datetime(2025, 6, 14, 0, 0, 0, tzinfo=timezone.utc)

    instance = task_executions_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == ['started_at', 'completed_at', 'collector_type']


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_task_executions_service_defaults_to_previous_day(mock_copy_pandas):
    """
    When no since/until is provided the query window covers the previous calendar day,
    filtering tasks_taskexecution by started_at.
    """
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = task_executions_service(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'started_at >=' in query
    assert 'started_at <' in query


@pytest.mark.parametrize('since,until', [
    (
        datetime(2025, 6, 13, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2025, 6, 14, 0, 0, 0, tzinfo=timezone.utc),
    ),
])
@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_task_executions_service_returns_dataframe_with_expected_columns(mock_copy_pandas, since, until):
    """Collector returns a DataFrame with the three expected columns."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({
        'started_at': [
            datetime(2025, 6, 13, 1, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 6, 13, 2, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 6, 13, 3, 0, 0, tzinfo=timezone.utc),
        ],
        'completed_at': [
            datetime(2025, 6, 13, 1, 0, 4, tzinfo=timezone.utc),
            datetime(2025, 6, 13, 2, 0, 5, tzinfo=timezone.utc),
            None,
        ],
        'collector_type': ['unified_jobs', 'unified_jobs', 'job_host_summary_service'],
    })

    instance = task_executions_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    assert isinstance(result, pd.DataFrame)
    assert 'started_at' in result.columns
    assert 'completed_at' in result.columns
    assert 'collector_type' in result.columns
    assert len(result) == 3


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_task_executions_service_covers_both_hourly_and_snapshot_collectors(mock_copy_pandas):
    """
    Collector covers both hourly collectors (24 runs/day) and snapshot collectors
    (1 run/day) — all recorded in tasks_taskexecution.
    Pipeline tasks are NOT included; they run after this collector.
    """
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({
        'started_at': [
            datetime(2025, 6, 13, 1, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 6, 13, 2, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 6, 13, 3, 0, 0, tzinfo=timezone.utc),
        ],
        'completed_at': [
            datetime(2025, 6, 13, 1, 0, 4, tzinfo=timezone.utc),
            datetime(2025, 6, 13, 2, 0, 3, tzinfo=timezone.utc),
            datetime(2025, 6, 13, 3, 0, 1, tzinfo=timezone.utc),
        ],
        'collector_type': [
            'unified_jobs',           # hourly
            'unified_jobs',           # hourly (second hour)
            'execution_environments', # snapshot
        ],
    })

    since = datetime(2025, 6, 13, 0, 0, 0, tzinfo=timezone.utc)
    until = datetime(2025, 6, 14, 0, 0, 0, tzinfo=timezone.utc)

    instance = task_executions_service(db=mock_db, since=since, until=until)
    result = instance.gather()

    assert isinstance(result, pd.DataFrame)
    collector_types = set(result['collector_type'].tolist())
    assert 'unified_jobs' in collector_types
    assert 'execution_environments' in collector_types
    # Pipeline tasks must not appear — the query does not ask for them
    assert 'daily_metrics_rollup' not in collector_types
    assert len(result) == 3
