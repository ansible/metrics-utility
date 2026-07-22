import datetime

from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.main_jobevent_service import (
    _build_job_created_ranges,
    _build_timestamp_where,
    _normalize_limit,
    _select_jobs_by_partition_density,
    main_jobevent_service,
)


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


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_no_jobs_returns_none(mock_copy_pandas):
    """Test that collector returns empty CSV with headers when no jobs are found."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # No jobs found
    mock_cursor.fetchall.return_value = []
    mock_copy_pandas.return_value = pd.DataFrame()

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

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
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Mock jobs
    job_created1 = datetime.datetime(2024, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)
    job_created2 = datetime.datetime(2024, 1, 16, 14, 45, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(100, job_created1), (101, job_created2)]

    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2, 3], 'job_id': [100, 100, 101]})

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

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
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    job_created = datetime.datetime(2024, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(100, job_created)]
    mock_copy_pandas.return_value = pd.DataFrame()

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

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
    assert 'octet_length(e.event_data)' in query
    assert 'event_data_length' in query

    # Should have ansible_version from unified_job
    assert 'uj.ansible_version' in query or 'ansible_version' in query

    # ignore_errors must fall back to res._ansible_ignore_errors, since the
    # awx_display callback only sets the top-level key for runner_on_failed
    # and leaves runner_item_on_failed/runner_on_async_failed without it.
    assert "event_data->>'ignore_errors'" in query
    assert "event_data->'res'->>'_ansible_ignore_errors'" in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_builds_temp_table_and_hourly_ranges(mock_copy_pandas):
    """Test that query uses job_id IN clause and builds hourly timestamp ranges."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    job_created1 = datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=datetime.timezone.utc)
    job_created2 = datetime.datetime(2024, 1, 16, 14, 45, 30, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(100, job_created1), (200, job_created2)]
    mock_copy_pandas.return_value = pd.DataFrame()

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

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
def test_main_jobevent_service_initial_query_parameters(mock_copy_pandas):
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


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_annotation_events_only(mock_copy_pandas):
    """Test that query collects warning/deprecated and excludes playbook_on_* lifecycle events."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    job_created = datetime.datetime(2024, 1, 15, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(100, job_created)]
    mock_copy_pandas.return_value = pd.DataFrame()

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_jobevent_service(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert "'warning'" in query
    assert "'deprecated'" in query
    assert 'playbook_on_task_start' not in query
    assert 'playbook_on_stats' not in query
    assert 'artifact_data' not in query


# ---------------------------------------------------------------------------
# _normalize_limit
# ---------------------------------------------------------------------------


def test_normalize_limit_none_returns_none():
    assert _normalize_limit(None, 100, 'x') is None


def test_normalize_limit_valid_positive():
    assert _normalize_limit(500, 100, 'x') == 500


def test_normalize_limit_zero_returns_none():
    assert _normalize_limit(0, 100, 'x') is None


def test_normalize_limit_negative_falls_back_to_default():
    assert _normalize_limit(-1, 100, 'x') == 100


def test_normalize_limit_invalid_string_falls_back_to_default():
    assert _normalize_limit('bad', 100, 'x') == 100


def test_normalize_limit_string_number_coerced():
    assert _normalize_limit('42', 100, 'x') == 42


# ---------------------------------------------------------------------------
# _select_jobs_by_partition_density
# ---------------------------------------------------------------------------


def _make_jobs(hour, count, start_id=0):
    """Helper: create (job_id, job_created) tuples for a given hour."""
    ts = datetime.datetime(2024, 1, 1, hour, 30, tzinfo=datetime.timezone.utc)
    return [(start_id + i, ts) for i in range(count)]


def test_select_jobs_density_picks_densest_partition_first():
    sparse = _make_jobs(1, 2, start_id=0)  # 2 jobs in hour 1
    dense = _make_jobs(2, 10, start_id=100)  # 10 jobs in hour 2
    all_jobs = sparse + dense

    selected = _select_jobs_by_partition_density(all_jobs, job_limit=5)

    selected_ids = {j[0] for j in selected}
    # All 5 should come from the dense partition (ids 100-109)
    assert all(jid >= 100 for jid in selected_ids)
    assert len(selected) == 5


def test_select_jobs_density_fills_budget_across_partitions():
    p1 = _make_jobs(1, 8, start_id=0)
    p2 = _make_jobs(2, 6, start_id=100)
    all_jobs = p1 + p2

    selected = _select_jobs_by_partition_density(all_jobs, job_limit=10)
    assert len(selected) == 10


def test_select_jobs_density_no_limit_returns_all():
    all_jobs = _make_jobs(1, 5) + _make_jobs(2, 3)
    selected = _select_jobs_by_partition_density(all_jobs, job_limit=None)
    assert len(selected) == 8


def test_select_jobs_density_budget_exhausted_breaks_early():
    """When budget fills exactly, remaining partitions are not added."""
    p1 = _make_jobs(1, 5, start_id=0)
    p2 = _make_jobs(2, 5, start_id=100)
    p3 = _make_jobs(3, 5, start_id=200)
    all_jobs = p1 + p2 + p3

    selected = _select_jobs_by_partition_density(all_jobs, job_limit=5)
    assert len(selected) == 5


def test_select_jobs_density_null_job_created_last():
    normal = _make_jobs(1, 3, start_id=0)
    null_jobs = [(99, None), (98, None)]
    all_jobs = normal + null_jobs

    selected = _select_jobs_by_partition_density(all_jobs, job_limit=3)
    selected_ids = {j[0] for j in selected}
    # Normal jobs should be preferred over NULL-created jobs
    assert 98 not in selected_ids
    assert 99 not in selected_ids


# ---------------------------------------------------------------------------
# _build_job_created_ranges
# ---------------------------------------------------------------------------


def test_build_ranges_consecutive_hours_merged():
    jobs = [
        (1, datetime.datetime(2024, 1, 1, 1, 30, tzinfo=datetime.timezone.utc)),
        (2, datetime.datetime(2024, 1, 1, 2, 15, tzinfo=datetime.timezone.utc)),
        (3, datetime.datetime(2024, 1, 1, 3, 45, tzinfo=datetime.timezone.utc)),
    ]
    ranges = _build_job_created_ranges(jobs)
    assert len(ranges) == 1
    assert ranges[0][0].hour == 1
    assert ranges[0][1].hour == 4


def test_build_ranges_null_job_created_skipped():
    jobs = [(1, None), (2, datetime.datetime(2024, 1, 1, 5, tzinfo=datetime.timezone.utc))]
    ranges = _build_job_created_ranges(jobs)
    assert len(ranges) == 1


def test_build_ranges_non_consecutive_hours_separate():
    jobs = [
        (1, datetime.datetime(2024, 1, 1, 1, tzinfo=datetime.timezone.utc)),
        (2, datetime.datetime(2024, 1, 1, 5, tzinfo=datetime.timezone.utc)),
    ]
    ranges = _build_job_created_ranges(jobs)
    assert len(ranges) == 2


# ---------------------------------------------------------------------------
# _build_timestamp_where
# ---------------------------------------------------------------------------


def test_build_timestamp_where_empty_ranges_returns_false():
    assert _build_timestamp_where([]) == 'FALSE'


def test_build_timestamp_where_single_range():
    r = (
        datetime.datetime(2024, 1, 1, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2024, 1, 1, 2, tzinfo=datetime.timezone.utc),
    )
    result = _build_timestamp_where([r])
    assert 'OR' not in result
    assert '2024-01-01T01:00:00' in result


# ---------------------------------------------------------------------------
# Logging paths in main_jobevent_service
# ---------------------------------------------------------------------------


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_job_limit_logs_info(mock_copy_pandas, caplog):
    """Job limit reached → info log."""
    import logging

    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Return more jobs than the limit
    job_created = datetime.datetime(2024, 1, 1, 1, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(i, job_created) for i in range(5)]
    mock_copy_pandas.return_value = pd.DataFrame()

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)

    with caplog.at_level(logging.INFO):
        instance = main_jobevent_service(db=mock_db, since=since, until=until, job_limit=2)
        instance.gather()

    assert any('job limit reached' in r.message for r in caplog.records)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_jobevent_service_row_limit_logs_info(mock_copy_pandas, caplog):
    """Row limit reached → info log."""
    import logging

    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    job_created = datetime.datetime(2024, 1, 1, 1, tzinfo=datetime.timezone.utc)
    mock_cursor.fetchall.return_value = [(1, job_created)]
    # Return a DataFrame with exactly row_limit rows to trigger the log
    mock_copy_pandas.return_value = pd.DataFrame({'id': range(3)})

    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)

    with caplog.at_level(logging.INFO):
        instance = main_jobevent_service(db=mock_db, since=since, until=until, row_limit=3)
        instance.gather()

    assert any('row limit reached' in r.message for r in caplog.records)
