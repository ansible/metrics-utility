import datetime
import logging

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.library.collectors.controller.main_jobevent_created_service import (
    _normalize_row_limit,
    main_jobevent_created_service,
)

_UTC = datetime.timezone.utc

_SINCE = datetime.datetime(2025, 6, 13, 10, 0, 0, tzinfo=_UTC)
_UNTIL = datetime.datetime(2025, 6, 13, 11, 0, 0, tzinfo=_UTC)


# ---------------------------------------------------------------------------
# Basic instantiation
# ---------------------------------------------------------------------------


def test_basic_instantiation():
    mock_db = MagicMock()
    instance = main_jobevent_created_service(db=mock_db, since=_SINCE, until=_UNTIL)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == _SINCE
    assert instance.kwargs['until'] == _UNTIL


# ---------------------------------------------------------------------------
# Window validation
# ---------------------------------------------------------------------------


def test_raises_when_since_is_none():
    mock_db = MagicMock()
    instance = main_jobevent_created_service(db=mock_db, since=None, until=_UNTIL)
    with pytest.raises(ValueError, match='both since and until must be provided'):
        instance.gather()


def test_raises_when_until_is_none():
    mock_db = MagicMock()
    instance = main_jobevent_created_service(db=mock_db, since=_SINCE, until=None)
    with pytest.raises(ValueError, match='both since and until must be provided'):
        instance.gather()


def test_raises_when_window_is_not_one_hour():
    mock_db = MagicMock()
    bad_until = _SINCE + datetime.timedelta(hours=2)
    instance = main_jobevent_created_service(db=mock_db, since=_SINCE, until=bad_until)
    with pytest.raises(ValueError, match='since-until window must be exactly one hour'):
        instance.gather()


def test_raises_when_window_is_less_than_one_hour():
    mock_db = MagicMock()
    bad_until = _SINCE + datetime.timedelta(minutes=30)
    instance = main_jobevent_created_service(db=mock_db, since=_SINCE, until=bad_until)
    with pytest.raises(ValueError, match='since-until window must be exactly one hour'):
        instance.gather()


# ---------------------------------------------------------------------------
# Query structure
# ---------------------------------------------------------------------------


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_query_filters_on_job_created(mock_copy_pandas):
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL)
    instance.gather()

    query = mock_copy_pandas.call_args[0][1]
    assert 'e.job_created >=' in query
    assert 'e.job_created <' in query
    assert _SINCE.isoformat() in query
    assert _UNTIL.isoformat() in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_query_has_no_job_id_in_clause(mock_copy_pandas):
    """New collector should not do a pre-fetch of job IDs."""
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL)
    instance.gather()

    query = mock_copy_pandas.call_args[0][1]
    assert 'job_id IN' not in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_query_joins_main_unifiedjob(mock_copy_pandas):
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL)
    instance.gather()

    query = mock_copy_pandas.call_args[0][1]
    assert 'main_jobevent' in query
    assert 'main_unifiedjob' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_query_extracts_json_fields(mock_copy_pandas):
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL)
    instance.gather()

    query = mock_copy_pandas.call_args[0][1]
    for field in ('task_action', 'resolved_action', 'duration', 'warnings', 'deprecations'):
        assert field in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_query_has_playbook_on_stats_case(mock_copy_pandas):
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL)
    instance.gather()

    query = mock_copy_pandas.call_args[0][1]
    assert 'playbook_on_stats' in query
    assert 'CASE' in query
    assert 'artifact_data' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_query_has_ansible_version(mock_copy_pandas):
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL)
    instance.gather()

    query = mock_copy_pandas.call_args[0][1]
    assert 'ansible_version' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_query_has_limit_clause_by_default(mock_copy_pandas):
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL)
    instance.gather()

    query = mock_copy_pandas.call_args[0][1]
    assert 'LIMIT' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_query_has_no_limit_clause_when_row_limit_zero(mock_copy_pandas):
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL, row_limit=0)
    instance.gather()

    query = mock_copy_pandas.call_args[0][1]
    assert 'LIMIT' not in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_no_cursor_execute_called(mock_copy_pandas):
    """Collector must not do a separate cursor pre-query for jobs."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_jobevent_created_service(db=mock_db, since=_SINCE, until=_UNTIL)
    instance.gather()

    mock_db.cursor.assert_not_called()


# ---------------------------------------------------------------------------
# Row limit logging
# ---------------------------------------------------------------------------


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_row_limit_reached_logs_info(mock_copy_pandas, caplog):
    mock_copy_pandas.return_value = pd.DataFrame({'id': range(5)})

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL, row_limit=5)
    with caplog.at_level(logging.INFO):
        instance.gather()

    assert any('row limit reached' in r.message for r in caplog.records)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_row_limit_not_reached_no_log(mock_copy_pandas, caplog):
    mock_copy_pandas.return_value = pd.DataFrame({'id': range(3)})

    instance = main_jobevent_created_service(db=MagicMock(), since=_SINCE, until=_UNTIL, row_limit=5)
    with caplog.at_level(logging.INFO):
        instance.gather()

    assert not any('row limit reached' in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _normalize_row_limit
# ---------------------------------------------------------------------------


def test_normalize_row_limit_none_returns_none():
    assert _normalize_row_limit(None) is None


def test_normalize_row_limit_valid_positive():
    assert _normalize_row_limit(500) == 500


def test_normalize_row_limit_zero_returns_none():
    assert _normalize_row_limit(0) is None


def test_normalize_row_limit_negative_falls_back_to_default():
    from metrics_utility.library.collectors.controller.main_jobevent_created_service import _DEFAULT_ROW_LIMIT
    assert _normalize_row_limit(-1) == _DEFAULT_ROW_LIMIT


def test_normalize_row_limit_invalid_string_falls_back_to_default():
    from metrics_utility.library.collectors.controller.main_jobevent_created_service import _DEFAULT_ROW_LIMIT
    assert _normalize_row_limit('bad') == _DEFAULT_ROW_LIMIT


def test_normalize_row_limit_string_number_coerced():
    assert _normalize_row_limit('42') == 42
