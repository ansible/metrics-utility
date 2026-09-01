import datetime

from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.main_host import main_host, main_host_daily


def test_main_host_basic():
    """Test main_host collector basic functionality."""
    mock_db = MagicMock()

    instance = main_host(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_host_calls_copy_table(mock_copy_pandas):
    """Test that main_host calls copy_table with correct parameters."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2, 3], 'name': ['host1', 'host2', 'host3']})

    instance = main_host(db=mock_db)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_host_query_structure(mock_copy_pandas):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_host(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should query main_host and related tables
    assert 'main_host' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query
    assert 'main_jobhostsummary' in query
    assert 'main_unifiedjob' in query
    assert 'main_host.last_job_id' not in query
    assert 'main_host.last_job_host_summary_id' not in query
    assert 'ORDER BY main_jobhostsummary.id DESC' in query
    assert 'LIMIT 1' in query

    # Should have canonical_facts and facts columns
    assert 'canonical_facts' in query
    assert 'facts' in query
    assert 'ansible_host_variable' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_host_filters_enabled_hosts(mock_copy_pandas):
    """Test that query filters for enabled hosts."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_host(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should filter for enabled hosts
    assert "enabled='t'" in query or 'enabled = true' in query.lower()


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_host_uses_yaml_json_functions(mock_copy_pandas):
    """Test that query uses metrics_utility helper functions."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_host(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should use helper functions for parsing YAML/JSON
    assert 'metrics_utility_is_valid_json' in query
    assert 'metrics_utility_parse_yaml_field' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_host_daily_uses_latest_summary_query(mock_copy_pandas):
    """The incremental collector uses the same post-0210-compatible lookup."""
    mock_copy_pandas.return_value = pd.DataFrame()

    main_host_daily(
        db=MagicMock(),
        since=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
        until=datetime.datetime(2025, 1, 2, tzinfo=datetime.UTC),
    ).gather()

    query = mock_copy_pandas.call_args[0][1]
    assert 'main_jobhostsummary.host_id = main_host.id' in query
    assert 'main_unifiedjob.id = latest_job_host_summary.job_id' in query
    assert 'main_host.last_job_id' not in query
    assert 'main_host.last_job_host_summary_id' not in query
