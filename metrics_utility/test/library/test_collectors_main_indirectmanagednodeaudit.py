from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.main_indirectmanagednodeaudit import main_indirectmanagednodeaudit
from metrics_utility.test.util import utcdt


def test_main_indirectmanagednodeaudit_basic():
    """Test main_indirectmanagednodeaudit collector basic functionality."""
    mock_db = MagicMock()
    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_indirectmanagednodeaudit_calls_copy_table(mock_copy_pandas):
    """Test that main_indirectmanagednodeaudit calls copy_table."""
    mock_db = MagicMock()
    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2], 'canonical_facts': ['{}', '{}']})

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_indirectmanagednodeaudit_query_contains_time_range(mock_copy_pandas):
    """Test that the query includes the time range."""
    mock_db = MagicMock()
    since = utcdt('2024-03-15T10:30:00')
    until = utcdt('2024-03-16T18:45:00')
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Query should contain time boundaries using created field
    assert '2024-03-15' in query
    assert '2024-03-16' in query
    assert 'main_indirectmanagednodeaudit.created >=' in query
    assert 'main_indirectmanagednodeaudit.created <' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_indirectmanagednodeaudit_query_structure(mock_copy_pandas):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = utcdt('2024-01-01')
    until = utcdt('2024-02-01')
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Should query expected tables
    assert 'main_indirectmanagednodeaudit' in query
    assert 'main_job' in query
    assert 'main_unifiedjob' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query

    # Should have expected columns
    assert 'canonical_facts' in query
    assert 'facts' in query
    assert 'events' in query
    assert 'task_runs' in query or 'count' in query
