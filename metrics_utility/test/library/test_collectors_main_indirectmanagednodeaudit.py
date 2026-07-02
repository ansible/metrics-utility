from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.main_indirectmanagednodeaudit import main_indirectmanagednodeaudit


SINCE = datetime(2025, 6, 13, 0, 0, 0, tzinfo=timezone.utc)
UNTIL = datetime(2025, 6, 14, 0, 0, 0, tzinfo=timezone.utc)


def test_main_indirectmanagednodeaudit_basic():
    """Test main_indirectmanagednodeaudit collector basic functionality."""
    mock_db = MagicMock()

    instance = main_indirectmanagednodeaudit(db=mock_db, since=SINCE, until=UNTIL)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == SINCE
    assert instance.kwargs['until'] == UNTIL


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_indirectmanagednodeaudit_calls_copy_table(mock_copy_pandas):
    """Test that main_indirectmanagednodeaudit calls copy_table."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2], 'canonical_facts': ['{}', '{}']})

    instance = main_indirectmanagednodeaudit(db=mock_db, since=SINCE, until=UNTIL)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_indirectmanagednodeaudit_query_has_date_filter(mock_copy_pandas):
    """Test that the SQL query filters by main_unifiedjob.finished when since/until are provided."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_indirectmanagednodeaudit(db=mock_db, since=SINCE, until=UNTIL)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'main_indirectmanagednodeaudit' in query
    assert 'main_job' in query
    assert 'main_unifiedjob' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query

    assert 'canonical_facts' in query
    assert 'facts' in query
    assert 'events' in query
    assert 'task_runs' in query

    assert 'main_unifiedjob.finished' in query
    assert '>=' in query
    assert '<' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_indirectmanagednodeaudit_no_date_filter_when_none(mock_copy_pandas):
    """Test that passing since=None, until=None produces WHERE true (no filtering)."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = main_indirectmanagednodeaudit(db=mock_db, since=None, until=None)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'WHERE' in query
    assert 'true' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_indirectmanagednodeaudit_null_join_references(mock_copy_pandas):
    """Records with NULL job_id, inventory_id, or organization_id are returned without error.

    The LEFT JOINs produce NULL for the joined columns; the collector does not filter or raise.
    """
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame(
        {
            'id': [1],
            'created': [None],
            'host_name': ['device-01'],
            'host_remote_id': ['host-abc-1'],
            'canonical_facts': ['{"fqdn": "device-01.example.com"}'],
            'facts': ['{}'],
            'events': ['[]'],
            'task_runs': [3],
            'job_created': [None],
            'job_remote_id': [None],
            'job_template_remote_id': [None],
            'job_template_name': [None],
            'inventory_remote_id': [None],
            'inventory_name': [None],
            'organization_remote_id': [None],
            'organization_name': [None],
            'project_remote_id': [None],
            'project_name': [None],
        }
    )

    instance = main_indirectmanagednodeaudit(db=mock_db, since=SINCE, until=UNTIL)
    result = instance.gather()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result['job_remote_id'].iloc[0] is None or pd.isna(result['job_remote_id'].iloc[0])
    assert result['inventory_remote_id'].iloc[0] is None or pd.isna(result['inventory_remote_id'].iloc[0])
    assert result['organization_remote_id'].iloc[0] is None or pd.isna(result['organization_remote_id'].iloc[0])


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_main_indirectmanagednodeaudit_orphaned_records(mock_copy_pandas):
    """Records referencing a deleted job, inventory, or organization are returned without error.

    A deleted foreign key produces the same NULL join result as a missing FK value;
    the collector returns the row without filtering or raising.
    """
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame(
        {
            'id': [2],
            'created': [None],
            'host_name': ['orphaned-device'],
            'host_remote_id': ['host-abc-2'],
            'canonical_facts': ['{}'],
            'facts': ['{}'],
            'events': ['[]'],
            'task_runs': [1],
            'job_created': [None],
            'job_remote_id': [999],
            'job_template_remote_id': [None],
            'job_template_name': [None],
            'inventory_remote_id': [None],
            'inventory_name': [None],
            'organization_remote_id': [None],
            'organization_name': [None],
            'project_remote_id': [None],
            'project_name': [None],
        }
    )

    instance = main_indirectmanagednodeaudit(db=mock_db, since=SINCE, until=UNTIL)
    result = instance.gather()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result['job_remote_id'].iloc[0] == 999
    assert result['organization_remote_id'].iloc[0] is None or pd.isna(result['organization_remote_id'].iloc[0])
