import datetime

from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.awx.workflow_job_node_table import workflow_job_node_table


def test_workflow_job_node_table_basic():
    """Test workflow_job_node_table collector basic functionality."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)

    instance = workflow_job_node_table(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_node_table_calls_copy_table(mock_copy_pandas):
    """Test that workflow_job_node_table calls copy_table."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2]})

    instance = workflow_job_node_table(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_node_table_query_structure(mock_copy_pandas):
    """Test that the SQL query references expected tables."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_node_table(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'main_workflowjobnode' in query
    assert 'main_workflowjobnode_success_nodes' in query
    assert 'main_workflowjobnode_failure_nodes' in query
    assert 'main_workflowjobnode_always_nodes' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_node_table_array_agg(mock_copy_pandas):
    """Test that the SQL query uses ARRAY_AGG for node edges."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_node_table(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'ARRAY_AGG' in query
    assert 'success_nodes' in query
    assert 'failure_nodes' in query
    assert 'always_nodes' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_node_table_query_contains_time_range(mock_copy_pandas):
    """Test that the query includes the time range for modified."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 6, 2, 14, 30, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_node_table(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert '2024-06-01' in query
    assert '2024-06-02' in query
    assert 'main_workflowjobnode.modified >=' in query
    assert 'main_workflowjobnode.modified <' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_node_table_query_columns(mock_copy_pandas):
    """Test that the SQL query selects expected columns."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_node_table(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'job_id' in query
    assert 'unified_job_template_id' in query
    assert 'workflow_job_id' in query
    assert 'inventory_id' in query
    assert 'do_not_run' in query
    assert 'all_parents_must_converge' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_node_table_order_by(mock_copy_pandas):
    """Test that the query orders by id ASC."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_node_table(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'ORDER BY' in query
    assert 'id ASC' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_node_table_isoformat(mock_copy_pandas):
    """Test that datetime objects are converted to isoformat in query."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 7, 20, 8, 15, 30, tzinfo=datetime.UTC)
    until = datetime.datetime(2024, 7, 21, 16, 45, 0, tzinfo=datetime.UTC)
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_node_table(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert '2024-07-20T08:15:30+00:00' in query
    assert '2024-07-21T16:45:00+00:00' in query
