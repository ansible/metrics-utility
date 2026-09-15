from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.awx.workflow_job_template_node_table import (
    workflow_job_template_node_table,
)


def test_workflow_job_template_node_table_basic():
    """Test workflow_job_template_node_table collector basic functionality."""
    mock_db = MagicMock()

    instance = workflow_job_template_node_table(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_template_node_table_calls_copy_table(mock_copy_pandas):
    """Test that workflow_job_template_node_table calls copy_table."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2]})

    instance = workflow_job_template_node_table(db=mock_db)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_template_node_table_query_structure(mock_copy_pandas):
    """Test that the SQL query references expected tables."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_template_node_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'main_workflowjobtemplatenode' in query
    assert 'main_workflowjobtemplatenode_success_nodes' in query
    assert 'main_workflowjobtemplatenode_failure_nodes' in query
    assert 'main_workflowjobtemplatenode_always_nodes' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_template_node_table_array_agg(mock_copy_pandas):
    """Test that the SQL query uses ARRAY_AGG for node edges."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_template_node_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'ARRAY_AGG' in query
    assert 'success_nodes' in query
    assert 'failure_nodes' in query
    assert 'always_nodes' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_template_node_table_query_columns(mock_copy_pandas):
    """Test that the SQL query selects expected columns."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_template_node_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'unified_job_template_id' in query
    assert 'workflow_job_template_id' in query
    assert 'inventory_id' in query
    assert 'all_parents_must_converge' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_workflow_job_template_node_table_order_by(mock_copy_pandas):
    """Test that the query orders by id ASC."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = workflow_job_template_node_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'ORDER BY' in query
    assert 'id ASC' in query
