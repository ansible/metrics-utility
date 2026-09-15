from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.awx.unified_job_template_table import unified_job_template_table


def test_unified_job_template_table_basic():
    """Test unified_job_template_table collector basic functionality."""
    mock_db = MagicMock()

    instance = unified_job_template_table(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_job_template_table_calls_copy_table(mock_copy_pandas):
    """Test that unified_job_template_table calls copy_table with correct parameters."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2], 'name': ['tmpl1', 'tmpl2']})

    instance = unified_job_template_table(db=mock_db)
    result = instance.gather()

    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_job_template_table_query_structure(mock_copy_pandas):
    """Test that the SQL query references expected tables."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = unified_job_template_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'main_unifiedjobtemplate' in query
    assert 'django_content_type' in query
    assert 'main_executionenvironment' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_job_template_table_query_columns(mock_copy_pandas):
    """Test that the SQL query selects expected columns."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = unified_job_template_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'polymorphic_ctype_id' in query
    assert 'execution_environment_image' in query
    assert 'created_by_id' in query
    assert 'modified_by_id' in query
    assert 'last_job_failed' in query
    assert 'last_job_run' in query
    assert 'next_job_run' in query
    assert 'next_schedule_id' in query
    assert 'status' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_job_template_table_order_by(mock_copy_pandas):
    """Test that the query orders by id ASC."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = pd.DataFrame()

    instance = unified_job_template_table(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    assert 'ORDER BY' in query
    assert 'id ASC' in query
