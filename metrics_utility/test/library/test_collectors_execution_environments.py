from unittest.mock import MagicMock, patch

import pandas as pd

from metrics_utility.library.collectors.controller.execution_environments import execution_environments


def test_execution_environments_basic():
    """Test execution_environments collector basic functionality."""
    mock_db = MagicMock()

    instance = execution_environments(db=mock_db)

    # Should have gather method from @collector decorator
    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_execution_environments_calls_copy_table(mock_copy_pandas):
    """Test that execution_environments calls copy_table with correct parameters."""
    mock_db = MagicMock()
    # Return a DataFrame instead of file paths
    mock_copy_pandas.return_value = pd.DataFrame({'id': [1, 2], 'name': ['ee1', 'ee2']})

    instance = execution_environments(db=mock_db)
    result = instance.gather()

    # Should call copy_table
    mock_copy_pandas.assert_called_once()
    call_args = mock_copy_pandas.call_args

    # Verify parameters
    assert call_args[0][0] == mock_db
    assert len(call_args[0]) >= 2  # db, query

    # Should return the DataFrame from copy_table
    assert isinstance(result, pd.DataFrame)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_execution_environments_query_structure(mock_copy_pandas):
    """Test that the SQL query selects from main_executionenvironment."""
    mock_db = MagicMock()
    mock_copy_pandas.return_value = []

    instance = execution_environments(db=mock_db)
    instance.gather()

    call_args = mock_copy_pandas.call_args
    query = call_args[0][1]

    # Query should contain the expected table and columns
    assert 'main_executionenvironment' in query.lower()
    assert 'SELECT' in query
    assert 'id' in query
    assert 'created' in query
    assert 'modified' in query
    assert 'image' in query
    assert 'name' in query
