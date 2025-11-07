from unittest.mock import MagicMock, patch

from metrics_utility.library.collectors.controller.execution_environments import execution_environments


def test_execution_environments_basic():
    """Test execution_environments collector basic functionality."""
    mock_db = MagicMock()

    instance = execution_environments(db=mock_db)

    # Should have gather method from @collector decorator
    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


def test_execution_environments_with_output_dir():
    """Test execution_environments with custom output_dir."""
    mock_db = MagicMock()
    output_dir = '/tmp/test_output'

    instance = execution_environments(db=mock_db, output_dir=output_dir)

    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['output_dir'] == output_dir


@patch('metrics_utility.library.collectors.controller.execution_environments.copy_table')
def test_execution_environments_calls_copy_table(mock_copy_table):
    """Test that execution_environments calls copy_table with correct parameters."""
    mock_db = MagicMock()
    mock_copy_table.return_value = ['/tmp/main_executionenvironment_table.csv']

    instance = execution_environments(db=mock_db)
    result = instance.gather()

    # Should call copy_table
    mock_copy_table.assert_called_once()
    call_args = mock_copy_table.call_args

    # Verify parameters
    assert call_args[1]['db'] == mock_db
    assert call_args[1]['table'] == 'main_executionenvironment'
    assert 'query' in call_args[1]
    assert call_args[1]['output_dir'] is None

    # Should return the result from copy_table
    assert result == ['/tmp/main_executionenvironment_table.csv']


@patch('metrics_utility.library.collectors.controller.execution_environments.copy_table')
def test_execution_environments_query_structure(mock_copy_table):
    """Test that the SQL query selects from main_executionenvironment."""
    mock_db = MagicMock()
    mock_copy_table.return_value = []

    instance = execution_environments(db=mock_db)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Query should contain the expected table and columns
    assert 'main_executionenvironment' in query.lower()
    assert 'SELECT' in query
    assert 'id' in query
    assert 'created' in query
    assert 'modified' in query
    assert 'image' in query
    assert 'name' in query


@patch('metrics_utility.library.collectors.controller.execution_environments.copy_table')
def test_execution_environments_with_output_dir_passed_to_copy_table(mock_copy_table):
    """Test that output_dir is passed to copy_table."""
    mock_db = MagicMock()
    output_dir = '/custom/output'
    mock_copy_table.return_value = []

    instance = execution_environments(db=mock_db, output_dir=output_dir)
    instance.gather()

    call_args = mock_copy_table.call_args
    assert call_args[1]['output_dir'] == output_dir
