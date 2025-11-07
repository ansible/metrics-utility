from unittest.mock import MagicMock, patch

from metrics_utility.library.collectors.controller.main_host import main_host


def test_main_host_basic():
    """Test main_host collector basic functionality."""
    mock_db = MagicMock()

    instance = main_host(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


def test_main_host_with_output_dir():
    """Test main_host with custom output_dir."""
    mock_db = MagicMock()
    output_dir = '/tmp/test_output'

    instance = main_host(db=mock_db, output_dir=output_dir)

    assert instance.kwargs['output_dir'] == output_dir


@patch('metrics_utility.library.collectors.controller.main_host.copy_table')
def test_main_host_calls_copy_table(mock_copy_table):
    """Test that main_host calls copy_table with correct parameters."""
    mock_db = MagicMock()
    mock_copy_table.return_value = ['/tmp/main_host_table.csv']

    instance = main_host(db=mock_db)
    result = instance.gather()

    mock_copy_table.assert_called_once()
    call_args = mock_copy_table.call_args

    assert call_args[1]['db'] == mock_db
    assert call_args[1]['table'] == 'main_host'
    assert call_args[1]['prepend_query'] is True
    assert 'query' in call_args[1]
    assert result == ['/tmp/main_host_table.csv']


@patch('metrics_utility.library.collectors.controller.main_host.copy_table')
def test_main_host_query_structure(mock_copy_table):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    mock_copy_table.return_value = []

    instance = main_host(db=mock_db)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should query main_host and related tables
    assert 'main_host' in query
    assert 'main_inventory' in query
    assert 'main_organization' in query
    assert 'main_unifiedjob' in query

    # Should have canonical_facts and facts columns
    assert 'canonical_facts' in query
    assert 'facts' in query
    assert 'ansible_host_variable' in query


@patch('metrics_utility.library.collectors.controller.main_host.copy_table')
def test_main_host_filters_enabled_hosts(mock_copy_table):
    """Test that query filters for enabled hosts."""
    mock_db = MagicMock()
    mock_copy_table.return_value = []

    instance = main_host(db=mock_db)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should filter for enabled hosts
    assert "enabled='t'" in query or 'enabled = true' in query.lower()


@patch('metrics_utility.library.collectors.controller.main_host.copy_table')
def test_main_host_uses_yaml_json_functions(mock_copy_table):
    """Test that query uses metrics_utility helper functions."""
    mock_db = MagicMock()
    mock_copy_table.return_value = []

    instance = main_host(db=mock_db)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Should use helper functions for parsing YAML/JSON
    assert 'metrics_utility_is_valid_json' in query
    assert 'metrics_utility_parse_yaml_field' in query
