import datetime

from unittest.mock import MagicMock, patch

from metrics_utility.library.collectors.controller.main_indirectmanagednodeaudit import main_indirectmanagednodeaudit


def test_main_indirectmanagednodeaudit_basic():
    """Test main_indirectmanagednodeaudit collector basic functionality."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == since
    assert instance.kwargs['until'] == until


def test_main_indirectmanagednodeaudit_with_output_dir():
    """Test main_indirectmanagednodeaudit with custom output_dir."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    output_dir = '/tmp/test_output'

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until, output_dir=output_dir)

    assert instance.kwargs['output_dir'] == output_dir


@patch('metrics_utility.library.collectors.controller.main_indirectmanagednodeaudit.copy_table')
def test_main_indirectmanagednodeaudit_calls_copy_table(mock_copy_table):
    """Test that main_indirectmanagednodeaudit calls copy_table."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = ['/tmp/main_indirectmanagednodeaudit_table.csv']

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until)
    result = instance.gather()

    mock_copy_table.assert_called_once()
    call_args = mock_copy_table.call_args

    assert call_args[1]['db'] == mock_db
    assert call_args[1]['table'] == 'main_indirectmanagednodeaudit'
    assert 'query' in call_args[1]
    assert result == ['/tmp/main_indirectmanagednodeaudit_table.csv']


@patch('metrics_utility.library.collectors.controller.main_indirectmanagednodeaudit.copy_table')
def test_main_indirectmanagednodeaudit_query_contains_time_range(mock_copy_table):
    """Test that the query includes the time range."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 3, 15, 10, 30, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 3, 16, 18, 45, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

    # Query should contain time boundaries using created field
    assert '2024-03-15' in query
    assert '2024-03-16' in query
    assert 'main_indirectmanagednodeaudit.created >=' in query
    assert 'main_indirectmanagednodeaudit.created <' in query


@patch('metrics_utility.library.collectors.controller.main_indirectmanagednodeaudit.copy_table')
def test_main_indirectmanagednodeaudit_query_structure(mock_copy_table):
    """Test that the SQL query has expected structure."""
    mock_db = MagicMock()
    since = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc)
    mock_copy_table.return_value = []

    instance = main_indirectmanagednodeaudit(db=mock_db, since=since, until=until)
    instance.gather()

    call_args = mock_copy_table.call_args
    query = call_args[1]['query']

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
