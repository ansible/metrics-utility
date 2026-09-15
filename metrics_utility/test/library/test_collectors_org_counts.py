from unittest.mock import MagicMock

from metrics_utility.library.collectors.awx.org_counts import org_counts


def test_org_counts_basic():
    """Test org_counts collector basic functionality."""
    mock_db = MagicMock()

    instance = org_counts(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


def test_org_counts_output():
    """Test org_counts returns expected dict structure."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = [
        (1, 'Default', 15, 3),
        (2, 'Engineering', 42, 7),
    ]

    instance = org_counts(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert len(result) == 2

    assert result[1] == {'name': 'Default', 'users': 15, 'teams': 3}
    assert result[2] == {'name': 'Engineering', 'users': 42, 'teams': 7}


def test_org_counts_empty():
    """Test org_counts handles empty result."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = []

    instance = org_counts(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert len(result) == 0


def test_org_counts_zero_members():
    """Test org_counts handles orgs with zero users and teams."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = [
        (10, 'Empty Org', 0, 0),
    ]

    instance = org_counts(db=mock_db)
    result = instance.gather()

    assert result[10] == {'name': 'Empty Org', 'users': 0, 'teams': 0}


def test_org_counts_query_structure():
    """Test that the SQL query joins through role tables for user counts."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = []

    instance = org_counts(db=mock_db)
    instance.gather()

    call_args = mock_cursor.execute.call_args
    query = call_args[0][0]

    assert 'main_organization' in query
    assert 'main_rbac_roles' in query
    assert 'main_rbac_roles_members' in query
    assert 'main_team' in query
    assert 'GROUP BY' in query
