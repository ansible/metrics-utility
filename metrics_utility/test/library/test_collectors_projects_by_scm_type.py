from unittest.mock import MagicMock

from metrics_utility.library.collectors.awx.projects_by_scm_type import projects_by_scm_type


def test_projects_by_scm_type_basic():
    """Test projects_by_scm_type collector basic functionality."""
    mock_db = MagicMock()

    instance = projects_by_scm_type(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


def test_projects_by_scm_type_output():
    """Test projects_by_scm_type returns expected dict structure."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = [
        ('git', 5),
        ('manual', 3),
        ('svn', 1),
    ]

    instance = projects_by_scm_type(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert result['git'] == 5
    assert result['manual'] == 3
    assert result['svn'] == 1
    assert len(result) == 3


def test_projects_by_scm_type_empty():
    """Test projects_by_scm_type handles empty result."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = []

    instance = projects_by_scm_type(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert len(result) == 0


def test_projects_by_scm_type_query_structure():
    """Test that the SQL query references main_project."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = []

    instance = projects_by_scm_type(db=mock_db)
    instance.gather()

    call_args = mock_cursor.execute.call_args
    query = call_args[0][0]

    assert 'main_project' in query
    assert 'scm_type' in query
    assert 'GROUP BY' in query
    assert "GROUP BY COALESCE(NULLIF(scm_type, ''), 'manual')" in query
