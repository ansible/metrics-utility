from unittest.mock import MagicMock

from metrics_utility.library.collectors.awx.cred_type_counts import cred_type_counts


def test_cred_type_counts_basic():
    """Test cred_type_counts collector basic functionality."""
    mock_db = MagicMock()

    instance = cred_type_counts(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


def test_cred_type_counts_output():
    """Test cred_type_counts returns expected dict structure."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = [
        (1, 'Machine', True, 10),
        (2, 'Source Control', True, 5),
        (3, 'Custom', False, 2),
    ]

    instance = cred_type_counts(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert len(result) == 3

    assert result[1] == {'name': 'Machine', 'credential_count': 10, 'managed': True}
    assert result[2] == {'name': 'Source Control', 'credential_count': 5, 'managed': True}
    assert result[3] == {'name': 'Custom', 'credential_count': 2, 'managed': False}


def test_cred_type_counts_empty():
    """Test cred_type_counts handles empty result."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = []

    instance = cred_type_counts(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert len(result) == 0


def test_cred_type_counts_zero_credentials():
    """Test cred_type_counts handles types with zero credentials."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = [
        (1, 'Vault', True, 0),
    ]

    instance = cred_type_counts(db=mock_db)
    result = instance.gather()

    assert result[1]['credential_count'] == 0


def test_cred_type_counts_query_structure():
    """Test that the SQL query has expected joins and grouping."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = []

    instance = cred_type_counts(db=mock_db)
    instance.gather()

    call_args = mock_cursor.execute.call_args
    query = call_args[0][0]

    assert 'main_credentialtype' in query
    assert 'main_credential' in query
    assert 'LEFT JOIN' in query
    assert 'GROUP BY' in query
