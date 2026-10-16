from unittest.mock import MagicMock

from metrics_utility.library.collectors.controller.cred_type_counts import cred_type_counts


def test_cred_type_counts_basic():
    instance = cred_type_counts(db=MagicMock())

    assert hasattr(instance, 'gather')
    assert instance.kwargs['db'] is not None


def test_cred_type_counts_output():
    db = MagicMock()
    cursor = db.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        (1, 'Machine', True, 10),
        (2, 'Source Control', True, 5),
        (3, 'Custom', False, 2),
    ]

    result = cred_type_counts(db=db).gather()

    assert result == {
        1: {'name': 'Machine', 'credential_count': 10, 'managed': True},
        2: {'name': 'Source Control', 'credential_count': 5, 'managed': True},
        3: {'name': 'Custom', 'credential_count': 2, 'managed': False},
    }


def test_cred_type_counts_empty():
    db = MagicMock()
    cursor = db.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = []

    assert cred_type_counts(db=db).gather() == {}


def test_cred_type_counts_query_structure():
    db = MagicMock()
    cursor = db.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = []

    cred_type_counts(db=db).gather()
    query = cursor.execute.call_args.args[0]

    assert 'main_credentialtype' in query
    assert 'main_credential' in query
    assert 'LEFT JOIN' in query
    assert 'GROUP BY' in query
