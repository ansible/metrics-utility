import pytest

from django.db import connection

from metrics_utility.library.lock import lock
from metrics_utility.test.util import mock_cursor_db


def test_string_key_conversion():
    mock_connection, mock_cursor = mock_cursor_db()
    mock_cursor.execute.return_value = None
    mock_cursor.fetchone.return_value = [True]

    with lock('my_string_key', wait=False, db=mock_connection) as acquired:
        assert acquired is True
    executed_sql = mock_cursor.execute.call_args_list[0][0][0]  # This returns the argument for the first call to execute
    assert 'SELECT hashtext(%s)::bigint' in executed_sql
    assert 'my_string_key' not in executed_sql


def test_non_string_key_raises_value_error():
    mock_connection, _ = mock_cursor_db()
    with pytest.raises(ValueError, match='Cannot use'):
        with lock(123, wait=False, db=mock_connection):
            pytest.fail('this should be unreachable')


def test_lock_not_acquired_skips_release():
    mock_connection, mock_cursor = mock_cursor_db()
    mock_cursor.execute.return_value = None
    mock_cursor.fetchone.return_value = [False]

    with lock('test_key', wait=False, db=mock_connection) as acquired:
        assert acquired is False

    # hashtext + acquire = 2 calls, no release
    assert mock_cursor.execute.call_count == 2


def test_acquire_lock():
    with lock('test', wait=False, db=connection) as acquired:
        assert acquired is True
