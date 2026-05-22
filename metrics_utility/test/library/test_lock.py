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


def test_acquire_lock():
    with lock('test', wait=False, db=connection) as acquired:
        assert acquired is not None
        with pytest.raises(Exception):
            with lock('test', wait=False, db=connection):
                assert False, 'this should be unreachable'
