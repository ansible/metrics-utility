from unittest.mock import MagicMock, patch

import pytest

from django.db import connection

from metrics_utility.library.lock import get_lock_id, lock


class TestGetLockId:
    @staticmethod
    def _cursor(value):
        cursor = MagicMock()
        cursor.fetchone.return_value = [value]
        return cursor

    def test_uses_parameterized_hashtext_query(self):
        cursor = self._cursor(123)
        assert get_lock_id(cursor, 'my_string_key') == 123
        cursor.execute.assert_called_once_with('SELECT hashtext(%s)::bigint', ('my_string_key',))

    def test_normalizes_positive_hash_outside_signed_range(self):
        cursor = self._cursor(2**63 + 5)
        assert get_lock_id(cursor, 'key') == 5

    def test_normalizes_negative_hash(self):
        cursor = self._cursor(-(2**62))
        assert get_lock_id(cursor, 'key') == (2**63) - (2**62)

    def test_rejects_non_string_key(self):
        cursor = MagicMock()
        with pytest.raises(ValueError, match='Cannot use 42 as a lock id'):
            get_lock_id(cursor, 42)
        cursor.execute.assert_not_called()


class TestCollectorLocks:
    @patch('metrics_utility.automation_controller_billing.collector.connection')
    def test_string_key_conversion(self, mock_connection):
        # Mock the cursor and its operations
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = [True]  # Lock acquisition succeeds

        # Set up the cursor context manager properly
        mock_cursor_context = MagicMock()
        mock_cursor_context.__enter__.return_value = mock_cursor
        mock_cursor_context.__exit__.return_value = None

        mock_connection.cursor.return_value = mock_cursor_context

        with lock('my_string_key', wait=False, db=mock_connection) as acquired:
            assert acquired is True
        executed_sql = mock_cursor.execute.call_args_list[0][0][0]  # This returns the argument for the first call to execute
        assert 'SELECT hashtext(%s)::bigint' in executed_sql
        assert 'my_string_key' not in executed_sql

    def test_acquire_lock(self):
        with lock('test', wait=False, db=connection) as acquired:
            assert acquired is not None
            with pytest.raises(Exception), lock('test', wait=False, db=connection):
                assert False, 'this should be unreachable'
