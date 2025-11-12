from unittest.mock import MagicMock, patch

import pytest

from django.db import connection

from metrics_utility.library.lock import lock


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

        with lock('my_string_key', False, db=mock_connection) as acquired:
            assert acquired is True
        executed_sql = mock_cursor.execute.call_args_list[0][0][0]  # This returns the argument for the first call to execute
        assert 'SELECT hashtext(%s)::bigint' in executed_sql
        assert 'my_string_key' not in executed_sql

    def test_acquire_lock(self):
        with lock('test', False, db=connection) as acquired:
            assert acquired is not None
            with pytest.raises(Exception):
                with lock('test', False, db=connection):
                    assert False, 'this should be unreachable'
