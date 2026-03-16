from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np

from django.db import DatabaseError

from metrics_utility.automation_controller_billing.helpers import (
    _datetime_hook,
    get_last_entries_from_db,
    parse_json_array,
)


class TestGetLastEntriesFromDb:
    """Test cases for get_last_entries_from_db function"""

    @patch('metrics_utility.automation_controller_billing.helpers.connection')
    def test_successful_entries_retrieval(self, mock_connection):
        """Test successful last entries retrieval"""
        # Setup
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        test_json = '"{\\"config\\": \\"2024-01-01T00:00:00Z\\", \\"hosts\\": \\"2024-01-03T00:00:00Z\\", \\"jobs\\": \\"2024-01-02T00:00:00Z\\"}"'
        mock_cursor.fetchone.return_value = (test_json,)
        # Execute
        result = get_last_entries_from_db()

        # Assert - _datetime_hook parses datetime strings to datetime objects
        expected_result = {
            'config': datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            'hosts': datetime(2024, 1, 3, 0, 0, tzinfo=timezone.utc),
            'jobs': datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
        }
        assert result == expected_result
        mock_cursor.execute.assert_called_once()
        # Verify correct SQL query
        sql_call = mock_cursor.execute.call_args[0][0]
        assert 'AUTOMATION_ANALYTICS_LAST_ENTRIES' in sql_call

    @patch('metrics_utility.automation_controller_billing.helpers.connection')
    def test_no_entries_or_empty_value(self, mock_connection):
        """Test when no entries found or value is empty"""
        # Setup
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # Could be no row or (None,)

        # Execute
        result = get_last_entries_from_db()

        # Assert
        assert result == {}

    @patch('metrics_utility.automation_controller_billing.helpers.logger')
    @patch('metrics_utility.automation_controller_billing.helpers.connection')
    def test_database_error_handling(self, mock_connection, mock_logger):
        """Test error handling when database query fails"""
        # Setup
        mock_connection.cursor.side_effect = DatabaseError('Query failed')

        # Execute
        result = get_last_entries_from_db()

        # Assert
        assert result == {}
        mock_logger.error.assert_called_once()
        assert 'Error getting AUTOMATION_ANALYTICS_LAST_ENTRIES from database' in str(mock_logger.error.call_args)


class TestDatetimeHook:
    """Test cases for _datetime_hook function"""

    def test_empty_dict_handling(self):
        """Test handling of empty dictionary"""
        # Execute
        result = _datetime_hook({})

        # Assert
        assert result == {}

    def test_multiple_datetime_fields(self):
        """Test parsing multiple collector timestamps in one dict"""
        # Setup - realistic collector function names with timestamps
        test_data = {
            'config': '2024-01-01T10:00:00Z',
            'jobs': '2024-01-02T15:30:00Z',
            'hosts': '2024-01-03T08:45:00Z',
        }

        # Execute
        result = _datetime_hook(test_data)

        # Assert
        assert 'config' in result
        assert 'jobs' in result
        assert 'hosts' in result
        # All collector timestamps should be parsed
        assert str(result['config']).startswith('2024-01-01')
        assert str(result['jobs']).startswith('2024-01-02')
        assert str(result['hosts']).startswith('2024-01-03')


class TestParseJsonArray:
    """Test cases for parse_json_array function."""

    def test_list_input_returned_unchanged(self):
        """A Python list (psycopg3 JsonbLoader path) is returned as-is."""
        value = ['event1', 'event2']
        assert parse_json_array(value) is value

    def test_empty_list_returned_unchanged(self):
        """An empty list is returned as-is."""
        assert parse_json_array([]) == []

    def test_null_returns_empty_list(self):
        """None / NaN produces an empty list."""
        assert parse_json_array(None) == []
        assert parse_json_array(np.nan) == []

    def test_valid_json_list_string_parsed(self):
        """A JSON-encoded list string is parsed into a Python list."""
        assert parse_json_array('["a", "b", "c"]') == ['a', 'b', 'c']

    def test_valid_json_non_list_returns_empty(self):
        """A valid JSON value that is not a list returns []."""
        assert parse_json_array('{"key": "value"}') == []

    def test_invalid_json_string_returns_empty(self):
        """Malformed JSON returns []."""
        assert parse_json_array('not-json!!!') == []

    def test_type_error_returns_empty(self):
        """Non-string, non-null, non-list input that causes TypeError in json.loads returns []."""
        assert parse_json_array(42) == []


class TestDatetimeHookTypeError:
    """Additional _datetime_hook tests covering the TypeError branch."""

    def test_non_string_value_kept_as_is(self):
        """A non-string value (e.g. int) that parse_datetime raises TypeError for is kept."""
        result = _datetime_hook({'count': 5})
        # parse_datetime(5) raises TypeError → value is stored unchanged
        assert result == {'count': 5}

    def test_mixed_string_and_non_string(self):
        """Dict with both datetime strings and non-string values is handled correctly."""
        result = _datetime_hook({'ts': '2024-01-01T00:00:00Z', 'num': 99})
        assert result['ts'] == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert result['num'] == 99


class TestIntegration:
    """Integration tests for helper functions working together"""

    @patch('metrics_utility.automation_controller_billing.helpers.connection')
    def test_functions_work_with_real_data(self, mock_connection):
        """Test that all helper functions work with realistic data"""
        # Setup realistic database responses
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        test_json = '"{\\"config\\": \\"2024-01-01T00:00:00Z\\", \\"jobs\\": \\"2024-01-02T00:00:00Z\\"}"'  # Last entries result
        mock_cursor.fetchone.return_value = (test_json,)

        # Execute
        entries = get_last_entries_from_db()

        # Assert all return expected realistic data
        # _datetime_hook parses datetime strings to datetime objects
        expected_entries = {
            'config': datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            'jobs': datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
        }
        assert entries == expected_entries
