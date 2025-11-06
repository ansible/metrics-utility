from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from django.db import DatabaseError

from metrics_utility.automation_controller_billing.helpers import (
    datetime_hook,
    get_last_entries_from_db,
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

        # Assert - datetime_hook parses datetime strings to datetime objects
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
    """Test cases for datetime_hook function"""

    def test_empty_dict_handling(self):
        """Test handling of empty dictionary"""
        # Execute
        result = datetime_hook({})

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
        result = datetime_hook(test_data)

        # Assert
        assert 'config' in result
        assert 'jobs' in result
        assert 'hosts' in result
        # All collector timestamps should be parsed
        assert str(result['config']).startswith('2024-01-01')
        assert str(result['jobs']).startswith('2024-01-02')
        assert str(result['hosts']).startswith('2024-01-03')


class TestIntegration:
    """Integration tests for helper functions working together"""

    @patch('metrics_utility.automation_controller_billing.helpers.connection')
    def test_functions_work_with_real_data(self, mock_connection):
        """Test that helper functions work with realistic data"""
        # Setup realistic database responses
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        # Setup data for get_last_entries_from_db() - fetchone()
        test_json = '"{\\"config\\": \\"2024-01-01T00:00:00Z\\", \\"jobs\\": \\"2024-01-02T00:00:00Z\\"}"'  # Last entries result
        mock_cursor.fetchone.return_value = (test_json,)

        # Execute function
        entries = get_last_entries_from_db()

        # datetime_hook parses datetime strings to datetime objects
        expected_entries = {
            'config': datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            'jobs': datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
        }
        assert entries == expected_entries
